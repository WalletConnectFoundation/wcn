use {
    cluster::{keyspace, Cluster, NodeOperator},
    derive_where::derive_where,
    futures::{future::Either, stream, FutureExt, StreamExt},
    futures_concurrency::{future::Join as _, stream::Merge},
    read_repair::ReadRepair,
    std::{future, hash::BuildHasher, pin::pin},
    storage_api::{operation, Callback, Error, ErrorKind, Operation, StorageApi},
    tap::Pipe,
    wc::metrics,
    xxhash_rust::xxh3::Xxh3Builder,
};

mod read_repair;
mod reconciliation;

const RF: usize = keyspace::REPLICATION_FACTOR as usize;

/// Min number of agreeing replicas required to reach a majority [`Quorum`].
const MAJORITY_QUORUM_THRESHOLD: usize = RF / 2 + 1;

#[derive_where(Clone)]
pub struct Coordinator<C: cluster::Config> {
    cluster: Cluster<C>,
}

impl<C: cluster::Config> Coordinator<C> {
    /// Creates a new replication [`Coordinator`].
    pub fn new(cluster: Cluster<C>) -> Self {
        Self { cluster }
    }
}

impl<Cfg> StorageApi for Coordinator<Cfg>
where
    Cfg: cluster::Config<KeyspaceShards = keyspace::Shards>,
    NodeOperator<Cfg::Node>: StorageApi,
{
    async fn execute_callback<C: Callback>(
        &self,
        operation: Operation<'_>,
        callback: C,
    ) -> Result<(), C::Error> {
        static HASHER: Xxh3Builder = Xxh3Builder::new();

        let operation = &operation;

        // WARNING: WCN DB MUST use the same hashing algorithm, and it MUST hash the
        // same bytes (only the base key, without namespace).
        let key = HASHER.hash_one(operation.key());

        let is_write = operation.is_write();

        let cluster_view = &self.cluster.view();

        let mut primary_quorum = Quorum::new(cluster_view.primary_replica_set(key));
        let primary_replicas = primary_quorum.replica_set;
        let primary_replica_requests = primary_quorum
            .replica_set
            .map(|oper| oper.execute_ref(operation).map(move |resp| (oper, resp)))
            .map(stream::once)
            .merge();

        let is_primary_replica =
            |operator: &NodeOperator<_>| primary_replicas.iter().any(|op| op.id == operator.id);

        // If there's an ongoing data migration and this is a write, then we need to
        // replicate to an additional set of replicas.
        let mut secondary_quorum = is_write
            .then(|| cluster_view.secondary_replica_set(key).map(Quorum::new))
            .flatten();

        // Make sure that if an operator is in both primary and secondary
        // keyspaces we only replicate once.
        let send_secondary_replica_request = |operator| async move {
            if is_primary_replica(operator) {
                return None;
            }

            Some((operator, operator.execute_ref(operation).await))
        };

        let secondary_replica_requests = (&secondary_quorum).pipe(|opt| {
            let Some(quorum) = opt else {
                return Either::Left(stream::empty());
            };

            quorum
                .replica_set
                .map(send_secondary_replica_request)
                .map(stream::once)
                .merge()
                .filter_map(future::ready)
                .pipe(Either::Right)
        });

        let mut response_futures =
            pin!((primary_replica_requests, secondary_replica_requests).merge());

        let mut responses: ResponseBuffer = std::array::from_fn(|_| None);

        let quorum_response_idx = loop {
            let Some((operator_idx, response)) = response_futures.next().await else {
                break None;
            };

            let response_idx = receive_response(&mut responses, response);

            let primary_quorum_response =
                primary_quorum.response_received(operator_idx, response_idx);

            let secondary_quorum_response = secondary_quorum
                .as_mut()
                .and_then(|replica_set| replica_set.response_received(operator_idx, response_idx));

            match (primary_quorum_response, secondary_quorum_response) {
                (Some(idx), _) if secondary_quorum.is_none() => break Some(idx),

                // If both replica sets reached quorum, but the responses are different, then we
                // return `None` indicating that quorum hasn't been reached.
                (Some(a), Some(b)) => break Some(a).filter(|_| a == b),

                _ => {}
            };
        };

        let mut reconciled_response = None;

        // NOTE: currently the set of operations that can be reconciled and the set
        // of repairable operations do not intersect. Once it's no longer the case it
        // needs to be handled properly here.
        let quorum_response = match quorum_response_idx {
            None if is_write => None,

            // If we didn't get the quorum and this is a read operation, then try to reconcile
            // the responses.
            None => reconciliation::reconcile(operation, &responses[..RF])
                .map(|out| reconciled_response = Some(Ok(out)))
                .pipe(|_| reconciled_response.as_ref()),

            Some(idx) => responses[idx as usize].as_ref(),
        }
        .unwrap_or_else(|| {
            metrics::counter!("wcn_replication_coordinator_inconsistent_results").increment(1);
            const { &Err(Error::new(ErrorKind::Internal)) }
        });

        let callback_fut = callback.send_result(quorum_response);

        let mut read_repair = ReadRepair::new(operation, &responses, &primary_quorum);

        let complete_replication_fut = async {
            // Drive all futures to completion
            while let Some((operator_idx, response)) = response_futures.next().await {
                // If read repair is scheduled, check extra responses in case they also need to
                // be repaired.
                if let Some(repair) = &mut read_repair {
                    repair.check_response(operator_idx, response);
                }
            }

            if let Some(repair) = read_repair {
                repair.run().await;
            }
        };

        let (callback_result, _) = (callback_fut, complete_replication_fut).join().await;

        callback_result
    }

    async fn execute(&self, _operation: Operation<'_>) -> storage_api::Result<operation::Output> {
        tracing::error!("StorageApi::execute should not be used with replication::Coordinator");
        Err(Error::new(ErrorKind::Internal))
    }
}

type Response = storage_api::Result<operation::Output>;

// Worst case scenario RF * 2 responses, if every replica returns a different
// response.
type ResponseBuffer = [Option<Response>; RF * 2];

fn receive_response(buf: &mut ResponseBuffer, response: Response) -> u8 {
    for (idx, slot) in buf.iter_mut().enumerate() {
        match slot {
            Some(resp) if resp == &response => {}
            None => *slot = Some(response),
            _ => continue,
        }

        return idx as u8;
    }

    // should never happen as we make no more than 2 * RF requests
    panic!("too many responses")
}

struct Quorum<'a, N> {
    /// If the quorum is reached this is the position of the response in
    /// the [`ResponseBuffer`].
    response_idx: Option<u8>,

    replica_set: [&'a NodeOperator<N>; RF],

    /// Mapping to the corresponding response for each replica.
    replica_responses: [Option<u8>; RF],

    /// How many replicas returned a specific response.
    replicas_per_response: [u8; RF * 2],
}

impl<'a, N> Quorum<'a, N> {
    fn new(replica_set: [&'a NodeOperator<N>; RF]) -> Self {
        Self {
            response_idx: None,
            replica_set,
            replica_responses: Default::default(),
            replicas_per_response: Default::default(),
        }
    }

    fn response_received(&mut self, operator: &NodeOperator<N>, response_idx: u8) -> Option<u8> {
        let Some(replica_idx) = self.replica_set.iter().position(|op| op.id == operator.id) else {
            return self.response_idx;
        };

        self.replica_responses[replica_idx] = Some(response_idx);
        self.replicas_per_response[response_idx as usize] += 1;

        if self.replicas_per_response[response_idx as usize] >= MAJORITY_QUORUM_THRESHOLD as u8 {
            self.response_idx = Some(response_idx);
        }

        self.response_idx
    }
}
