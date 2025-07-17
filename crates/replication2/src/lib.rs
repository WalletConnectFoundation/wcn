use {
    cluster::{
        keyspace::{self, Shards, REPLICATION_FACTOR},
        node_operator,
        Cluster,
        Keyspace,
        NodeOperator,
    },
    consistency::ReplicationResults,
    derive_more::derive::AsRef,
    derive_where::derive_where,
    futures::{
        channel::oneshot,
        future::{Either, OptionFuture},
        stream::{self, FuturesUnordered},
        FutureExt,
        Stream,
        StreamExt,
    },
    futures_concurrency::{
        future::{Join as _, Race as _},
        stream::Merge,
    },
    std::{
        array,
        cell::RefCell,
        collections::{HashMap, HashSet},
        future::{self, Future},
        hash::BuildHasher,
        ops::Add,
        pin::pin,
        sync::Arc,
        time::Duration,
    },
    storage_api::{
        operation,
        rpc::client::ReplicaConnection,
        Callback,
        Error,
        ErrorKind,
        MapEntryBorrowed,
        Operation,
        Record,
        RecordBorrowed,
        StorageApi,
    },
    tap::{Pipe, TapFallible as _},
    wc::metrics::{
        self,
        enum_ordinalize::Ordinalize,
        future_metrics,
        EnumLabel,
        FutureExt as _,
        StringLabel,
    },
    xxhash_rust::xxh3::Xxh3Builder,
};

mod consistency;
mod reconciliation;

const RF: usize = keyspace::REPLICATION_FACTOR as usize;

#[derive_where(Clone)]
pub struct Coordinator<C: cluster::Config> {
    cluster: Cluster<C>,
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

        // IMPORTANT: WCN DB MUST use the same hashing algorithm, and it MUST hash the
        // same bytes (only the base key, without namespace).
        let key = HASHER.hash_one(operation.key());

        let is_write = operation.is_write();

        let cluster_view = &self.cluster.view();

        let replicate_to = |node_operator| async move {
            (operator_idx, node_operator.execute_ref(operation).map(|response| )
        };

        let mut primary_replica_set = ReplicaSet::new(key, cluster_view.keyspace());
        let primary_replica_futures = primary_replica_set
            .replicas
            .map(|operator_idx| stream::once(replicate_to(operator_idx)))
            .merge();

        let replicate_to_secondary = |operator_idx| async move {
            // Make sure that if an operator is in both primary and secondary
            // keyspaces we only replicate once.
            if !primary_replica_set.replicas.contains(&operator_idx) {
                return None;
            }

            Some(replicate_to(operator_idx).await)
        };

        // If there's an ongoing data migration and this is a write, then we need to
        // replicate to an additional set of replicas.
        let mut secondary_replica_set = cluster_view
            .migration()
            .filter(|_| is_write)
            .map(|migration| ReplicaSet::new(key, migration.keyspace()));

        let secondary_replica_futures = match &secondary_replica_set {
            Some(replica_set) => replica_set
                .replicas
                .map(|idx| stream::once(replicate_to_secondary(idx)))
                .merge()
                .filter_map(future::ready)
                .pipe(Either::Left),
            None => Either::Right(stream::empty()),
        };

        let mut response_futures =
            pin!((primary_replica_futures, secondary_replica_futures).merge());

        let mut responses: ResponseBuffer = std::array::from_fn(|_| None);

        let quorum_response_idx = loop {
            let Some((operator_idx, response)) = response_futures.next().await else {
                break None;
            };

            let response_idx = receive_response(&mut responses, response);

            let primary_quorum_response =
                primary_replica_set.response_received(operator_idx, response_idx);

            let secondary_quorum_response = secondary_replica_set
                .as_mut()
                .map(|replica_set| replica_set.response_received(operator_idx, response_idx))
                .flatten();

            match (primary_quorum_response, secondary_quorum_response) {
                (Some(idx), _) if secondary_replica_set.is_none() => break Some(idx),

                // If both replica sets reached quorum, but the responses are different, then we
                // return `None` indicating that quorum hasn't been reached.
                (Some(a), Some(b)) => break Some(a).filter(|_| a == b),

                _ => continue,
            };
        };

        let quorum_response = match quorum_response_idx {
            None if is_write => None,

            // If we didn't get the quorum and this is a read operation, then try to reconciliate
            // the responses.
            None => reconciliate(&responses[..RF]),

            Some(idx) => responses[idx as usize].as_ref(),
        }
        .unwrap_or_else(|| {
            metrics::counter!("wcn_replication_coordinator_inconsistent_results");
            const { &Err(Error::new(ErrorKind::Internal)) }
        });

        let callback_fut = callback.send_result(quorum_response);

        let mut read_repair = quorum_response
            .as_ref()
            .ok()
            .filter(|_| is_repairable(operation))
            .map(|output| ReadRepair {
                operation,
                output,
                targets: Default::default(),
                replica_set: &primary_replica_set,
            });

        let complete_replication_fut = async {
            // Drive all futures to completion
            while let Some((operator_idx, response)) = response_futures.next().await {
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

struct ReplicaSet {
    /// If the quorum is reached this is the position of the response in
    /// the [`ResponseBuffer`].
    quorum_response_idx: Option<u8>,

    replicas: [node_operator::Idx; RF],

    /// Mapping to the corresponding response for each replica.
    replica_responses: [Option<u8>; RF],

    /// How many replicas returned a specific response.
    replicas_per_response: [u8; RF * 2],
}

impl ReplicaSet {
    fn new(key: u64, keyspace: &Keyspace<Shards>) -> Self {
        Self {
            quorum_response_idx: None,
            replicas: keyspace.shard(key).replica_set(),
            replica_responses: Default::default(),
            replicas_per_response: Default::default(),
        }
    }

    fn response_received(
        &mut self,
        operator_idx: node_operator::Idx,
        response_idx: u8,
    ) -> Option<u8> {
        let Some(replica_idx) = self.replicas.iter().position(|idx| *idx == operator_idx) else {
            return self.quorum_response_idx;
        };

        self.replica_responses[replica_idx as usize] = Some(response_idx);
        self.replicas_per_response[response_idx as usize] += 1;

        if self.replicas_per_response[response_idx as usize] >= RF as u8 {
            self.quorum_response_idx = Some(response_idx);
        }

        self.quorum_response_idx
    }
}

fn reconciliate(responses: &[Option<Response>]) -> Option<&Response> {
    todo!()
}

// TODO: figure out how to repair `hscan`
struct ReadRepair<'a> {
    operation: &'a Operation<'a>,

    /// Successfull response to be used as the baseline for repair.
    output: &'a operation::Output,

    /// Repair targets.
    ///
    /// Can't be more than `RF / 2`.
    targets: [Option<node_operator::Idx>; RF / 2],

    replica_set: &'a ReplicaSet,
}

impl ReadRepair<'_> {
    fn check_response(&mut self, operator_idx: node_operator::Idx, response: Response) {
        match response {
            Ok(output) if &output != self.output => {}

            // Errors are not being repaired.
            _ => return,
        }

        let _ = self.targets.iter_mut().find_map(|slot| match slot {
            None => Some(*slot = Some(operator_idx)),
            Some(_) => None,
        });
    }

    async fn run(self) {
        self.targets.map(|operation_idx| )
    }
}

fn is_repairable(operation: &Operation<'_>) -> bool {
    use operation::{Borrowed, Owned};

    match operation {
        Operation::Owned(owned) => match owned {
            Owned::Get(_) | Owned::HGet(_) => true,
            Owned::Set(_)
            | Owned::Del(_)
            | Owned::GetExp(_)
            | Owned::SetExp(_)
            | Owned::HSet(_)
            | Owned::HDel(_)
            | Owned::HGetExp(_)
            | Owned::HSetExp(_)
            | Owned::HCard(_)
            | Owned::HScan(_) => false,
        },
        Operation::Borrowed(borrowed) => match borrowed {
            Borrowed::Get(_) | Borrowed::HGet(_) => true,
            Borrowed::Set(_)
            | Borrowed::Del(_)
            | Borrowed::GetExp(_)
            | Borrowed::SetExp(_)
            | Borrowed::HSet(_)
            | Borrowed::HDel(_)
            | Borrowed::HGetExp(_)
            | Borrowed::HSetExp(_)
            | Borrowed::HCard(_)
            | Borrowed::HScan(_) => false,
        },
    }
}

/// Given an [`Operation`] and it's successful [`operation::Output`] returns
/// another [`Operation`] meant to repair replicas that responded with a
/// wrong [`operation::Output`].
fn repair_operation<'a>(
    operation: &Operation<'a>,
    output: &operation::Output,
) -> Option<Operation<'a>> {
    use operation::{Borrowed, Owned};

    // TODO: consider refactoring `Operation` (again) in a way so:
    // `enum Get = GetBorrowed | GetOwned`
    Some(Operation::Borrowed(match op {
        Operation::Owned(owned) => match owned {
            Owned::Get(get) => match output {
                operation::Output::Record(Some(record)) => operation::SetBorrowed {
                    namespace: get.namespace,
                    key: &get.key,
                    record: record.borrow(),
                    keyspace_version: get.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Owned::HGet(hget) => match output {
                operation::Output::Record(Some(record)) => operation::HSetBorrowed {
                    namespace: hget.namespace,
                    key: &hget.key,
                    entry: MapEntryBorrowed {
                        field: &hget.field,
                        record: record.borrow(),
                    },
                    keyspace_version: hget.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Owned::Set(_)
            | Owned::Del(_)
            | Owned::GetExp(_)
            | Owned::SetExp(_)
            | Owned::HSet(_)
            | Owned::HDel(_)
            | Owned::HGetExp(_)
            | Owned::HSetExp(_)
            | Owned::HCard(_)
            | Owned::HScan(_) => return None,
        },
        Operation::Borrowed(owned) => match owned {
            Borrowed::Get(get) => match output {
                operation::Output::Record(Some(record)) => operation::SetBorrowed {
                    namespace: get.namespace,
                    key: &get.key,
                    record: record.borrow(),
                    keyspace_version: get.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Borrowed::HGet(hget) => match output {
                operation::Output::Record(Some(record)) => operation::HSetBorrowed {
                    namespace: hget.namespace,
                    key: &hget.key,
                    entry: MapEntryBorrowed {
                        field: &hget.field,
                        record: record.borrow(),
                    },
                    keyspace_version: hget.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Borrowed::Set(_)
            | Borrowed::Del(_)
            | Borrowed::GetExp(_)
            | Borrowed::SetExp(_)
            | Borrowed::HSet(_)
            | Borrowed::HDel(_)
            | Borrowed::HGetExp(_)
            | Borrowed::HSetExp(_)
            | Borrowed::HCard(_)
            | Borrowed::HScan(_) => return None,
        },
    }))
}

fn read_repair(replica_set: &ReplicaSet) -> impl Future<()> {}
