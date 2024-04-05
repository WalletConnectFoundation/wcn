use {
    crate::{
        cluster::{
            keyspace::{hashring::Positioned, KeyPosition, Keyspace},
            NodeOperationMode,
        },
        migration::StoreHinted,
        network::{self, HandleRequest},
        Network,
        Node,
        PeerId,
        Storage,
    },
    async_trait::async_trait,
    derive_more::Into,
    futures::{
        stream::{FuturesUnordered, StreamExt},
        FutureExt as _,
    },
    futures_util::future::BoxFuture,
    sealed::{Reconcile, Replication as _},
    serde::{Deserialize, Serialize},
    smallvec::{smallvec, SmallVec},
    std::{
        collections::HashMap,
        fmt::Debug,
        future,
        hash::Hash,
        sync::atomic::AtomicU32,
        time::Duration,
    },
    tokio::sync::OwnedSemaphorePermit,
    wc::{
        future::FutureExt,
        metrics::{self, otel, TaskMetrics},
    },
};

// #[cfg(test)]
// mod tests;

// #[cfg(any(test, feature = "testing"))]
// pub mod shared_tests;

static METRICS: TaskMetrics = TaskMetrics::new("irn_replication_task");

/// Maximum time a request can spend in queue awaiting the execution permit.
const REQUEST_QUEUE_TIMEOUT: Duration = Duration::from_millis(1500);

/// Wrapper to explicitly mark an operation as replicated.
#[derive(Clone, Copy, Debug)]
pub struct DispatchReplicated<Op> {
    pub operation: Op,
}

pub type CoordinatorResponse<T, SE, NE> = Result<ReplicaResponse<T, SE>, CoordinatorError<NE>>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ReplicatedRequest<Op> {
    pub key_position: KeyPosition,
    pub operation: Op,
    pub cluster_view_version: u128,
}

pub type ReplicaResponse<T, E> = Result<T, ReplicaError<E>>;

impl<C, N, S> Node<C, N, S> {
    /// Send a request to the replica set.
    ///
    /// If the coordinator node is part of the replica set, then one operation
    /// is applied locally, on coordinator.
    ///
    /// To get the result of the operation, required number of replicas must
    /// respond with the same value. If the required number of replicas don't
    /// produce the same value within the timeout, an error is returned. The
    /// exact number of required replicas is defined by consistency level of the
    /// selected replication strategy.
    pub async fn dispatch_replicated<Op>(
        &self,
        op: Op,
    ) -> CoordinatorResponse<S::Ok, S::Error, N::Error>
    where
        Op: ReplicatableOperation,
        Op::Output: std::fmt::Debug,
        Op::Key: std::fmt::Debug,
        C: Sync,
        N: Network
            + network::SendRequest<
                ReplicatedRequest<Op>,
                Response = Result<S::Ok, ReplicaError<S::Error>>,
            >,
        S: Storage<Positioned<Op>, Ok = Op::Output>,
        Result<N::Response, N::Error>: Clone + Eq,
        Self: sealed::Replication<Op, Op::Type, StorageError = S::Error>,
    {
        let _permit = self
            .try_acquire_replication_permit()
            .await
            .ok_or(CoordinatorError::Throttled)?;

        let cluster = self
            .cluster
            .read()
            .with_metrics(METRICS.with_name("cluster_read")) // Ivan asked to add this temporarily
            .await;

        // Ensure that coordinator can cater for the operation.
        if !cluster.node_reached_op_mode(&self.id, NodeOperationMode::Normal) {
            return Err(CoordinatorError::InvalidOperationMode);
        }

        let key = op.as_ref();
        let key_position = cluster.keyspace().key_position(key);

        // If the replica set is empty or doesn't have enough peers, return an error.
        let replica_set = cluster.keyspace().replica_set(key_position);
        if !replica_set.is_valid() {
            return Err(CoordinatorError::InvalidReplicaSet);
        }

        // dbg!(&cluster);
        // dbg!(&replica_set);

        let cluster_view_version = cluster.view().version();
        let mut required_replicas = replica_set.strategy().required_replica_count();

        let send_replicated = |peer_id: PeerId| {
            let op = ReplicatedRequest {
                key_position,
                operation: op.clone(),
                cluster_view_version,
            };

            let network = self.network.clone();
            async move { IdValue::new(peer_id, network.send_request(peer_id, op).await) }.boxed()
        };

        let mut futures: FuturesUnordered<_> = replica_set
            .remote_replicas(&self.id)
            .map(|id| send_replicated(*id))
            .collect();

        if Self::REQUIRES_HINTING {
            // If key belongs to some booting node, forward request to that node.
            // Normally, only a single node booting is enforced on consensus level, but we
            // process for arbitrary number of booting nodes here.
            let booting_peers: Vec<_> = cluster.booting_peers_owning_pos(key_position).collect();

            // If key belongs to some leaving node, forward request to new owners of
            // the key.
            let new_owners: Vec<_> = cluster.new_owners_for_pos(key_position).collect();

            required_replicas += booting_peers.len() + new_owners.len();

            let hinted_ops = booting_peers
                .into_iter()
                .chain(new_owners)
                .map(send_replicated);

            futures.extend(hinted_ops);
        };

        // If coordinator is in the replica set, it will be included in the list of
        // requests to process, but the operation will be performed locally.
        let coordinator_in_replica_set = replica_set.contains(&self.id);
        if coordinator_in_replica_set {
            let op = Positioned {
                position: key_position,
                inner: op.clone(),
            };

            let peer_id = self.id;
            let storage = self.storage.clone();
            futures.push(
                async move {
                    IdValue::new(
                        peer_id,
                        Ok(storage.exec(op).await.map_err(ReplicaError::Storage)),
                    )
                }
                .boxed(),
            );
        }

        // Release `RWLock` before .await point.
        drop(cluster);

        let result =
            match_values(required_replicas, futures, self.replication_request_timeout).await?;
        match result {
            // Matching value received.
            MatchResult::Ok(value, responses) => {
                // Attempt to repair the value on the coordinator node, only if it is in the
                // replica set.
                if !coordinator_in_replica_set {
                    return value.map_err(CoordinatorError::Network);
                }

                let value = value.map_err(CoordinatorError::Network)?;
                let values: Vec<IdValue<_, _>> = responses
                    .into_iter()
                    .filter_map(|res| match res.value {
                        Ok(Ok(value)) => Some(IdValue::new(res.id, value)),
                        _ => None,
                    })
                    .collect();

                match value {
                    Ok(value) => match self.repair(op, key_position, value, values).await {
                        Ok(repair_result) => Ok(Ok(repair_result)),
                        Err(e) => Ok(Err(e)),
                    },
                    Err(e) => Ok(Err(e)),
                }
            }
            // Not enough matching responses.
            MatchResult::Inconsistent(responses) => {
                {
                    use std::sync::atomic::Ordering;
                    static COUNTER: AtomicU32 = AtomicU32::new(0);
                    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
                    if counter % 10 == 0 {
                        let op_kind = std::any::type_name::<Op>();
                        tracing::debug!(?op_kind, ?responses, ?key, "inconsistent operation");
                    }
                }

                let values = responses
                    .into_iter()
                    .map(|res| res.value)
                    .filter_map(Result::ok)
                    .filter_map(Result::ok)
                    .collect();

                Self::reconcile_results(values, required_replicas)
                    .map(Ok)
                    .ok_or(CoordinatorError::InconsistentOperation)
            }
        }
    }

    pub async fn exec_replicated<Op>(
        &self,
        r: ReplicatedRequest<Op>,
    ) -> ReplicaResponse<Op::Output, S::Error>
    where
        Op: ReplicatableOperation,
        S: Storage<Positioned<Op>, Ok = Op::Output>,
        Self: sealed::Replication<Op, Op::Type, StorageError = S::Error>,
    {
        let _permit = self
            .try_acquire_replication_permit()
            .await
            .ok_or(ReplicaError::Throttled)?;

        if r.cluster_view_version != self.cluster.read().await.view().version() {
            return Err(ReplicaError::ClusterViewVersionMismatch);
        };

        let operation = Positioned {
            position: r.key_position,
            inner: r.operation,
        };

        sealed::Replication::exec_replicated(self, operation).await
    }

    async fn try_acquire_replication_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!(
            "irn_network_request_permits_available",
            self.replication_request_limiter.available_permits() as u64
        );

        metrics::gauge!(
            "irn_network_request_queue_permits_available",
            self.replication_request_limiter_queue.available_permits() as u64
        );

        if let Ok(permit) = self.replication_request_limiter.clone().try_acquire_owned() {
            // If we're below the concurrency limit, return the permit immediately.
            Some(permit)
        } else {
            // If no permits are available, join the queue.
            let Ok(_queue_permit) = self.replication_request_limiter_queue.try_acquire() else {
                // Request queue is also full. Nothing we can do.
                return None;
            };

            // Await until the request permit is available, while holding the queue permit.
            self.replication_request_limiter
                .clone()
                .acquire_owned()
                .with_timeout(REQUEST_QUEUE_TIMEOUT)
                .await
                .map_err(|_| {
                    metrics::counter!("irn_network_request_queue_timeout", 1);
                })
                .ok()?
                .ok()
        }
    }
}

#[async_trait]
impl<Op, C, N, S> HandleRequest<DispatchReplicated<Op>> for Node<C, N, S>
where
    Op: ReplicatableOperation,
    Op::Output: std::fmt::Debug,
    Op::Key: std::fmt::Debug,
    C: Send + Sync + 'static,
    N: Network
        + network::SendRequest<
            ReplicatedRequest<Op>,
            Response = Result<S::Ok, ReplicaError<S::Error>>,
        >,
    S: Storage<Positioned<Op>, Ok = Op::Output>,
    Result<N::Response, N::Error>: Clone + Eq,
    Self: sealed::Replication<Op, Op::Type, StorageError = S::Error>,
{
    type Response = CoordinatorResponse<S::Ok, S::Error, N::Error>;

    async fn handle_request(&self, req: DispatchReplicated<Op>) -> Self::Response {
        self.dispatch_replicated(req.operation).await
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoordinatorError<N> {
    /// Request coordinator is in a wrong mode.
    #[error("Invalid operation mode of the request coordinator")]
    InvalidOperationMode,

    /// Replica set doesn't have enough peers.
    #[error("Invalid replica set")]
    InvalidReplicaSet,

    /// Operation timed out.
    #[error("Operation timed out")]
    Timeout,

    #[error("Too many requests")]
    Throttled,

    /// Not enough peers on replicated operation.
    #[error("Inconsistent operation")]
    InconsistentOperation,

    /// Network error.
    #[error("Network error: {0}")]
    Network(N),
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaError<S> {
    /// Cluster view versions of requester and responder don't match
    #[error("ClusterView versions of requester and responder don't match")]
    ClusterViewVersionMismatch,

    #[error("Too many requests")]
    Throttled,

    /// Error of executing the operation.
    #[error("Local storage error: {0}")]
    Storage(S),
}

impl<N> From<tokio::time::error::Elapsed> for CoordinatorError<N> {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

pub trait ReplicatableOperation: AsRef<Self::Key> + Debug + Clone + Send + Sync + 'static {
    type Type;
    type Key: Hash + Send + Sync + 'static;
    type Output: Send + 'static;

    /// The corresponding repair operation type for the current operation.
    type RepairOperation;

    /// Returns an optional repair operation for the current operation.
    fn repair_operation(&self, _new_value: Self::Output) -> Option<Self::RepairOperation> {
        None
    }
}

pub type ReplicatableOperationOutput<Op> = <Op as ReplicatableOperation>::Output;

mod sealed {
    use {
        super::{async_trait, IdValue, Positioned, ReplicaError, ReplicatableOperation},
        crate::{cluster::keyspace::KeyPosition, PeerId},
    };

    #[async_trait]
    pub trait Replication<Op: ReplicatableOperation, Ty> {
        type StorageError;

        const REQUIRES_HINTING: bool;

        fn reconcile_results(
            _results: Vec<Op::Output>,
            _required_replicas: usize,
        ) -> Option<Op::Output> {
            None
        }

        /// Repairs a value on the coordinator node, for operations that support
        /// read repair.
        async fn repair(
            &self,
            _op: Op,
            _key_position: KeyPosition,
            new_value: Op::Output,
            _responses: Vec<IdValue<PeerId, Op::Output>>,
        ) -> Result<Op::Output, ReplicaError<Self::StorageError>> {
            Ok(new_value)
        }

        async fn exec_replicated(
            &self,
            op: Positioned<Op>,
        ) -> Result<Op::Output, ReplicaError<Self::StorageError>>;
    }

    pub trait Reconcile: Sized {
        fn reconcile(values: Vec<Self>, required_replicas: usize) -> Option<Self>;
    }
}

pub struct Read;

#[allow(clippy::trait_duplication_in_bounds)]
#[async_trait]
impl<C, N, S, Op, RepairOp, E> sealed::Replication<Op, Read> for Node<C, N, S>
where
    Op: ReplicatableOperation<Type = Read, RepairOperation = RepairOp>,
    RepairOp: Send,
    S: Storage<Positioned<Op>, Ok = Op::Output, Error = E>
        + Storage<Positioned<RepairOp>, Ok = (), Error = E>,
    E: Debug,
    <Op as ReplicatableOperation>::Output: Clone + PartialEq,
    <S as Storage<Positioned<Op>>>::Error: PartialEq,
    Self: Send + Sync,
{
    type StorageError = E;

    const REQUIRES_HINTING: bool = false;

    async fn repair(
        &self,
        op: Op,
        key_position: KeyPosition,
        new_value: Op::Output,
        responses: Vec<IdValue<PeerId, Op::Output>>,
    ) -> Result<Op::Output, ReplicaError<Self::StorageError>> {
        // If the operation doesn't support repair, return early.
        let repair_op = if let Some(op) = op.repair_operation(new_value.clone()) {
            op
        } else {
            return Ok(new_value);
        };

        // Repair is required if the coordinator node has diverged (either no response
        // at all or a different value).
        let needs_repair = responses
            .iter()
            .find(|res| res.id == self.id)
            .map_or(true, |res| res.value != new_value);

        if needs_repair {
            let op = Positioned {
                position: key_position,
                inner: repair_op,
            };
            self.storage.exec(op).await.map_err(ReplicaError::Storage)?
        }

        Ok(new_value)
    }

    async fn exec_replicated(&self, op: Positioned<Op>) -> Result<Op::Output, ReplicaError<E>> {
        self.storage.exec(op).await.map_err(ReplicaError::Storage)
    }
}

pub struct ReconciledRead;

#[async_trait]
impl<C, N, S, Op> sealed::Replication<Op, ReconciledRead> for Node<C, N, S>
where
    Op: ReplicatableOperation<Type = ReconciledRead>,
    Op::Output: Reconcile,
    S: Storage<Positioned<Op>, Ok = Op::Output>,
    Self: Send + Sync,
{
    type StorageError = S::Error;

    const REQUIRES_HINTING: bool = false;

    fn reconcile_results(results: Vec<Op::Output>, required_replicas: usize) -> Option<Op::Output> {
        Op::Output::reconcile(results, required_replicas)
    }

    async fn exec_replicated(
        &self,
        op: Positioned<Op>,
    ) -> Result<Op::Output, ReplicaError<S::Error>> {
        self.storage.exec(op).await.map_err(ReplicaError::Storage)
    }
}

pub struct Write;

#[async_trait]
impl<C, N, S, Op, E> sealed::Replication<Op, Write> for Node<C, N, S>
where
    Op: ReplicatableOperation<Type = Write, Output = ()>,
    E: Send,
    N: Send + Sync,
    S: Storage<Positioned<Op>, Ok = (), Error = E>
        + Storage<StoreHinted<Positioned<Op>>, Ok = (), Error = E>,
    Self: Send + Sync,
{
    type StorageError = E;

    const REQUIRES_HINTING: bool = true;

    async fn exec_replicated(
        &self,
        operation: Positioned<Op>,
    ) -> Result<Op::Output, ReplicaError<E>> {
        if let Some(op) = self
            .migration_manager
            .store_hinted(operation)
            .await
            .map_err(ReplicaError::Storage)?
        {
            return self.storage.exec(op).await.map_err(ReplicaError::Storage);
        }

        Ok(())
    }
}

impl<I, T: Eq + Hash + Ord> Reconcile for I
where
    I: IntoIterator<Item = T> + FromIterator<T>,
{
    fn reconcile(values: Vec<Self>, required_replicas: usize) -> Option<Self> {
        let items = reconcile_collection(values.into_iter(), required_replicas, None);

        Some(items)
    }
}

#[derive(
    Debug, Clone, Copy, Into, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Cardinality(pub u64);

impl Reconcile for Cardinality {
    fn reconcile(values: Vec<Self>, required_replicas: usize) -> Option<Self> {
        let reconciled: SmallVec<[Cardinality; 3]> = reconcile_collection(
            values.iter().copied().map(|card| smallvec![card]),
            required_replicas,
            Some(3),
        );

        // If there's no consensus on the collection cardinality, return the lowest
        // value that replicas agree on.
        if reconciled.is_empty() {
            values.iter().copied().min()
        } else {
            Some(values[0])
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub has_more: bool,
}

impl<T> From<Page<T>> for Vec<T> {
    fn from(page: Page<T>) -> Self {
        page.items
    }
}

impl<T: Eq + Hash + Ord> Reconcile for Page<T> {
    fn reconcile(values: Vec<Self>, required_replicas: usize) -> Option<Self> {
        let has_more = values.iter().filter(|v| v.has_more).count() >= required_replicas;
        let iter = values.into_iter().map(|data| data.items);
        let items = reconcile_collection(iter, required_replicas, None);

        Some(Page { items, has_more })
    }
}

fn reconcile_collection<T, I, U>(
    iter: I,
    required_replicas: usize,
    estimated_size: Option<usize>,
) -> U
where
    I: Iterator<Item = U>,
    U: IntoIterator<Item = T> + FromIterator<T>,
    T: Hash + PartialEq + Eq + Ord,
{
    let mut counters = if let Some(capacity) = estimated_size {
        HashMap::with_capacity(capacity)
    } else {
        HashMap::new()
    };

    let mut min: Option<usize> = None;
    let mut max: Option<usize> = None;

    let iter = iter.map(|data| {
        let iter = data.into_iter();
        let (_, num_items) = iter.size_hint();

        if let Some(num_items) = num_items {
            if let Some(val) = min.as_mut() {
                *val = (*val).min(num_items);
            } else {
                min = Some(num_items)
            }

            if let Some(val) = max.as_mut() {
                *val = (*val).max(num_items);
            } else {
                max = Some(num_items)
            }
        }

        iter
    });

    for item in iter.flatten() {
        *counters.entry(item).or_insert(0) += 1;
    }

    let mut items: Vec<_> = counters
        .into_iter()
        .filter_map(move |(item, count)| (count >= required_replicas).then_some(item))
        .collect();

    reconciliation_metrics(
        metrics::counter!("irn_collection_reconciliation"),
        min,
        max,
        items.len(),
    );

    // TODO: Optimize / don't try to preserve order when not necessary

    // Lexicographic sort to find the last key to use as the cursor.
    items.sort_unstable();

    U::from_iter(items)
}

fn reconciliation_metrics(
    counter: &otel::metrics::Counter<u64>,
    min: Option<usize>,
    max: Option<usize>,
    result: usize,
) {
    const BUCKETS: [usize; 17] = [
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
    ];

    let items_kv = |name: &'static str, num_items: usize| {
        otel::KeyValue::new(name, metrics::value_bucket(num_items, &BUCKETS) as i64)
    };

    counter.add(&otel::Context::new(), 1, &[
        items_kv("min", min.unwrap_or(0)),
        items_kv("max", max.unwrap_or(0)),
        items_kv("result", result),
    ]);
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MatchResult<K, V> {
    Ok(V, Vec<IdValue<K, V>>),
    Inconsistent(Vec<IdValue<K, V>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdValue<K, V> {
    id: K,
    value: V,
}

impl<K, V> IdValue<K, V> {
    fn new(id: K, value: V) -> Self {
        Self { id, value }
    }
}

/// Wait on futures until `k` matching values are found, or timeout.
///
/// If `k` matching values are found, return the value (along with result tuples
/// for all the replicas that responded), and spawn a task to wait on the
/// remaining futures (if any). Timer is reset, so the remaining futures have
/// the full TTL to complete.
///
/// If no `k` matching values are found, return [`MatchResult::Inconsistent`]
/// which will be turned into reconciled result, for types supporting
/// reconciliation, or `None` for other types.
async fn match_values<K: Clone + 'static, V: Eq + Clone + 'static>(
    k: usize,
    mut futures_stream: FuturesUnordered<BoxFuture<'static, IdValue<K, V>>>,
    ttl: Duration,
) -> Result<MatchResult<K, V>, tokio::time::error::Elapsed> {
    tokio::time::timeout(ttl, async {
        let mut results = vec![];
        loop {
            let Some(res) = futures_stream.next().await else {
                return MatchResult::Inconsistent(results);
            };

            results.push(res.clone());

            // If we have k matching values, return the value.
            if results.iter().filter(|&v| v.value == res.value).count() >= k {
                // Spawn a task to wait for the remaining futures.
                tokio::spawn(async move {
                    let handle =
                        futures_stream.for_each_concurrent(None, |_future| future::ready(()));

                    // Wait for the handle to complete, or timeout.
                    let _ = tokio::time::timeout(ttl, handle).await;
                });
                return MatchResult::Ok(res.value, results);
            }
        }
    })
    .await
}
