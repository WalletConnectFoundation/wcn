#![allow(async_fn_in_trait)]

use {
    crate::{
        cluster::{self, consensus, Consensus, Node as _},
        AuthorizationOpts,
    },
    derive_more::Into,
    futures::{
        future,
        stream::{FuturesUnordered, StreamExt},
    },
    serde::{Deserialize, Serialize},
    smallvec::{smallvec, SmallVec},
    std::{
        collections::HashMap,
        convert::Infallible,
        error::Error as StdError,
        fmt::{self, Debug},
        future::Future,
        hash::{BuildHasher, Hash},
        sync::Arc,
        time::Duration,
    },
    tokio::sync::Semaphore,
};

#[cfg(test)]
mod tests;

pub trait Network<Node, S: Storage<Op>, Op: StorageOperation>:
    Clone + Send + Sync + 'static
{
    type Error: Clone + Eq + StdError;

    fn send(
        &self,
        to: &Node,
        operation: Op,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<ReplicaResponse<S, Op>, Self::Error>> + Send;
}

pub trait Storage<Op: StorageOperation>: Clone + Send + Sync + 'static {
    type Error: Eq + StdError + Send + Clone;

    fn exec(
        &self,
        key_hash: u64,
        op: Op,
    ) -> impl Future<Output = Result<Op::Output, Self::Error>> + Send;
}

pub type StorageError<S, Op> = <S as Storage<Op>>::Error;

pub type CoordinatorResponse<S, Op> = Result<ReplicaResponse<S, Op>, CoordinatorError>;

pub type ReplicaResponse<S, Op> =
    Result<<Op as StorageOperation>::Output, ReplicaError<<S as Storage<Op>>::Error>>;

#[derive(Clone, Debug)]
pub struct Coordinator<C: Consensus, N, S, H> {
    replica: Replica<C, S, H>,
    network: N,

    authorization: Option<Arc<AuthorizationOpts>>,
}

#[derive(Clone, Debug)]
pub struct Replica<C: Consensus, S, H> {
    node: C::Node,

    consensus: C,
    storage: S,
    hasher_builder: H,

    throttling: Throttling,
}

#[derive(Clone, Debug)]
pub struct Throttling {
    pub request_timeout: Duration,
    pub request_limiter: Arc<Semaphore>,
    pub request_limiter_queue: Arc<Semaphore>,
}

impl<C, N, S, H> Coordinator<C, N, S, H>
where
    C: Consensus,
{
    pub fn new(
        replica: Replica<C, S, H>,
        network: N,
        authorization: Option<AuthorizationOpts>,
    ) -> Self {
        Self {
            replica,
            network,
            authorization: authorization.map(Arc::new),
        }
    }

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
    #[allow(clippy::trait_duplication_in_bounds)] // false positive
    pub async fn replicate<Op>(
        &self,
        client_id: &libp2p::PeerId,
        operation: Op,
    ) -> CoordinatorResponse<S, Op>
    where
        Op: StorageOperation,
        S: Storage<Op> + Storage<Op::RepairOperation>,
        H: BuildHasher,
        N: Network<C::Node, S, Op>,
    {
        if let Some(auth) = &self.authorization {
            if !auth.allowed_coordinator_clients.contains(client_id) {
                return Err(CoordinatorError::Unauthorized);
            }
        }

        let key = operation.as_ref();
        let key_hash = self.replica.hasher_builder.hash_one(key);

        let cluster = self.replica.consensus.cluster();
        let replica_set = cluster.replica_set(key_hash, Op::IS_WRITE)?;
        let keyspace_version = cluster.keyspace_version();

        let send_replicated = |node: &C::Node| {
            let node = node.clone();
            let storage = self.replica.storage.clone();
            let network = self.network.clone();
            let operation = operation.clone();

            let is_coordinator = node.id() == self.replica.node.id();
            async move {
                // If coordinator is the replica perform the operation locally.
                let resp = if is_coordinator {
                    Ok(storage
                        .exec(key_hash, operation)
                        .await
                        .map_err(ReplicaError::Storage))
                } else {
                    network.send(&node, operation, keyspace_version).await
                };

                IdValue::new(node.id().clone(), resp)
            }
        };

        let futures: FuturesUnordered<_> = replica_set.nodes.map(send_replicated).collect();

        let result = match_values(
            replica_set.required_count,
            futures,
            self.replica.throttling.request_timeout,
        )
        .await?;
        match result {
            // Matching value received.
            MatchResult::Ok(value, responses) => {
                // Attempt to repair the value on the coordinator node, only if it is in the
                // replica set.
                if !responses
                    .iter()
                    .any(|r| &r.id == self.replica.node.id() && r.value != value)
                {
                    return value.map_err(|e| CoordinatorError::Network(e.to_string()));
                }

                let output = match value {
                    Ok(Ok(out)) => out,
                    result => return result.map_err(|e| CoordinatorError::Network(e.to_string())),
                };

                if let Some(op) = operation.repair_operation(&output) {
                    let _ = self
                        .replica
                        .storage
                        .exec(key_hash, op)
                        .await
                        .map_err(|err| tracing::warn!(?err, "read repair failed"));
                }

                Ok(Ok(output))
            }
            // Not enough matching responses.
            MatchResult::Inconsistent(responses) => {
                let values: Vec<_> = responses
                    .into_iter()
                    .map(|res| res.value)
                    .filter_map(Result::ok)
                    .filter_map(Result::ok)
                    .collect();

                Op::reconcile_results(&values, replica_set.required_count)
                    .map(Ok)
                    .ok_or(CoordinatorError::InconsistentOperation)
            }
        }
    }

    pub fn node(&self) -> &C::Node {
        &self.replica.node
    }

    pub fn storage(&self) -> &S {
        &self.replica.storage
    }

    pub fn consenus(&self) -> &C {
        &self.replica.consensus
    }

    pub fn network(&self) -> &N {
        &self.network
    }

    pub fn replica(&self) -> &Replica<C, S, H> {
        &self.replica
    }
}

impl<C: Consensus, S, H> Replica<C, S, H> {
    pub fn new(
        node: C::Node,
        consensus: C,
        storage: S,
        hasher_builder: H,
        throttling: Throttling,
    ) -> Self {
        Self {
            node,
            consensus,
            storage,
            hasher_builder,
            throttling,
        }
    }

    pub async fn handle_replication<Op>(
        &self,
        coordinator_id: &consensus::NodeId<C>,
        operation: Op,
        keyspace_version: u64,
    ) -> ReplicaResponse<S, Op>
    where
        H: BuildHasher,
        S: Storage<Op>,
        Op: StorageOperation,
    {
        self.consensus.cluster_view().peek(|cluster| {
            if !cluster.contains_node(coordinator_id) {
                return Err(ReplicaError::NotClusterMember);
            }

            if cluster.keyspace_version() != keyspace_version {
                return Err(ReplicaError::KeyspaceVersionMismatch);
            }

            Ok(())
        })?;

        let key: &Op::Key = operation.as_ref();
        let key_hash = self.hasher_builder.hash_one(key);

        self.storage
            .exec(key_hash, operation)
            .await
            .map_err(ReplicaError::Storage)
    }

    pub fn node(&self) -> &C::Node {
        &self.node
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoordinatorError {
    #[error("Cluster is not bootstrapped yet")]
    ClusterNotBootstrapped,

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
    Network(String),

    #[error("Client is not authorized to use this coordinator")]
    Unauthorized,

    #[error("Other: {_0:?}")]
    Other(String),
}

impl From<cluster::Error> for CoordinatorError {
    fn from(err: cluster::Error) -> Self {
        use cluster::Error;
        match err {
            Error::NotBootstrapped => Self::ClusterNotBootstrapped,
            Error::NodeAlreadyExists
            | Error::NodeAlreadyStarted
            | Error::UnknownNode
            | Error::TooManyNodes
            | Error::TooFewNodes
            | Error::NotNormal
            | Error::NoMigration
            | Error::KeyspaceVersionMismatch
            | Error::InvalidNode(_)
            | Error::Bug(_) => Self::Other(err.to_string()),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaError<S> {
    /// Keyspace versions of requester and responder don't match
    #[error("Keyspace versions of requester and responder don't match")]
    KeyspaceVersionMismatch,

    #[error("Too many requests")]
    Throttled,

    /// Error of executing the operation.
    #[error("Local storage error: {0}")]
    Storage(S),

    #[error("Coordinator is not a cluster member")]
    NotClusterMember,
}

impl From<tokio::time::error::Elapsed> for CoordinatorError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

pub trait StorageOperation: AsRef<Self::Key> + Debug + Clone + Send + Sync + 'static {
    const IS_WRITE: bool;

    type Key: Hash + Send + Sync + 'static;
    type Output: Clone + fmt::Debug + Eq + Send + 'static;

    /// The corresponding repair operation type for the current operation.
    type RepairOperation: StorageOperation;

    /// Returns an optional repair operation for the current operation.
    fn repair_operation(&self, _new_value: &Self::Output) -> Option<Self::RepairOperation> {
        None
    }

    fn reconcile_results(
        _results: &[Self::Output],
        _required_replicas: usize,
    ) -> Option<Self::Output> {
        None
    }
}

pub trait Reconcile: Sized {
    fn reconcile(values: Vec<Self>, required_replicas: usize) -> Option<Self>;
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

    // TODO: Optimize / don't try to preserve order when not necessary

    // Lexicographic sort to find the last key to use as the cursor.
    items.sort_unstable();

    U::from_iter(items)
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
    mut futures_stream: FuturesUnordered<impl Future<Output = IdValue<K, V>> + Send + 'static>,
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

#[derive(Clone, Debug)]
pub struct NoRepair;

impl AsRef<()> for NoRepair {
    fn as_ref(&self) -> &() {
        &()
    }
}

impl StorageOperation for NoRepair {
    const IS_WRITE: bool = false;

    type Key = ();
    type Output = ();
    type RepairOperation = Self;
}

impl<S: Clone + Send + Sync + 'static> Storage<NoRepair> for S {
    type Error = Infallible;

    fn exec(
        &self,
        _key_hash: u64,
        _op: NoRepair,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}
