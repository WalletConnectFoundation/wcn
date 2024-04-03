pub use {
    self::{consensus::Consensus, identity::PeerId, network::Network},
    storage::Storage,
};
use {
    async_trait::async_trait,
    cluster::{replication::Strategy, Cluster},
    derive_more::AsRef,
    futures::{Future, FutureExt},
    pin_project::pin_project,
    std::{
        pin::{pin, Pin},
        sync::{Arc, Mutex},
        task,
        time::Duration,
    },
    tokio::sync::{oneshot, RwLock, Semaphore},
};

pub mod cluster;
pub mod consensus;
pub mod identity;
pub mod network;
pub mod storage;

#[cfg(feature = "testing")]
pub mod testing;

mod fsm;
pub mod migration;
pub mod replication;

/// [`Node`] configuration options.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct NodeOpts {
    /// Replication strategy to use.
    pub replication_strategy: Strategy,

    /// Timeout of a request from a coordinator to a replica.
    pub replication_request_timeout: Duration,

    /// Maximum number of concurrent operations this [`Node`] should handle
    /// before starting to throttle.
    pub replication_concurrency_limit: usize,

    /// Maximum number of operations awaiting to obtain permission to execute.
    pub replication_request_queue: usize,

    /// Extra time a node should take to "warmup" before transitioning from
    /// `Restarting` to `Normal`.
    pub warmup_delay: Duration,
}

/// Shared [`Handle`] to a [`Node`].
#[derive(AsRef, Clone, Debug)]
pub struct Node<C, N, S> {
    #[as_ref]
    id: PeerId,

    cluster: Arc<RwLock<Cluster>>,
    consensus: C,
    network: N,
    storage: S,

    migration_manager: migration::Manager<N, S>,

    shutdown: Arc<Mutex<Option<oneshot::Sender<ShutdownReason>>>>,

    replication_request_timeout: Duration,
    replication_request_limiter: Arc<Semaphore>,
    replication_request_limiter_queue: Arc<Semaphore>,
    warmup_delay: Duration,
}

impl<C, N, S> AsRef<Self> for Node<C, N, S> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<C, N, S> Node<C, N, S> {
    /// Returns [`PeerId`] of this [`Node`].
    pub fn id(&self) -> &PeerId {
        &self.id
    }

    /// Returns [`Storage`] of this [`Node`].
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns [`Cluster`] of this [`Node`].
    pub fn cluster(&self) -> Arc<RwLock<Cluster>> {
        self.cluster.clone()
    }

    /// Returns [`Consensus`] impl of this [`Node`].
    pub fn consensus(&self) -> &C {
        &self.consensus
    }

    /// Returns [`Network`] impl of this [`Node`].
    pub fn network(&self) -> &N {
        &self.network
    }
}

impl<C, N, S> Node<C, N, S>
where
    C: Consensus,
    N: Network,
    S: Clone + Send + Sync + 'static,
    migration::Manager<N, S>: BootingMigrations + LeavingMigrations,
{
    /// Creates a new [`Node`] using the provided [`NodeOpts`] and dependencies.
    pub fn new(id: PeerId, opts: NodeOpts, consensus: C, network: N, storage: S) -> Self {
        let cluster = Arc::new(RwLock::new(Cluster::new(opts.replication_strategy)));

        let migration_manager =
            migration::Manager::new(id, network.clone(), storage.clone(), cluster.clone());

        Node {
            id,
            storage,
            cluster,
            consensus,
            network,
            migration_manager,
            shutdown: Arc::new(Mutex::new(None)),
            replication_request_timeout: opts.replication_request_timeout,
            replication_request_limiter: Arc::new(Semaphore::new(
                opts.replication_concurrency_limit,
            )),
            replication_request_limiter_queue: Arc::new(Semaphore::new(
                opts.replication_request_queue,
            )),
            warmup_delay: opts.warmup_delay,
        }
    }

    /// Runs this [`Node`].
    pub async fn run(self) -> Result<ShutdownReason, AlreadyRunning> {
        let shutdown_rx = {
            let mut shared_tx = self.shutdown.lock().unwrap();
            if shared_tx.is_some() {
                return Err(AlreadyRunning);
            }

            let (tx, rx) = oneshot::channel();
            let _ = shared_tx.insert(tx);
            rx
        };

        fsm::run(
            self.id,
            self.consensus.clone(),
            self.network.clone(),
            self.cluster.clone(),
            self.migration_manager.clone(),
            self.warmup_delay,
            shutdown_rx,
        )
        .map(Ok)
        .await
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Node already running")]
pub struct AlreadyRunning;

impl<C, N, S> Node<C, N, S> {
    /// Initiates shut down process of this [`Node`].
    pub fn shutdown(&self, reason: ShutdownReason) -> Result<(), ShutdownError> {
        let tx = self.shutdown.lock().unwrap().take().ok_or(ShutdownError)?;
        tx.send(reason).map_err(|_| ShutdownError)
    }
}

/// Error of [`Node::shutdown`].
#[derive(Debug, thiserror::Error)]
#[error("Shutdown is already in progress")]
pub struct ShutdownError;

/// Reason for shutting down a [`Node`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownReason {
    /// The [`Node`] is being decommissioned and isn't expected to be back
    /// online.
    ///
    /// It is required to perform data migrations in order to decommission a
    /// [`Node`].
    Decommission,

    /// The [`Node`] is being restarted and is expected to be back online ASAP.
    ///
    /// Data migrations are not being performed.
    Restart,
}

#[async_trait]
pub trait Migrations:
    BootingMigrations + LeavingMigrations + Clone + Send + Sync + 'static
{
    // TODO: `migration::Manager` should own the `PendingRange`s, but that would
    // require to extract them from the `Cluster`. `()` is a placeholder for
    // now.
    // For now this function is only used to trigger a post-migration clean-up.
    // But the idea is to make FSM to recalculate pending ranges on cluster
    // updates and give them to the `migration::Manager` here.
    async fn update_pending_ranges(&self, _ranges: ());
}

#[async_trait]
pub trait BootingMigrations: Clone + Send + 'static {
    async fn run_booting_migrations(self);
}

#[async_trait]
pub trait LeavingMigrations: Clone + Send + 'static {
    async fn run_leaving_migrations(self);
}

/// Represents a running service as a non-detached [`Future`] and gives access
/// to it's handle.
///
/// It implements [`Future`] so you can `.await` it directly.
#[pin_project]
pub struct Running<Handle, Fut> {
    /// `Handle` of this [`Running`] service.
    pub handle: Handle,

    /// Runtime [`Future`] of the service.
    #[pin]
    pub future: Fut,
}

impl<H, F> Future for Running<H, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

#[tokio::test]
async fn full_node_rotation() {
    test::full_node_rotation::<stub::Cluster, _, _>().await
}

#[tokio::test]
async fn restart_and_decommision() {
    test::restart_and_decommission::<stub::Cluster>().await
}

#[cfg(any(feature = "testing", test))]
pub mod test {
    use {
        super::async_trait,
        crate::{
            cluster::{self, keyspace::Keyspace, replication::Strategy, NodeOperationMode},
            network::HandleRequest,
            replication::{
                CoordinatorResponse,
                DispatchReplicated,
                Read,
                ReplicatableOperation,
                ReplicatableOperationOutput,
                Write,
            },
            Consensus,
            Network,
            PeerId,
        },
        futures::{stream, StreamExt},
        std::{fmt, pin::pin, time::Duration},
    };

    #[async_trait]
    pub trait Cluster: Sized {
        type Node: AsRef<Node<Self>>;

        type Consensus: Consensus;
        type Network: Network;
        type Storage;
        type Keyspace: Keyspace;

        /// Basic key value operations.
        type ReadOperation: ReplicatableOperation<Type = Read>;
        type WriteOperation: ReplicatableOperation<Type = Write>;
        type RemoveOperation: ReplicatableOperation<Type = Write>;

        async fn run(size: usize, strategy: Option<Strategy>) -> Self;

        async fn spinup_node(&mut self);
        async fn decommission_node(&mut self, idx: usize);
        async fn restart_node(&mut self, idx: usize);

        fn node(&self, idx: usize) -> &Self::Node;

        fn keyspace(&self) -> &Self::Keyspace;

        fn gen_write_read_assert(&self, seed: Option<u64>) -> WriteReadAssert<Self>;
        fn gen_write_assert(&self, seed: Option<u64>) -> WriteAssert<Self>;
        fn gen_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self>;
        fn gen_empty_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self>;
        fn gen_remove_assert(&self, seed: Option<u64>) -> RemoveAssert<Self>;
    }

    /// Test case that performs the following steps:
    /// - executes write operation, asserting no errors occurred
    /// - executes read operation, asserting that output matches the expected
    ///   one
    pub struct WriteReadAssert<C: Cluster> {
        pub write: C::WriteOperation,
        pub read: C::ReadOperation,
        pub expected_output: <C::ReadOperation as ReplicatableOperation>::Output,
    }

    pub struct WriteAssert<C: Cluster> {
        pub operation: C::WriteOperation,
        pub expected_output: <C::WriteOperation as ReplicatableOperation>::Output,
    }

    pub struct ReadAssert<C: Cluster> {
        pub operation: C::ReadOperation,
        pub expected_output: <C::ReadOperation as ReplicatableOperation>::Output,
    }

    pub struct RemoveAssert<C: Cluster> {
        pub operation: C::RemoveOperation,
        pub expected_output: <C::RemoveOperation as ReplicatableOperation>::Output,
    }

    pub type Node<C> =
        super::Node<<C as Cluster>::Consensus, <C as Cluster>::Network, <C as Cluster>::Storage>;

    pub type ReadOutput<C> = ReplicatableOperationOutput<<C as Cluster>::ReadOperation>;
    pub type WriteOutput<C> = ReplicatableOperationOutput<<C as Cluster>::WriteOperation>;
    pub type RemoveOutput<C> = ReplicatableOperationOutput<<C as Cluster>::RemoveOperation>;

    /// Test of Theseus. If all nodes in the cluster are replaced
    /// ~~is it the same cluster?~~ does it still possess all the data?
    pub async fn full_node_rotation<C: Cluster, SE: fmt::Debug, NE: fmt::Debug>()
    where
        C::Node: HandleRequest<
                DispatchReplicated<C::ReadOperation>,
                Response = CoordinatorResponse<ReadOutput<C>, SE, NE>,
            > + HandleRequest<
                DispatchReplicated<C::WriteOperation>,
                Response = CoordinatorResponse<WriteOutput<C>, SE, NE>,
            >,
        ReadOutput<C>: fmt::Debug + PartialEq,
    {
        const RECORDS_NUM: usize = 10000;
        const NODES_NUM: usize = 5;
        const REQUEST_CONCURRENCY: usize = 10;

        tracing::info!("Initializing cluster");
        let mut cluster: C = Cluster::run(5, None).await;
        for idx in 0..NODES_NUM {
            let node = cluster.node(idx).as_ref();
            node.wait_op_mode(node.id, NodeOperationMode::Normal).await;
        }

        tracing::info!("Populating cluster");

        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async {
                let assert = cluster.gen_write_read_assert(None);

                cluster
                    .node(0)
                    .handle_request(DispatchReplicated {
                        operation: assert.write,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                ReadAssert::<C> {
                    operation: assert.read,
                    expected_output: assert.expected_output,
                }
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        for _ in 0..NODES_NUM {
            // spin up new node and wait for it to become `Normal`
            {
                tracing::info!("Spinning up a new node");

                cluster.spinup_node().await;
                let id = cluster.node(NODES_NUM).as_ref().id;

                tracing::info!(%id, "Node started, waiting for it to become `Normal`");
                for idx in 0..NODES_NUM + 1 {
                    let node = cluster.node(idx).as_ref();
                    node.wait_op_mode(id, NodeOperationMode::Normal).await;
                }
            }

            // decomission a node and wait for it to become `Left`
            {
                let id = cluster.node(0).as_ref().id;
                tracing::info!(%id, "Decomissioning a node");

                cluster.decommission_node(0).await;

                tracing::info!(%id, "Node decommissioned, waiting for it to become `Left`");
                for idx in 0..NODES_NUM {
                    let node = cluster.node(idx).as_ref();
                    node.wait_op_mode(id, NodeOperationMode::Left).await;
                }
            }
        }

        // Even after waiting for consensus changes there's still a race condition with
        // FSM updating the `Cluster`.
        // TODO: Revisit this
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("Validating data");

        let node = cluster.node(0);
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = node
                    .handle_request(DispatchReplicated {
                        operation: assert.operation,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                usize::from(assert.expected_output != output)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    pub async fn restart_and_decommission<C: Cluster>() {
        let mut cluster: C = Cluster::run(5, None).await;
        for idx in 0..5 {
            let node = cluster.node(idx).as_ref();
            node.wait_op_mode(node.id, NodeOperationMode::Normal).await;
        }

        cluster.restart_node(0).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let id = cluster.node(0).as_ref().id;
        cluster.decommission_node(0).await;
        for idx in 0..4 {
            let node = cluster.node(idx).as_ref();
            node.wait_op_mode(id, NodeOperationMode::Left).await;
        }
    }

    impl<C, N, S> super::Node<C, N, S> {
        pub async fn wait_op_mode(&self, peer_id: PeerId, mode: cluster::NodeOperationMode)
        where
            C: Consensus,
        {
            tokio::time::timeout(Duration::from_secs(60), async {
                let mut changes = pin!(self.consensus.changes());

                loop {
                    let view = changes.next().await.unwrap();
                    match view.nodes().get(&peer_id) {
                        Some(n) if n.mode == mode => return,
                        None if mode == NodeOperationMode::Left => return,
                        _ => {}
                    }
                }
            })
            .await
            .unwrap();
        }
    }
}

#[cfg(test)]
mod stub {
    use {
        super::Node,
        crate::{
            cluster::{
                self,
                keyspace::{hashring, Keyspace},
                replication::Strategy,
                ClusterView,
                NodeOperationMode,
            },
            consensus,
            network,
            storage::{
                self,
                stub::{Data, Del, Get, Set},
            },
            test::{self, ReadAssert, RemoveAssert, WriteAssert, WriteReadAssert},
            Network,
            NodeOpts,
            PeerId,
            ShutdownReason,
        },
        async_trait::async_trait,
        rand::{prelude::StdRng, Rng, SeedableRng},
        std::{collections::HashMap, time::Duration},
    };

    #[cfg(test)]
    pub type StubbedNode = Node<consensus::Stub, network::Stub, storage::Stub>;

    #[cfg(test)]
    impl StubbedNode {
        fn new_stubbed(consensus: consensus::Stub, strategy: Strategy) -> Self {
            let opts = NodeOpts {
                replication_strategy: strategy,
                replication_request_timeout: Duration::from_secs(15),
                replication_concurrency_limit: 1000,
                replication_request_queue: 4096,
                warmup_delay: Duration::from_millis(10),
            };

            Node::new(
                PeerId::random(),
                opts,
                consensus,
                network::Stub::new(),
                storage::Stub::new(),
            )
            .register()
        }

        fn view(&self, mode: NodeOperationMode) -> cluster::Node {
            cluster::Node {
                peer_id: self.id,
                addr: self.network.local_multiaddr(),
                mode,
            }
        }
    }

    #[derive(Debug)]
    pub struct Cluster {
        pub nodes: Vec<StubbedNode>,
        pub keyspace: hashring::Keyspace,
        pub handles: HashMap<PeerId, tokio::task::JoinHandle<ShutdownReason>>,
    }

    #[async_trait]
    impl test::Cluster for Cluster {
        type Node = StubbedNode;

        type Consensus = consensus::Stub;
        type Network = network::Stub;
        type Storage = storage::Stub;
        type Keyspace = hashring::Keyspace;

        type ReadOperation = Get;
        type WriteOperation = Set;
        type RemoveOperation = Del;

        async fn run(size: usize, strategy: Option<Strategy>) -> Self {
            let mut cluster = Self::new(size, strategy).await;
            cluster.run();
            cluster
        }

        async fn spinup_node(&mut self) {
            let new = Node::new_stubbed(
                self.nodes[0].consensus.clone(),
                self.keyspace.replication_strategy().clone(),
            );

            for node in &self.nodes {
                node.network
                    .register_peer_address(new.id, new.network.local_multiaddr())
                    .await;
                new.network
                    .register_peer_address(node.id, node.network.local_multiaddr())
                    .await;
            }

            self.keyspace.add_node(&new).expect("keyspace updated");
            self.nodes.push(new.clone());

            self.nodes[0]
                .consensus
                .set_node(new.view(NodeOperationMode::Started));

            self.run_node(&new);
        }

        async fn decommission_node(&mut self, idx: usize) {
            let node = self.nodes.remove(idx);
            self.keyspace.remove_node(&node).unwrap();
            node.shutdown(ShutdownReason::Decommission).unwrap();
        }

        async fn restart_node(&mut self, idx: usize) {
            let node = self.nodes.get(idx).unwrap().clone();
            node.shutdown(ShutdownReason::Restart).unwrap();
            let _ = self.handles.remove(&node.id).unwrap().await.unwrap();

            self.run_node(&node);
            *node.cluster.write().await =
                crate::Cluster::new(self.keyspace.replication_strategy().clone());
        }

        fn node(&self, idx: usize) -> &Self::Node {
            self.nodes.get(idx).unwrap()
        }

        fn keyspace(&self) -> &Self::Keyspace {
            &self.keyspace
        }

        fn gen_write_read_assert(&self, seed: Option<u64>) -> WriteReadAssert<Self> {
            let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

            WriteReadAssert {
                write: Set(key, value),
                read: Get(key),
                expected_output: Some(value),
            }
        }

        fn gen_write_assert(&self, seed: Option<u64>) -> WriteAssert<Self> {
            let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));
            WriteAssert {
                operation: Set(key, value),
                expected_output: (),
            }
        }

        fn gen_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self> {
            let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

            ReadAssert {
                operation: Get(key),
                expected_output: Some(value),
            }
        }

        fn gen_empty_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self> {
            let (key, _) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

            ReadAssert {
                operation: Get(key),
                expected_output: None,
            }
        }

        fn gen_remove_assert(&self, seed: Option<u64>) -> RemoveAssert<Self> {
            let (key, _) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

            RemoveAssert {
                operation: Del(key),
                expected_output: (),
            }
        }
    }

    impl Cluster {
        pub async fn new_dummy(n: usize) -> Self {
            let cluster = Self::new(n, None).await;

            let mut cluster_view = ClusterView::new();
            cluster_view.set_peers(
                cluster
                    .nodes
                    .iter()
                    .map(|node| (node.id, node.view(NodeOperationMode::Normal)))
                    .collect(),
            );

            for node in &cluster.nodes {
                let _ = node
                    .cluster
                    .write()
                    .await
                    .install_view_update(cluster_view.clone());
            }

            cluster
        }

        async fn new(n: usize, strategy: Option<Strategy>) -> Self {
            let consensus = consensus::Stub::new();

            let nodes: Vec<_> = (0..n)
                .map(|_| Node::new_stubbed(consensus.clone(), strategy.clone().unwrap_or_default()))
                .collect();

            let mut keyspace = hashring::Keyspace::new(strategy.unwrap_or_default());

            for node in &nodes {
                keyspace.add_node(node).unwrap();
                for peer in &nodes {
                    if node.id != peer.id {
                        node.network
                            .register_peer_address(peer.id, peer.network.local_multiaddr())
                            .await
                    }
                }
            }

            Self {
                nodes,
                keyspace,
                handles: HashMap::new(),
            }
        }

        fn run(&mut self) {
            for node in self.nodes.clone() {
                node.consensus
                    .set_node(node.view(NodeOperationMode::Started));
                self.run_node(&node)
            }
        }

        fn run_node(&mut self, node: &StubbedNode) {
            let node = node.clone();
            let id = node.id;
            let handle = tokio::spawn(async { node.run().await.unwrap() });
            self.handles.insert(id, handle);
        }

        pub async fn populate_storages(&self) {
            let data = Data::generate();

            for node in &self.nodes {
                node.storage.populate(data.clone());
            }
        }

        pub async fn add_dummy_node(&mut self, strategy: Option<Strategy>) {
            let new = Node::new_stubbed(
                self.nodes[0].consensus.clone(),
                strategy.unwrap_or_default(),
            );

            let mut cluster_view = self.nodes[0].cluster.read().await.view().clone();
            let mut peers = cluster_view.nodes().clone();
            peers.insert(new.id, new.view(NodeOperationMode::Booting));
            cluster_view.set_peers(peers);

            for node in &self.nodes {
                node.network
                    .register_peer_address(new.id, new.network.local_multiaddr())
                    .await;
                new.network
                    .register_peer_address(node.id, node.network.local_multiaddr())
                    .await;

                node.cluster
                    .write()
                    .await
                    .install_view_update(cluster_view.clone());
            }

            new.cluster.write().await.install_view_update(cluster_view);

            self.keyspace.add_node::<&StubbedNode>(&new).unwrap();
            self.nodes.push(new);
        }

        pub async fn set_node_op_mode(&self, idx: usize, mode: NodeOperationMode) {
            let mut cluster_view = self.nodes[0].cluster.read().await.view().clone();
            let mut peers = cluster_view.nodes().clone();
            peers.get_mut(&self.nodes[idx].id).unwrap().mode = mode;
            cluster_view.set_peers(peers);

            for node in &self.nodes {
                node.cluster
                    .write()
                    .await
                    .install_view_update(cluster_view.clone());
            }
        }

        fn gen_key_value(seed: u64) -> (u64, u64) {
            let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
            let key = rng.gen::<u64>();
            let value = rng.gen::<u64>();
            (key, value)
        }
    }
}
