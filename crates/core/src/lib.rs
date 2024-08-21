#![allow(clippy::manual_async_fn)]

pub use cluster::Cluster;
use {
    cluster::{consensus, Consensus, Node as _},
    futures::{Future, FutureExt as _},
    pin_project::pin_project,
    replication::Replica,
    std::{
        collections::HashSet,
        pin::{pin, Pin},
        sync::{Arc, Mutex},
        task,
        time::Duration,
    },
    tokio::sync::{oneshot, Semaphore},
};

pub mod cluster;
pub mod fsm;
pub mod migration;
pub mod replication;

/// [`Node`] configuration options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeOpts {
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

    /// Authorization options.
    ///
    /// When `None` client authorization is disabled.
    pub authorization: Option<AuthorizationOpts>,
}

/// [`Node`] authorization options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationOpts {
    /// List of clients which are allowed to use a [`Node`] as a coordinator.
    pub allowed_coordinator_clients: HashSet<libp2p::PeerId>,
}

/// Shared [`Handle`] to a [`Node`].
#[derive(Clone, Debug)]
pub struct Node<C: Consensus, N, S, H> {
    replication_coordinator: replication::Coordinator<C, N, S, H>,
    migration_manager: migration::Manager<C, N, S>,

    shutdown: Arc<Mutex<Option<oneshot::Sender<fsm::ShutdownReason>>>>,
    warmup_delay: Duration,
}

impl<C: Consensus, N, S, H> Node<C, N, S, H> {
    /// Returns ID of this [`Node`].
    pub fn id(&self) -> &consensus::NodeId<C> {
        self.replication_coordinator.node().id()
    }

    /// Returns storage of this [`Node`].
    pub fn storage(&self) -> &S {
        self.replication_coordinator.storage()
    }

    /// Returns [`Consensus`] of this [`Node`].
    pub fn consensus(&self) -> &C {
        self.replication_coordinator.consenus()
    }

    /// Returns Network impl of this [`Node`].
    pub fn network(&self) -> &N {
        self.replication_coordinator.network()
    }

    pub fn coordinator(&self) -> &replication::Coordinator<C, N, S, H> {
        &self.replication_coordinator
    }

    pub fn replica(&self) -> &replication::Replica<C, S, H> {
        self.replication_coordinator.replica()
    }

    pub fn migration_manager(&self) -> &migration::Manager<C, N, S> {
        &self.migration_manager
    }

    /// Returns representation of this [`Node`] in the [`Cluster`].
    pub fn cluster_node(&self) -> &C::Node {
        self.replica().node()
    }
}

impl<C, N, S, H> Node<C, N, S, H>
where
    C: Consensus + Clone,
    N: migration::Network<C::Node>,
    S: migration::StorageImport<N::DataStream> + Clone,
{
    /// Creates a new [`Node`] using the provided [`NodeOpts`] and dependencies.
    pub fn new(
        inner: C::Node,
        opts: NodeOpts,
        consensus: C,
        network: N,
        storage: S,
        hasher_builder: H,
    ) -> Self {
        let id = inner.id().clone();

        let throttling = replication::Throttling {
            request_timeout: opts.replication_request_timeout,
            request_limiter: Arc::new(Semaphore::new(opts.replication_concurrency_limit)),
            request_limiter_queue: Arc::new(Semaphore::new(opts.replication_request_queue)),
        };

        let replica = Replica::new(
            inner,
            consensus.clone(),
            storage.clone(),
            hasher_builder,
            throttling,
        );

        Node {
            replication_coordinator: replication::Coordinator::new(
                replica,
                network.clone(),
                opts.authorization,
            ),
            migration_manager: migration::Manager::new(id, consensus, network, storage),
            shutdown: Arc::new(Mutex::new(None)),
            warmup_delay: opts.warmup_delay,
        }
    }

    /// Runs this [`Node`].
    pub async fn run(self) -> Result<fsm::ShutdownReason, AlreadyRunning> {
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
            self.replication_coordinator.node().clone(),
            self.consensus().clone(),
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

impl<C: Consensus, N, S, H> Node<C, N, S, H> {
    /// Initiates shut down process of this [`Node`].
    pub fn shutdown(&self, reason: fsm::ShutdownReason) -> Result<(), ShutdownError> {
        let tx = self.shutdown.lock().unwrap().take().ok_or(ShutdownError)?;
        tx.send(reason).map_err(|_| ShutdownError)
    }
}

/// Error of [`Node::shutdown`].
#[derive(Debug, thiserror::Error)]
#[error("Shutdown is already in progress")]
pub struct ShutdownError;

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

impl<C, N, S> fsm::MigrationManager<C::Node> for migration::Manager<C, N, S>
where
    C: Consensus,
    N: migration::Network<C::Node>,
    S: migration::StorageImport<N::DataStream> + Clone,
{
    fn pull_keyranges(
        &self,
        plan: Arc<cluster::keyspace::MigrationPlan<C::Node>>,
    ) -> impl Future<Output = ()> + Send {
        migration::Manager::pull_ranges(self, plan)
    }
}
