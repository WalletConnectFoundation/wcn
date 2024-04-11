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

#[cfg(any(feature = "testing", test))]
pub mod test;

mod fsm;
pub mod migration;
pub mod replication;

/// [`Node`] configuration options.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

#[cfg(any(feature = "testing", test))]
pub type StubbedNode = Node<consensus::Stub, network::Stub, storage::Stub>;
