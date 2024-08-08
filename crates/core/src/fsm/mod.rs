use {
    crate::cluster::{
        self,
        keyspace::{self, MigrationPlan},
        node,
        Consensus,
        Node,
    },
    backoff::{future::retry, ExponentialBackoff},
    derive_more::From,
    futures::{
        future::{FusedFuture, OptionFuture},
        stream,
        FutureExt,
        TryFutureExt,
    },
    std::{error::Error as StdError, future::Future, pin::pin, sync::Arc, time::Duration},
    tokio::sync::oneshot,
    tokio_stream::StreamExt as _,
};

pub trait MigrationManager<N: Node>: Send + Sync + 'static {
    fn pull_keyranges(&self, plan: Arc<MigrationPlan<N>>) -> impl Future<Output = ()> + Send;
}

/// Finite-state machine of a running node.
struct Fsm<C: Consensus, M> {
    node: C::Node,

    consensus: C,
    migration_manager: M,

    warmup_delay: Duration,
}

/// Reason for shutting down a node FSM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownReason {
    /// The node is being decommissioned and isn't expected to be back online.
    ///
    /// It is required to perform data migrations in order to decommission a
    /// node.
    Decommission,

    /// The node is being restarted and is expected to be back online ASAP.
    ///
    /// Data migrations will not be performed.
    Restart,
}

/// Event happened in a context of [`Fsm`].
#[derive(Debug)]
enum Event {
    /// Notification indicating that the [`Cluster`] has been updated.
    ClusterUpdated,

    /// Once node receives a shutdown signal, it will trigger this event.
    ShutdownTriggered(ShutdownReason),
}

/// Runs finite-state machine of a node.
pub(super) fn run<C: Consensus>(
    node: C::Node,
    consensus: C,
    migration_manager: impl MigrationManager<C::Node>,
    warmup_delay: Duration,
    shutdown_rx: oneshot::Receiver<ShutdownReason>,
) -> impl Future<Output = ShutdownReason> + Send {
    Fsm {
        node,
        consensus,
        migration_manager,
        warmup_delay,
    }
    .run(shutdown_rx)
}

#[derive(Debug)]
enum Task<N: Node> {
    AddNode,

    CompletePulling(Arc<keyspace::MigrationPlan<N>>),

    ShutdownNode,
    StartupNode,

    DecommissionNode,
}

impl<C, M> Fsm<C, M>
where
    C: Consensus,
    M: MigrationManager<C::Node>,
{
    async fn run(self, shutdown_rx: oneshot::Receiver<ShutdownReason>) -> ShutdownReason {
        let cluster_view = self.consensus.cluster_view();
        let cluster_updates = cluster_view.updates().map(|_| Event::ClusterUpdated);

        let shutdown_signal = shutdown_rx
            .into_stream()
            .map(|reason| Event::ShutdownTriggered(reason.unwrap_or(ShutdownReason::Restart)));
        let mut pending_shutdown = None;

        let events = cluster_updates
            .merge(shutdown_signal)
            .merge(stream::pending());
        let mut events = pin!(events);

        let mut state = None;

        let mut fut = pin!(OptionFuture::from(None));

        loop {
            if let Some(shutdown_reason) = cluster_view.peek(|c| {
                let cluster_state = c.node_state(self.node.id());
                if state != cluster_state {
                    tracing::info!(node_id = ?self.node.id(), prev_state = ?state, new_state = ?cluster_state);
                }

                // Here we're comparing the node's local understandig of it's state with it's
                // actual state within the `Cluster`.
                match (&state, &cluster_state) {
                    // If node just started and it's not in the `Cluster` it needs to join the
                    // cluster.
                    (None, None) => fut.set(Some(self.execute(Task::AddNode)).into()),

                    // If both states are the same then the correct task is already running
                    // (or completed).
                    (a, b) if a == b => {}

                    // If the node was removed from the cluster it means that it was decommissioned.
                    (Some(_), None) => return Some(ShutdownReason::Decommission),

                    // If node just started and we see `node::State::Restarting` in the `Cluster`
                    // state it means that the node is coming back from a restart.
                    (None, Some(node::State::Restarting)) => {
                        fut.set(Some(self.execute(Task::StartupNode)).into())
                    }

                    // If the node has transitioned to `Restarting` from some other state, then we
                    // need to do the restart itself.
                    (Some(_), Some(node::State::Restarting)) => {
                        return Some(ShutdownReason::Restart)
                    }

                    // `Cluster` state is the ultimate source of thuth, no matter the current local
                    // `node::State`, so for these states we always follow what `Cluster` says.
                    (_, Some(node::State::Pulling(plan))) => {
                        fut.set(Some(self.execute(Task::CompletePulling(plan.clone()))).into());
                    }
                    (_, Some(node::State::Normal)) => match pending_shutdown {
                        None => fut.set(None.into()),
                        Some(ShutdownReason::Restart) => {
                            fut.set(Some(self.execute(Task::ShutdownNode)).into())
                        }
                        Some(ShutdownReason::Decommission) => {
                            fut.set(Some(self.execute(Task::DecommissionNode)).into())
                        }
                    },
                    (_, Some(node::State::Decommissioning)) => fut.set(None.into()),
                };

                state = cluster_state;
                None
            }) {
                return shutdown_reason;
            }

            let evt = tokio::select! {
                _ = &mut fut, if !fut.is_terminated() => continue,
                Some(evt) = events.next() => evt,
            };

            let shutdown_reason = match evt {
                // next iteration of the loop will check the `Cluster`
                Event::ClusterUpdated => continue,
                Event::ShutdownTriggered(reason) => reason,
            };

            pending_shutdown = Some(shutdown_reason);

            match (shutdown_reason, &state) {
                // If the node is joining the cluster or is pulling data then shutdown should be
                // delayed.
                (reason, None) => {
                    tracing::warn!(
                        ?reason,
                        "Node is joining the cluster, delaying the shutdown"
                    );
                }
                (reason, Some(node::State::Pulling { .. })) => {
                    tracing::warn!(?reason, "Node is `Pulling`, delaying the shutdown");
                }

                // We just went back from `Restarting`, the `StartupNode` task is running and we get
                // a new `Restart` signal, so we need to delay it until the task is completed.
                (reason, Some(node::State::Restarting)) => {
                    tracing::warn!(?reason, "Node is starting up, delaying the shutdown");
                }

                // Weird, but can theoretically happen if something else updates our state in the
                // `Cluster`, nothing to do.
                (reason, Some(node::State::Decommissioning)) => {
                    tracing::warn!(?reason, "Node is decommissioning, ignoring the shutdown");
                }

                // Regular decommissioning.
                (ShutdownReason::Decommission, Some(node::State::Normal)) => {
                    fut.set(Some(self.execute(Task::DecommissionNode)).into());
                }

                // Regular restart.
                (ShutdownReason::Restart, Some(node::State::Normal)) => {
                    fut.set(Some(self.execute(Task::ShutdownNode)).into());
                }
            }
        }
    }

    fn execute(&self, task: Task<C::Node>) -> impl FusedFuture + Send + '_ {
        async move {
            let backoff = ExponentialBackoff {
                initial_interval: Duration::from_secs(1),
                max_interval: Duration::from_secs(10),
                max_elapsed_time: None,
                ..Default::default()
            };

            let _ = retry(backoff, || {
                async {
                    match &task {
                        Task::AddNode => self.add_node().await,
                        Task::CompletePulling(plan) => {
                            self.complete_pulling(Arc::clone(plan)).await
                        }
                        Task::ShutdownNode => self.shutdown_node().await,
                        Task::StartupNode => self.startup_node().await,
                        Task::DecommissionNode => self.decommission_node().await,
                    }
                }
                .map_err(Error::into_backoff)
            })
            .await;
        }
        .fuse()
    }

    async fn add_node(&self) -> Result<(), Error> {
        self.consensus
            .add_node(&self.node)
            .await
            .map_err(Error::from_consensus)
    }

    async fn complete_pulling(
        &self,
        plan: Arc<keyspace::MigrationPlan<C::Node>>,
    ) -> Result<(), Error> {
        let keyspace_version = plan.keyspace_version();

        self.migration_manager.pull_keyranges(plan).await;

        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            ..Default::default()
        };

        let _ = retry(backoff, || {
            self.consensus
                .complete_pull(self.node.id(), keyspace_version)
                .map_err(Error::from_consensus)
                .map_err(Error::into_backoff)
        })
        .await;

        Ok(())
    }

    async fn shutdown_node(&self) -> Result<(), Error> {
        self.consensus
            .shutdown_node(self.node.id())
            .await
            .map_err(Error::from_consensus)
    }

    async fn startup_node(&self) -> Result<(), Error> {
        tokio::time::sleep(self.warmup_delay).await;

        self.consensus
            .startup_node(&self.node)
            .await
            .map_err(Error::from_consensus)
    }

    async fn decommission_node(&self) -> Result<(), Error> {
        self.consensus
            .decommission_node(&self.node.id())
            .await
            .map_err(Error::from_consensus)
    }
}

type Result<T, E> = std::result::Result<T, E>;

pub struct Error(backoff::Error<anyhow::Error>);

impl Error {
    fn permanent(err: impl StdError + Send + Sync + 'static) -> Self {
        Self(backoff::Error::permanent(anyhow::Error::new(err)))
    }

    fn transient(err: impl StdError + Send + Sync + 'static) -> Self {
        Self(backoff::Error::transient(anyhow::Error::new(err)))
    }

    fn from_consensus<E>(err: E) -> Self
    where
        E: TryInto<cluster::Error, Error = E> + StdError + Send + Sync + 'static,
    {
        use cluster::Error as CE;

        match err.try_into() {
            Ok(
                err @ (CE::NodeAlreadyExists
                | CE::NodeAlreadyStarted
                | CE::UnknownNode
                | CE::NoMigration
                | CE::KeyspaceVersionMismatch),
            ) => {
                tracing::warn!(?err);
                Self::permanent(err)
            }
            Ok(err @ (CE::Bug(_) | CE::NotBootstrapped)) => {
                tracing::error!(?err);
                Self::permanent(err)
            }
            Ok(err @ (CE::MigrationInProgress | CE::AnotherNodeRestarting)) => {
                tracing::warn!(?err);
                Self::transient(err)
            }
            Ok(err @ (CE::TooManyNodes | CE::TooFewNodes | CE::InvalidNode(_))) => {
                tracing::error!(?err);
                Self::transient(err)
            }
            Err(err) => {
                tracing::error!(?err);
                Self::transient(err)
            }
        }
    }

    fn into_backoff(self) -> backoff::Error<anyhow::Error> {
        self.0
    }
}

impl<E: StdError + Send + Sync + 'static> From<E> for Error {
    fn from(err: E) -> Self {
        Self::transient(err)
    }
}
