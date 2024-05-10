use {
    crate::{
        cluster::{self, Cluster},
        Consensus,
        Migrations,
        Network,
        PeerId,
        ShutdownReason,
    },
    backoff::{future::retry, ExponentialBackoff},
    cluster::NodeOperationMode as Mode,
    futures::{
        future::{self, BoxFuture},
        stream::{self, BoxStream, FuturesUnordered},
        Future,
        FutureExt,
        Stream,
        StreamExt,
    },
    std::{
        pin::{pin, Pin},
        sync::Arc,
        time::Duration,
    },
    tokio::sync::{oneshot, RwLock},
};

/// Finite-state machine of a running node.
struct Fsm<'a, C, N, M> {
    id: PeerId,

    consensus: C,
    network: N,
    cluster: Arc<RwLock<Cluster>>,
    migrations: M,

    streams: Pin<&'a mut Streams>,

    op_mode: Mode,
    op_mode_commit_in_progress: Option<Mode>,
    pending_shutdown: Option<ShutdownReason>,

    warmup_delay: Duration,
}

// TODO: Once TAIT (type alias impl trait) feature is stabilized, we shouldn't
// use dynamic dispatch here.
/// Combination of all [`Future`]s and [`Stream`]s spawned inside the
/// [`Fsm`] context.
type Streams = stream::Select<
    stream::FilterMap<
        FuturesUnordered<BoxFuture<'static, Option<Event>>>,
        future::Ready<Option<Event>>,
        fn(Option<Event>) -> future::Ready<Option<Event>>,
    >,
    stream::SelectAll<BoxStream<'static, Event>>,
>;

/// Event happened in a context of [`Fsm`] produced by a spawned [`Future`] or
/// [`Stream`].
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Event {
    /// Notification indicating that the node has received an update from the
    /// consensus module and applied it to its cluster view.
    ClusterViewUpdated(cluster::Diff),

    /// Notification indicating that the node has broadcast a heartbeat to the
    /// network.
    HeartbeatTicked,

    /// Once node receives a shutdown signal, it will trigger this event.
    ShutdownTriggered(ShutdownReason),

    /// Node completed warming up period after restart.
    WarmupCompleted,

    /// Notification indicating that the node has completed its booting
    /// migrations.
    BootingMigrationsCompleted,

    /// Notification indicating that the node has completed its leaving
    /// migrations.
    LeavingMigrationsCompleted,
}

/// Runs finite-state machine of a node.
pub(super) async fn run(
    node_id: PeerId,
    consensus: impl Consensus,
    network: impl Network,
    cluster: Arc<RwLock<Cluster>>,
    migrations: impl Migrations,
    warmup_delay: Duration,
    shutdown_rx: oneshot::Receiver<ShutdownReason>,
) -> ShutdownReason {
    let futures = FuturesUnordered::new();
    // So that `futures` never finishes.
    futures.push(futures::future::pending().boxed());

    let streams = pin!(stream::select(
        futures.filter_map(future::ready as fn(Option<Event>) -> future::Ready<Option<Event>>),
        stream::SelectAll::new()
    ));

    Fsm {
        id: node_id,
        consensus,
        network,
        cluster,
        migrations,
        streams,
        op_mode: Mode::Started,
        op_mode_commit_in_progress: None,
        pending_shutdown: None,
        warmup_delay,
    }
    .run(shutdown_rx)
    .await
}

type ControlFlow = std::ops::ControlFlow<ShutdownReason>;

impl<'a, C, N, M> Fsm<'a, C, N, M>
where
    C: Consensus,
    N: Network,
    M: Migrations,
{
    async fn run(&mut self, shutdown_rx: oneshot::Receiver<ShutdownReason>) -> ShutdownReason {
        self.listen_for_shutdown_signal(shutdown_rx);
        self.handle_cluster_view_updates();
        self.broadcast_heartbeat();

        loop {
            match self.streams.next().await.unwrap() {
                Event::ClusterViewUpdated(diff) => {
                    if let ControlFlow::Break(reason) = self.handle_cluster_view_update(diff) {
                        break reason;
                    }
                }
                Event::ShutdownTriggered(reason) => self.handle_shutdown(reason),
                Event::BootingMigrationsCompleted => {
                    self.commit_op_mode(cluster::NodeOperationMode::Normal);
                }
                Event::LeavingMigrationsCompleted => {
                    self.commit_op_mode(cluster::NodeOperationMode::Left);
                }
                Event::HeartbeatTicked => {}
                Event::WarmupCompleted => self.commit_op_mode(cluster::NodeOperationMode::Normal),
            }
        }
    }

    fn handle_cluster_view_update(&mut self, diff: cluster::Diff) -> ControlFlow {
        let Some(mode) = diff.node_op_mode(self.id) else {
            return ControlFlow::Continue(());
        };

        let prev_op_mode = self.op_mode;
        self.op_mode = mode;
        tracing::info!(id = ?self.id, from = ?prev_op_mode, to = ?mode, "Transitioned");
        self.resolve_op_mode_commit(mode);

        match mode {
            Mode::Started => self.commit_op_mode(Mode::Booting),
            Mode::Booting => self.run_booting_migrations(),
            Mode::Normal => {}
            Mode::Leaving => self.run_leaving_migrations(),

            // An edge case when a node decommisions itself and comes back online
            // quick enough.
            Mode::Left if prev_op_mode == Mode::Started => {
                self.commit_op_mode(Mode::Booting);
            }

            Mode::Left => return ControlFlow::Break(ShutdownReason::Decommission),

            // Recovering after restart.
            Mode::Restarting if prev_op_mode == Mode::Started => self.warmup(),
            // Restarting
            Mode::Restarting => return ControlFlow::Break(ShutdownReason::Restart),
        };

        ControlFlow::Continue(())
    }

    // To make a graceful shutdown, we only do it if there's no pending op mode
    // commits. If there is a commit we do a delayed shutdown, after the commit
    // is done.
    fn resolve_op_mode_commit(&mut self, mode: Mode) {
        if let Some(in_progress) = self.op_mode_commit_in_progress {
            // There's also a possibility that the real state jumps forward, so we do >=
            // here.
            if mode >= in_progress {
                self.op_mode_commit_in_progress = None;
                self.try_handle_pending_shutdown();
            }
        }
    }

    fn handle_shutdown(&mut self, reason: ShutdownReason) {
        if self.is_delayed_shutdown_required(reason) {
            self.pending_shutdown = Some(reason);
        } else {
            self.shutdown(reason)
        }
    }

    fn is_delayed_shutdown_required(&self, reason: ShutdownReason) -> bool {
        self.op_mode_commit_in_progress.is_some()
            || (reason == ShutdownReason::Restart && self.op_mode == Mode::Booting)
    }

    fn try_handle_pending_shutdown(&mut self) {
        match (self.op_mode, self.pending_shutdown.take()) {
            // If we transitioned to `Booting` delay shutdown again.
            (Mode::Booting, Some(ShutdownReason::Restart)) => {
                let _ = self.pending_shutdown.insert(ShutdownReason::Restart);
            }
            (_, Some(reason)) => self.shutdown(reason),
            _ => {}
        }
    }

    fn shutdown(&mut self, reason: ShutdownReason) {
        match reason {
            ShutdownReason::Decommission => self.decommission(),
            ShutdownReason::Restart => self.restart(),
        }
    }

    fn decommission(&mut self) {
        match self.op_mode {
            // We may go directly to `Left` state if this `Node` isn't operational yet.
            Mode::Started | Mode::Booting => self.commit_op_mode(Mode::Left),
            Mode::Normal => self.commit_op_mode(Mode::Leaving),
            mode @ (Mode::Leaving | Mode::Left | Mode::Restarting) => {
                tracing::warn!(?mode, "Trying to decomission while already shutting down");
            }
        }
    }

    fn restart(&mut self) {
        match self.op_mode {
            Mode::Started | Mode::Normal => self.commit_op_mode(Mode::Restarting),
            Mode::Booting => tracing::warn!("Trying to restart while Booting"),
            mode @ (Mode::Leaving | Mode::Left | Mode::Restarting) => {
                tracing::warn!(?mode, "Trying to restart while already shutting down")
            }
        }
    }

    fn warmup(&mut self) {
        let warmup_delay = self.warmup_delay;

        self.spawn_future(async move {
            tokio::time::sleep(warmup_delay).await;
            Some(Event::WarmupCompleted)
        });
    }

    fn commit_op_mode(&mut self, mode: cluster::NodeOperationMode)
    where
        C: Consensus,
    {
        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            ..Default::default()
        };

        let id = self.id;
        let consensus = self.consensus.clone();
        let fut = async move {
            let _ = retry(backoff, move || {
                let consensus = consensus.clone();

                async move {
                    consensus
                        .update_node_op_mode(id, mode)
                        .await
                        .map_err(|err| {
                            tracing::error!(?id, ?err, "Consensus::update_node_operation_mode")
                        })
                        .map_err(backoff::Error::transient)?
                        .map_err(|err| {
                            // These errors are mostly expected to happen, except some possible
                            // bugs, so let's use `info` here.
                            tracing::info!(?id, ?err, "ClusterView::update_node_operation_mode")
                        })
                        .map_err(backoff::Error::transient)
                }
            })
            .await;

            tracing::info!(?id, ?mode, "Operation mode change committed");
            None
        };

        self.op_mode_commit_in_progress = Some(mode);
        self.spawn_future(fut);
    }

    fn listen_for_shutdown_signal(&mut self, shutdown_rx: oneshot::Receiver<ShutdownReason>) {
        let id = self.id;
        let fut = shutdown_rx.map(move |res| match res {
            Ok(reason) => {
                tracing::info!(?id, "Shutdown signal received: {reason:?}");
                reason
            }
            Err(_) => {
                // Shouldn't normally happen
                tracing::warn!(?id, "All shutdown handles are dropped. Decommissioning...");
                ShutdownReason::Decommission
            }
        });

        self.spawn_future(fut.map(Event::ShutdownTriggered).map(Some));
    }

    fn handle_cluster_view_updates(&mut self) {
        let cluster = self.cluster.clone();
        let network = self.network.clone();
        let migrations = self.migrations.clone();
        let id = self.id;

        let st = self.consensus.changes().then(move |view| {
            let cluster = cluster.clone();
            let network = network.clone();
            let migrations = migrations.clone();

            async move {
                let diff = cluster.write().await.install_view_update(view);

                migrations.update_pending_ranges(()).await;

                for node in &diff.added {
                    network
                        .register_peer_address(node.peer_id, node.addr.clone())
                        .await;
                }

                for node in &diff.removed {
                    network
                        .unregister_peer_address(node.peer_id, node.addr.clone())
                        .await;
                }

                tracing::debug!(?id, ?diff, "updates stream: cluster view updated");
                Event::ClusterViewUpdated(diff)
            }
        });

        self.spawn_stream(st);
    }

    fn broadcast_heartbeat(&mut self) {
        let interval = tokio::time::interval(Duration::from_secs(1));

        let network = self.network.clone();
        let id = self.id;
        let st = tokio_stream::wrappers::IntervalStream::new(interval).then(move |_| {
            let network = network.clone();

            async move {
                // TODO: Handle heartbeat by the leader and do evictions
                if let Err(err) = network.broadcast_heartbeat().await {
                    tracing::warn!(?id, ?err, "failed to broadcast heartbeat");
                };

                Event::HeartbeatTicked
            }
        });

        self.spawn_stream(st);
    }

    fn run_booting_migrations(&mut self) {
        self.spawn_future(
            self.migrations
                .clone()
                .run_booting_migrations()
                .map(|_| Some(Event::BootingMigrationsCompleted)),
        );
    }

    fn run_leaving_migrations(&mut self) {
        self.spawn_future(
            self.migrations
                .clone()
                .run_leaving_migrations()
                .map(|_| Some(Event::LeavingMigrationsCompleted)),
        );
    }

    /// Spawns a [`Future`] into the context of this [`NodeFsm`].
    fn spawn_future(&mut self, fut: impl Future<Output = Option<Event>> + Send + 'static) {
        let futures = self.streams.as_mut().get_mut().get_mut().0;
        futures.get_mut().push(fut.boxed());
    }

    /// Spawns a [`Stream`] into the context of this [`NodeFsm`].
    fn spawn_stream(&mut self, st: impl Stream<Item = Event> + Send + 'static) {
        let streams = self.streams.as_mut().get_mut().get_mut().1;
        streams.push(st.boxed());
    }
}
