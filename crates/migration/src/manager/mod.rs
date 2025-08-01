use {
    anyhow::{anyhow, Context, Result},
    backoff::ExponentialBackoffBuilder,
    cluster::{
        keyspace,
        migration,
        smart_contract::{self, Write},
        Cluster,
        NodeOperator,
        PeerId,
    },
    derive_where::derive_where,
    futures::{stream, FutureExt as _, Stream, StreamExt, TryFutureExt},
    futures_concurrency::future::Race,
    std::{
        fmt,
        future::{self, Future},
        ops::RangeInclusive,
        pin::pin,
        sync::Arc,
        time::Duration,
    },
    storage_api::StorageApi,
    tap::{Pipe as _, TapFallible},
};

#[cfg(test)]
mod test;

/// Migration [`Manager`] config.
pub trait Config:
    cluster::Config<Node: AsRef<Self::OutboundReplicaConnection> + AsRef<PeerId>> + Clone
{
    /// Type of the outbound connections to WCN Replicas.
    type OutboundReplicaConnection: StorageApi;

    /// Type of the outbound connection to the WCN Database.
    type OutboundDatabaseConnection: StorageApi + Clone;

    /// Specifies how many shards migration [`Manager`] is allowed to transfer
    /// at the same time.
    fn concurrency(&self) -> usize;
}

/// WCN Migration Manager.
///
/// Manages data migration activities of a specific node operator within a WCN
/// Cluster.
#[derive(Clone)]
pub struct Manager<C: Config> {
    config: Arc<C>,
    cluster: Cluster<C>,
    database: C::OutboundDatabaseConnection,
}

impl<C: Config> Manager<C> {
    /// Creates a new migration [`Manager`].
    pub fn new(config: C, cluster: Cluster<C>, database: C::OutboundDatabaseConnection) -> Self {
        Self {
            config: Arc::new(config),
            cluster,
            database,
        }
    }

    /// Establishes a new [`InboundConnection`].
    ///
    /// Returns `None` if the peer is not authorized to use this migration
    /// [`Manager`].
    pub fn new_inbound_connection(&self, peer_id: PeerId) -> Option<InboundConnection<C>> {
        if !self.cluster.contains_node(&peer_id) {
            return None;
        }

        Some(InboundConnection {
            _peer_id: peer_id,
            manager: self.clone(),
        })
    }

    /// Runs a task managing all data migration related activities of a node
    /// operator.
    ///
    /// There should only be a single such task running at any point in time
    /// across all node operator nodes / infrastructure services.
    pub fn run(&self) -> impl Future<Output = ()> + Send
    where
        C: Config<KeyspaceShards = keyspace::Shards, SmartContract: smart_contract::Write>,
    {
        Task {
            manager: self.clone(),
            state: State::Idle,
        }
        .run()
    }
}

impl<C: Config> storage_api::Factory<PeerId> for Manager<C> {
    type StorageApi = InboundConnection<C>;

    fn new_storage_api(&self, peer_id: PeerId) -> storage_api::Result<Self::StorageApi> {
        self.new_inbound_connection(peer_id)
            .ok_or_else(storage_api::Error::unauthorized)
    }
}

/// Inbound connection to the local migration [`Manager`] from a remote peer.
#[derive_where(Clone)]
pub struct InboundConnection<C: Config> {
    _peer_id: PeerId,
    manager: Manager<C>,
}

impl<C: Config> StorageApi for InboundConnection<C> {
    async fn execute_ref(
        &self,
        _operation: &storage_api::Operation<'_>,
    ) -> storage_api::Result<storage_api::operation::Output> {
        // Migration manager do not handle regular storage operations.
        Err(storage_api::Error::unauthorized())
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> storage_api::Result<impl Stream<Item = storage_api::Result<storage_api::DataItem>> + Send>
    {
        let manager = &self.manager;

        if !manager.cluster.validate_keyspace_version(keyspace_version) {
            return Err(storage_api::Error::keyspace_version_mismatch());
        }

        manager.database.read_data(keyrange, keyspace_version).await
    }
}

struct Task<C: Config> {
    manager: Manager<C>,
    state: State,
}

impl<C> Task<C>
where
    C: Config<SmartContract: smart_contract::Write, KeyspaceShards = keyspace::Shards>,
{
    async fn run(self) {
        let mut watch = self.cluster().watch();
        let mut state_fut = pin!(self.state_future(State::Idle));

        loop {
            let cluster_update_fut = watch.cluster_updated().map(|_| Event::ClusterUpdate);

            let new_state = match (cluster_update_fut, &mut state_fut).race().await {
                Event::ClusterUpdate => self.sync_state(),
                Event::StateTransition(state) => Some(state),
            };

            if let Some(state) = new_state {
                tracing::info!(" -> {state:?}");
                state_fut.set(self.state_future(state));
            }
        }
    }

    fn sync_state(&self) -> Option<State> {
        let local_migration_id = self.state.migration_id();
        let cluster_migration_id = self.cluster().view().migration().map(|mig| mig.id());

        match (local_migration_id, cluster_migration_id) {
            // State is up to date.
            (Some(a), Some(b)) if a == b => None,
            (None, None) => None,

            // We are doing something, but cluster no longer has an ongoing migration.
            // Drop everything and go back to `Idle`.
            (Some(_), None) => Some(State::Idle),

            // We are either `Idle` or handling a wrong migration.
            // Kick-off a new migration process.
            (_, Some(id)) => Some(State::TransferringData(id)),
        }
    }

    async fn state_future(&self, state: State) -> Event {
        retry(|| async {
            match state {
                State::TransferringData(migration_id) => self
                    .transfer_data(migration_id)
                    .await
                    .context("Manager::transfer_data"),
                State::CompletingMigration(migration_id) => self
                    .complete_migration(migration_id)
                    .await
                    .context("Manager::complete_migration"),
                State::AwaitingMigrationCompletion(migration_id) => self
                    .await_migration_completion(migration_id)
                    .await
                    .context("Manager::await_migration_completion"),
                State::Idle => future::pending().await,
            }
            .tap_err(|err| tracing::error!(%err))
        })
        .map(Event::StateTransition)
        .await
    }

    async fn transfer_data(&self, migration_id: migration::Id) -> Result<State> {
        let cluster_view = self.cluster().view();
        let keyspace_version = cluster_view
            .migration()
            .ok_or_else(|| anyhow!("Missing migration"))?
            .keyspace()
            .version();

        let primary_shards = cluster_view.primary_keyspace_shards();
        let secondary_shards = cluster_view
            .secondary_keyspace_shards()
            .ok_or_else(|| anyhow!("Missing secondary keyspace shards"))?;

        let operator_id = self.cluster().smart_contract().signer().address();

        let replica_idx = |shard: &keyspace::Shard<&NodeOperator<_>>| {
            shard
                .replica_set()
                .iter()
                .position(|op| &op.id == operator_id)
        };

        primary_shards
            .zip(secondary_shards)
            .filter_map(|((shard_id, primary), (_, secondary))| {
                // If operator is not in the new replica set skip this shard.
                let secondary_idx = replica_idx(&secondary)?;

                // Also skip if operator is in the old replica set. Meaning that it's in both,
                // so we don't need to transfer any data.
                let None = replica_idx(&primary) else {
                    return None;
                };

                // Pull data only from the operator previously occupying the same replica index
                // to guarantee consistency.
                // TODO: We should have a way to override this behaviour for special
                // circumstances, for example if a node operator is completely dead and we are
                // forcefully removing it.
                let source = primary.replica_set()[secondary_idx];

                Some((shard_id, source))
            })
            .pipe(stream::iter)
            .for_each_concurrent(Some(self.manager.config.concurrency()), |(shard_id, source)| {
                retry(move || {
                    // TODO: This log may spam in case of an outage, figure out how to rate limit it.
                    self.transfer_shard(shard_id, source, keyspace_version)
                        .map_err(move |err| tracing::warn!(?err, %shard_id, source = %source.id, "Failed to transfer shard"))
                })
            })
            .await;

        Ok(State::CompletingMigration(migration_id))
    }

    async fn transfer_shard(
        &self,
        shard_id: keyspace::ShardId,
        source_operator: &NodeOperator<C::Node>,
        keyspace_version: u64,
    ) -> storage_api::Result<()> {
        use storage_api::ErrorKind;

        let keyrange = keyspace::keyrange(shard_id);

        let mut res = Err(storage_api::Error::internal());

        // Retry transport errors using different nodes.
        for node in source_operator.nodes_lb_iter() {
            let conn: &C::OutboundReplicaConnection = node.as_ref();
            res = conn.read_data(keyrange.clone(), keyspace_version).await;

            if matches!(&res, Err(err) if err.kind() == ErrorKind::Transport) {
                continue;
            };

            return self.database().write_data(res?).await;
        }

        res.map(drop)
    }

    async fn complete_migration(&self, migration_id: migration::Id) -> Result<State> {
        use cluster::CompleteMigrationError as Error;

        match self.cluster().complete_migration(migration_id).await {
            Ok(_) => {}
            // This error is theoretically possible under an extreme race condition when something
            // else completes the migration.
            Err(Error::OperatorNotPulling(_)) => {
                return Ok(State::AwaitingMigrationCompletion(migration_id))
            }
            Err(
                err @ (Error::UnknownOperator(_)
                | Error::NoMigration(_)
                | Error::WrongMigrationId(_)
                | Error::SmartContract(_)),
            ) => return Err(err.into()),
        };

        let operator_id = self.cluster().smart_contract().signer().address();

        // Wait until our commit to SC is observable.
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if !self
                    .cluster()
                    .using_view(|view| view.is_pulling(operator_id))
                {
                    return State::AwaitingMigrationCompletion(migration_id);
                }

                tokio::time::sleep(Duration::from_secs(10)).await
            }
        })
        .await
        .context("Waiting for is_pulling == false")
    }

    async fn await_migration_completion(&self, migration_id: migration::Id) -> Result<State> {
        let operator_id = self.cluster().smart_contract().signer().address();

        loop {
            // We have completed the migration already, but somehow the cluster shows again
            // that we are pulling.
            // It indicates that our commit to the SC didn't go through somehow, probably
            // because of a chain reorg.
            //
            // Go back to `CompletingMigration` state.
            if self
                .cluster()
                .using_view(|view| view.is_pulling(operator_id))
            {
                tracing::warn!("Going back to `State::CompletingMigration`");
                return Ok(State::CompletingMigration(migration_id));
            }

            // Just loop enlessly.
            //
            // The task will be canceled once we receive `ClusterUpdate` and there's no
            // longer an ongoing migration within the cluster.
            tokio::time::sleep(Duration::from_secs(10)).await
        }
    }

    fn cluster(&self) -> &Cluster<C> {
        &self.manager.cluster
    }

    fn database(&self) -> &C::OutboundDatabaseConnection {
        &self.manager.database
    }
}

enum Event {
    ClusterUpdate,
    StateTransition(State),
}

#[derive(Clone, Copy, Debug)]
enum State {
    TransferringData(migration::Id),
    CompletingMigration(migration::Id),
    AwaitingMigrationCompletion(migration::Id),
    Idle,
}

impl State {
    fn migration_id(self) -> Option<migration::Id> {
        Some(match self {
            State::TransferringData(migration_id) => migration_id,
            State::CompletingMigration(migration_id) => migration_id,
            State::AwaitingMigrationCompletion(migration_id) => migration_id,
            State::Idle => return None,
        })
    }
}

async fn retry<Ok, E, F, Fut>(f: F) -> Ok
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Ok, E>>,
    E: fmt::Debug,
{
    let backoff = ExponentialBackoffBuilder::new()
        .with_randomization_factor(0.5)
        .with_initial_interval(Duration::from_secs(1))
        .with_max_interval(Duration::from_secs(60))
        .with_max_elapsed_time(None)
        .build();

    let f = || async { f().await.map_err(backoff::Error::transient) };

    // NOTE(unwrap): we use `.with_max_elapsed_time(None)`, the error won't be
    // emitted.
    backoff::future::retry(backoff, f).await.unwrap()
}
