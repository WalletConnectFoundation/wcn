//! WalletConnect Network Cluster.

use {
    arc_swap::ArcSwap,
    futures::Stream,
    itertools::Itertools,
    smart_contract::evm,
    std::{collections::HashSet, sync::Arc},
    tokio::sync::watch,
};

pub mod smart_contract;
pub use smart_contract::SmartContract;

pub mod view;
pub use view::View;

pub mod settings;
pub use settings::Settings;

pub mod ownership;
pub use ownership::Ownership;

pub mod client;
pub use client::Client;

pub mod node;
pub use node::Node;

pub mod node_operator;
pub use node_operator::NodeOperator;

pub mod node_operators;
pub use node_operators::NodeOperators;

pub mod keyspace;
pub use keyspace::Keyspace;

pub mod migration;
pub use migration::Migration;

pub mod maintenance;
pub use maintenance::Maintenance;

mod task;
use task::Task;

/// Maximum number of [`NodeOperator`]s within a WCN [`Cluster`].
pub const MAX_OPERATORS: usize = keyspace::MAX_OPERATORS;

/// Minimum number of [`NodeOperator`]s within a WCN [`Cluster`].
pub const MIN_OPERATORS: u8 = keyspace::REPLICATION_FACTOR;

/// WCN cluster.
///
/// Thin wrapper around the underlying [`SmartContract`] implementation.
///
/// Performs preliminary invariant validation before calling the actual
/// [`SmartContract`] methods.
pub struct Cluster<SC = evm::SmartContract, Shards = ()> {
    inner: Arc<Inner<SC, Shards>>,
    _task_guard: Arc<task::Guard>,
}

struct Inner<SC, Shards> {
    smart_contract: SC,
    view: ArcSwap<View<Shards>>,
    watch: watch::Receiver<()>,
}

/// Version of a WCN [`Cluster`].
///
/// Should only monotonically increase. If you observe a jump backwards it means
/// that a chain reorg has occured on the underlying [`SmartContract`] chain.
///
/// For each version bump a corresponding [`Event`] should be emitted.
pub type Version = u128;

/// Events happening within a WCN [`Cluster`].
#[derive(Debug)]
pub enum Event<D = node_operator::Data> {
    /// [`Migration`] has started.
    MigrationStarted(migration::Started),

    /// [`NodeOperator`] has completed the data pull.
    MigrationDataPullCompleted(migration::DataPullCompleted),

    /// [`Migration`] has been completed.
    MigrationCompleted(migration::Completed),

    /// [`Migration`] has been aborted.
    MigrationAborted(migration::Aborted),

    /// [`Maintenance`] has started.
    MaintenanceStarted(maintenance::Started),

    /// [`Maintenance`] has been finished.
    MaintenanceFinished(maintenance::Finished),

    /// [`NodeOperator`] has been updated.
    NodeOperatorAdded(node_operator::Added<D>),

    /// [`NodeOperator`] has been updated.
    NodeOperatorUpdated(node_operator::Updated<D>),

    /// [`NodeOperator`] has been removed.
    NodeOperatorRemoved(node_operator::Removed),

    /// [`Settings`] have been updated.
    SettingsUpdated(settings::Updated),
}

/// [`Event`] with [`node_operator::SerializedData`].
pub type SerializedEvent = Event<node_operator::SerializedData>;

impl<SC, Shards> Cluster<SC, Shards>
where
    SC: smart_contract::Read,
    Shards: Clone + Send + Sync + 'static,
    Keyspace: keyspace::sealed::Calculate<Shards>,
{
    /// Deploys a new WCN [`Cluster`].
    pub async fn deploy(
        deployer: &impl smart_contract::Deployer<SC>,
        initial_settings: Settings,
        initial_operators: Vec<NodeOperator>,
    ) -> Result<Self, DeploymentError> {
        let operators: Vec<_> = initial_operators
            .into_iter()
            .map(|op| {
                let op = op.serialize()?;
                op.data.validate(&initial_settings)?;
                Ok::<_, DeploymentError>(op)
            })
            .try_collect()?;

        let operators = NodeOperators::new(operators.into_iter().map(Some))?;

        let contract = deployer.deploy(initial_settings, operators).await?;

        Self::new(contract).await.map_err(Into::into)
    }

    /// Connects to an existing WCN [`Cluster`].
    pub async fn connect(
        connector: &impl smart_contract::Connector<SC>,
        contract_address: smart_contract::Address,
    ) -> Result<Self, ConnectionError> {
        let contract = connector.connect(contract_address).await?;
        Self::new(contract).await.map_err(Into::into)
    }

    async fn new(contract: SC) -> Result<Self, view::FetchError> {
        let events = contract.events().await?;
        let view = View::fetch(&contract).await?.calculate_keyspace().await;

        let (tx, rx) = watch::channel(());
        let inner = Arc::new(Inner {
            smart_contract: contract,
            view: ArcSwap::new(Arc::new(view)),
            watch: rx,
        });

        let guard = Task {
            initial_events: Some(events),
            inner: inner.clone(),
            watch: tx,
        }
        .spawn();

        Ok(Self {
            inner,
            _task_guard: Arc::new(guard),
        })
    }
}

impl<SC, Shards> Cluster<SC, Shards> {
    /// Passes the current [`cluster::View`] into the provided closure.
    ///
    /// More efficient than calling [`Cluster::view`].
    pub fn using_view<T>(&self, f: impl FnOnce(&View<Shards>) -> T) -> T {
        f(&self.inner.view.load())
    }

    /// Returns the current [`cluster::View`].
    pub fn view(&self) -> Arc<View<Shards>> {
        self.inner.view.load_full()
    }

    /// Returns a [`Stream`] that emits an item each time a [`Cluster`] update
    /// occurs.
    ///
    /// Caller is expected to use [`Cluster::using_view`] or [`Cluster::view`]
    /// to see the updated state.
    pub fn updates(&self) -> impl Stream<Item = ()> + Send + 'static {
        // TODO: periodically check with the SC to prevent drift

        tokio_stream::wrappers::WatchStream::new(self.inner.watch.clone())
    }

    /// Returns reference to the underlying [`SmartContract`].
    pub fn smart_contract(&self) -> &SC {
        &self.inner.smart_contract
    }
}

impl<SC: SmartContract, Shards> Cluster<SC, Shards> {
    /// Builds a new [`Keyspace`] using the provided [`migration::Plan`] and
    /// calls [`SmartContract::start_migration`].
    pub async fn start_migration(&self, plan: migration::Plan) -> Result<(), StartMigrationError> {
        let new_keyspace = self.using_view(move |view| {
            view.ownership().require_owner(&self.inner.smart_contract)?;
            view.require_no_migration()?;
            view.require_no_maintenance()?;

            let operators = view.node_operators();
            let keyspace = view.keyspace();

            let mut new_operators: HashSet<_> = view.node_operators().occupied_indexes().collect();

            for id in plan.remove {
                let idx = operators.require_idx(&id)?;

                if !keyspace.contains_operator(idx) {
                    return Err(StartMigrationError::NotInKeyspace(id));
                }

                new_operators.remove(&idx);
            }

            for id in plan.add {
                let idx = operators.require_idx(&id)?;

                if keyspace.contains_operator(idx) {
                    return Err(StartMigrationError::AlreadyInKeyspace(id));
                }

                new_operators.insert(idx);
            }

            let new_keyspace = Keyspace::new(
                new_operators,
                plan.replication_strategy,
                keyspace.version() + 1,
            )?;

            keyspace.require_diff(&new_keyspace)?;

            Ok(new_keyspace)
        })?;

        self.inner
            .smart_contract
            .start_migration(new_keyspace)
            .await?;

        Ok(())
    }

    /// Calls [`SmartContract::complete_migration`].
    pub async fn complete_migration(
        &self,
        id: migration::Id,
    ) -> Result<(), CompleteMigrationError> {
        self.using_view(|view| {
            let operator_idx = view
                .node_operators()
                .require_idx(self.inner.smart_contract.signer().address())?;

            view.require_migration()?
                .require_id(id)?
                .require_pulling(operator_idx)?;

            Ok::<_, CompleteMigrationError>(())
        })?;

        self.inner.smart_contract.complete_migration(id).await?;

        Ok(())
    }

    /// Calls [`SmartContract::abort_migration`].
    pub async fn abort_migration(&self, id: migration::Id) -> Result<(), AbortMigrationError> {
        self.using_view(|view| {
            view.ownership().require_owner(&self.inner.smart_contract)?;
            view.require_migration()?.require_id(id)?;

            Ok::<_, AbortMigrationError>(())
        })?;

        self.inner
            .smart_contract
            .abort_migration(id)
            .await
            .map_err(Into::into)
    }

    /// Calls [`SmartContract::start_maintenance`].
    pub async fn start_maintenance(&self) -> Result<(), StartMaintenanceError> {
        let signer = self.inner.smart_contract.signer().address();

        self.using_view(move |view| {
            if !(view.node_operators().contains(signer) || view.ownership().is_owner(signer)) {
                return Err(StartMaintenanceError::Unauthorized);
            }

            view.require_no_migration()?;
            view.require_no_maintenance()?;

            Ok::<_, StartMaintenanceError>(())
        })?;

        self.inner.smart_contract.start_maintenance().await?;

        Ok(())
    }

    /// Calls [`SmartContract::finish_maintenance`].
    pub async fn finish_maintenance(&self) -> Result<(), FinishMaintenanceError> {
        let signer = self.inner.smart_contract.signer().address();

        self.using_view(|view| {
            let maintenance = view.require_maintenance()?;

            if !(signer == maintenance.slot() || view.ownership().is_owner(signer)) {
                return Err(FinishMaintenanceError::Unauthorized);
            }

            Ok(())
        })?;

        self.inner.smart_contract.finish_maintenance().await?;

        Ok(())
    }

    /// Calls [`SmartContract::add_node_operator`].
    pub async fn add_node_operator(
        &self,
        operator: NodeOperator,
    ) -> Result<(), AddNodeOperatorError> {
        let operator = self.using_view(|view| {
            view.ownership().require_owner(&self.inner.smart_contract)?;
            view.node_operators().require_not_exists(&operator.id)?;
            view.node_operators().require_not_full()?;

            let operator = operator.serialize()?;
            operator.data.validate(view.settings())?;

            Ok::<_, AddNodeOperatorError>(operator)
        })?;

        self.inner
            .smart_contract
            .add_node_operator(operator)
            .await?;

        Ok(())
    }

    /// Calls [`SmartContract::update_node_operator`].
    pub async fn update_node_operator(
        &self,
        operator: NodeOperator,
    ) -> Result<(), UpdateNodeOperatorError> {
        let signer = self.inner.smart_contract.signer().address();

        let operator = self.using_view(|view| {
            if !(signer == &operator.id || view.ownership().is_owner(signer)) {
                return Err(UpdateNodeOperatorError::Unauthorized);
            }

            let _idx = view.node_operators().require_idx(&operator.id)?;

            let operator = operator.serialize()?;
            operator.data.validate(view.settings())?;

            Ok::<_, UpdateNodeOperatorError>(operator)
        })?;

        self.inner
            .smart_contract
            .update_node_operator(operator)
            .await?;

        Ok(())
    }

    /// Calls [`SmartContract::remove_node_operator`].
    pub async fn remove_node_operator(
        &self,
        id: node_operator::Id,
    ) -> Result<(), RemoveNodeOperatorError> {
        self.using_view(|view| {
            let idx = view.node_operators().require_idx(&id)?;

            if view.keyspace().contains_operator(idx) {
                return Err(RemoveNodeOperatorError::InKeyspace);
            }

            if let Some(keyspace) = view.migration().map(Migration::keyspace) {
                if keyspace.contains_operator(idx) {
                    return Err(RemoveNodeOperatorError::InKeyspace);
                }
            }

            Ok::<_, RemoveNodeOperatorError>(())
        })?;

        self.inner.smart_contract.remove_node_operator(id).await?;

        Ok(())
    }

    /// Calls [`SmartContract::update_settings`].
    pub async fn update_settings(&self, new_settings: Settings) -> Result<(), UpdateSettingsError> {
        self.using_view(move |view| view.ownership().require_owner(&self.inner.smart_contract))?;

        self.inner
            .smart_contract
            .update_settings(new_settings)
            .await?;

        Ok(())
    }
}

/// [`Cluster::deploy`] error.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentError {
    #[error(transparent)]
    NodeOperatorsCreation(#[from] node_operators::CreationError),

    #[error(transparent)]
    DataSerialization(#[from] node_operator::DataSerializationError),

    #[error(transparent)]
    DataDeserialization(#[from] node_operator::DataDeserializationError),

    #[error(transparent)]
    DataTooLarge(#[from] node_operator::DataTooLargeError),

    #[error(transparent)]
    FetchView(#[from] view::FetchError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::DeploymentError),
}

/// [`Cluster::connect`] error.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    SmartContract(#[from] smart_contract::ConnectionError),

    #[error(transparent)]
    FetchView(#[from] view::FetchError),
}

/// [`Cluster::start_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum StartMigrationError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error(transparent)]
    MigrationInProgress(#[from] migration::InProgressError),

    #[error(transparent)]
    MaintenanceInProgress(#[from] maintenance::InProgressError),

    #[error(transparent)]
    UnknownOperator(#[from] node_operator::NotFoundError),

    #[error("NodeOperator(id: {_0}) is not in the Keyspace")]
    NotInKeyspace(node_operator::Id),

    #[error("NodeOperator(id: {_0}) is already in the Keyspace")]
    AlreadyInKeyspace(node_operator::Id),

    #[error(transparent)]
    KeyspaceCreation(#[from] keyspace::CreationError),

    #[error(transparent)]
    SameKeyspace(#[from] keyspace::SameKeyspaceError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::complete_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum CompleteMigrationError {
    #[error(transparent)]
    UnknownOperator(#[from] node_operator::NotFoundError),

    #[error(transparent)]
    NoMigration(#[from] migration::NotFoundError),

    #[error(transparent)]
    WrongMigrationId(#[from] migration::WrongIdError),

    #[error(transparent)]
    OperatorNotPulling(#[from] migration::OperatorNotPullingError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::abort_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum AbortMigrationError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error(transparent)]
    NoMigration(#[from] migration::NotFoundError),

    #[error(transparent)]
    WrongMigrationId(#[from] migration::WrongIdError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::start_maintenance`] error.
#[derive(Debug, thiserror::Error)]
pub enum StartMaintenanceError {
    #[error("Signer should either be a node operator or the owner")]
    Unauthorized,

    #[error(transparent)]
    MigrationInProgress(#[from] migration::InProgressError),

    #[error(transparent)]
    MaintenanceInProgress(#[from] maintenance::InProgressError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::finish_maintenance`] error.
#[derive(Debug, thiserror::Error)]
pub enum FinishMaintenanceError {
    #[error(transparent)]
    NoMaintenance(#[from] maintenance::NotFoundError),

    #[error("Signer should either be a node operator that started the maintenance or the owner")]
    Unauthorized,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::add_node_operator`] error.
#[derive(Debug, thiserror::Error)]
pub enum AddNodeOperatorError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error(transparent)]
    AlreadyExists(#[from] node_operator::AlreadyExistsError),

    #[error(transparent)]
    NoAvailableSlots(#[from] node_operators::NoAvailableSlotsError),

    #[error(transparent)]
    DataSerialization(#[from] node_operator::DataSerializationError),

    #[error(transparent)]
    DataTooLarge(#[from] node_operator::DataTooLargeError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::update_node_operator`] error.
#[derive(Debug, thiserror::Error)]
pub enum UpdateNodeOperatorError {
    #[error("Signer should either be the NodeOperator being updated or the owner")]
    Unauthorized,

    #[error(transparent)]
    NotFoundError(#[from] node_operator::NotFoundError),

    #[error(transparent)]
    DataSerialization(#[from] node_operator::DataSerializationError),

    #[error(transparent)]
    DataTooLarge(#[from] node_operator::DataTooLargeError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::remove_node_operator`] error.
#[derive(Debug, thiserror::Error)]
pub enum RemoveNodeOperatorError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error(transparent)]
    NotFoundError(#[from] node_operator::NotFoundError),

    #[error("Node operator is still within a Keyspace")]
    InKeyspace,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::update_settings`] error.
#[derive(Debug, thiserror::Error)]
pub enum UpdateSettingsError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

/// [`Cluster::transfer_ownership`] error.
#[derive(Debug, thiserror::Error)]
pub enum TransferOwnershipError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::WriteError),
}

impl Event {
    fn cluster_version(&self) -> Version {
        match self {
            Event::MigrationStarted(evt) => evt.cluster_version,
            Event::MigrationDataPullCompleted(evt) => evt.cluster_version,
            Event::MigrationCompleted(evt) => evt.cluster_version,
            Event::MigrationAborted(evt) => evt.cluster_version,
            Event::MaintenanceStarted(evt) => evt.cluster_version,
            Event::MaintenanceFinished(evt) => evt.cluster_version,
            Event::NodeOperatorAdded(evt) => evt.cluster_version,
            Event::NodeOperatorUpdated(evt) => evt.cluster_version,
            Event::NodeOperatorRemoved(evt) => evt.cluster_version,
            Event::SettingsUpdated(evt) => evt.cluster_version,
        }
    }
}

impl SerializedEvent {
    fn deserialize(self) -> Result<Event, node_operator::DataDeserializationError> {
        Ok(match self {
            Event::MigrationStarted(evt) => Event::MigrationStarted(evt),
            Event::MigrationDataPullCompleted(evt) => Event::MigrationDataPullCompleted(evt),
            Event::MigrationCompleted(evt) => Event::MigrationCompleted(evt),
            Event::MigrationAborted(evt) => Event::MigrationAborted(evt),
            Event::MaintenanceStarted(evt) => Event::MaintenanceStarted(evt),
            Event::MaintenanceFinished(evt) => Event::MaintenanceFinished(evt),
            Event::NodeOperatorAdded(evt) => Event::NodeOperatorAdded(evt.deserialize()?),
            Event::NodeOperatorUpdated(evt) => Event::NodeOperatorUpdated(evt.deserialize()?),
            Event::NodeOperatorRemoved(evt) => Event::NodeOperatorRemoved(evt),
            Event::SettingsUpdated(evt) => Event::SettingsUpdated(evt),
        })
    }
}
