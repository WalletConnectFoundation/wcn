use {
    arc_swap::ArcSwap,
    itertools::Itertools,
    smart_contract::evm,
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        sync::Arc,
    },
};

pub mod smart_contract;
pub use smart_contract::SmartContract;

pub mod settings;
pub use settings::Settings;

pub mod ownership;
pub use ownership::Ownership;

pub mod client;
pub use client::{Client, ClientRef};

pub mod node;
pub use node::{Node, NodeRef};

pub mod node_operator;
pub use node_operator::{NodeOperator, SerializedNodeOperator, VersionedNodeOperator};

pub mod keyspace;
pub use keyspace::Keyspace;

pub mod migration;
pub use migration::Migration;

pub mod maintenance;
pub use maintenance::Maintenance;

/// Maximum number of [`NodeOperator`]s within a WCN [`Cluster`].
pub const MAX_OPERATORS: usize = 256;

/// WCN cluster.
///
/// Thin wrapper around the underlying [`SmartContract`] implementation.
///
/// Performs preliminary invariant validation before calling the actual
/// [`SmartContract`] methods.
pub struct Cluster<SC = evm::SmartContract> {
    smart_contract: SC,
    view: ArcSwap<Arc<View>>,
}

/// Version of a WCN [`Cluster`].
///
/// Should only monotonically increase. If you observe a jump backwards it means
/// that a chain reorg has occured on the underlying [`SmartContract`] chain.
///
/// For each version bump a corresponding [`Event`] should be emitted.
pub type Version = u128;

/// Events happening within a WCN [`Cluster`].
pub enum Event {
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
    NodeOperatorAdded(node_operator::Added),

    /// [`NodeOperator`] has been updated.
    NodeOperatorUpdated(node_operator::Updated),

    /// [`NodeOperator`] has been removed.
    NodeOperatorRemoved(node_operator::Removed),

    /// [`Settings`] have been updated.
    SettingsUpdated(settings::Updated),

    /// [`Ownership`] has been transferred.
    OwnershipTransferred(ownership::Transferred),
}

impl<SC: SmartContract> Cluster<SC> {
    /// Deploys a new WCN [`Cluster`].
    pub async fn deploy(
        signer: smart_contract::Signer,
        rpc_url: smart_contract::RpcUrl,
        initial_settings: Settings,
        initial_operators: Vec<NodeOperator>,
    ) -> Result<Self, DeploymentError> {
        if initial_operators
            .iter()
            .map(VersionedNodeOperator::id)
            .dedup()
            .count()
            != initial_operators.len()
        {
            return Err(DeploymentError::OperatorIdDuplicate);
        }

        if initial_operators.len() > MAX_OPERATORS {
            return Err(DeploymentError::TooManyOperators);
        }

        let contract = SC::deploy(signer, rpc_url, initial_settings, initial_operators).await?;

        let view = contract.cluster_view().await?;

        Ok(Self {
            smart_contract: contract,
            view: ArcSwap::new(Arc::new(Arc::new(view))),
        })
    }

    /// Prepares a new [`migration::Plan`] and calls
    /// [`SmartContract::start_migration`] using the prepared plan.
    pub async fn start_migration(
        &self,
        remove: Vec<node_operator::Id>,
        add: Vec<NodeOperator>,
    ) -> Result<(), StartMigrationError> {
        let plan = self.using_view(move |view| {
            view.ownership.validate_signer(&self.smart_contract)?;

            if view.migration().is_some() {
                return Err(StartMigrationError::MigrationInProgress);
            }

            if view.maintenance().is_some() {
                return Err(StartMigrationError::MaintenanceInProgress);
            }

            let add = add
                .into_iter()
                .map(|operator| {
                    let operator = operator.serialize()?;

                    operator
                        .data()
                        .validate_size(view.settings.max_node_operator_data_bytes)?;

                    Ok::<_, StartMigrationError>(operator)
                })
                .try_collect()?;

            migration::Plan::new(view, remove, add).map_err(Into::into)
        })?;

        self.smart_contract.start_migration(plan).await?;

        Ok(())
    }

    /// Calls [`SmartContract::complete_migration`].
    pub async fn complete_migration(
        &self,
        id: migration::Id,
    ) -> Result<(), CompleteMigrationError> {
        let signer = self.smart_contract.signer();

        let operator_idx = self.using_view(|view| {
            let migration = view
                .migration()
                .ok_or(CompleteMigrationError::NoMigration)?;

            if migration.id() != id {
                return Err(CompleteMigrationError::WrongMigrationId);
            }

            let operator_idx = migration
                .keyspace()
                .operators()
                .get_idx(&signer)
                .ok_or(CompleteMigrationError::NotOperator)?;

            if !migration.is_pulling(&signer) {
                return Err(CompleteMigrationError::NotPulling);
            }

            Ok(operator_idx)
        })?;

        self.smart_contract
            .complete_migration(id, operator_idx)
            .await
            .map_err(Into::into)
    }

    /// Calls [`SmartContract::abort_migration`].
    pub async fn abort_migration(&self, id: migration::Id) -> Result<(), AbortMigrationError> {
        self.using_view(|view| {
            view.ownership.validate_signer(&self.smart_contract)?;

            let migration = view.migration().ok_or(AbortMigrationError::NoMigration)?;

            if migration.id() != id {
                return Err(AbortMigrationError::WrongMigrationId);
            }

            Ok(())
        })?;

        self.smart_contract
            .abort_migration(id)
            .await
            .map_err(Into::into)
    }

    /// Calls [`SmartContract::start_maintenance`].
    pub async fn start_maintenance(&self) -> Result<(), StartMaintenanceError> {
        let operator_idx = self.using_view(move |view| {
            if view.migration().is_some() {
                return Err(StartMaintenanceError::MigrationInProgress);
            }

            if view.maintenance().is_some() {
                return Err(StartMaintenanceError::MaintenanceInProgress);
            }

            view.keyspace()
                .operators()
                .get_idx(&self.smart_contract.signer())
                .ok_or(StartMaintenanceError::NotOperator)
        })?;

        self.smart_contract.start_maintenance(operator_idx).await?;

        Ok(())
    }

    /// Calls [`SmartContract::finish_maintenance`].
    pub async fn finish_maintenance(&self) -> Result<(), CompleteMaintenanceError> {
        self.using_view(|view| {
            let maintenance = view
                .maintenance()
                .ok_or(CompleteMaintenanceError::NoMaintenance)?;

            if maintenance.operator() != &self.smart_contract.signer() {
                return Err(CompleteMaintenanceError::WrongOperator);
            }

            Ok(())
        })?;

        self.smart_contract.finish_maintenance().await?;

        Ok(())
    }

    /// Calls [`SmartContract::update_node_operator`].
    pub async fn update_node_operator(
        &self,
        operator: NodeOperator,
    ) -> Result<(), UpdateNodeOperatorError> {
        let (idx, data) = self.using_view(|view| {
            let idx = view
                .operator_idx(operator.id())
                .ok_or(UpdateNodeOperatorError::UnknownOperator)?;

            let data = data.serialize()?;

            data.validate_size(view.settings.max_node_operator_data_bytes)?;
            Ok::<_, UpdateNodeOperatorError>((idx, data))
        })?;

        self.smart_contract
            .update_node_operator(id, idx, data)
            .await
            .map_err(Into::into)
    }

    fn using_view<T>(&self, f: impl FnOnce(&View) -> T) -> T {
        f(&self.view.load())
    }
}

/// [`Cluster::deploy`] error.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentError {
    #[error("Too many initial operators provided, should be <= {MAX_OPERATORS}")]
    TooManyOperators,

    #[error("Initial operator list contains duplicates")]
    OperatorIdDuplicate,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::start_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum StartMigrationError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error("Another migration in progress")]
    MigrationInProgress,

    #[error("Maintenance in progress")]
    MaintenanceInProgress,

    #[error("Data serialization: {0}")]
    DataSerialization(#[from] node_operator::DataSerializationError),

    #[error(transparent)]
    NodeOperatorDataTooLarge(#[from] node_operator::DataTooLargeError),

    #[error("Plan: {0}")]
    Plan(#[from] migration::PlanError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::complete_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum CompleteMigrationError {
    #[error("No migration is currently in progress")]
    NoMigration,

    #[error("Provided migration id doesn't match the actual one")]
    WrongMigrationId,

    #[error("Signer is not a node operator")]
    NotOperator,

    #[error("Signer has already completed the data pull")]
    NotPulling,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::abort_migration`] error.
#[derive(Debug, thiserror::Error)]
pub enum AbortMigrationError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error("No migration is currently in progress")]
    NoMigration,

    #[error("Provided migration id doesn't match the actual one")]
    WrongMigrationId,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::start_maintenance`] error.
#[derive(Debug, thiserror::Error)]
pub enum StartMaintenanceError {
    #[error("Migration in progress")]
    MigrationInProgress,

    #[error("Another maintenance in progress")]
    MaintenanceInProgress,

    #[error("Signer is not a node operator")]
    NotOperator,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::complete_maintenance`] error.
#[derive(Debug, thiserror::Error)]
pub enum CompleteMaintenanceError {
    #[error("No maintenance is currently in progress")]
    NoMaintenance,

    #[error("Signer is not under maintenance")]
    WrongOperator,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::abort_maintenance`] error.
#[derive(Debug, thiserror::Error)]
pub enum AbortMaintenanceError {
    #[error(transparent)]
    NotOwner(#[from] ownership::NotOwnerError),

    #[error("No maintenance is currently in progress")]
    NoMaintenance,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// [`Cluster::update_node_operator`] error.
#[derive(Debug, thiserror::Error)]
pub enum UpdateNodeOperatorError {
    #[error("Provided node operator is not a member of this cluster")]
    UnknownOperator,

    #[error("Data serialization: {0}")]
    DataSerialization(#[from] node_operator::DataSerializationError),

    #[error(transparent)]
    DataTooLarge(#[from] node_operator::DataTooLargeError),

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
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
            Event::OwnershipTransferred(evt) => evt.cluster_version,
        }
    }
}

/// Read-only view of a WCN cluster.
pub struct View {
    node_operators: HashMap<node_operator::Id, Arc<VersionedNodeOperator>>,

    ownership: Ownership,
    settings: Settings,

    keyspace: Keyspace,
    migration: Option<Migration>,
    maintenance: Option<Maintenance>,

    cluster_version: Version,
}

impl View {
    fn no_migration(&self) -> Result<(), LogicalError> {
        if let Some(migration) = self.migration() {
            return Err(LogicalError::MigrationInProgress(migration.id()));
        }

        Ok(())
    }

    fn no_maintenance(&self) -> Result<(), LogicalError> {
        if self.maintenance().is_some() {
            return Err(LogicalError::MaintenanceInProgress);
        }

        Ok(())
    }

    fn has_migration(&mut self) -> Result<&mut Migration, LogicalError> {
        self.migration.as_mut().ok_or(LogicalError::NoMigration)
    }

    fn has_maintenance(&mut self) -> Result<&mut Maintenance, LogicalError> {
        self.maintenance.as_mut().ok_or(LogicalError::NoMaintenance)
    }

    /// Applies the provided [`Event`] to this [`View`].
    pub async fn apply_event(mut self, event: Event) -> Result<Self, ApplyEventError> {
        let new_version = event.cluster_version();

        if new_version != self.cluster_version + 1 {
            return Err(ApplyEventError::ClusterVersionMismatch {
                current: self.cluster_version,
                event: new_version,
            });
        }

        match event {
            Event::MigrationStarted(evt) => evt.apply(&mut self).await,
            Event::MigrationDataPullCompleted(evt) => evt.apply(&mut self),
            Event::MigrationCompleted(evt) => evt.apply(&mut self),
            Event::MigrationAborted(evt) => evt.apply(&mut self),
            Event::MaintenanceStarted(evt) => evt.apply(&mut self),
            Event::MaintenanceFinished(evt) => evt.apply(&mut self),
            Event::NodeOperatorAdded(evt) => evt.apply(&mut self),
            Event::NodeOperatorUpdated(evt) => evt.apply(&mut self),
            Event::NodeOperatorRemoved(evt) => evt.apply(&mut self),
            Event::SettingsUpdated(evt) => evt.apply(&mut self),
            Event::OwnershipTransferred(evt) => evt.apply(&mut self),
        }?;

        self.cluster_version = new_version;

        Ok(self)
    }

    /// Returns the primary [`Keyspace`] of this WCN cluster.
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Returns the ongoing data [`Migration`] of this WCN cluster.
    pub fn migration(&self) -> Option<&Migration> {
        self.migration.as_ref()
    }

    /// Returns the ongoing [`Maintenance`] of this WCN cluster.
    pub fn maintenance(&self) -> Option<&Maintenance> {
        self.maintenance.as_ref()
    }

    /// Indicates whether the [`NodeOperator`] is a member of the [`Cluster`].
    pub fn is_member(&self, id: &node_operator::Id) -> bool {
        self.operator_idx(id).is_some()
    }

    fn operator_idx(&self, id: &node_operator::Id) -> Option<node_operator::Idx> {
        self.keyspace.operators().get_idx(&id).or_else(|| {
            self.migration
                .as_ref()
                .and_then(|migration| migration.keyspace().operators().get_idx(&id))
        })
    }
}

/// Error of [`View::apply_event`]
#[derive(Debug, thiserror::Error)]
pub enum ApplyEventError {
    /// [`Version`] inside the [`Event`] wasn't monotonic.
    #[error("Cluster version mismatch (current: {current}, event: {event})")]
    ClusterVersionMismatch { current: Version, event: Version },

    /// [`LogicalError`] observed while applying an [`Event`].
    #[error("Logic: {0}")]
    Logic(#[from] LogicalError),
}

/// Logical error caused by a race condition or by an incorrect implementation
/// of the [`SmartContract`]
#[derive(Debug, thiserror::Error)]
pub enum LogicalError {
    #[error("Migration(id: {0}) is in progress, but it shouldn't be")]
    MigrationInProgress(migration::Id),

    #[error("Maintenance is in progress, but it shouldn't be")]
    MaintenanceInProgress,

    #[error("No migration, but there should be")]
    NoMigration,

    #[error("No maintenance, but there should be")]
    NoMaintenance,

    #[error("Migration ID mismatch (event: {event}, local: {event})")]
    MigrationIdMismatch {
        event: migration::Id,
        local: migration::Id,
    },

    #[error("Node operator (id: {0}) is not currently pulling the data")]
    NodeOperatorNotPulling(node_operator::Id),

    #[error("There are still pulling operators remaining, but ther shouldn't be")]
    PullingOperatorsRemaining,

    #[error("Node operator (id: {0}) is not within the cluster, but should be")]
    UnknownOperator(node_operator::Id),

    #[error("Settings unchanged, but they should have changed")]
    SettingsUnchanged,

    #[error("Owner unchanged, but it should have changed")]
    OwnerUnchanged,
}
