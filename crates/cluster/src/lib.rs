use {
    arc_swap::ArcSwap,
    itertools::Itertools,
    smart_contract::evm,
    std::{collections::HashSet, future::Future, sync::Arc},
};

pub mod smart_contract;
pub use smart_contract::SmartContract;

pub mod client;
pub use client::{Client, ClientRef};

pub mod node;
pub use node::{Node, NodeRef};

pub mod node_operator;
pub use node_operator::{NewNodeOperator, NodeOperator};

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

/// WCN [`Cluster`] settings.
pub struct Settings {
    /// Maximum number of on-chain bytes stored for a single [`NodeOperator`].
    pub max_node_operator_data_bytes: u16,
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

    /// [`Maintenance`] has been completed.
    MaintenanceCompleted(maintenance::Completed),

    /// [`Maintenance`] has been aborted.
    MaintenanceAborted(maintenance::Aborted),

    /// On-chain state of an [`NodeOperator`] has been updated.
    NodeOperatorUpdated(node_operator::Updated),
}

impl<SC: SmartContract> Cluster<SC> {
    /// Deploys a new WCN [`Cluster`].
    pub async fn deploy(
        signer: smart_contract::Signer,
        rpc_url: smart_contract::RpcUrl,
        initial_settings: Settings,
        initial_operators: Vec<NewNodeOperator>,
    ) -> Result<Self, DeploymentError> {
        if initial_operators.iter().map(|op| &op.id).dedup().count() != initial_operators.len() {
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
        add: Vec<NewNodeOperator>,
    ) -> Result<(), StartMigrationError> {
        let plan = self.using_view(move |view| {
            if self.smart_contract.signer() != view.owner {
                return Err(StartMigrationError::NotOwner);
            }

            if view.migration().is_some() {
                return Err(StartMigrationError::MigrationInProgress);
            }

            if view.maintenance().is_some() {
                return Err(StartMigrationError::MaintenanceInProgress);
            }

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
            if self.smart_contract.signer() != view.owner {
                return Err(AbortMigrationError::NotOwner);
            }

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

    /// Calls [`SmartContract::complete_maintenance`].
    pub async fn complete_maintenance(&self) -> Result<(), CompleteMaintenanceError> {
        self.using_view(|view| {
            let maintenance = view
                .maintenance()
                .ok_or(CompleteMaintenanceError::NoMaintenance)?;

            if maintenance.operator() != &self.smart_contract.signer() {
                return Err(CompleteMaintenanceError::WrongOperator);
            }

            Ok(())
        })?;

        self.smart_contract.complete_maintenance().await?;

        Ok(())
    }

    /// Calls [`SmartContract::abort_maintenance`].
    pub async fn abort_maintenance(&self) -> Result<(), AbortMaintenanceError> {
        self.using_view(|view| {
            if self.smart_contract.signer() != view.owner {
                return Err(AbortMaintenanceError::NotOwner);
            }

            view.maintenance()
                .ok_or(AbortMaintenanceError::NoMaintenance)?;

            Ok(())
        })?;

        self.smart_contract
            .abort_maintenance()
            .await
            .map_err(Into::into)
    }

    /// Updates on-chain data of a [`NodeOperator`].
    ///
    /// Emits [`node_operator::Updated`] event on success.
    pub async fn update_node_operator(
        &self,
        id: node_operator::Id,
        data: node_operator::Data,
    ) -> Result<(), UpdateNodeOperatorError> {
        let idx = self
            .view
            .load()
            .operator_idx(&id)
            .ok_or(UpdateNodeOperatorError::UnknownOperator)?;

        let data = data.serialize()?;

        if data.len() > self.view.load().settings.max_node_operator_data_bytes as usize {
            return Err(UpdateNodeOperatorError::DataTooLarge);
        }

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
    #[error("Signer is not the owner of the smart-contract")]
    NotOwner,

    #[error("Another migration in progress")]
    MigrationInProgress,

    #[error("Maintenance in progress")]
    MaintenanceInProgress,

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
    #[error("Signer is not the owner of the smart-contract")]
    NotOwner,

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
    #[error("Signer is not the owner of the smart-contract")]
    NotOwner,

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

    #[error("Size of the provided operator data exceeds the configured setting")]
    DataTooLarge,

    #[error("Smart-contract: {0}")]
    SmartContract(#[from] smart_contract::Error),
}

/// Read-only view of a WCN cluster.
pub struct View {
    owner: smart_contract::PublicKey,
    settings: Settings,

    keyspace: Keyspace,
    migration: Option<Migration>,
    maintenance: Option<Maintenance>,

    version: u128,
}

impl View {
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
