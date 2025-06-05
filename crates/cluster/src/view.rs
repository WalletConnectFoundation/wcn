//! Read-only view of a WCN cluster.

use {
    crate::{
        self as cluster,
        keyspace,
        maintenance,
        migration,
        node_operator,
        node_operators,
        ownership,
        settings,
        Event,
        Keyspace,
        Maintenance,
        Migration,
        NodeOperators,
        Ownership,
        Settings,
    },
    futures::future::OptionFuture,
};

/// Read-only view of a WCN cluster.
pub struct View<Shards = (), OperatorData = node_operator::Data> {
    pub(super) node_operators: NodeOperators<OperatorData>,

    pub(super) ownership: Ownership,
    pub(super) settings: Settings,

    pub(super) keyspace: Keyspace<Shards>,
    pub(super) migration: Option<Migration<Shards>>,
    pub(super) maintenance: Option<Maintenance>,

    pub(super) cluster_version: cluster::Version,
}

impl<Shards> View<Shards> {
    /// Returns [`Ownership`] of the WCN cluster.
    pub fn ownership(&self) -> &Ownership {
        &self.ownership
    }

    /// Returns [`Settings`] of the WCN cluster.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Returns the primary [`Keyspace`] of the WCN cluster.
    pub fn keyspace(&self) -> &Keyspace<Shards> {
        &self.keyspace
    }

    /// Returns the ongoing [`Migration`] of the WCN cluster.
    pub fn migration(&self) -> Option<&Migration<Shards>> {
        self.migration.as_ref()
    }

    /// Returns the ongoing [`Maintenance`] of the WCN cluster.
    pub fn maintenance(&self) -> Option<&Maintenance> {
        self.maintenance.as_ref()
    }

    /// Returns [`NodeOperators`] of the WCN cluster.
    pub fn node_operators(&self) -> &NodeOperators {
        &self.node_operators
    }

    pub(super) fn require_no_migration(&self) -> Result<(), migration::InProgressError> {
        if let Some(migration) = self.migration() {
            return Err(migration::InProgressError(migration.id()));
        }

        Ok(())
    }

    pub(super) fn require_no_maintenance(&self) -> Result<(), maintenance::InProgressError> {
        if let Some(maintenance) = self.maintenance() {
            return Err(maintenance::InProgressError(*maintenance.slot()));
        }

        Ok(())
    }

    pub(super) fn require_migration(&self) -> Result<&Migration<Shards>, migration::NotFoundError> {
        self.migration.as_ref().ok_or(migration::NotFoundError)
    }

    pub(super) fn require_maintenance(&self) -> Result<&Maintenance, maintenance::NotFoundError> {
        self.maintenance.as_ref().ok_or(maintenance::NotFoundError)
    }

    /// Applies the provided [`Event`] to this [`View`].
    pub async fn apply_event(mut self, event: Event) -> Result<Self, Error>
    where
        Keyspace: keyspace::sealed::Calculate<Shards>,
    {
        let new_version = event.cluster_version();

        if new_version != self.cluster_version + 1 {
            return Err(Error::ClusterVersionNotMonotonic(
                self.cluster_version,
                new_version,
            ));
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
}

impl View {
    pub(super) async fn calculate_keyspace_shards(self) -> View<keyspace::Shards> {
        let (keyspace, migration) = tokio::join!(
            self.keyspace.calculate_shards(),
            OptionFuture::from(self.migration.map(Migration::calculate_keyspace_shards))
        );

        View {
            node_operators: self.node_operators,
            ownership: self.ownership,
            settings: self.settings,
            keyspace,
            migration,
            maintenance: self.maintenance,
            cluster_version: self.cluster_version,
        }
    }
}

impl<Shards> View<Shards, node_operator::SerializedData> {
    pub(super) fn deserialize(
        self,
    ) -> Result<View<Shards>, node_operator::DataDeserializationError> {
        Ok(View {
            node_operators: self.node_operators.deserialize()?,
            ownership: self.ownership,
            settings: self.settings,
            keyspace: self.keyspace,
            migration: self.migration,
            maintenance: self.maintenance,
            cluster_version: self.cluster_version,
        })
    }
}

impl migration::Started {
    async fn apply<Shards>(self, view: &mut View<Shards>) -> Result<()>
    where
        Keyspace: keyspace::sealed::Calculate<Shards>,
    {
        view.require_no_migration()?;
        view.require_no_maintenance()?;
        view.keyspace().require_diff(&self.new_keyspace)?;

        view.migration = Some(Migration::new(
            self.migration_id,
            self.new_keyspace.calculate_shards().await,
            view.node_operators.occupied_indexes(),
        ));

        Ok(())
    }
}

impl migration::DataPullCompleted {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        let idx = view.node_operators.require_idx(&self.operator_id)?;
        view.require_migration()?
            .require_id(self.migration_id)?
            .require_pulling(idx)?;

        if let Some(migration) = view.migration.as_mut() {
            migration.complete_pull(idx);
        }

        Ok(())
    }
}

impl migration::Completed {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        let idx = view.node_operators.require_idx(&self.operator_id)?;
        view.require_migration()?
            .require_id(self.migration_id)?
            .require_pulling(idx)?
            .require_pulling_count(1)?;

        view.keyspace = view.migration.take().unwrap().into_keyspace();

        Ok(())
    }
}

impl migration::Aborted {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.require_migration()?.require_id(self.migration_id)?;
        view.migration = None;

        Ok(())
    }
}

impl maintenance::Started {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.require_no_migration()?;
        view.require_no_maintenance()?;

        view.maintenance = Some(Maintenance::new(self.operator_id));

        Ok(())
    }
}

impl maintenance::Finished {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.require_maintenance()?;
        view.maintenance = None;

        Ok(())
    }
}

impl node_operator::Added {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.node_operators()
            .require_not_exists(&self.operator.id)?
            .require_free_slot(self.idx)?;

        view.node_operators.set(self.idx, Some(self.operator));

        Ok(())
    }
}

impl node_operator::Updated {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        let idx = view.node_operators.require_idx(&self.operator.id)?;
        view.node_operators.set(idx, Some(self.operator));

        Ok(())
    }
}

impl node_operator::Removed {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        let idx = view.node_operators.require_idx(&self.id)?;
        view.node_operators.set(idx, None);

        Ok(())
    }
}

impl settings::Updated {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.settings = self.settings;

        Ok(())
    }
}

impl ownership::Transferred {
    pub(super) fn apply<S>(self, view: &mut View<S>) -> Result<()> {
        view.ownership.transfer(self.new_owner);

        Ok(())
    }
}

/// Error of [`View::apply_event`] caused by a race condition or by an incorrect
/// implementation of the [`SmartContract`](crate::SmartContract).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Cluster version change wasn't monotonic: {_0} -> {_1}")]
    ClusterVersionNotMonotonic(cluster::Version, cluster::Version),

    #[error(transparent)]
    MigrationInProgress(#[from] migration::InProgressError),

    #[error(transparent)]
    MaintenanceInProgress(#[from] maintenance::InProgressError),

    #[error(transparent)]
    NoMigration(#[from] migration::NotFoundError),

    #[error(transparent)]
    NoMaintenance(#[from] maintenance::NotFoundError),

    #[error(transparent)]
    WrongMigrationId(#[from] migration::WrongIdError),

    #[error(transparent)]
    SameKeyspace(#[from] keyspace::SameKeyspaceError),

    #[error(transparent)]
    NotPulling(#[from] migration::OperatorNotPullingError),

    #[error(transparent)]
    WrongPullingOperatorsCount(#[from] migration::WrongPullingOperatorsCountError),

    #[error(transparent)]
    OperatorNotFound(#[from] node_operator::NotFoundError),

    #[error(transparent)]
    OperatorAlreadyExists(#[from] node_operator::AlreadyExistsError),

    #[error(transparent)]
    OperatorSlotOccupied(#[from] node_operators::SlotOccupiedError),
}

/// Result of [`View::apply_event`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
