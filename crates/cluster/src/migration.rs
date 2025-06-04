use {
    crate::{
        keyspace::{self, ReplicationStrategy},
        node_operator,
        Keyspace,
        Version as ClusterVersion,
    },
    std::collections::HashSet,
};

/// Identifier of a [`Migration`].
pub type Id = u64;

/// Data migration process within a WCN cluster.
pub struct Migration<Shards = ()> {
    id: Id,
    keyspace: Keyspace<Shards>,
    pulling_operators: HashSet<node_operator::Idx>,
}

/// [`Migration`] plan.
pub struct Plan {
    /// Set of [`node_operator`]s to remove from the current [`Keyspace`].
    pub remove: HashSet<node_operator::Id>,

    /// Set of [`node_operator`]s to add to the current [`Keyspace`].
    pub add: HashSet<node_operator::Id>,

    /// New [`ReplicationStrategy`] to use.
    pub replication_strategy: ReplicationStrategy,
}

impl<Shards> Migration<Shards> {
    /// Creates a new [`Migration`].
    pub(super) fn new(
        id: Id,
        keyspace: Keyspace<Shards>,
        pulling_operators: impl IntoIterator<Item = node_operator::Idx>,
    ) -> Self {
        Self {
            id,
            keyspace,
            pulling_operators: pulling_operators.into_iter().collect(),
        }
    }

    /// Returns [`Id`] of this [`Migration`].
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the new [`Keyspace`] this [`Migration`] is migrating to.
    pub fn keyspace(&self) -> &Keyspace<Shards> {
        &self.keyspace
    }

    // /// Mutable version of [`Migration::keyspace`].
    // pub fn keyspace_mut(&mut self) -> &mut Keyspace {
    //     &mut self.keyspace
    // }

    pub(super) fn into_keyspace(self) -> Keyspace<Shards> {
        self.keyspace
    }

    /// Indicates whether the specified [`node_operator`] is still in process of
    /// pulling the data.
    pub fn is_pulling(&self, idx: node_operator::Idx) -> bool {
        self.pulling_operators.contains(&idx)
    }

    pub(crate) fn pulling_operators_count(&self) -> usize {
        self.pulling_operators.len()
    }

    pub(crate) fn complete_pull(&mut self, idx: node_operator::Idx) {
        self.pulling_operators.remove(&idx);
    }

    pub(crate) fn require_id(&self, id: Id) -> Result<&Migration<Shards>, WrongIdError> {
        if id != self.id {
            return Err(WrongIdError(id, self.id));
        }

        Ok(self)
    }

    pub(crate) fn require_pulling(
        &self,
        idx: node_operator::Idx,
    ) -> Result<&Migration<Shards>, OperatorNotPullingError> {
        if !self.is_pulling(idx) {
            return Err(OperatorNotPullingError(idx));
        }

        Ok(self)
    }

    pub(crate) fn require_pulling_count(
        &self,
        expected: usize,
    ) -> Result<&Migration<Shards>, WrongPullingOperatorsCountError> {
        let count = self.pulling_operators.len();
        if count != expected {
            return Err(WrongPullingOperatorsCountError(expected, count));
        }

        Ok(self)
    }
}

impl Migration {
    pub(crate) async fn calculate_keyspace_shards(self) -> Migration<keyspace::Shards> {
        Migration {
            id: self.id,
            keyspace: self.keyspace.calculate_shards().await,
            pulling_operators: self.pulling_operators,
        }
    }
}

/// [`Migration`] has started.
pub struct Started {
    /// [`Id`] of the [`Migration`] being started.
    pub migration_id: Id,

    /// New [`Keyspace`] to migrate to.
    pub new_keyspace: Keyspace,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

/// [`NodeOperator`](crate::NodeOperator) has completed the data pull.
pub struct DataPullCompleted {
    /// [`Id`] of the [`Migration`].
    pub migration_id: Id,

    /// ID of the [`node_operator`] that completed the pull.
    pub operator_id: node_operator::Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

/// [`Migration`] has been completed.
pub struct Completed {
    /// [`Id`] of the completed [`Migration`].
    pub migration_id: Id,

    /// ID of the [`node_operator`] that completed the last data pull.
    pub operator_id: node_operator::Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

/// [`Migration`] has been aborted.
pub struct Aborted {
    /// [`Id`] of the [`Migration`].
    pub migration_id: Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

#[derive(Debug, thiserror::Error)]
#[error("Migration(id: {_0}) in progress")]
pub struct InProgressError(pub Id);

#[derive(Debug, thiserror::Error)]
#[error("No migration")]
pub struct NotFoundError;

#[derive(Debug, thiserror::Error)]
#[error("Wrong migration ID: {_0} != {_1}")]
pub struct WrongIdError(pub Id, pub Id);

#[derive(Debug, thiserror::Error)]
#[error("NodeOperator(idx: {_0}) is not currently pulling data")]
pub struct OperatorNotPullingError(pub node_operator::Idx);

#[derive(Debug, thiserror::Error)]
#[error("Wrong pulling operators count: {_0} != {_1}")]
pub struct WrongPullingOperatorsCountError(pub usize, pub usize);
