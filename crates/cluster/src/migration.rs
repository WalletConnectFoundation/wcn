use {
    crate::{
        keyspace::{self, ReplicationStrategy},
        node,
        Keyspace,
        View as ClusterView,
    },
    std::{collections::HashSet, sync::Arc},
};

/// Data migration process within a WCN cluster.
pub struct Migration {
    id: u64,
    keyspace: Keyspace,
    pulling_operators: HashSet<node::OperatorId>,
}

impl Migration {
    /// Returns the new [`Keyspace`] this [`Migration`] is migrating to.
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }
}

/// [`Migration`] plan.
pub struct Plan {
    cluster_view: Arc<ClusterView>,

    slots: Vec<(node::OperatorIdx, Option<node::OperatorId>)>,
    replication_strategy: ReplicationStrategy,
}

impl Plan {
    /// Creates a new migration [`Plan`].
    pub fn new(
        cluster_view: Arc<ClusterView>,
        remove: Vec<node::OperatorId>,
        add: Vec<node::OperatorId>,
    ) -> Result<Self, PlanError> {
        if cluster_view.migration.is_some() {
            return Err(PlanError::MigrationInProgress);
        }

        let mut plan = Self {
            cluster_view,
            slots: Vec::with_capacity(remove.len() + add.len()),
            replication_strategy: keyspace::ReplicationStrategy::default(),
        };

        let operators = cluster_view.keyspace.operators();

        for id in remove {
            if !operators.contains(&id) {
                return Err(PlanError::UnknownOperator(id));
            }

            plan.push_slot()
        }

        Ok()
    }

    fn push_slot(&mut self, idx: usize, id: Option<node::OperatorId>) -> Result<(), PlanError> {
        let idx = node::OperatorIdx::try_from(idx).map_err(|_| PlanError::IndexOverflow)?;
        self.slots.push((idx, id));
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Another migration is already in progress")]
    MigrationInProgress,

    #[error("Unknown operator: {0}")]
    UnknownOperator(node::OperatorId),

    #[error("Operator already exists: {0}")]
    OperatorAlreadyExists(node::OperatorId),

    #[error("Too many operators")]
    TooManyOperators,

    #[error("Index overflow")]
    IndexOverflow,
}
