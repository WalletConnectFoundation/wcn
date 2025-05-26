use {
    crate::{
        keyspace::{self, ReplicationStrategy},
        node_operator,
        Keyspace,
        NewNodeOperator,
        Version,
        View as ClusterView,
    },
    itertools::Itertools,
    std::collections::HashSet,
};

/// Identifier of a [`Migration`].
pub type Id = u64;

/// Data migration process within a WCN cluster.
pub struct Migration {
    id: Id,
    keyspace: Keyspace,
    pulling_operators: HashSet<node_operator::Id>,
}

impl Migration {
    /// Returns [`Id`] of this [`Migration`].
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the new [`Keyspace`] this [`Migration`] is migrating to.
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Indicates whether the specified [`node_operator`] is still in process of
    /// pulling the data.
    pub fn is_pulling(&self, id: &node_operator::Id) -> bool {
        self.pulling_operators.contains(&id)
    }
}

/// [`Migration`] plan.
pub struct Plan {
    slots: Vec<(node_operator::Idx, Option<NewNodeOperator>)>,
    replication_strategy: ReplicationStrategy,
}

impl Plan {
    /// Creates a new migration [`Plan`].
    pub(super) fn new(
        cluster_view: &ClusterView,
        remove: Vec<node_operator::Id>,
        add: Vec<NewNodeOperator>,
    ) -> Result<Self, PlanError> {
        let slot_count = remove
            .iter()
            .chain(add.iter().map(|op| &op.id))
            .dedup()
            .count();

        if slot_count != remove.len() + add.len() {
            return Err(PlanError::DuplicateIds);
        }

        if cluster_view.migration.is_some() {
            return Err(PlanError::MigrationInProgress);
        }

        let mut slots = Vec::with_capacity(slot_count);
        let operators = cluster_view.keyspace.operators();

        for id in remove {
            operators
                .get_idx(&id)
                .map(|idx| slots.push((idx, None)))
                .ok_or_else(|| PlanError::UnknownOperator(id))?;
        }

        let add = &mut add.into_iter();

        // First populate the slots that were cleared just now.
        for (id, slot) in add.zip(slots.iter_mut()) {
            slot.1 = Some(id);
        }

        for item in add.zip_longest(operators.freeSlots()) {
            match item.left_and_right() {
                (Some(_), None) => return Err(PlanError::TooManyOperators),
                (None, _) => break,
                (Some(operator), _) if operators.contains(&operator.id) => {
                    return Err(PlanError::OperatorAlreadyExists(operator.id))
                }
                (Some(id), Some(idx)) => slots.push((idx, Some(id))),
            };
        }

        Ok(Self {
            slots,
            replication_strategy: keyspace::ReplicationStrategy::default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Another migration is already in progress")]
    MigrationInProgress,

    #[error("Duplicate operator IDs provided")]
    DuplicateIds,

    #[error("Unknown operator: {0}")]
    UnknownOperator(node_operator::Id),

    #[error("Operator already exists: {0}")]
    OperatorAlreadyExists(node_operator::Id),

    #[error("Too many operators")]
    TooManyOperators,
}

/// [`Migration`] has started.
pub struct Started {
    /// [`Id`] of the [`Migration`] being started.
    pub id: Id,

    /// Migration [`Plan`] being used.
    pub plan: Plan,

    /// Updated cluster [`Version`].
    pub version: Version,
}

/// [`NodeOperator`](crate::NodeOperator) has completed the data pull.
pub struct DataPullCompleted {
    /// [`Id`] of the [`Migration`].
    pub id: Id,

    /// ID of the [`node_operator`] that completed the pull.
    pub operator_id: node_operator::Id,

    /// Updated cluster [`Version`].
    pub version: u128,
}

/// [`Migration`] has been completed.
pub struct Completed {
    /// [`Id`] of the completed [`Migration`].
    pub id: Id,

    /// ID of the [`node_operator`] that completed the last data pull.
    pub operator_id: node_operator::Id,

    /// Updated cluster [`Version`].
    pub version: u128,
}

/// [`Migration`] has been aborted.
pub struct Aborted {
    /// [`Id`] of the [`Migration`].
    pub id: Id,

    /// Updated cluster [`Version`].
    pub version: u128,
}
