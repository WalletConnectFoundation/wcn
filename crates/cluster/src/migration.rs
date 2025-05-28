use {
    crate::{
        keyspace::{self, ReplicationStrategy},
        node_operator,
        Keyspace,
        LogicalError,
        NodeOperator,
        SerializedNodeOperator,
        Version as ClusterVersion,
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
    /// Creates a new [`Migration`].
    pub(super) async fn new(id: Id, old_keyspace: &Keyspace, plan: Plan) -> Self {
        let mut operators = old_keyspace.operators().clone();
        for (idx, slot) in plan.slots {
            operators.set(idx, slot);
        }

        let new_keyspace = Keyspace::new(
            operators,
            plan.replication_strategy,
            old_keyspace.version() + 1,
        )
        .await;

        let pulling_operators = new_keyspace
            .operators()
            .slots()
            .iter()
            .filter_map(|slot| slot.as_ref().map(|operator| *operator.id()))
            .collect();

        Self {
            id,
            keyspace: new_keyspace,
            pulling_operators,
        }
    }

    /// Returns [`Id`] of this [`Migration`].
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the new [`Keyspace`] this [`Migration`] is migrating to.
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Mutable version of [`Migration::keyspace`].
    pub fn keyspace_mut(&mut self) -> &mut Keyspace {
        &mut self.keyspace
    }

    /// Indicates whether the specified [`node_operator`] is still in process of
    /// pulling the data.
    pub fn is_pulling(&self, id: &node_operator::Id) -> bool {
        self.pulling_operators.contains(&id)
    }
}

/// [`Plan`] that is not yet published on-chain.
pub type NewPlan = Plan<SerializedNodeOperator>;

/// [`Migration`] plan.
pub struct Plan<Operator = NodeOperator> {
    slots: Vec<(node_operator::Idx, Option<Operator>)>,
    replication_strategy: ReplicationStrategy,
}

impl Plan {
    /// Creates a new migration [`Plan`].
    pub(super) fn new(
        cluster_view: &ClusterView,
        remove: Vec<node_operator::Id>,
        add: Vec<SerializedNodeOperator>,
    ) -> Result<NewPlan, PlanError> {
        let slot_count = remove
            .iter()
            .chain(add.iter().map(NodeOperator::id))
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
                (Some(operator), _) if operators.contains(operator.id()) => {
                    return Err(PlanError::OperatorAlreadyExists(*operator.id()))
                }
                (Some(id), Some(idx)) => slots.push((idx, Some(id))),
            };
        }

        Ok(NewPlan {
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
    pub migration_id: Id,

    /// Migration [`Plan`] being used.
    pub plan: Plan,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Started {
    pub(super) async fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        view.no_migration()?;
        view.no_maintenance()?;

        view.migration = Some(Migration::new(self.migration_id, &view.keyspace, self.plan).await);

        Ok(())
    }
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

impl DataPullCompleted {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        let migration = view.has_migration()?.with_correct_id(self.migration_id)?;

        if !migration.pulling_operators.remove(&self.operator_id) {
            return Err(LogicalError::NodeOperatorNotPulling(self.operator_id));
        }

        Ok(())
    }
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

impl Completed {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        let migration = view.has_migration()?.with_correct_id(self.migration_id)?;

        if !migration.pulling_operators.remove(&self.operator_id) {
            return Err(LogicalError::NodeOperatorNotPulling(self.operator_id));
        }

        if !migration.pulling_operators.is_empty() {
            return Err(LogicalError::PullingOperatorsRemaining);
        }

        view.keyspace = view.migration.take().unwrap().keyspace;

        Ok(())
    }
}

/// [`Migration`] has been aborted.
pub struct Aborted {
    /// [`Id`] of the [`Migration`].
    pub migration_id: Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Aborted {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        view.has_migration()?.with_correct_id(self.migration_id)?;
        view.migration = None;

        Ok(())
    }
}

impl Migration {
    fn with_correct_id(&mut self, id: Id) -> Result<&mut Self, LogicalError> {
        if id != self.id {
            return Err(LogicalError::MigrationIdMismatch {
                event: id,
                local: self.id,
            });
        }
        Ok(self)
    }
}
