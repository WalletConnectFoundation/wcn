use std::collections::HashSet;

pub mod contract;
pub use contract::SmartContract;

pub mod node;
pub use node::Node;

const REPLICATION_FACTOR: usize = 5;

type Keyspace = sharding::Keyspace<u8, REPLICATION_FACTOR>;

/// Read-only view of a WCN cluster.
pub struct View {
    ///
    pub operators: node::Operators,

    migration: Migration,
    maintenance: Maintenance,

    keyspace: Keyspace,
    keyspace_version: u64,

    version: u128,
}

/// Data migration process within a regional WCN cluster.
pub struct Migration {
    /// List of [`node::Operator`]s to be removed from the cluster.
    pub operators_to_remove: Vec<node::OperatorId>,

    /// List of [`node::Operator`]s to be added to the cluster.
    pub operators_to_add: Vec<node::Operator>,

    /// List of [`node::Operator`]s still pulling the data.
    pub pulling_operators: HashSet<node::OperatorId>,
}

/// Maintenance process within a regional WCN cluster.
///
/// Only a single [`node::Operator`] at a time is allowed to be under
/// maintenance.
pub struct Maintenance {
    /// [`node::OperatorId`] of the [`node::Operator`] currently under
    /// maintenance (if any).
    pub slot: Option<node::OperatorId>,
}
