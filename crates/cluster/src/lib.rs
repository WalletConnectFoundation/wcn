use std::{collections::HashSet, sync::Arc};

pub mod contract;
pub use contract::SmartContract;

pub mod node;
pub use node::Node;

mod keyspace;
pub use keyspace::Keyspace;

pub mod migration;
pub use migration::Migration;

/// Read-only view of a WCN cluster.
pub struct View {
    keyspace: Keyspace,
    migration: Option<Migration>,
    maintenance: Maintenance,
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
}

/// Maintenance process within a WCN cluster.
///
/// Only a single [`node::Operator`] at a time is allowed to be under
/// maintenance.
struct Maintenance {
    /// [`node::OperatorId`] of the [`node::Operator`] currently under
    /// maintenance (if any).
    slot: Option<node::OperatorId>,
}
