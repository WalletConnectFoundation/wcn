use crate::{node_operator, Version as ClusterVersion};

/// Maintenance process within a WCN cluster.
///
/// Only a single [`node_operator`] at a time is allowed to be under
/// maintenance.
pub struct Maintenance {
    slot: node_operator::Id,
}

impl Maintenance {
    pub fn new(slot: node_operator::Id) -> Self {
        Self { slot }
    }

    /// Returns [`node_operator::Id`] that occupies the [`Maintenance`] slot.
    pub fn slot(&self) -> &node_operator::Id {
        &self.slot
    }
}

/// [`Maintenance`] has started.
pub struct Started {
    /// ID of the [`node_operator`] that started the [`Maintenance`].
    pub operator_id: node_operator::Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

/// [`Maintenance`] has been finished.
pub struct Finished {
    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

#[derive(Debug, thiserror::Error)]
#[error("Maintenance(slot: {_0}) in progress")]
pub struct InProgressError(pub node_operator::Id);

#[derive(Debug, thiserror::Error)]
#[error("No maintenance")]
pub struct NotFoundError;
