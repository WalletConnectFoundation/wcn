use {
    crate::{node_operator, smart_contract, Version as ClusterVersion},
    serde::{Deserialize, Serialize},
};

#[allow(unused_imports)]
use crate::Cluster; // for doc comments

/// Maintenance process within a WCN cluster.
///
/// Only a single [`node_operator`] at a time is allowed to be under
/// maintenance.
///
/// Owner of the [`Cluster`] is also allowed to start [`Maintenance`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Maintenance {
    slot: smart_contract::AccountAddress,
}

impl Maintenance {
    /// Creates a new [`Maintenance`] with the slot being occupied by the
    /// provided [`smart_contract::AccountAddress`].
    pub fn new(slot: smart_contract::AccountAddress) -> Self {
        Self { slot }
    }

    /// Returns [`smart_contract::AccountAddress`] that occupies the
    /// [`Maintenance`] slot.
    pub fn slot(&self) -> &smart_contract::AccountAddress {
        &self.slot
    }
}

/// [`Maintenance`] has started.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Started {
    /// [`smart_contract::AccountAddress`] of the account that
    /// started the [`Maintenance`].
    pub by: smart_contract::AccountAddress,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

/// [`Maintenance`] has been finished.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
