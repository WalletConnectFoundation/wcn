//! Ownership of a WCN cluster.

use crate::{
    smart_contract,
    LogicalError,
    SmartContract,
    Version as ClusterVersion,
    View as ClusterView,
};

/// Ownership of a WCN cluster.
///
/// Cluster has a single owner, and some [`smart_contract`] methods are
/// resticted to be executed only by the owner.
pub struct Ownership {
    owner: smart_contract::PublicKey,
}

impl Ownership {
    pub(super) fn validate_signer(
        &self,
        smart_contract: &impl SmartContract,
    ) -> Result<(), NotOwnerError> {
        if self.owner != smart_contract.signer() {
            return Err(NotOwnerError);
        }

        Ok(())
    }
}

/// Event of [`Ownership`] being transferred.
pub struct Transferred {
    /// New owner of the WCN cluster.
    pub new_owner: smart_contract::PublicKey,

    /// Update [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Transferred {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        if view.ownership.owner == self.new_owner {
            return Err(LogicalError::OwnerUnchanged);
        }

        view.ownership.owner = self.new_owner;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Smart-contract signer is not the owner")]
pub struct NotOwnerError;
