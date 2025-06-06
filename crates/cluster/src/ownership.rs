//! Ownership of a WCN cluster.

use crate::{smart_contract, SmartContract, Version as ClusterVersion};

/// Ownership of a WCN cluster.
///
/// Cluster has a single owner, and some [`smart_contract`] methods are
/// resticted to be executed only by the owner.
#[derive(Clone)]
pub struct Ownership {
    owner: smart_contract::AccountAddress,
}

impl Ownership {
    pub(super) fn new(owner: smart_contract::AccountAddress) -> Self {
        Self { owner }
    }

    pub(super) fn is_owner(&self, address: &smart_contract::AccountAddress) -> bool {
        address == &self.owner
    }

    pub(super) fn require_owner(
        &self,
        smart_contract: &impl SmartContract,
    ) -> Result<(), NotOwnerError> {
        if !self.is_owner(smart_contract.signer().address()) {
            return Err(NotOwnerError);
        }

        Ok(())
    }

    pub(super) fn transfer(&mut self, new_owner: smart_contract::AccountAddress) {
        self.owner = new_owner;
    }
}

/// Event of [`Ownership`] being transferred.
#[derive(Debug)]
pub struct Transferred {
    /// New owner of the WCN cluster.
    pub new_owner: smart_contract::AccountAddress,

    /// Update [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

#[derive(Debug, thiserror::Error)]
#[error("Smart-contract signer is not the owner")]
pub struct NotOwnerError;
