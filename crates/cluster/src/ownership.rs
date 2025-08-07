//! Ownership of a WCN cluster.

use {
    crate::smart_contract,
    serde::{Deserialize, Serialize},
};

/// Ownership of a WCN cluster.
///
/// Cluster has a single owner, and some [`smart_contract`] methods are
/// restricted to be executed only by the owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        smart_contract_signer: &smart_contract::Signer,
    ) -> Result<(), NotOwnerError> {
        if !self.is_owner(smart_contract_signer.address()) {
            return Err(NotOwnerError);
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Smart-contract signer is not the owner")]
pub struct NotOwnerError;
