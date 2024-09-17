use alloy::sol;

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    config,
    "generated/config.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    staking,
    "generated/staking.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    permissioned_node_registry,
    "generated/permissioned_node_registry.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    reward_manager,
    "generated/reward_manager.json"
);

mod manager;
mod performance_reporter;
mod signer;
mod stake_validator;
mod status_reporter;

pub(crate) use manager::*;
pub use {performance_reporter::*, signer::*, stake_validator::*, status_reporter::*};
