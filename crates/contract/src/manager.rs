use {
    crate::{permissioned_node_registry, reward_manager, staking},
    alloy::{providers::RootProvider, transports::http::Http},
    reqwest::Client,
};

pub(crate) type Transport = Http<Client>;
pub(crate) type Staking = staking::stakingInstance<Transport, RootProvider<Transport>>;
pub(crate) type PermissionedNodeRegistry =
    permissioned_node_registry::permissioned_node_registryInstance<
        Transport,
        RootProvider<Transport>,
    >;
pub(crate) type RewardManager<P> = reward_manager::reward_managerInstance<Transport, P>;
pub(crate) type RewardManagerError = reward_manager::reward_managerErrors;

pub(crate) fn reward_manager_error(err: &alloy::contract::Error) -> Option<RewardManagerError> {
    let alloy::contract::Error::TransportError(e) = err else {
        return None;
    };

    let resp = e.as_error_resp()?;

    resp.as_decoded_error::<RewardManagerError>(true)
}
