use {
    crate::{config, permissioned_node_registry, staking, PermissionedNodeRegistry, Staking},
    alloy::providers::ProviderBuilder,
    anyhow::{Context, Result},
};

#[derive(Clone)]
pub struct StakeValidator {
    staking: Staking,
    permissioned_node_registry: PermissionedNodeRegistry,
}

impl StakeValidator {
    pub async fn new(rpc_url: &str, config_contract_address: &str) -> Result<Self> {
        let provider = ProviderBuilder::new().on_http(rpc_url.parse()?);

        let config = config::new(config_contract_address.parse()?, provider.clone());

        let config::getStakingReturn {
            _0: staking_address,
        } = config
            .getStaking()
            .call()
            .await
            .context("failed to retrieve stake mapping")?;

        let config::getPermissionedNodeRegistryReturn {
            _0: permissioned_node_registry_address,
        } = config.getPermissionedNodeRegistry().call().await?;

        Ok(Self {
            staking: staking::new(staking_address, provider.clone()),
            permissioned_node_registry: permissioned_node_registry::new(
                permissioned_node_registry_address,
                provider.clone(),
            ),
        })
    }

    pub async fn validate_stake(&self, operator_address: &str) -> Result<()> {
        let addr = operator_address.parse()?;

        let permissioned_node_registry::isNodeWhitelistedReturn { _0: is_whitelisted } = self
            .permissioned_node_registry
            .isNodeWhitelisted(addr)
            .call()
            .await?;

        if !is_whitelisted {
            return Err(anyhow::anyhow!("Node operator address is not whitelisted"));
        }

        let staking::stakesReturn { amount: stake } = self.staking.stakes(addr).call().await?;
        let staking::minStakeAmountReturn { _0: min_stake } =
            self.staking.minStakeAmount().call().await?;

        if min_stake > stake {
            return Err(anyhow::anyhow!(
                "Node operator has less than the minimum stake amount required -> Node's stake {} \
                 < minStakeAmount {}",
                stake,
                min_stake
            ));
        }

        Ok(())
    }
}
