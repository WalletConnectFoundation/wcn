use {
    alloy::{
        primitives::{Address, Bytes, U256},
        providers::{network::EthereumSigner, Provider, ProviderBuilder, RootProvider},
        signers::wallet::{coins_bip39, MnemonicBuilder},
        sol,
        sol_types::SolInterface,
        transports::http::Http,
    },
    anyhow::{Context, Result},
    reqwest::Client,
    std::{future::Future, str::FromStr, sync::Arc},
};

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    config,
    "src/contract/config.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    staking,
    "src/contract/staking.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    permissioned_node_registry,
    "src/contract/permissioned_node_registry.json"
);

sol!(
    #[allow(clippy::empty_structs_with_brackets)]
    #[allow(missing_docs)]
    #[sol(rpc)]
    reward_manager,
    "src/contract/reward_manager.json"
);

type Transport = Http<Client>;
type Staking = staking::stakingInstance<Transport, RootProvider<Transport>>;
type PermissionedNodeRegistry = permissioned_node_registry::permissioned_node_registryInstance<
    Transport,
    RootProvider<Transport>,
>;

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

#[derive(Clone, Debug)]
pub struct StatusData {
    pub stake: f64,
}

pub trait StatusReporter: Clone + Send + Sync + 'static {
    fn report_status(&self) -> impl futures::Future<Output = Result<StatusData>> + Send;
}

#[derive(Clone)]
struct StatusReporterImpl {
    eth_address: Address,
    staking: Arc<Staking>,
}

impl StatusReporter for StatusReporterImpl {
    async fn report_status(&self) -> Result<StatusData> {
        let staking::stakesReturn { amount: stake } = self
            .staking
            .stakes(self.eth_address)
            .call()
            .await
            .map_err(ReportStatusError::FailedToReadStake)?;

        // TODO: query balance and claimed vs pending rewards
        Ok(StatusData {
            stake: stake.into(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum ReportStatusError {
    #[error("Failed to retrieve stake mapping from contract: {0}")]
    FailedToReadStake(alloy::contract::Error),

    #[error("Other: {0}")]
    Other(String),
}

pub async fn new_status_reporter(
    rpc_url: &str,
    config_contract_address: &str,
    eth_address: &str,
) -> Result<impl StatusReporter> {
    let eth_address =
        Address::from_str(eth_address).map_err(|err| ReportStatusError::Other(err.to_string()))?;

    let provider = ProviderBuilder::new().on_http(rpc_url.parse()?);

    let config = config::new(config_contract_address.parse()?, provider.clone());

    let config::getStakingReturn {
        _0: staking_address,
    } = config.getStaking().call().await.map_err(|err| {
        ReportStatusError::Other(format!("failed to retrieve staking stake mapping: {err}"))
    })?;

    let status_reporter = StatusReporterImpl {
        eth_address,
        staking: Arc::new(staking::new(staking_address, provider.clone())),
    };

    Ok(status_reporter)
}
