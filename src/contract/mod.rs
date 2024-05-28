use {
    alloy::{
        primitives::{Address, Bytes, U256},
        providers::{network::EthereumSigner, Provider, ProviderBuilder, RootProvider},
        signers::wallet::{coins_bip39, MnemonicBuilder},
        sol,
        sol_types::SolInterface,
        transports::http::Http,
    },
    anyhow::Result,
    reqwest::Client,
    std::str::FromStr,
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
type RewardManager<P> = reward_manager::reward_managerInstance<Transport, P>;
type RewardManagerError = reward_manager::reward_managerErrors;

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
        } = config.getStaking().call().await?;

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

pub trait PerformanceReporter {
    async fn report_performance(&self, data: PerformanceData) -> Result<()>;
}

struct PerformanceReporterImpl<P> {
    signer_address: Address,
    reward_manager: RewardManager<P>,
}

#[derive(Debug, thiserror::Error)]
enum ReportPerformanceError {
    #[error("PerformanceDataAlreadyUpdated")]
    PerformanceDataAlreadyUpdated,

    #[error("Other: {0}")]
    Other(String),
}

impl<P: Provider<Transport> + Send + Sync> PerformanceReporter for PerformanceReporterImpl<P> {
    async fn report_performance(&self, data: PerformanceData) -> Result<()> {
        tracing::info!(
            ?data,
            signer_address = %self.signer_address,
            "sending performance report"
        );

        let nodes = data.nodes.iter().map(|n| n.address).collect();
        let performance = data.nodes.iter().map(|n| n.performance).collect();

        let performance_data = reward_manager::PerformanceData {
            nodes,
            performance,
            reportingEpoch: U256::from(data.epoch),
        };

        let call = self
            .reward_manager
            .postPerformanceRecords(performance_data)
            .from(self.signer_address);

        let res = call.send().await.map_err(|err| {
            reward_manager_error(&err)
                .map(|e| match e {
                    RewardManagerError::PerformanceDataAlreadyUpdated(_) => {
                        ReportPerformanceError::PerformanceDataAlreadyUpdated
                    }
                    _ => ReportPerformanceError::Other(err.to_string()),
                })
                .unwrap_or_else(|| ReportPerformanceError::Other(format!("{:?}", err)))
        });

        let tx = match res {
            Ok(tx) => tx,
            Err(ReportPerformanceError::PerformanceDataAlreadyUpdated) => {
                tracing::warn!("epoch already reported");
                return Ok(());
            }
            Err(ReportPerformanceError::Other(e)) => return Err(anyhow::anyhow!(e)),
        };

        let receipt = tx.get_receipt().await?;

        tracing::info!(?receipt, "Performance reported");

        Ok(())
    }
}

fn reward_manager_error(err: &alloy::contract::Error) -> Option<RewardManagerError> {
    let alloy::contract::Error::TransportError(e) = err else {
        return None;
    };

    let resp = e.as_error_resp()?;
    let data = resp.data.as_ref()?.get();

    let data = Bytes::from_str(data)
        .map_err(|err| tracing::warn!(?err, "alloy::Bytes::from_str"))
        .ok()?;

    RewardManagerError::abi_decode(&data, true)
        .map_err(|err| tracing::warn!(?err, "RewardManagerError::abi_decode"))
        .ok()
}

#[derive(Clone, Debug)]
pub struct PerformanceData {
    pub epoch: u64,
    pub nodes: Vec<NodePerformanceData>,
}

#[derive(Clone, Debug)]
pub struct NodePerformanceData {
    address: Address,
    performance: U256,
}

impl NodePerformanceData {
    pub fn new(addr: &str, performance: u8) -> Result<Self> {
        let address = addr.parse()?;
        let performance = match performance {
            1..=100 => U256::from(performance),
            _ => return Err(anyhow::anyhow!("Performance should be in range 1..=100")),
        };

        Ok(Self {
            address,
            performance,
        })
    }
}

pub async fn new_performance_reporter(
    rpc_url: &str,
    config_contract_address: &str,
    signer_mnemonic: &str,
) -> Result<impl PerformanceReporter> {
    let wallet = MnemonicBuilder::<coins_bip39::English>::default()
        .phrase(signer_mnemonic)
        .index(2)?
        .build()?;

    let signer_address = wallet.address();

    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .signer(EthereumSigner::from(wallet))
        .on_http(rpc_url.parse()?);

    let config = config::new(config_contract_address.parse()?, provider.clone());

    let config::getRewardManagerReturn {
        _0: reward_manager_address,
    } = config.getRewardManager().call().await?;

    Ok(PerformanceReporterImpl {
        signer_address,
        reward_manager: reward_manager::new(reward_manager_address, provider.clone()),
    })
}
