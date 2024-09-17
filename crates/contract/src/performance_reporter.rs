use {
    crate::{
        config,
        reward_manager,
        reward_manager_error,
        RewardManager,
        RewardManagerError,
        Transport,
    },
    alloy::{
        self,
        network::{EthereumWallet, TxSigner},
        primitives::{Address, U256},
        providers::{Provider, ProviderBuilder},
    },
    anyhow::Result,
};

#[allow(async_fn_in_trait)]
pub trait PerformanceReporter {
    async fn report_performance(&self, data: PerformanceData) -> Result<()>;
}

#[derive(Debug, thiserror::Error)]
enum ReportPerformanceError {
    #[error("PerformanceDataAlreadyUpdated")]
    PerformanceDataAlreadyUpdated,

    #[error("TotalPerformanceZero")]
    TotalPerformanceZero,

    #[error("MismatchedDataLengths")]
    MismatchedDataLengths,

    #[error("NonAuthorized")]
    NonAuthorized,

    #[error("OwnableUnauthorizedAccount: {0}")]
    OwnableUnauthorizedAccount(String),

    #[error("OwnableInvalidOwner")]
    OwnableInvalidOwner(String),

    #[error("ZeroAddress")]
    ZeroAddress,

    #[error("Paused")]
    Paused,

    #[error("Other: {0}")]
    Other(String),
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

pub trait SignerType: TxSigner<alloy::primitives::Signature> + Send + Sync + 'static {}
impl<T: TxSigner<alloy::primitives::Signature> + Send + Sync + 'static> SignerType for T {}

pub async fn new_performance_reporter(
    rpc_url: &str,
    config_contract_address: &str,
    signer: impl SignerType,
) -> Result<impl PerformanceReporter> {
    let signer_address = signer.address();

    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet)
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

struct PerformanceReporterImpl<P> {
    signer_address: Address,
    reward_manager: RewardManager<P>,
}

impl<P: Provider<Transport> + Send + Sync> PerformanceReporter for PerformanceReporterImpl<P> {
    async fn report_performance(&self, data: PerformanceData) -> Result<()> {
        tracing::info!(
            ?data,
            signer_address = %self.signer_address,
            "sending performance report"
        );

        let nodes = data.nodes.iter().map(|n| n.address).collect();
        let performance = data
            .nodes
            .iter()
            .map(|n| n.performance)
            .collect::<Vec<U256>>();

        if performance.is_empty() {
            return Err(anyhow::anyhow!("No performance data to report"));
        }

        let performance_data = reward_manager::PerformanceData {
            nodes,
            performance,
            reportingEpoch: U256::from(data.epoch),
        };

        let call = reward_manager::reward_managerInstance::postPerformanceRecords(
            &self.reward_manager,
            performance_data,
        )
        .from(self.signer_address);

        let res = call.send().await.map_err(|err| {
            reward_manager_error(&err)
                .map(|e| e.into())
                .unwrap_or_else(|| ReportPerformanceError::Other(format!("{:?}", err)))
        });

        let tx = match res {
            Ok(tx) => tx,
            Err(err) => return handle_txn_error(err),
        };

        let receipt = tx.get_receipt().await?;

        tracing::info!(?receipt, "Performance reported");

        Ok(())
    }
}

fn handle_txn_error(err: ReportPerformanceError) -> Result<()> {
    match err {
        ReportPerformanceError::PerformanceDataAlreadyUpdated => {
            tracing::warn!("Epoch already reported");

            metrics::counter!("irn_performance_reporter_data_already_reported").increment(1);

            Ok(())
        }
        ReportPerformanceError::Paused => {
            tracing::error!("reward_manager paused");

            metrics::counter!("irn_performance_reporter_paused").increment(1);

            Err(anyhow::anyhow!("reward_manager paused"))
        }
        ReportPerformanceError::NonAuthorized => {
            tracing::error!("reward_manager paused");

            metrics::counter!("irn_performance_reporter_non_authorized").increment(1);

            Err(anyhow::anyhow!("reward_manager paused"))
        }
        ReportPerformanceError::TotalPerformanceZero => {
            tracing::error!("Attempted to report performance with total performance value of zero");

            metrics::counter!("irn_performance_reporter_total_performance_zero").increment(1);

            Err(anyhow::anyhow!("Total performance for node is zero"))
        }
        ReportPerformanceError::OwnableUnauthorizedAccount(address) => {
            tracing::error!("Unathorized ownable account: {}", address);

            metrics::counter!("irn_performance_reporter_owner_unauthorized").increment(1);

            Err(anyhow::anyhow!("Unathorized ownable account: {}", address))
        }
        ReportPerformanceError::Other(e) => Err(anyhow::anyhow!(e)),
        _ => Err(anyhow::anyhow!("Unknown error from RewardManager")),
    }
}

impl From<RewardManagerError> for ReportPerformanceError {
    fn from(err: RewardManagerError) -> Self {
        match err {
            RewardManagerError::PerformanceDataAlreadyUpdated(_) => {
                ReportPerformanceError::PerformanceDataAlreadyUpdated
            }
            RewardManagerError::MismatchedDataLengths(_) => {
                ReportPerformanceError::MismatchedDataLengths
            }
            RewardManagerError::OwnableInvalidOwner(owner) => {
                ReportPerformanceError::OwnableInvalidOwner(owner.owner.to_string())
            }
            RewardManagerError::OwnableUnauthorizedAccount(owner) => {
                ReportPerformanceError::OwnableUnauthorizedAccount(owner.account.to_string())
            }
            RewardManagerError::TotalPerformanceZero(_) => {
                ReportPerformanceError::TotalPerformanceZero
            }
            RewardManagerError::ZeroAddress(_) => ReportPerformanceError::ZeroAddress,
            _ => ReportPerformanceError::Other("unknown error from contract".to_string()),
        }
    }
}
