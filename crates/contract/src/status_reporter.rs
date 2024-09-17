use {
    crate::{config, staking, Staking},
    alloy::{primitives::Address, providers::ProviderBuilder},
    anyhow::Result,
    std::{str::FromStr, sync::Arc},
};

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
