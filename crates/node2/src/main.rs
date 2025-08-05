use {
    anyhow::Context,
    futures::FutureExt as _,
    futures_concurrency::future::Race as _,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
    serde::Deserialize,
    std::pin::pin,
    wcn_cluster::smart_contract,
    wcn_node::Config,
    wcn_rpc::{identity::Keypair, server2::ShutdownSignal},
};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

// Each field name in this struct corresponds to the evironment variable
// (upper-cased).
#[derive(Debug, Deserialize)]
struct EnvConfig {
    secret_key: String,

    primary_rpc_server_port: u16,
    secondary_rpc_server_port: u16,
    metrics_server_port: u16,

    database_rpc_server_address: String,
    database_peer_id: String,
    database_primary_rpc_server_port: u16,
    database_secondary_rpc_server_port: u16,

    smart_contract_address: String,
    smart_contract_signer_private_key: Option<String>,
    rpc_provider_url: String,
}

fn main() -> anyhow::Result<()> {
    let _logger = logging::Logger::init(logging::LogFormat::Json, None, None);

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .context("install Prometheus recorder")?;

    for (key, value) in vergen_pretty::vergen_pretty_env!() {
        if let Some(value) = value {
            tracing::warn!(key, value, "build info");
        }
    }

    let version: f64 = include_str!("../../../VERSION")
        .trim_end()
        .parse()
        .map_err(|err| tracing::warn!(?err, "Failed to parse VERSION file"))
        .unwrap_or_default();
    wc::metrics::gauge!("wcn_node_version").set(version);

    let env: EnvConfig = envy::from_env()?;
    let cfg = new_config(&env, prometheus_handle).context("failed to parse config")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            enum FutureOutput {
                CtrlC,
                Node(wcn_node::Result<()>),
            }

            let shutdown_signal = cfg.shutdown_signal.clone();

            let mut shutdown_fut =
                pin!(tokio::signal::ctrl_c().map(|_| FutureOutput::CtrlC).fuse());

            let mut node_fut = pin!(wcn_node::run(cfg).map(FutureOutput::Node));

            loop {
                match (&mut shutdown_fut, &mut node_fut).race().await {
                    FutureOutput::CtrlC => shutdown_signal.emit(),
                    FutureOutput::Node(res) => return res.context("wcn_node::run"),
                }
            }
        })
}

fn new_config(env: &EnvConfig, prometheus_handle: PrometheusHandle) -> anyhow::Result<Config> {
    let secret_key = base64::decode(&env.secret_key).context("SECRET_KEY")?;

    let keypair = Keypair::ed25519_from_bytes(secret_key).context("SECRET_KEY")?;

    let smart_contract_address = env
        .smart_contract_address
        .parse()
        .context("SMART_CONTRACT_ADDRESS")?;

    let smart_contract_signer = env
        .smart_contract_signer_private_key
        .as_ref()
        .map(|key| smart_contract::Signer::try_from_private_key(key))
        .transpose()
        .context("SMART_CONTRACT_SIGNER_PRIVATE_KEY")?;

    let rpc_provider_url = env.rpc_provider_url.parse().context("RPC_PROVIDER_URL")?;

    let database_rpc_server_address = env
        .database_rpc_server_address
        .parse()
        .context("DATABASE_RPC_SERVER_ADDRESS")?;

    let database_peer_id = env.database_peer_id.parse().context("DATABASE_PEER_ID")?;

    Ok(Config {
        keypair,
        primary_rpc_server_port: env.primary_rpc_server_port,
        secondary_rpc_server_port: env.secondary_rpc_server_port,
        metrics_server_port: env.metrics_server_port,
        smart_contract_address,
        smart_contract_signer,
        rpc_provider_url,
        database_rpc_server_address,
        database_peer_id,
        database_primary_rpc_server_port: env.database_primary_rpc_server_port,
        database_secondary_rpc_server_port: env.database_secondary_rpc_server_port,
        shutdown_signal: ShutdownSignal::new(),
        prometheus_handle,
    })
}
