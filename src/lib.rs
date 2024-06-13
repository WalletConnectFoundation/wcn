use {
    anyhow::Context as _,
    futures::{
        future::{FusedFuture, OptionFuture},
        FutureExt,
    },
    irn::ShutdownReason,
    metrics_exporter_prometheus::PrometheusBuilder,
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, future::Future, pin::pin, time::Duration},
    tap::Pipe,
};
pub use {
    config::Config,
    consensus::Consensus,
    logger::Logger,
    network::{Multiaddr, Multihash, Network, RemoteNode},
    storage::Storage,
};

pub mod config;
pub mod consensus;
pub mod logger;
pub mod metrics;
pub mod network;
pub mod signal;
pub mod storage;

mod contract;
mod performance;

/// Version of the node in the testnet.
/// For "performance" tracking purposes only.
const NODE_VERSION: u64 = 1;

pub type Node = irn::Node<Consensus, Network, Storage>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid metrics address: {0}")]
    InvalidMetricsAddress(#[from] std::net::AddrParseError),

    #[error("Metrics server error: {0}")]
    MetricsServer(#[from] hyper::Error),

    #[error("Failed to start API server: {0:?}")]
    ApiServer(#[from] api::Error),

    #[error("Failed to initialize networking: {0:?}")]
    Network(#[from] ::network::Error),

    #[error("Failed to initialize storage: {0:?}")]
    Storage(storage::Error),

    #[error("Failed to initialize Consensus: {0:?}")]
    Consensus(consensus::InitializationError),

    #[error("Failed to interact with smart contract: {0:?}")]
    Contract(anyhow::Error),

    #[error("Failed to initialize performance tracker: {0:?}")]
    PerformanceTracker(anyhow::Error),
}

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

pub fn exec() -> anyhow::Result<()> {
    let _logger = Logger::init(logger::LogFormat::Json, None, None);

    for (key, value) in vergen_pretty::vergen_pretty_env!() {
        if let Some(value) = value {
            tracing::warn!(key, value, "build info");
        }
    }

    let cfg = Config::from_env().context("failed to parse config")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            run(signal::shutdown_listener()?, &cfg).await?.await;
            Ok(())
        })
}

pub async fn run(
    shutdown_fut: impl Future<Output = ShutdownReason>,
    cfg: &Config,
) -> Result<impl Future<Output = ()>, Error> {
    let storage = Storage::new(cfg).map_err(Error::Storage)?;
    let network = Network::new(cfg)?;

    let (stake_validator, performance_tracker) = if let Some(c) = &cfg.smart_contract {
        let rpc_url = &c.eth_rpc_url;
        let addr = &c.config_address;

        let sv = contract::StakeValidator::new(rpc_url, addr)
            .await
            .map(Some)
            .map_err(Error::Contract)?;

        let pt = if let Some(p) = &c.performance_reporter {
            let dir = p.tracker_dir.clone();

            let reporter = contract::new_performance_reporter(rpc_url, addr, &p.signer_mnemonic)
                .await
                .map_err(Error::Contract)?;

            performance::Tracker::new(network.clone(), reporter, dir, NODE_VERSION)
                .await
                .map(Some)
                .map_err(Error::PerformanceTracker)?
        } else {
            None
        };

        (sv, pt)
    } else {
        (None, None)
    };

    let consensus = Consensus::new(cfg, network.clone(), stake_validator)
        .await
        .map_err(Error::Consensus)?;

    consensus.init(cfg).await.map_err(Error::Consensus)?;

    let node_opts = irn::NodeOpts {
        replication_strategy: cfg.replication_strategy.clone(),
        replication_request_timeout: Duration::from_millis(cfg.replication_request_timeout),
        replication_concurrency_limit: cfg.request_concurrency_limit,
        replication_request_queue: cfg.request_limiter_queue,
        warmup_delay: Duration::from_millis(cfg.warmup_delay),
        authorization: cfg
            .authorized_clients
            .as_ref()
            .map(|ids| irn::AuthorizationOpts {
                allowed_coordinator_clients: ids.clone(),
            }),
    };

    let node = irn::Node::new(cfg.id, node_opts, consensus, network, storage);

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder");

    Network::spawn_servers(cfg, node.clone(), Some(prometheus.clone()))?;

    let metrics_srv = metrics::serve(cfg.clone(), node.clone(), prometheus)?.pipe(tokio::spawn);

    let node_clone = node.clone();
    let node_fut = async move {
        match node.clone().run().await {
            Ok(shutdown_reason) => node.consensus().shutdown(shutdown_reason).await,
            Err(err) => tracing::warn!(?err, "Node::run"),
        };
    };

    let performance_tracker_fut: OptionFuture<_> = if let Some(pt) = performance_tracker {
        Some(pt.run()).into()
    } else {
        None.into()
    };

    Ok(async move {
        let mut shutdown_fut = pin!(shutdown_fut.fuse());
        let mut metrics_server_fut = pin!(metrics_srv.fuse());
        let mut node_fut = pin!(node_fut);
        let mut performance_tracker_fut = pin!(performance_tracker_fut.fuse());

        loop {
            tokio::select! {
                biased;

                reason = &mut shutdown_fut, if !shutdown_fut.is_terminated() => {
                    if let Err(err) = node_clone.shutdown(reason) {
                        tracing::warn!("{err}");
                    }
                }

                _ = &mut metrics_server_fut, if !metrics_server_fut.is_terminated() => {
                    tracing::warn!("metrics server unexpectedly finished");
                }

                _ = &mut node_fut => {
                    break;
                }

                _ = &mut performance_tracker_fut, if !performance_tracker_fut.is_terminated() => {
                    tracing::warn!("performance tracker unexpectedly finished");
                }
            }
        }
    })
}

#[derive(
    Clone, Copy, Debug, Default, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct TypeConfig;
