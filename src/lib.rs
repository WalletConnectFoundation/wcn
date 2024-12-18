#![allow(clippy::manual_async_fn)]

use {
    anyhow::Context,
    futures::{
        future::{FusedFuture, OptionFuture},
        FutureExt,
    },
    irn::fsm,
    irn_rpc::quic::{self, socketaddr_to_multiaddr},
    metrics_exporter_prometheus::{
        BuildError as PrometheusBuildError,
        PrometheusBuilder,
        PrometheusHandle,
    },
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, future::Future, io, pin::pin, time::Duration},
    tap::Pipe,
    time::{macros::datetime, OffsetDateTime},
};
pub use {
    cluster::Cluster,
    config::{Config, RocksdbDatabaseConfig},
    consensus::Consensus,
    logger::Logger,
    network::{Network, RemoteNode},
    storage::Storage,
};

pub mod cluster;
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
const NODE_VERSION: u64 = 0;

/// Deadline after which operator nodes that haven't switched to the updated
/// [`NODE_VERSION`] are going to receive reduced rewards.
const NODE_VERSION_UPDATE_DEADLINE: OffsetDateTime = datetime!(2024-07-25 12:00:00 -0);

pub type Node = irn::Node<Consensus, Network, Storage>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid metrics address: {0}")]
    InvalidMetricsAddress(#[from] std::net::AddrParseError),

    #[error("Metrics server error: {0}")]
    MetricsServer(#[from] hyper::Error),

    #[error("Failed to start Client API server: {0:?}")]
    ClientApiServer(#[from] client_api::server::ServeError),

    #[error("Failed to start Admin API server: {0:?}")]
    AdminApiServer(#[from] admin_api::server::Error),

    #[error("Failed to initialize networking: {0:?}")]
    Network(#[from] quic::Error),

    #[error("Failed to initialize storage: {0:?}")]
    Storage(storage::Error),

    #[error("Failed to initialize Consensus: {0:?}")]
    Consensus(consensus::InitializationError),

    #[error("Failed to interact with smart contract: {0:?}")]
    Contract(anyhow::Error),

    #[error("Failed to initialize performance tracker: {0:?}")]
    PerformanceTracker(anyhow::Error),

    #[error("Status reporter error: {0:?}")]
    StatusReporter(anyhow::Error),

    #[error("Failed to initialize prometheus: {0:?}")]
    Prometheus(PrometheusBuildError),

    #[error("Failed to read network interfaces: {0:?}")]
    ReadNetworkInterfaces(io::Error),

    #[error("Failed to find a public IP of this node")]
    NoPublicIp,
}

#[cfg(feature = "memory_profiler")]
mod alloc {
    use wc::alloc::{self, profiler};

    #[global_allocator]
    static GLOBAL: profiler::Alloc<alloc::Jemalloc, profiler::JemallocSingleBinFilter> =
        profiler::Alloc::new(alloc::Jemalloc, profiler::JemallocSingleBinFilter::new(160));
}

#[cfg(not(feature = "memory_profiler"))]
mod alloc {
    #[global_allocator]
    static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;
}

pub fn exec() -> anyhow::Result<()> {
    let _logger = Logger::init(logger::LogFormat::Json, None, None);

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .map_err(Error::Prometheus)?;

    let cfg = Config::from_env().context("failed to parse config")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            run(signal::shutdown_listener()?, prometheus, &cfg)
                .await?
                .await;
            Ok(())
        })
}

pub async fn run(
    shutdown_fut: impl Future<Output = fsm::ShutdownReason> + Send,
    prometheus: PrometheusHandle,
    cfg: &Config,
) -> Result<impl Future<Output = ()> + Send, Error> {
    let vergen = vergen_pretty::vergen_pretty_env!();
    let vergen_attr = |key| vergen.get(key).copied().flatten().unwrap_or_default();

    for (key, value) in &vergen {
        if let Some(value) = value {
            tracing::warn!(key, value, "build info");
        }
    }

    // TODO: populate `Node` state with it once all nodes in the networks are
    // updated.
    let _node_version = format!(
        "{}-{}",
        vergen_attr("VERGEN_GIT_COMMIT_DATE"),
        vergen_attr("VERGEN_GIT_SHA")
    );

    let storage = Storage::new(cfg).map_err(Error::Storage)?;
    let network = Network::new(cfg)?;

    tracing::info!(addr = %cfg.server_addr, node_id = %cfg.id, "Running");

    let stake_validator = if let Some(c) = &cfg.smart_contract {
        let rpc_url = &c.eth_rpc_url;
        let addr = &c.config_address;

        contract::StakeValidator::new(rpc_url, addr)
            .await
            .map(Some)
            .map_err(Error::Contract)?
    } else {
        None
    };

    let consensus = Consensus::new(cfg, network.clone(), stake_validator)
        .await
        .map_err(Error::Consensus)?;

    let (performance_tracker, status_reporter) = if let Some(c) = &cfg.smart_contract {
        let rpc_url = &c.eth_rpc_url;
        let addr = &c.config_address;

        let pr = if let Some(pr) = &c.performance_reporter {
            let dir = pr.tracker_dir.clone();

            let reporter = contract::new_performance_reporter(rpc_url, addr, &pr.signer_mnemonic)
                .await
                .map_err(Error::Contract)?;

            performance::Tracker::new(
                network.clone(),
                consensus.clone(),
                reporter,
                dir,
                NODE_VERSION,
                NODE_VERSION_UPDATE_DEADLINE,
            )
            .await
            .map(Some)
            .map_err(Error::PerformanceTracker)?
        } else {
            None
        };

        let sr = if let Some(eth_address) = &cfg.eth_address {
            Some(
                contract::new_status_reporter(rpc_url, addr, eth_address)
                    .await
                    .map_err(Error::Contract)?,
            )
        } else {
            None
        };

        (pr, sr)
    } else {
        (None, None)
    };

    let node_opts = irn::NodeOpts {
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

    let node = irn::Node::new(
        cluster::Node {
            id: cfg.id,
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.replica_api_server_port)),
            region: cfg.region,
            organization: cfg.organization.clone(),
            eth_address: cfg.eth_address.clone(),
            version: None,
        },
        node_opts,
        consensus,
        network,
        storage,
    );

    Network::spawn_servers(cfg, node.clone(), prometheus.clone(), status_reporter)?;

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
