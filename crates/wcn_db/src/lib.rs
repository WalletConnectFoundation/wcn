#![allow(clippy::manual_async_fn)]

use {
    anyhow::Context,
    futures::{future::FusedFuture, FutureExt},
    metrics_exporter_prometheus::{
        BuildError as PrometheusBuildError, PrometheusBuilder, PrometheusHandle,
    },
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, future::Future, io, pin::pin, time::Duration},
    tap::Pipe,
    // wcn::fsm,
    // wcn_rpc::quic::{self, socketaddr_to_multiaddr},
};
pub use {logger::Logger, storage::Storage};

pub mod logger;
pub mod metrics;
pub mod signal;
pub mod storage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid metrics address: {0}")]
    InvalidMetricsAddress(#[from] std::net::AddrParseError),

    #[error("Metrics server error: {0}")]
    MetricsServer(#[from] hyper::Error),

    // #[error("Failed to start Client API server: {0:?}")]
    // ClientApiServer(#[from] client_api::server::ServeError),
    #[error("Failed to start Admin API server: {0:?}")]
    AdminApiServer(#[from] admin_api::server::Error),

    // #[error("Failed to initialize networking: {0:?}")]
    // Network(#[from] quic::Error),
    #[error("Failed to initialize Migration API client: {0:?}")]
    MigrationApiClient(#[from] migration_api::client::CreationError),

    #[error("Failed to initialize storage: {0:?}")]
    Storage(storage::Error),

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

pub fn exec() -> anyhow::Result<()> {
    let _logger = Logger::init(logger::LogFormat::Json, None, None);

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .map_err(Error::Prometheus)?;

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

    // wc::metrics::gauge!("wcn_node_version").set(version);
    //
    // let cfg = Config::from_env().context("failed to parse config")?;
    //
    // tokio::runtime::Builder::new_multi_thread()
    //     .enable_all()
    //     .build()
    //     .unwrap()
    //     .block_on(async move {
    //         run(signal::shutdown_listener()?, prometheus, &cfg)
    //             .await?
    //             .await;
    //         Ok(())
    //     })
    todo!()
}
