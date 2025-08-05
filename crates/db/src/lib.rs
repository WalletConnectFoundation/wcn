use {
    crate::{config::Config, storage::Storage},
    futures_concurrency::future::Join as _,
    metrics_exporter_prometheus::BuildError as PrometheusBuildError,
    std::{future::Future, io},
    wcn_rpc::server2::{Server, ShutdownSignal},
};

pub mod metrics;
mod server;
mod storage;

pub mod config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to initialize prometheus: {0:?}")]
    Prometheus(PrometheusBuildError),

    #[error("Failed to initialize storage: {0:?}")]
    Storage(#[from] storage::Error),

    #[error("Metrics server error: {0}")]
    MetricsServer(io::Error),

    #[error("Database server error: {0}")]
    DatabaseServer(#[from] wcn_rpc::server2::Error),
}

pub fn run(
    shutdown_signal: ShutdownSignal,
    cfg: Config,
) -> Result<impl Future<Output = ()> + Send, Error> {
    let id = cfg.id();

    tracing::info!(ports = ?[cfg.primary_rpc_server_port, cfg.secondary_rpc_server_port], %id, "starting database server");

    let storage = Storage::new(&cfg)?;

    let metrics_guard = metrics::run_update_loop(metrics::Config {
        rocksdb_dir: cfg.rocksdb_dir.clone(),
        rocksdb: cfg.rocksdb.enable_metrics.then(|| storage.db().clone()),
    });

    let primary_rpc_server_cfg = wcn_rpc::server2::Config {
        name: "primary",
        port: cfg.primary_rpc_server_port,
        keypair: cfg.keypair.clone(),
        connection_timeout: cfg.connection_timeout,
        max_connections: cfg.max_connections,
        max_connections_per_ip: cfg.max_connections_per_ip,
        max_connection_rate_per_ip: cfg.max_connection_rate_per_ip,
        max_concurrent_rpcs: cfg.max_concurrent_rpcs,
        priority: wcn_rpc::transport::Priority::High,
        shutdown_signal: shutdown_signal.clone(),
    };

    let secondary_rpc_server_cfg = wcn_rpc::server2::Config {
        name: "secondary",
        port: cfg.secondary_rpc_server_port,
        keypair: cfg.keypair,
        connection_timeout: cfg.connection_timeout,
        max_connections: cfg.max_connections,
        max_connections_per_ip: cfg.max_connections_per_ip,
        max_connection_rate_per_ip: cfg.max_connection_rate_per_ip,
        max_concurrent_rpcs: cfg.max_concurrent_rpcs,
        priority: wcn_rpc::transport::Priority::Low,
        shutdown_signal,
    };

    let primary_rpc_server_fut =
        storage_api::rpc::server::database(server::Server::new(storage.clone()))
            .serve(primary_rpc_server_cfg)?;

    let secondary_rpc_server_fut = storage_api::rpc::server::database(server::Server::new(storage))
        .serve(secondary_rpc_server_cfg)?;

    Ok(async move {
        let _metrics_guard = metrics_guard;

        (primary_rpc_server_fut, secondary_rpc_server_fut)
            .join()
            .await;
    })
}
