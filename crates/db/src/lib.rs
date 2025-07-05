use {
    crate::{config::Config, storage::Storage},
    metrics_exporter_prometheus::BuildError as PrometheusBuildError,
    std::{future::Future, io},
    tokio::sync::oneshot,
    wcn_rpc::server2::Server,
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

pub async fn run(
    shutdown_rx: oneshot::Receiver<()>,
    cfg: Config,
) -> Result<impl Future<Output = ()> + Send, Error> {
    let id = cfg.id();

    tracing::info!(port = %cfg.db_port, %id, "starting database server");

    let storage = Storage::new(&cfg)?;

    let metrics_guard = metrics::run_update_loop(metrics::Config {
        rocksdb_dir: cfg.rocksdb_dir.clone(),
        rocksdb: cfg.rocksdb.enable_metrics.then(|| storage.db().clone()),
    });

    let db_srv_fut = storage_api::rpc::server::database(server::Server::new(storage)).serve(
        wcn_rpc::server2::Config {
            name: "db",
            port: cfg.db_port,
            keypair: cfg.keypair,
            connection_timeout: cfg.connection_timeout,
            max_connections: cfg.max_connections,
            max_connections_per_ip: cfg.max_connections_per_ip,
            max_connection_rate_per_ip: cfg.max_connection_rate_per_ip,
            max_concurrent_rpcs: cfg.max_concurrent_rpcs,
            priority: wcn_rpc::transport::Priority::High,
        },
    )?;

    Ok(async move {
        let _metrics_guard = metrics_guard;

        // TODO: Should there be any extra logic, e.g. shutdown reason?
        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!("shutdown signal received");
            }

            _ = db_srv_fut => {
                tracing::info!("database server stopped");
            }
        }
    })
}
