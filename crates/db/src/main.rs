use {
    anyhow::Context as _,
    futures::{future::FusedFuture as _, FutureExt as _},
    metrics_exporter_prometheus::PrometheusBuilder,
    std::pin,
    wcn_db::{config, Error},
    wcn_rpc::server2::ShutdownSignal,
};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

fn main() -> anyhow::Result<()> {
    let _logger = logging::Logger::init(logging::LogFormat::Json, None, None);

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
    wc::metrics::gauge!("wcn_db_version").set(version);

    let cfg = config::Config::from_env().context("failed to parse config")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let shutdown_signal = ShutdownSignal::new();
            let mut shutdown_fut = pin::pin!(tokio::signal::ctrl_c().fuse());

            let metrics_srv_fut =
                wcn_db::metrics::serve(([0, 0, 0, 0], cfg.metrics_server_port).into(), prometheus);
            let mut metrics_srv_fut = pin::pin!(tokio::spawn(metrics_srv_fut).fuse());

            let db_srv_fut = wcn_db::run(shutdown_signal.clone(), cfg)?;
            let mut db_srv_fut = pin::pin!(db_srv_fut.fuse());

            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown_fut, if !shutdown_fut.is_terminated() => {
                        shutdown_signal.emit();
                    }

                    _ = &mut metrics_srv_fut, if !metrics_srv_fut.is_terminated() => {
                        tracing::warn!("metrics server unexpectedly finished");
                    }

                    _ = &mut db_srv_fut => {
                        tracing::info!("database server stopped");
                        break;
                    }
                }
            }

            Ok(())
        })
}
