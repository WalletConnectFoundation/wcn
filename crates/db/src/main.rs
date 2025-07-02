use {
    anyhow::Context as _,
    futures::{future::FusedFuture as _, FutureExt as _},
    metrics_exporter_prometheus::PrometheusBuilder,
    std::pin,
    tap::Pipe,
    tokio::sync::oneshot,
    wcn_db::{config, Error},
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

    let metrics_srv_fut =
        wcn_db::metrics::serve(([0, 0, 0, 0], cfg.metrics_port).into(), prometheus)
            .context("failed to start metrics server")?
            .pipe(tokio::spawn);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let mut shutdown_tx = Some(shutdown_tx);
            let mut shutdown_fut = pin::pin!(tokio::signal::ctrl_c().fuse());
            let mut db_srv_fut = pin::pin!(wcn_db::run(shutdown_rx, cfg).fuse());
            let mut metrics_srv_fut = pin::pin!(metrics_srv_fut.fuse());

            loop {
                tokio::select! {
                    biased;

                    _ = &mut shutdown_fut, if !shutdown_fut.is_terminated() => {
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                    }

                    _ = &mut metrics_srv_fut, if !metrics_srv_fut.is_terminated() => {
                        tracing::warn!("metrics server unexpectedly finished");
                    }

                    res = &mut db_srv_fut => {
                        if let Err(err) = res {
                            tracing::error!(?err, "database server stopped with error");
                        } else {
                            tracing::info!("database server stopped");
                        }

                        break;
                    }
                }
            }

            Ok(())
        })
}
