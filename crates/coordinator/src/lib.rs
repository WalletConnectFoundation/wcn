use {
    anyhow::Context,
    metrics_exporter_prometheus::{BuildError as PrometheusBuildError, PrometheusBuilder},
};

mod server;

pub mod config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to initialize prometheus: {0:?}")]
    Prometheus(PrometheusBuildError),
}

pub fn main() -> anyhow::Result<()> {
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
    wc::metrics::gauge!("wcn_coordinator_version").set(version);

    let cfg = config::Config::from_env().context("failed to parse config")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            // run()

            tracing::warn!("hello world");

            Ok(())
        })
}
