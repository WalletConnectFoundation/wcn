use {
    crate::{logger::Logger, signal::shutdown_signal, Config},
    anyhow::Context as _,
};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

pub fn exec() -> anyhow::Result<()> {
    let _logger = Logger::init();

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
            crate::run::run(shutdown_signal()?, &cfg).await?.await;
            Ok(())
        })
}
