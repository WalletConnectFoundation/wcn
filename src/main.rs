use {
    anyhow::Context as _,
    futures::Future,
    irn::ShutdownReason,
    lib::Config,
    std::pin::pin,
    tokio::signal::unix::{self, Signal, SignalKind},
    tracing_appender::non_blocking::WorkerGuard,
    tracing_subscriber::{prelude::*, EnvFilter},
    wc::metrics::otel,
};

#[global_allocator]
static GLOBAL: wc::alloc::Jemalloc = wc::alloc::Jemalloc;

/// The default log level for the stderr logger, which is used as a fallback if
/// no other can be found.
const DEFAULT_LOG_LEVEL_STDERR: tracing::Level = tracing::Level::WARN;

/// The environment variable used to control the stderr logger.
const ENV_LOG_LEVEL_STDERR: &str = "LOG_LEVEL";

pub struct Logger {
    _guard: WorkerGuard,
}

impl Logger {
    pub fn init() -> Self {
        let stderr_filter = EnvFilter::try_from_env(ENV_LOG_LEVEL_STDERR)
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL_STDERR.to_string()));

        let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());

        let fmt = tracing_subscriber::fmt::format()
            .json()
            .with_span_list(true)
            .with_current_span(false);

        let logger = tracing_subscriber::fmt::layer()
            .event_format(fmt)
            .fmt_fields(tracing_subscriber::fmt::format::JsonFields::default())
            .with_target(true)
            .with_ansi(atty::is(atty::Stream::Stderr))
            .with_writer(writer)
            .with_filter(stderr_filter)
            .boxed();

        tracing_subscriber::registry().with(logger).init();

        Self { _guard: guard }
    }

    pub fn stop(self) {
        // Consume self to trigger drop.
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        otel::global::shutdown_tracer_provider();
    }
}

fn main() -> anyhow::Result<()> {
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
        .block_on(async move { Ok(lib::run(shutdown_signal()?, &cfg).await?.await) })
}

fn shutdown_signal() -> anyhow::Result<impl Future<Output = ShutdownReason>> {
    let mut sigterm = signal_listener(SignalKind::terminate())?;
    let mut sigusr1 = signal_listener(SignalKind::user_defined1())?;

    Ok(async move {
        let mut sigterm = pin!(sigterm.recv());
        let mut sigusr1 = pin!(sigusr1.recv());

        tokio::select! {
            _ = &mut sigterm => ShutdownReason::Restart,
            _ = &mut sigusr1 => ShutdownReason::Decommission,
        }
    })
}

fn signal_listener(kind: SignalKind) -> anyhow::Result<Signal> {
    unix::signal(kind).context("Failed to initialize {kind:?} listener")
}
