use {
    std::path::PathBuf,
    tracing_appender::{
        non_blocking::WorkerGuard,
        rolling::{RollingFileAppender, Rotation},
    },
    tracing_subscriber::{prelude::*, EnvFilter},
};

/// The default log level for the stderr logger, which is used as a fallback if
/// no other can be found.
const DEFAULT_LOG_LEVEL_STDERR: tracing::Level = tracing::Level::INFO;

/// The environment variable used to control the stderr logger.
const ENV_LOG_LEVEL_STDERR: &str = "LOG_LEVEL";

pub enum LogFormat {
    Text,
    Json,
}

pub struct Logger {
    _guard: Option<WorkerGuard>,
}

impl Logger {
    pub fn init(format: LogFormat, filter: Option<&str>, log_file: Option<PathBuf>) -> Self {
        let filter = filter
            .map(EnvFilter::new)
            .or_else(|| EnvFilter::try_from_env(ENV_LOG_LEVEL_STDERR).ok())
            .unwrap_or_else(|| EnvFilter::new(DEFAULT_LOG_LEVEL_STDERR.to_string()));

        let (logger, guard) = match format {
            LogFormat::Json => {
                let layer = tracing_subscriber::fmt::layer()
                    .event_format(
                        tracing_subscriber::fmt::format()
                            .json()
                            .with_span_list(true)
                            .with_current_span(false),
                    )
                    .fmt_fields(tracing_subscriber::fmt::format::JsonFields::default())
                    .with_target(true);

                if let Some(log_file) = log_file {
                    (
                        layer
                            .with_writer(init_file_writer(log_file))
                            .with_filter(filter)
                            .boxed(),
                        None,
                    )
                } else {
                    let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());

                    (
                        layer.with_writer(writer).with_filter(filter).boxed(),
                        Some(guard),
                    )
                }
            }

            LogFormat::Text => {
                let layer = tracing_subscriber::fmt::layer()
                    .event_format(tracing_subscriber::fmt::format())
                    .with_target(true)
                    .with_ansi(false);

                if let Some(log_file) = log_file {
                    (
                        layer
                            .with_writer(init_file_writer(log_file))
                            .with_filter(filter)
                            .boxed(),
                        None,
                    )
                } else {
                    let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());

                    (
                        layer.with_writer(writer).with_filter(filter).boxed(),
                        Some(guard),
                    )
                }
            }
        };

        tracing_subscriber::registry().with(logger).init();

        Self { _guard: guard }
    }

    pub fn stop(self) {
        // Consume self to trigger drop.
    }
}

fn init_file_writer(mut log_file: PathBuf) -> RollingFileAppender {
    let file_name = log_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("irn.log")
        .to_string();

    // Turn file name into directory name.
    log_file.pop();

    RollingFileAppender::new(Rotation::NEVER, &log_file, file_name)
}
