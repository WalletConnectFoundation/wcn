use metrics_exporter_prometheus::BuildError;

mod config;
mod decommission;
mod profile;
mod start;
mod status;
mod stop;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

impl From<LogFormat> for wcn_logging::LogFormat {
    fn from(value: LogFormat) -> Self {
        match value {
            LogFormat::Text => Self::Text,
            LogFormat::Json => Self::Json,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct NodeCmd {
    #[command(subcommand)]
    commands: NodeSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum NodeSub {
    /// Starts an WCN Node.
    ///
    /// A node instance requires a separate working directory, where the
    /// operational data will be stored, as well as a configuration file.
    Start(start::StartCmd),

    /// Retrieves the status of an WCN Node.
    ///
    /// The command connects to a running node instance and triggers its
    /// satatus RPC which retrieves the rewards address configured, the
    /// current stake amount, and the network version it's running on.
    Status(status::StatusCmd),

    /// Stops a running WCN Node.
    ///
    /// The command finds a running node instance based on the working
    /// directory, and sends it a termination signal, waiting for the node
    /// process to terminate.
    Stop(stop::StopCmd),

    /// Decommissions an WCN node.
    Decommission(decommission::Cmd),

    /// Run memory profiler on an WCN node.
    Profile(profile::Cmd),
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Working directory is inaccessible")]
    InaccessibleWorkingDir,

    #[error("Invalid working directory")]
    InvalidWorkingDir,

    #[error("Invalid data directory")]
    InvalidDataDir,

    #[error("Invalid consensus directory")]
    InvalidConsensusDir,

    #[error("Failed to lock working directory")]
    PidLock,

    #[error("Failed to load config: {0}")]
    Config(::config::ConfigError),

    #[error("Running node instance not found")]
    InstanceNotFound,

    #[error("Invalid PID of running node instance")]
    InvalidPid,

    #[error("Failed to fork process: error code {0}")]
    Fork(i32),

    #[error("Failed to initialize prometheus: {0:?}")]
    Prometheus(BuildError),
}

pub async fn exec(cmd: NodeCmd) -> anyhow::Result<()> {
    match cmd.commands {
        NodeSub::Start(args) => start::exec(args).await,
        NodeSub::Stop(args) => stop::exec(args).await,
        NodeSub::Status(args) => status::exec(args).await,
        NodeSub::Decommission(args) => decommission::exec(args).await,
        NodeSub::Profile(args) => profile::exec(args).await,
    }
}

struct Lockfile(pidlock::Pidlock);

impl Lockfile {
    fn new() -> Self {
        Self(pidlock::Pidlock::new("wcn.pid"))
    }

    fn acquire(&mut self) -> Result<(), Error> {
        self.0.acquire().map_err(|_| Error::PidLock)?;
        Ok(())
    }

    fn owner(&self) -> Option<u32> {
        self.0.get_owner()
    }
}

impl Drop for Lockfile {
    fn drop(&mut self) {
        if self.0.locked() {
            let _ = self.0.release();
        }
    }
}
