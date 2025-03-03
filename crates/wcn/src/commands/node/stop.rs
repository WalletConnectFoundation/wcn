use {
    super::{Error, Lockfile},
    std::time::Duration,
};

#[derive(Debug, clap::Args)]
pub struct StopCmd {
    #[clap(short, long)]
    /// Working directory for this node instance.
    ///
    /// Each running instance should use its own working directory, and the
    /// directories are locked to that node's process ID.
    working_dir: Option<String>,

    #[clap(short, long)]
    /// Whether the node is being restarted or fully decommissionned.
    ///
    /// By default, stopping the node would fully decommission it, removing it
    /// entirely from the cluster. Setting this flag to `true` would mean that
    /// the node will be stopped without being removed from the cluster. The
    /// downtime of a node should be as short as possible, as nodes in the
    /// cluster can only be restarted one at a time.
    ///
    /// The flag should be used very carefully, since if the node configuration
    /// is lost, it would not be able to rejoin the cluster. This can
    /// potentially lead to a broken cluster state.
    ///
    /// Note that fully a decommissionned node can not be started again, meaning
    /// the working directory can not be reused.
    restart: bool,
}

pub async fn exec(args: StopCmd) -> anyhow::Result<()> {
    if let Some(working_dir) = &args.working_dir {
        std::fs::create_dir_all(working_dir).map_err(|_| Error::InvalidWorkingDir)?;
        std::env::set_current_dir(working_dir).map_err(|_| Error::InaccessibleWorkingDir)?;
    }

    let _logger = node::Logger::init(node::logger::LogFormat::Text, Some("info"), None);

    let pid = Lockfile::new().owner().ok_or(Error::InstanceNotFound)?;

    tracing::info!(pid, "running node instance found");

    let pid = pid.try_into().map_err(|_| Error::InvalidPid)?;

    if args.restart {
        node::signal::restart(pid)?;
    } else {
        node::signal::decommission(pid)?;
    }

    tracing::info!("signal sent. awaiting process termination");

    let pid = nix::unistd::Pid::from_raw(pid);

    while nix::sys::signal::kill(pid, None).is_ok() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    tracing::info!("node process has been terminated");

    Ok(())
}
