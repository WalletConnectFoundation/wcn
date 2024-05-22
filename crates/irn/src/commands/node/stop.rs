use {
    super::{Error, Lockfile},
    std::time::Duration,
};

#[derive(Debug, clap::Args)]
pub struct StopCmd {
    #[clap(short, long)]
    working_dir: Option<String>,

    #[clap(short, long)]
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
