use {
    anyhow::Context as _,
    futures::Future,
    tokio::signal::unix::{self, Signal, SignalKind},
    wcn::fsm::ShutdownReason,
};

pub fn shutdown_listener() -> anyhow::Result<impl Future<Output = ShutdownReason>> {
    let mut sigint = listener(SignalKind::interrupt())?;
    let mut sigterm = listener(SignalKind::terminate())?;
    let mut sigusr1 = listener(SignalKind::user_defined1())?;

    Ok(async move {
        tokio::select! {
            _ = sigint.recv() => ShutdownReason::Restart,
            _ = sigterm.recv() => ShutdownReason::Restart,
            _ = sigusr1.recv() => ShutdownReason::Decommission,
        }
    })
}

pub fn restart(pid: i32) -> anyhow::Result<()> {
    use nix::sys::signal::Signal;

    Ok(nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        Signal::SIGTERM,
    )?)
}

pub fn decommission(pid: i32) -> anyhow::Result<()> {
    use nix::sys::signal::Signal;

    Ok(nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        Signal::SIGUSR1,
    )?)
}

fn listener(kind: SignalKind) -> anyhow::Result<Signal> {
    unix::signal(kind).context("Failed to initialize listener")
}
