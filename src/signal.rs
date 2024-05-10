use {
    anyhow::Context as _,
    futures::Future,
    irn::ShutdownReason,
    std::pin::pin,
    tokio::signal::unix::{self, Signal, SignalKind},
};

pub(crate) fn shutdown() -> anyhow::Result<impl Future<Output = ShutdownReason>> {
    let mut sigterm = listener(SignalKind::terminate())?;
    let mut sigusr1 = listener(SignalKind::user_defined1())?;

    Ok(async move {
        let mut sigterm = pin!(sigterm.recv());
        let mut sigusr1 = pin!(sigusr1.recv());

        tokio::select! {
            _ = &mut sigterm => ShutdownReason::Restart,
            _ = &mut sigusr1 => ShutdownReason::Decommission,
        }
    })
}

fn listener(kind: SignalKind) -> anyhow::Result<Signal> {
    unix::signal(kind).context("Failed to initialize {kind:?} listener")
}
