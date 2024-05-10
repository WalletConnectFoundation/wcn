use std::net::SocketAddr;

const DEFAULT_ADDR_STR: &str = ":10999";

#[derive(Debug, clap::Args)]
pub struct RunCmd {
    #[clap(short, long, default_value = DEFAULT_ADDR_STR)]
    addr: SocketAddr,
}

#[tracing::instrument(skip(_args))]
pub fn exec(_args: RunCmd) -> anyhow::Result<()> {
    node::exec()
}
