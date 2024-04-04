use std::net::{SocketAddr, SocketAddrV4};

const DEFAULT_ADDR_STR: &str = "127.0.0.1:10999";

#[derive(Debug, clap::Args)]
pub struct RunCmd {
    #[clap(short, long, default_value = DEFAULT_ADDR_STR)]
    addr: SocketAddr,
}

#[tracing::instrument(skip(args))]
pub fn exec(args: RunCmd) -> anyhow::Result<()> {
    irn_core::run::run()
}
