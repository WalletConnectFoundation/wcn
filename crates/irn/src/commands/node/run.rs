use std::net::SocketAddr;

const DEFAULT_ADDR_STR: &str = "127.0.0.1:10999";

#[derive(Debug, clap::Args)]
pub struct RunCmd {
    #[clap(short, long, default_value = DEFAULT_ADDR_STR)]
    addr: SocketAddr,
}

pub fn exec(_args: RunCmd) -> anyhow::Result<()> {
    node::exec()
}
