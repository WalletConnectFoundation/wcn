use std::net::SocketAddr;

const DEFAULT_ADDR_STR: &str = "127.0.0.1:10999";

#[derive(Debug, clap::Args)]
pub struct RunCmd {
    #[clap(short, long, default_value = DEFAULT_ADDR_STR)]
    addr: SocketAddr,

    #[clap(short, long)]
    dettached: bool,
}

pub fn exec(args: RunCmd) -> anyhow::Result<()> {
    if args.dettached {
        run_dettached()
    } else {
        run_blocking()
    }
}

fn run_blocking() -> anyhow::Result<()> {
    node::exec()
}


fn run_dettached() -> anyhow::Result<()> {
    // TODO: run in background
    node::exec()
}
