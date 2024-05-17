mod node_config;
mod run;
mod stop;

#[derive(Debug, clap::Args)]
pub struct NodeCmd {
    #[command(subcommand)]
    commands: NodeSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum NodeSub {
    /// Starts a IRN Node
    Run(run::RunCmd),

    /// Stops a running IRN Node
    Stop(stop::StopCmd),

    /// Manage a node's configuration
    Config(node_config::NodeConfigCmd),
}

pub fn exec(cmd: NodeCmd) -> anyhow::Result<()> {
    match cmd.commands {
        NodeSub::Run(args) => run::exec(args),
        NodeSub::Stop(args) => stop::exec(args),
        NodeSub::Config(args) => node_config::exec(args),
    }
}
