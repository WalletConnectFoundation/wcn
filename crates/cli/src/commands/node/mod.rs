mod node_config;
mod run;

#[derive(Debug, clap::Args)]
pub struct NodeCmd {
    // TODO: add node flags to be inherited by subcommands here
    #[command(subcommand)]
    commands: NodeSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum NodeSub {
    /// Starts a IRN Node
    Run(run::RunCmd),
    /// Manage a node's configuration
    Config(node_config::NodeConfigCmd),
}

pub fn exec(cmd: NodeCmd) -> anyhow::Result<()> {
    match cmd.commands {
        NodeSub::Run(args) => run::exec(args),

        NodeSub::Config(args) => node_config::exec(args),

        _ => {
            anyhow::bail!("Not supported yet");
        }
    }
}
