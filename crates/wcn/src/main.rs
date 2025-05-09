use clap::Parser;

mod commands;

/// Control nodes and clusters in the WCN Network
#[derive(clap::Parser, Debug)]
#[clap(
    author,
    version,
    about,
    long_about = None,
    arg_required_else_help(true)
)]
pub struct App {
    #[command(subcommand)]
    commands: commands::SubCmd,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    match app.commands {
        commands::SubCmd::Cluster(args) => commands::cluster::exec(args).await,
        commands::SubCmd::Node(args) => commands::node::exec(args).await,
        // commands::SubCmd::Config(args) => commands::config::exec(args),
        commands::SubCmd::Key(args) => commands::key::exec(args),
        commands::SubCmd::Storage(args) => commands::storage::exec(args).await,
    }
}
