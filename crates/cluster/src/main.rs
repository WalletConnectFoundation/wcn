use clap::Parser;

mod commands;

/// Cluster management tool for the WCN Network
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

    match app.commands {}
}
