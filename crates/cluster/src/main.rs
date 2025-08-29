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

    match app.commands {
        commands::SubCmd::Show(args) => commands::show::exec(args).await,
        commands::SubCmd::Migration(args) => commands::migration::exec(args).await,
        commands::SubCmd::Maintenance(args) => commands::maintenance::exec(args).await,
        commands::SubCmd::Operator(args) => commands::operator::exec(args).await,
        commands::SubCmd::Settings(args) => commands::settings::exec(args).await,
        commands::SubCmd::Deploy(args) => commands::deploy::exec(args).await,
        commands::SubCmd::Key(args) => commands::key::exec(args).await,
        _ => {
            eprintln!("Unsupported command");
            std::process::exit(1);
        }
    }
}
