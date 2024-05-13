use clap::Parser;
mod commands;

/// Control nodes and clusters in the IRN Network
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

fn main() -> anyhow::Result<()> {
    let app = App::parse();

    match app.commands {
        commands::SubCmd::Node(args) => commands::node::exec(args),
        commands::SubCmd::Config(args) => commands::config::exec(args),
        commands::SubCmd::Key(args) => commands::key::exec(args),
    }
}
