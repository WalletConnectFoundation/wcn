use {anyhow::Context, clap::Parser};
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

pub fn exec() -> anyhow::Result<()> {
    let app = App::parse();

    match app.commands {
        commands::SubCmd::Node(args) => commands::node::exec(args),
        commands::SubCmd::Config(args) => commands::config::exec(args),
        _ => {
            anyhow::bail!("Not supported yet");
        }
    }
}
