use clap::Parser;
use anyhow::Context;
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

// TODO: fix issues
pub fn exec() -> anyhow::Result<()> {
    let app = App::parse();

    // TODO: move these to preliminary step
    //
    // let _logger = irn_core::run::Logger::init();
    //
    // for (key, value) in vergen_pretty::vergen_pretty_env!() {
    //     if let Some(value) = value {
    //         tracing::warn!(key, value, "build info");
    //     }
    // }
    //
    // let cfg = irn_core::config::Config::from_env().context("failed to parse config")?;

    match app.commands {
        commands::SubCmd::Node(args) => {
            commands::node::exec(args)
        }

        commands::SubCmd::Config(args) => {
            commands::config::exec(args)
        }

        _ => {
            anyhow::bail!("Not supported yet");
        } 
    }
}
