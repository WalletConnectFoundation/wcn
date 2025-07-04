use libp2p::PeerId;
use wcn_cluster::smart_contract::Write;

use crate::commands::SharedArgs;

#[derive(Debug, clap::Args)]
pub struct MigrationCmd {
    #[clap(flatten)]
    shared_args: SharedArgs,

    #[command(subcommand)]
    commands: MigrationSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum MigrationSub {
    Start(StartCmd),
    Complete(CompleteCmd),
    Abort(AbortCmd),
}

pub async fn exec(cmd: MigrationCmd) -> anyhow::Result<()> {
    let client = cmd.shared_args.new_client().await?;

    match cmd.commands {
        MigrationSub::Start(_) => {
            todo!()
        }
        MigrationSub::Complete(_) => {
            todo!()
        }
        MigrationSub::Abort(_) => {
            todo!()
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct StartCmd {}

#[derive(Debug, clap::Args)]
pub struct CompleteCmd {}

#[derive(Debug, clap::Args)]
pub struct AbortCmd {}
