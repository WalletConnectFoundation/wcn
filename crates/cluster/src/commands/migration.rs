#[derive(Debug, clap::Args)]
pub struct MigrationCmd {
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
    todo!()
}

#[derive(Debug, clap::Args)]
pub struct StartCmd {}

#[derive(Debug, clap::Args)]
pub struct CompleteCmd {}

#[derive(Debug, clap::Args)]
pub struct AbortCmd {}
