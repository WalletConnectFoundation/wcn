#![allow(warnings)]

#[derive(Debug, clap::Args)]
pub struct MaintenanceCmd {
    #[command(subcommand)]
    commands: MaintenanceSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum MaintenanceSub {
    Start(StartCmd),
    Finish(FinishCmd),
}

pub async fn exec(cmd: MaintenanceCmd) -> anyhow::Result<()> {
    todo!()
}

#[derive(Debug, clap::Args)]
pub struct StartCmd {}

#[derive(Debug, clap::Args)]
pub struct FinishCmd {}
