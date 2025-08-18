#![allow(warnings)]

#[derive(Debug, clap::Args)]
pub struct OperatorCmd {
    #[command(subcommand)]
    commands: OperatorSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum OperatorSub {
    Add(AddCmd),
    Update(UpdateCmd),
    Remove(RemoveCmd),
}

pub async fn exec(cmd: OperatorCmd) -> anyhow::Result<()> {
    todo!()
}

#[derive(Debug, clap::Args)]
pub struct AddCmd {}

#[derive(Debug, clap::Args)]
pub struct UpdateCmd {}

#[derive(Debug, clap::Args)]
pub struct RemoveCmd {}
