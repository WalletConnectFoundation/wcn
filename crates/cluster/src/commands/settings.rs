#![allow(warnings)]

#[derive(Debug, clap::Args)]
pub struct SettingsCmd {
    #[command(subcommand)]
    commands: SettingsSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum SettingsSub {
    Update(UpdateCmd),
}

pub async fn exec(cmd: SettingsCmd) -> anyhow::Result<()> {
    todo!()
}

#[derive(Debug, clap::Args)]
pub struct UpdateCmd {}
