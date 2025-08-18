#![allow(warnings)]

#[derive(Debug, clap::Args)]
pub struct ShowCmd {}

pub async fn exec(cmd: ShowCmd) -> anyhow::Result<()> {
    todo!()
}
