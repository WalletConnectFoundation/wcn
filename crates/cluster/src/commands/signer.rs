#![allow(warnings)]

#[derive(Debug, clap::Args)]
pub struct SignerCmd {}

pub async fn exec(cmd: SignerCmd) -> anyhow::Result<()> {
    todo!()
}
