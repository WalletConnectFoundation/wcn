#![allow(warnings)]

use wcn_cluster::EncryptionKey;

mod update;

#[derive(Debug, clap::Args)]
pub struct OperatorCmd {
    #[command(flatten)]
    shared_args: super::SharedArgs,

    #[command(subcommand)]
    commands: OperatorSub,
}

#[derive(clap::Subcommand, Debug)]
pub enum OperatorSub {
    Add(AddCmd),
    Update(update::UpdateCmd),
    Remove(RemoveCmd),
}

pub async fn exec(cmd: OperatorCmd) -> anyhow::Result<()> {
    let client = cmd.shared_args.new_client().await?;

    let encryption_key = EncryptionKey::from_hex(&cmd.shared_args.encryption_key)
        .map_err(|e| anyhow::anyhow!("Failed to decode encryption key: {}", e))?;

    match cmd.commands {
        OperatorSub::Update(cmd) => update::exec(cmd, client, encryption_key).await,
        _ => {
            todo!()
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct AddCmd {}

#[derive(Debug, clap::Args)]
pub struct RemoveCmd {}
