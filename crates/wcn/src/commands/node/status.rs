use {anyhow::Context as _, wcn::AdminApiArgs};

#[derive(Debug, clap::Args)]
pub struct StatusCmd {
    #[clap(flatten)]
    admin_api_args: AdminApiArgs,
}

pub async fn exec(cmd: StatusCmd) -> anyhow::Result<()> {
    let status = cmd
        .admin_api_args
        .new_client()?
        .get_node_status()
        .await
        .context("wcn_admin_api::Client::get_node_status")?;

    println!("Network Version: {}", status.node_version);
    println!("Wallet Address: {}", status.eth_address.unwrap_or_default());

    Ok(())
}
