use {anyhow::Context as _, irn::AdminApiArgs, irn_rpc::PeerId};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(flatten)]
    admin_api_args: AdminApiArgs,

    /// ID of the node to decommission.
    #[clap(long)]
    id: PeerId,

    /// If set the node is going to be decommissioned even if it's not in the
    /// `Normal` state.
    #[clap(long, short, action)]
    force: bool,
}

pub async fn exec(cmd: Cmd) -> anyhow::Result<()> {
    cmd.admin_api_args
        .new_client()?
        .decommission_node(cmd.id, cmd.force)
        .await
        .context("irn_admin_api::Client::decommission_node")?;

    println!("Decommissioning");

    Ok(())
}
