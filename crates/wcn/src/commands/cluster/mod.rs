use wcn::AdminApiArgs;

mod view;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(flatten)]
    admin_api_args: AdminApiArgs,

    #[command(subcommand)]
    subcommand: SubCmd,
}

#[derive(clap::Subcommand, Debug)]
pub enum SubCmd {
    /// Shows a view of the WCN cluster.
    View,
}

pub async fn exec(cmd: Cmd) -> anyhow::Result<()> {
    let admin_api_client = cmd.admin_api_args.new_client()?;

    match cmd.subcommand {
        SubCmd::View => view::exec(&admin_api_client).await,
    }
}
