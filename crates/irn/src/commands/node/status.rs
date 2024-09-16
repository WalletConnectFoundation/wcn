use {irn::Keypair, irn_rpc::Multiaddr};

#[derive(Debug, clap::Args)]
pub struct StatusCmd {
    #[clap(short, long, env = "IRN_ADMIN_API_ADDRESS")]
    address: Multiaddr,

    #[clap(
        short = 'k',
        long = "private_key",
        env = "IRN_ADMIN_API_CLIENT_PRIVATE_KEY"
    )]
    /// Client keypair used for authorization.
    keypair: Keypair,
}

struct StatusClient {
    client: irn_admin_api::Client,
}

impl StatusClient {
    pub fn new(client: irn_admin_api::Client) -> anyhow::Result<Self> {
        Ok(Self { client })
    }

    async fn status(&self) -> anyhow::Result<()> {
        let status = self.client.get_node_status().await?;

        println!("Network Version: {}", status.node_version);
        println!("Wallet Address: {}", status.eth_address.unwrap_or_default());
        println!("Stake: {}", status.stake_amount);

        Ok(())
    }
}

pub async fn exec(cmd: StatusCmd) -> anyhow::Result<()> {
    let api_client = irn_admin_api::Client::new(
        irn_admin_api::client::Config::new(cmd.address).with_keypair(cmd.keypair.0),
    )?;

    let status_client = StatusClient::new(api_client)?;

    status_client.status().await
}
