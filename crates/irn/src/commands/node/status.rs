use {
    irn::PrivateKey,
    irn_api::{client, Client},
    std::{net::SocketAddr, time::Duration},
};

#[derive(Debug, clap::Args)]
pub struct StatusCmd {
    #[clap(short, long, env = "IRN_STORAGE_ADDRESS")]
    address: SocketAddr,

    #[clap(short, long, env = "IRN_STORAGE_PRIVATE_KEY")]
    /// Client private key used for authorization.
    private_key: PrivateKey,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Failed to run health check: {0}")]
    Client(#[from] client::Error),

    #[error("Failed to write data to stdout")]
    Io(#[from] std::io::Error),
}

struct StatusClient {
    client: Client,
}

impl StatusClient {
    pub fn new(client: Client) -> anyhow::Result<Self> {
        Ok(Self { client })
    }

    async fn status(&self) -> anyhow::Result<()> {
        let status = self.client.status().await?;

        println!("Network Version: {}", status.node_version);
        println!("Wallet Address: {}", status.eth_address.unwrap_or_default());
        println!("Stake: {}", status.stake_amount);

        Ok(())
    }
}

pub async fn exec(cmd: StatusCmd) -> anyhow::Result<()> {
    // Currently, the client doesn't use or verify the peer ID of the provided node
    // address. So we can use any peer ID and not require it as an input parameter.
    //
    // TODO: consider figuring out a way to map addresses to peer IDs and have the
    // CLI read that mapping when constructing the client.
    //
    let peer_id = network::Keypair::generate_ed25519().public().to_peer_id();
    let address = (peer_id, network::socketaddr_to_multiaddr(cmd.address));

    let api_client = Client::new(client::Config {
        key: cmd.private_key.0,
        nodes: [address].into(),
        shadowing_nodes: Default::default(),
        shadowing_factor: 0.0,
        request_timeout: Duration::from_secs(1),
        max_operation_time: Duration::from_millis(2500),
        connection_timeout: Duration::from_secs(1),
        udp_socket_count: 1,
        namespaces: Default::default(),
    })?;

    let status_client = StatusClient::new(api_client)?;

    status_client.status().await
}
