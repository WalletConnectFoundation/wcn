use {
    derive_more::Deref,
    std::str::FromStr,
    wcn_rpc::{quic, Multiaddr},
};

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("Failed to initialize namespace")]
    Namespace,

    #[error("Invalid key encoding: must be base64")]
    KeyEncoding,

    #[error("Invalid key length: must be 32 byte ed25519 private key")]
    KeyLength,

    #[error("Failed to decode parameter: {0}")]
    Decoding(&'static str),

    #[error("Failed to write data to stdout")]
    Io(#[from] std::io::Error),

    #[error("Text encoding must be utf8")]
    TextEncoding,
}

#[derive(Clone, Debug, Deref)]
pub struct Keypair(pub wcn_rpc::identity::Keypair);

impl FromStr for Keypair {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes: Vec<u8> = data_encoding::BASE64
            .decode(s.as_bytes())
            .map_err(|_| CliError::KeyEncoding)?[..]
            .into();

        wcn_rpc::identity::Keypair::ed25519_from_bytes(bytes)
            .map_err(|_| CliError::KeyLength)
            .map(Self)
    }
}

#[derive(Debug, clap::Args)]
pub struct AdminApiArgs {
    /// Admin API address.
    #[clap(short, long, env = "WCN_ADMIN_API_ADDRESS")]
    address: Multiaddr,

    #[clap(
        short = 'k',
        long = "private_key",
        env = "WCN_ADMIN_API_CLIENT_PRIVATE_KEY"
    )]
    /// Admin API client private key.
    keypair: Keypair,
}

impl AdminApiArgs {
    pub fn new_client(self) -> anyhow::Result<wcn_admin_api::Client<quic::Connector>> {
        let socket = quic::client::Connector::new(self.keypair.0)?;
        let cfg = wcn_admin_api::client::Config::new(self.address);
        Ok(wcn_admin_api::Client::new(socket, cfg))
    }
}
