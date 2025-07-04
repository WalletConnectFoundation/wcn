use std::str::FromStr;

use alloy::providers::bindings;
use derive_more::derive::Deref;
use libp2p::{
    identity::{self, ed25519::SecretKey, PublicKey},
    Multiaddr, PeerId,
};
use wcn_cluster::{
    smart_contract::{
        self,
        evm::{RpcProvider, SmartContract},
        Connector,
    },
    Cluster,
};

pub mod maintenance;
pub mod migration;
pub mod operator;
pub mod settings;
pub mod show;
pub mod signer;

#[allow(clippy::large_enum_variant)]
#[derive(clap::Subcommand, Debug)]
pub enum SubCmd {
    Signer(signer::SignerCmd),
    Show(show::ShowCmd),
    Migration(migration::MigrationCmd),
    Maintenance(maintenance::MaintenanceCmd),
    Operator(operator::OperatorCmd),
    Settings(settings::SettingsCmd),
}

pub type SmartContractClient = SmartContract<smart_contract::Signer>;

#[derive(Debug, clap::Args)]
pub struct SharedArgs {
    #[arg(
        short = 'f', 
        long = "key-file", 
        value_hint = clap::ValueHint::FilePath,
        env = "WCN_CLUSTER_KEY_FILE"
    )]
    key_file: String,

    #[clap(
        short = 'p',
        long = "provider-url",
        env = "WCN_CLUSTER_RPC_PROVIDER_URL"
    )]
    provider_url: String,

    #[clap(
        short = 'r',
        long = "contract-address",
        env = "WCN_CLUSTER_SMART_CONTRACT_ADDRESS"
    )]
    contract_address: smart_contract::Address,
}

impl SharedArgs {
    /// Reads a base64 encoded key file and returngm s the decoded bytes.
    #[allow(clippy::allow_unused)]
    pub(crate) fn key_from_file(&self) -> anyhow::Result<Vec<u8>> {
        let contents =
            std::fs::read_to_string(&self.key_file).map_err(|_| CliError::KeyEncoding)?;

        let bytes: Vec<u8> = data_encoding::BASE64
            .decode(contents.as_bytes())
            .map_err(|_| CliError::KeyEncoding)?[..]
            .into();

        Ok(bytes)
    }

    /// Create a new SmartContract client using the provided key file and provider URL.
    pub(crate) async fn new_client(&self) -> anyhow::Result<SmartContractClient> {
        let address = self.contract_address.clone();
        let provider_url = self.provider_url.clone();

        let private_key = std::fs::read_to_string(&self.key_file).map_err(|_| CliError::KeyEncoding)?;
        let signer = smart_contract::Signer::try_from_private_key(&private_key)?;

        let provider_url = provider_url.parse().unwrap();
        let provider = RpcProvider::new(provider_url, signer).await?;

        let sc = provider.connect(address).await?;

        Ok(sc)
    }
}

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
