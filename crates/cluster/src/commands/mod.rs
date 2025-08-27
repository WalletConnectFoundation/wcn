use {
    derive_more::AsRef,
    itertools::Itertools,
    std::path::PathBuf,
    tokio::{fs::File, io::AsyncReadExt},
    wcn_cluster::{
        smart_contract::{
            self,
            evm::{RpcProvider, SmartContract},
            Connector,
        },
        EncryptionKey,
        Node,
        NodeOperator,
    },
};

pub mod deploy;
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
    Deploy(deploy::DeployCmd),
}

#[derive(Debug, clap::Args)]
pub struct SharedArgs {
    /// Private key used to sign transactions.
    #[arg(
        env = "WCN_SIGNER_PRIVATE_KEY", 
        short = 's', // TODO: consider document as env var using after_help 
        long = "signer-key",
    )]
    signer_key: String,

    #[clap(
        short = 'p',
        long = "provider-url",
        env = "WCN_CLUSTER_RPC_PROVIDER_URL"
    )]
    provider_url: String,

    #[clap(
        short = 'a',
        long = "contract-address",
        env = "WCN_CLUSTER_SMART_CONTRACT_ADDRESS"
    )]
    contract_address: Option<smart_contract::Address>,

    #[clap(
        long = "encryption-key",
        short = 'k',
        env = "WCN_CLUSTER_ENCRYPTION_KEY"
    )]
    /// Key used to encrypt the node operator data stored on-chain.
    encryption_key: String,
}

impl SharedArgs {
    pub(crate) async fn provider(&self) -> anyhow::Result<RpcProvider> {
        let signer = smart_contract::Signer::try_from_private_key(&self.signer_key)?;

        let provider_url = self.provider_url.parse().unwrap();
        let provider = RpcProvider::new(provider_url, signer).await?;

        Ok(provider)
    }

    /// Create a new SmartContract client using the provided key file and
    /// provider URL.
    pub(crate) async fn new_client(&self) -> anyhow::Result<SmartContract> {
        let address = self.contract_address.ok_or(anyhow::anyhow!(
            "No contract address provided. Use --contract-address to specify it."
        ))?;
        let provider_url = self.provider_url.clone();

        let signer = smart_contract::Signer::try_from_private_key(&self.signer_key)?;

        let provider_url = provider_url.parse().unwrap();
        let provider = RpcProvider::new(provider_url, signer).await?;

        let sc = provider.connect(address).await?;

        Ok(sc)
    }
}

#[allow(dead_code)]
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

/// Entity operating a set of [`Node`]s within a WCN cluster.
#[derive(AsRef, Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Operator<N = wcn_cluster::Node> {
    /// ID of this [`Operator`].
    #[as_ref]
    id: wcn_cluster::node_operator::Id,

    /// Name of the [`Operator`].
    name: wcn_cluster::node_operator::Name,

    /// List of [`Client`]s authorized to use the WCN cluster on behalf of the
    /// [`Operator`].
    clients: Vec<wcn_cluster::Client>,

    /// List of [`Node`]s of the [`Operator`].
    nodes: Vec<N>,
}

fn parse_operators_from_str(operators_str: &str) -> anyhow::Result<Vec<NodeOperator>> {
    let operators = serde_json::from_str::<Vec<Operator>>(operators_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse operators JSON: {e}"))?;

    let operators = operators
        .into_iter()
        .map(|op| NodeOperator::new(op.id, op.name, op.nodes, op.clients))
        .try_collect()?;

    Ok(operators)
}

async fn read_operators_from_file(path: &PathBuf) -> anyhow::Result<Vec<NodeOperator>> {
    let mut contents = File::open(path).await?;
    let mut buf = String::new();

    contents.read_to_string(&mut buf).await?;

    let operators = serde_json::from_str::<Vec<Operator>>(buf.as_str())
        .map_err(|e| anyhow::anyhow!("Failed to parse operators file: {e}"))?;

    let operators = operators
        .into_iter()
        .map(|op| NodeOperator::new(op.id, op.name, op.nodes, op.clients))
        .try_collect()?;

    Ok(operators)
}

#[derive(AsRef, Clone, Copy)]
/// Transient struct to enable node serialization for cluster contract
/// interactions
struct ClusterConfig {
    #[as_ref]
    encryption_key: EncryptionKey,
}

impl wcn_cluster::Config for ClusterConfig {
    type SmartContract = smart_contract::evm::SmartContract;
    type KeyspaceShards = ();
    type Node = Node;

    fn new_node(&self, _operator_id: wcn_cluster::node_operator::Id, node: Node) -> Self::Node {
        node
    }
}
