use {
    derive_more::AsRef,
    itertools::Itertools,
    std::path::PathBuf,
    tokio::{fs::File, io::AsyncReadExt},
    wcn_cluster::{
        node_operator::{self},
        smart_contract::{evm, Deployer, Read},
        EncryptionKey,
        Node,
        NodeOperator,
        NodeOperators,
        Settings,
    },
};

// TODO: discuss if ideal
/// Maximum number of on-chain bytes stored for a single
const MAX_NODE_OPERATOR_DATA_BYTES: u16 = 4096;

#[derive(Debug, clap::Args)]
pub struct DeployCmd {
    #[command(flatten)]
    shared_args: super::SharedArgs,

    #[clap(long = "operators-file", short = 'n')]
    /// Path to a file containing a serialized list initial node operators.
    operators_file: Option<PathBuf>,

    #[clap(long = "operators", short = 'o')]
    /// JSON string containing a serialized list of initial node operators.
    operators: Option<String>,

    #[clap(
        long = "encryption-key",
        short = 'k',
        env = "WCN_CLUSTER_ENCRYPTION_KEY"
    )]
    /// Key used to encrypt the node operator data stored on-chain.
    encryption_key: String,
}

pub async fn exec(cmd: DeployCmd) -> anyhow::Result<()> {
    let provider = cmd.shared_args.provider().await?;

    // TOOD: maybe consider impl a default
    let settings = Settings {
        max_node_operator_data_bytes: MAX_NODE_OPERATOR_DATA_BYTES,
    };

    if cmd.operators.is_none() && cmd.operators_file.is_none() {
        // TODO: discuss if worth emitting a warning instead of an error
        return Err(anyhow::anyhow!(
            "No operators provided, use --operators or --operators-file"
        ));
    }

    let initial_operators = {
        if let Some(operators) = cmd.operators {
            parse_operators_from_str(&operators)?
        } else if let Some(path) = cmd.operators_file {
            read_operators_from_file(&path).await?
        } else {
            vec![]
        }
    };

    let encryption_key = EncryptionKey::from_base64(&cmd.encryption_key)
        .map_err(|e| anyhow::anyhow!("Failed to decode encryption key: {}", e))?;

    let cfg = DeployConfig { encryption_key };
    let slots = initial_operators.into_iter().map(Some);

    let operators = NodeOperators::from_slots(slots)?
        .into_slots()
        .into_iter()
        .filter_map(|slot| slot.map(|operator| operator.serialize(cfg.as_ref())))
        .try_collect()?;

    let contract = provider.deploy(settings, operators).await?;
    let address = contract.address()?;

    println!("Cluster contract deployed at address: {address}");

    Ok(())
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
/// Transient struct to enable node serialization for cluste contract
/// deployment.
struct DeployConfig {
    #[as_ref]
    encryption_key: EncryptionKey,
}

impl wcn_cluster::Config for DeployConfig {
    type SmartContract = evm::SmartContract;
    type KeyspaceShards = ();
    type Node = Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: Node) -> Self::Node {
        node
    }
}

/// Entity operating a set of [`Node`]s within a WCN cluster.
#[derive(AsRef, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Operator<N = Node> {
    /// ID of this [`NodeOperator`].
    #[as_ref]
    pub id: node_operator::Id,

    /// Name of the [`NodeOperator`].
    pub name: node_operator::Name,

    /// List of [`Client`]s authorized to use the WCN cluster on behalf of the
    /// [`NodeOperator`].
    pub clients: Vec<wcn_cluster::Client>,

    /// List of [`Node`]s of the [`NodeOperator`].
    nodes: Vec<N>,
}
