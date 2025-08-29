use {
    crate::commands::{parse_operators_from_str, read_operators_from_file, ClusterConfig},
    itertools::Itertools,
    std::path::PathBuf,
    wcn_cluster::{
        smart_contract::{Deployer, Read},
        EncryptionKey,
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

    let encryption_key = EncryptionKey::from_hex(&cmd.shared_args.encryption_key)
        .map_err(|e| anyhow::anyhow!("Failed to decode encryption key: {}", e))?;

    let cfg = ClusterConfig { encryption_key };
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
