use {
    crate::commands::{
        parse_operators_from_str,
        read_operators_from_file,
        ClusterConfig,
        SharedArgs,
    },
    itertools::Itertools,
    std::path::PathBuf,
    wcn_cluster::{Cluster, EncryptionKey, NodeOperator, NodeOperators, SmartContract},
};

#[derive(Debug, clap::Args)]
pub struct UpdateCmd {
    #[clap(long = "operators-file", short = 'n')]
    /// Path to a file containing a serialized list node operators to update.
    operators_file: Option<PathBuf>,

    #[clap(long = "operators", short = 'o')]
    /// JSON string containing a serialized list of node operators to update.
    operators: Option<String>,

    #[clap(long = "view-after-update", short = 'r')]
    /// Whether to issue a call to read the current list of node operators after
    /// updating.
    view_after_update: bool,

    #[clap(long = "verbose", short = 'v', default_value_t = false)]
    verbose: bool,
}

pub async fn exec<S: SmartContract>(
    cmd: UpdateCmd,
    client: S,
    encryption_key: EncryptionKey,
) -> anyhow::Result<()> {
    let operators = {
        if let Some(operators) = cmd.operators {
            parse_operators_from_str(&operators)?
        } else if let Some(path) = cmd.operators_file {
            read_operators_from_file(&path).await?
        } else {
            vec![]
        }
    };

    let target_ids = operators.iter().map(|op| op.id).collect::<Vec<_>>();

    let mut successful_updates = 0;

    let cfg = ClusterConfig { encryption_key };

    // TODO: dont stop on first error
    for operator in operators {
        if cmd.verbose {
            println!("Updating node operator: {:?}", operator);
        }

        let op = operator.serialize(&cfg.encryption_key)?;

        client.update_node_operator(op).await?;

        if cmd.verbose {
            println!("Update result: {:?}", ());
        }

        successful_updates += 1;
    }

    if cmd.view_after_update {
        let cluster_view = client.cluster_view().await?;

        let operators_read: Vec<NodeOperator> = cluster_view
            .node_operators
            .into_iter()
            .flatten()
            .map(|sop| sop.deserialize(&cfg))
            .filter_ok(|op| target_ids.contains(&op.id))
            .try_collect()?;

        let operators = serde_json::to_string(&operators_read).unwrap();

        println!("Updated node operators:\n{}", operators);
    }

    println!("Updated {} operators", successful_updates);

    Ok(())
}
