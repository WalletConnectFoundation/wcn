use {
    alloy::{
        node_bindings::{Anvil, AnvilInstance},
        signers::local::PrivateKeySigner,
    },
    assert_cmd::assert,
    hex::ToHex,
    libp2p::PeerId,
    std::{
        collections::HashSet, io::Write, net::SocketAddrV4, path::PathBuf, str::FromStr,
        time::Duration,
    },
    tracing_subscriber::EnvFilter,
    wcn_cluster::{
        keyspace::ReplicationStrategy,
        node_operator,
        smart_contract::{
            self,
            evm::{self, RpcProvider},
            Read, Signer,
        },
        Cluster, Node, NodeOperator, Settings, SmartContract,
    },
};

#[derive(Clone, Copy)]
struct Config;

impl wcn_cluster::Config for Config {
    type SmartContract = evm::SmartContract<Signer>;
    type KeyspaceShards = ();
    type Node = wcn_cluster::Node;

    fn new_node(&self, addr: SocketAddrV4, peer_id: PeerId) -> Self::Node {
        wcn_cluster::Node::new(addr, peer_id)
    }
}

#[tokio::test]
async fn test_suite() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Spin up a local Anvil instance automatically
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(31337)
        .try_spawn()
        .unwrap();

    let cfg = Config;

    let settings = Settings {
        max_node_operator_data_bytes: 4096,
    };

    // Use Anvil's first key for deployment - convert PrivateKeySigner to our Signer
    let private_key_signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let signer =
        Signer::try_from_private_key(&format!("{:#x}", private_key_signer.to_bytes())).unwrap();

    let provider = provider(signer, &anvil).await;

    let operators = (1..=5).map(|n| new_node_operator(n, &anvil)).collect();

    let cluster = Cluster::deploy(cfg, &provider, settings, operators)
        .await
        .unwrap();

    for id in 6..=8 {
        cluster
            .add_node_operator(new_node_operator(id, &anvil))
            .await
            .unwrap();
    }

    cluster
        .start_migration(wcn_cluster::migration::Plan {
            remove: [operator_id(1, &anvil)].into_iter().collect(),
            add: [
                operator_id(6, &anvil),
                operator_id(7, &anvil),
                operator_id(8, &anvil),
            ]
            .into_iter()
            .collect(),
            replication_strategy: ReplicationStrategy::UniformDistribution,
        })
        .await
        .unwrap();

    for idx in 1..=7 {
        connect(idx + 1, cluster.smart_contract().address(), &anvil)
            .await
            .complete_migration(1)
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    cluster
        .start_migration(wcn_cluster::migration::Plan {
            remove: HashSet::default(),
            add: [operator_id(1, &anvil)].into_iter().collect(),
            replication_strategy: ReplicationStrategy::UniformDistribution,
        })
        .await
        .unwrap();

    cluster.abort_migration(2).await.unwrap();

    cluster.start_maintenance().await.unwrap();
    cluster.finish_maintenance().await.unwrap();

    cluster
        .remove_node_operator(operator_id(1, &anvil))
        .await
        .unwrap();

    let mut operator2 = new_node_operator(2, &anvil);
    operator2.name = node_operator::Name::new("NewName").unwrap();
    cluster.update_node_operator(operator2).await.unwrap();

    cluster
        .update_settings(Settings {
            max_node_operator_data_bytes: 10000,
        })
        .await
        .unwrap();
}

fn new_node_operator(n: u8, anvil: &AnvilInstance) -> NodeOperator {
    NodeOperator::new(
        operator_id(n, anvil),
        node_operator::Name::new(format!("Operator{n}")).unwrap(),
        vec![
            Node {
                peer_id: PeerId::random(),
                addr: SocketAddrV4::new([127, 0, 0, 1].into(), 40000 + n as u16),
            },
            Node {
                peer_id: PeerId::random(),
                addr: SocketAddrV4::new([127, 0, 0, 1].into(), 50000 + n as u16),
            },
        ],
        vec![],
    )
    .unwrap()
}

fn operator_id(n: u8, anvil: &AnvilInstance) -> node_operator::Id {
    *new_signer(n, anvil).address()
}

fn new_signer(n: u8, anvil: &AnvilInstance) -> Signer {
    let private_key_signer: PrivateKeySigner = anvil.keys()[n as usize].clone().into();
    let private_key = &format!("{:#x}", private_key_signer.to_bytes());
    Signer::try_from_private_key(private_key).unwrap()
}

async fn provider(signer: Signer, anvil: &AnvilInstance) -> RpcProvider<Signer> {
    let ws_url = anvil.endpoint_url().to_string().replace("http://", "ws://");
    RpcProvider::new(ws_url.parse().unwrap(), signer)
        .await
        .unwrap()
}

async fn connect(
    operator: u8,
    address: smart_contract::Address,
    anvil: &AnvilInstance,
) -> Cluster<Config> {
    let provider = provider(new_signer(operator, anvil), anvil).await;
    Cluster::connect(Config, &provider, address).await.unwrap()
}

#[tokio::test]
pub async fn cli_test_suite() {
    // Spin up a local Anvil instance automatically
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(31337)
        .try_spawn()
        .unwrap();

    let cfg = Config;

    let settings = Settings {
        max_node_operator_data_bytes: 4096,
    };

    // Use Anvil's first key for deployment - convert PrivateKeySigner to our Signer
    let private_key_signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let signer =
        Signer::try_from_private_key(&format!("{:#x}", private_key_signer.to_bytes())).unwrap();

    let provider = provider(signer, &anvil).await;

    let operators = (1..=5).map(|n| new_node_operator(n, &anvil)).collect();

    let cluster = Cluster::deploy(cfg, &provider, settings, operators)
        .await
        .unwrap();

    let sc = cluster.smart_contract();

    let mut tempdir = std::env::temp_dir();
    tempdir.push("test_key_file");

    let key_bytes = anvil.keys()[0].to_bytes().to_vec();
    let key_bytes = hex::encode(key_bytes).into_bytes();

    let mut file = std::fs::File::create(&tempdir).unwrap();
    file.write_all(&key_bytes).unwrap();

    test_migration_start(&anvil, &tempdir, sc).unwrap();

    panic!();
}

fn test_migration_start(
    anvil: &AnvilInstance,
    dir: &PathBuf,
    sc: &impl SmartContract,
) -> anyhow::Result<()> {
    let mut cmd = assert_cmd::Command::cargo_bin("wcn_cluster").unwrap();

    cmd.arg("migration")
        .arg("--key-file")
        .arg(dir.display().to_string())
        .arg("--provider-url")
        .arg(anvil.ws_endpoint_url().to_string())
        .arg("--contract-address")
        .arg(sc.address().to_string())
        .arg("start")
        .assert()
        .success();

    Ok(())
}
