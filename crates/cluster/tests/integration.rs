use {
    libp2p::PeerId,
    std::{collections::HashSet, net::SocketAddrV4, time::Duration},
    tracing_subscriber::EnvFilter,
    wcn_cluster::{
        keyspace::ReplicationStrategy,
        node_operator,
        smart_contract::{
            self,
            evm::{self, RpcProvider},
            Read,
            Signer,
        },
        Cluster,
        Node,
        NodeOperator,
        Settings,
    },
};

// Anvil private keys with balances
const PRIVATE_KEYS: [&str; 10] = [
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
    "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
    "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6",
    "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a",
    "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba",
    "0x92db14e403b83dfe3df233f83dfa3a0d7096f21ca9b0d6d6b8d88b2b4ec1564e",
    "0x4bbbf85ce3377467afe5d46f804f221813b2bb87f24d81f60f1fcdbf7cbf4356",
    "0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97",
    "0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6",
];

// TODO: Setup Anvil on CI
#[ignore]
#[tokio::test]
async fn test_suite() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let settings = Settings {
        max_node_operator_data_bytes: 4096,
    };

    let signer = Signer::try_from_private_key(
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    )
    .unwrap();

    let provider = provider(signer).await;

    let operators = (1..=5).map(new_node_operator).collect();

    let cluster = Cluster::<evm::SmartContract<Signer>>::deploy(&provider, settings, operators)
        .await
        .unwrap();

    for id in 6..=8 {
        cluster
            .add_node_operator(new_node_operator(id))
            .await
            .unwrap();
    }

    cluster
        .start_migration(wcn_cluster::migration::Plan {
            remove: [operator_id(1)].into_iter().collect(),
            add: [operator_id(6), operator_id(7), operator_id(8)]
                .into_iter()
                .collect(),
            replication_strategy: ReplicationStrategy::UniformDistribution,
        })
        .await
        .unwrap();

    for idx in 1..=7 {
        connect(idx + 1, cluster.smart_contract().address())
            .await
            .complete_migration(1)
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    cluster
        .start_migration(wcn_cluster::migration::Plan {
            remove: HashSet::default(),
            add: [operator_id(1)].into_iter().collect(),
            replication_strategy: ReplicationStrategy::UniformDistribution,
        })
        .await
        .unwrap();

    cluster.abort_migration(2).await.unwrap();

    cluster.start_maintenance().await.unwrap();
    cluster.finish_maintenance().await.unwrap();

    cluster.remove_node_operator(operator_id(1)).await.unwrap();

    let mut operator2 = new_node_operator(2);
    operator2.data.name = node_operator::Name::new("NewName").unwrap();
    cluster.update_node_operator(operator2).await.unwrap();

    cluster
        .update_settings(Settings {
            max_node_operator_data_bytes: 10000,
        })
        .await
        .unwrap();
}

fn new_node_operator(n: u8) -> NodeOperator {
    let data = node_operator::Data {
        name: node_operator::Name::new(format!("Operator{n}")).unwrap(),
        nodes: vec![Node {
            peer_id: PeerId::random(),
            addr: SocketAddrV4::new([127, 0, 0, 1].into(), 40000 + n as u16),
        }],
        clients: vec![],
    };

    NodeOperator {
        id: operator_id(n),
        data,
    }
}

fn operator_id(n: u8) -> node_operator::Id {
    *new_signer(n).address()
}

fn new_signer(n: u8) -> Signer {
    Signer::try_from_private_key(PRIVATE_KEYS[n as usize]).unwrap()
}

async fn provider(signer: Signer) -> RpcProvider<Signer> {
    RpcProvider::new("ws://127.0.0.1:8545".parse().unwrap(), signer)
        .await
        .unwrap()
}

async fn connect(
    operator: u8,
    address: smart_contract::Address,
) -> Cluster<evm::SmartContract<Signer>> {
    let provider = provider(new_signer(operator)).await;
    Cluster::<evm::SmartContract<Signer>>::connect(&provider, address)
        .await
        .unwrap()
}
