use {
    crate::Manager,
    cluster::{
        keyspace::{self},
        migration,
        node_operator,
        smart_contract::{self, testing::FakeSmartContract, Read},
        Cluster,
    },
    derive_more::derive::AsRef,
    futures::{stream, FutureExt as _, StreamExt},
    std::{collections::HashSet, hash::BuildHasher as _, time::Duration},
    storage_api::{operation, testing::FakeStorage, Operation, Record, RecordVersion, StorageApi},
    tracing_subscriber::EnvFilter,
    wcn_rpc::PeerId,
    xxhash_rust::xxh3::Xxh3Builder,
};

#[derive(Clone, Default)]
struct Config {
    smart_contract_registry: smart_contract::testing::FakeRegistry,
    storage_registry: storage_api::testing::FakeRegistry<node_operator::Id>,
}

impl cluster::Config for Config {
    type SmartContract = FakeSmartContract;
    type KeyspaceShards = keyspace::Shards;
    type Node = Node;

    fn new_node(&self, operator_id: node_operator::Id, node: cluster::Node) -> Node {
        Node {
            peer_id: node.peer_id,
            storage: self.storage_registry.get(operator_id),
        }
    }
}

impl super::Config for Config {
    type OutboundReplicaConnection = FakeStorage;
    type OutboundDatabaseConnection = FakeStorage;

    fn concurrency(&self) -> usize {
        100
    }
}

#[derive(AsRef, Clone)]
struct Node {
    #[as_ref]
    peer_id: PeerId,

    #[as_ref]
    storage: FakeStorage,
}

#[tokio::test]
async fn transfers_data_and_writes_to_smart_contract() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let namespace = "0x14Cb1e6fb683A83455cA283e10f4959740A49ed7/0"
        .parse()
        .unwrap();

    let cfg = Config::default();

    let cluster = Cluster::deploy(
        cfg.clone(),
        &cfg.smart_contract_registry
            .deployer(smart_contract::testing::signer(42)),
        cluster::Settings {
            max_node_operator_data_bytes: 1024,
        },
        (0..8)
            .map(|idx| cluster::testing::node_operator(idx as u8))
            .collect(),
    )
    .await
    .unwrap();

    let new_operator = cluster::testing::node_operator(10);

    cluster
        .add_node_operator(new_operator.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    cluster
        .start_migration(migration::Plan {
            remove: HashSet::new(),
            add: [new_operator.id].into_iter().collect(),
            replication_strategy: keyspace::ReplicationStrategy::UniformDistribution,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cluster_view = cluster.view();

    let mut expect_get_ops = Vec::new();

    for i in 0..100000 {
        let key = i.to_string().into_bytes();
        let hash = key_hash(&key);

        let set_op = |value| operation::Set {
            namespace,
            key: key.clone(),
            record: Record {
                value: format!("{value}{i}").into_bytes(),
                expiration: Duration::from_secs(60).into(),
                version: RecordVersion::now(),
            },
            keyspace_version: Some(1),
        };

        let new_operator_replica_idx = cluster_view
            .secondary_replica_set(hash)
            .unwrap()
            .iter()
            .position(|op| op.id == new_operator.id);

        let correct_data = set_op("Correct");
        let wrong_data = set_op("Wrong");

        for (idx, replica) in cluster_view
            .primary_replica_set(hash)
            .into_iter()
            .enumerate()
        {
            // Put wrong data into shards that shouldn't be transferred and into operators
            // that are not the primary data transfer source.
            cfg.storage_registry
                .get(replica.id)
                .execute(
                    operation::Owned::Set(if Some(idx) == new_operator_replica_idx {
                        correct_data.clone()
                    } else {
                        wrong_data.clone()
                    })
                    .into(),
                )
                .await
                .unwrap();
        }

        let get_op = operation::Get {
            namespace,
            key,
            keyspace_version: Some(1),
        };

        let expected_record = if new_operator_replica_idx.is_some() {
            Some(correct_data.record.clone())
        } else {
            None
        };
        expect_get_ops.push((get_op, operation::Output::Record(expected_record)))
    }

    let new_operator_storage = cfg.storage_registry.get(new_operator.id);
    let new_operator_cluster = Cluster::connect(
        cfg.clone(),
        &cfg.smart_contract_registry
            .connector(smart_contract::testing::signer(10)),
        cluster.smart_contract().address(),
    )
    .await
    .unwrap();

    let manager = Manager::new(
        cfg.clone(),
        new_operator_cluster,
        new_operator_storage.clone(),
    );

    tokio::spawn(manager.run());

    tokio::time::sleep(Duration::from_secs(5)).await;

    stream::iter(expect_get_ops)
        .for_each_concurrent(100, |(get, expected_out)| {
            new_operator_storage
                .execute(Operation::Owned(operation::Owned::Get(get)))
                .map(|res| assert_eq!(res, Ok(expected_out)))
        })
        .await;

    assert!(!cluster.view().is_pulling(&new_operator.id));
}

fn key_hash(bytes: &[u8]) -> u64 {
    static HASHER: Xxh3Builder = Xxh3Builder::new();

    HASHER.hash_one(bytes)
}
