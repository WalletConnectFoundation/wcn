use {
    super::{hash, Response},
    crate::coordinator::Coordinator,
    cluster::{
        keyspace::{self, ReplicaSet},
        migration,
        node_operator,
        smart_contract::{self, testing::FakeSmartContract},
        Cluster,
        Node,
        NodeOperator,
        PeerId,
    },
    derive_more::derive::AsRef,
    std::{collections::HashSet, sync::Arc, time::Duration},
    storage_api::{operation, testing::FakeStorage, Operation, RecordBorrowed, StorageApi},
};

#[derive(Clone, Default)]
struct Config {
    smart_contract_registry: smart_contract::testing::FakeRegistry,
    storage_registry: storage_api::testing::FakeRegistry<node_operator::Id>,
}

#[derive(AsRef, Clone)]
struct Replica {
    #[as_ref]
    peer_id: PeerId,

    #[as_ref]
    storage: storage_api::testing::FakeStorage,
}

impl cluster::Config for Config {
    type SmartContract = FakeSmartContract;
    type KeyspaceShards = keyspace::Shards;
    type Node = Replica;

    fn new_node(&self, operator_id: node_operator::Id, node: Node) -> Replica {
        Replica {
            peer_id: node.peer_id,
            storage: self.storage_registry.get(operator_id),
        }
    }
}

impl super::Config for Config {
    type OutboundReplicaConnection = FakeStorage;
}

impl StorageApi for Replica {
    async fn execute_ref(
        &self,
        operation: &storage_api::Operation<'_>,
    ) -> storage_api::Result<operation::Output> {
        self.storage.execute_ref(operation).await
    }
}

struct Context {
    config: Config,

    cluster_view: Arc<cluster::View<Config>>,

    coordinator: Coordinator<Config>,
}

impl Context {
    async fn new() -> Self {
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

        Self {
            config: cfg.clone(),
            cluster_view: cluster.view(),
            coordinator: Coordinator::new(cfg, cluster),
        }
    }

    fn primary_replica_set(&self, key: &str) -> ReplicaSet<&NodeOperator<Replica>> {
        self.cluster_view.primary_replica_set(hash(key.as_bytes()))
    }

    fn secondary_replica_set(&self, key: &str) -> Option<ReplicaSet<&NodeOperator<Replica>>> {
        self.cluster_view
            .secondary_replica_set(hash(key.as_bytes()))
    }

    fn storage(&self, operator_id: node_operator::Id) -> FakeStorage {
        self.config.storage_registry.get(operator_id)
    }

    async fn replace_all_operators(&mut self) {
        let current_operators: HashSet<_> = self
            .cluster_view
            .node_operators()
            .slots()
            .iter()
            .filter_map(|opt| opt.as_ref().map(|op| op.id))
            .collect();

        let mut new_operators = HashSet::new();

        for n in 0..current_operators.len() {
            let operator = cluster::testing::node_operator(100 + n as u8);
            new_operators.insert(operator.id);

            self.coordinator
                .cluster
                .add_node_operator(operator)
                .await
                .unwrap()
        }

        let plan = migration::Plan {
            remove: current_operators,
            add: new_operators,
            replication_strategy: keyspace::ReplicationStrategy::UniformDistribution,
        };

        // wait for the cluster view to be updated in the background
        tokio::time::sleep(Duration::from_millis(100)).await;

        self.coordinator
            .cluster
            .start_migration(plan)
            .await
            .unwrap();

        // wait for the cluster view to be updated in the background
        // (including keyspace re-calculation)
        tokio::time::sleep(Duration::from_secs(1)).await;

        self.cluster_view = self.coordinator.cluster.view();
    }
}

struct WriteTestCase<'a> {
    operation: Operation<'a>,
    expected_response: Response,

    validate: ReadTestCase<'a>,
}

struct ReadTestCase<'a> {
    operation: Operation<'a>,
    expected_response: Response,
}

fn set_test_case<'a>(key: &'a str, value: &'a str) -> WriteTestCase<'a> {
    let namespace = "0x14Cb1e6fb683A83455cA283e10f4959740A49ed7/0"
        .parse()
        .unwrap();

    let record = RecordBorrowed::new(value.as_bytes(), Duration::from_secs(60));

    let set = operation::SetBorrowed {
        namespace,
        key: key.as_bytes(),
        record,
        keyspace_version: None,
    };

    let get = operation::GetBorrowed {
        namespace,
        key: key.as_bytes(),
        keyspace_version: None,
    };

    WriteTestCase {
        operation: Operation::Borrowed(set.into()),
        expected_response: Ok(operation::Output::none()),
        validate: ReadTestCase {
            operation: Operation::Borrowed(get.into()),
            expected_response: Ok(Some(record.into_owned()).into()),
        },
    }
}

#[tokio::test]
async fn replicates_only_to_replica_set() {
    let ctx = Context::new().await;

    let set = set_test_case("a", "1");

    let resp = ctx.coordinator.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    let resp = ctx.coordinator.execute_ref(&set.validate.operation).await;
    assert_eq!(&resp, &set.validate.expected_response);

    let replica_set = ctx.primary_replica_set("a");
    for operator in ctx.cluster_view.node_operators().slots().iter().flatten() {
        let resp = ctx
            .storage(operator.id)
            .execute_ref(&set.validate.operation)
            .await;

        if replica_set.iter().any(|op| op.id == operator.id) {
            assert_eq!(&resp, &set.validate.expected_response);
        } else {
            assert_eq!(resp, Ok(operation::Output::Record(None)));
        }
    }
}

#[tokio::test]
async fn tolerates_up_to_2_broken_replicas() {
    let set = set_test_case("a", "1");

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();

        let resp = ctx.coordinator.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        let resp = ctx.coordinator.execute_ref(&set.validate.operation).await;
        assert_eq!(&resp, &set.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();

        let resp = ctx.coordinator.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        let resp = ctx.coordinator.execute_ref(&set.validate.operation).await;
        assert_eq!(&resp, &set.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();

        let resp = ctx.coordinator.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        ctx.storage(replica_set[2].id).break_();

        let resp = ctx.coordinator.execute_ref(&set.validate.operation).await;
        assert!(resp.is_err());
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();
        ctx.storage(replica_set[2].id).break_();

        let resp = ctx.coordinator.execute_ref(&set.operation).await;
        assert!(resp.is_err());
    }
}

#[tokio::test]
async fn tolerates_up_to_2_inconsistent_responses() {
    let set1 = set_test_case("a", "1");
    let set2 = set_test_case("a", "2");
    let set3 = set_test_case("a", "3");
    let set4 = set_test_case("a", "4");

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");

        let resp = ctx.coordinator.execute_ref(&set1.operation).await;
        assert_eq!(resp, set1.expected_response);

        let resp = ctx
            .storage(replica_set[0].id)
            .execute_ref(&set2.operation)
            .await;
        assert_eq!(resp, set2.expected_response);

        let resp = ctx.coordinator.execute_ref(&set1.validate.operation).await;
        assert_eq!(&resp, &set1.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");

        let resp = ctx.coordinator.execute_ref(&set1.operation).await;
        assert_eq!(resp, set1.expected_response);

        let resp = ctx
            .storage(replica_set[0].id)
            .execute_ref(&set2.operation)
            .await;
        assert_eq!(resp, set2.expected_response);

        let resp = ctx
            .storage(replica_set[1].id)
            .execute_ref(&set3.operation)
            .await;
        assert_eq!(resp, set3.expected_response);

        let resp = ctx.coordinator.execute_ref(&set1.validate.operation).await;
        assert_eq!(&resp, &set1.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");

        let resp = ctx.coordinator.execute_ref(&set1.operation).await;
        assert_eq!(resp, set1.expected_response);

        let resp = ctx
            .storage(replica_set[0].id)
            .execute_ref(&set2.operation)
            .await;
        assert_eq!(resp, set2.expected_response);

        let resp = ctx
            .storage(replica_set[1].id)
            .execute_ref(&set3.operation)
            .await;
        assert_eq!(resp, set3.expected_response);

        let resp = ctx
            .storage(replica_set[2].id)
            .execute_ref(&set3.operation)
            .await;
        assert_eq!(resp, set4.expected_response);

        let resp = ctx.coordinator.execute_ref(&set1.validate.operation).await;
        assert!(resp.is_err());
    }
}

#[tokio::test]
async fn replicates_to_both_replica_sets_during_migrations() {
    let mut ctx = Context::new().await;
    ctx.replace_all_operators().await;

    let set = set_test_case("a", "1");

    let resp = ctx.coordinator.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    let resp = ctx.coordinator.execute_ref(&set.validate.operation).await;
    assert_eq!(&resp, &set.validate.expected_response);

    let primary_replica_set = ctx.primary_replica_set("a");
    let secondary_replica_set = ctx.secondary_replica_set("a").unwrap();
    for operator in ctx.cluster_view.node_operators().slots().iter().flatten() {
        let resp = ctx
            .storage(operator.id)
            .execute_ref(&set.validate.operation)
            .await;

        if primary_replica_set.iter().any(|op| op.id == operator.id)
            || secondary_replica_set.iter().any(|op| op.id == operator.id)
        {
            assert_eq!(&resp, &set.validate.expected_response);
        } else {
            assert_eq!(resp, Ok(operation::Output::Record(None)));
        }
    }
}

#[tokio::test]
async fn tolerates_up_to_2_broken_replicas_in_each_replica_set() {
    let set = set_test_case("a", "1");
    let set2 = set_test_case("a", "2");

    let mut ctx = Context::new().await;
    ctx.replace_all_operators().await;

    let primary_replica_set = ctx.primary_replica_set("a");
    ctx.storage(primary_replica_set[0].id).break_();
    ctx.storage(primary_replica_set[1].id).break_();

    let secondary_replica_set = ctx.secondary_replica_set("a").unwrap();
    ctx.storage(secondary_replica_set[0].id).break_();
    ctx.storage(secondary_replica_set[1].id).break_();

    let resp = ctx.coordinator.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    ctx.storage(secondary_replica_set[2].id).break_();

    let resp = ctx.coordinator.execute_ref(&set2.operation).await;
    assert!(resp.is_err());
}

#[tokio::test]
async fn repairs_replicas_with_inconsistent_responses() {
    let ctx = Context::new().await;

    let set1 = set_test_case("a", "1");
    let set2 = set_test_case("a", "2");
    let set3 = set_test_case("a", "3");

    let replica_set = ctx.primary_replica_set("a");

    let resp = ctx.coordinator.execute_ref(&set1.operation).await;
    assert_eq!(resp, set1.expected_response);

    let resp = ctx.coordinator.execute_ref(&set1.validate.operation).await;
    assert_eq!(&resp, &set1.validate.expected_response);

    let resp = ctx
        .storage(replica_set[0].id)
        .execute_ref(&set2.operation)
        .await;
    assert_eq!(resp, set2.expected_response);

    let resp = ctx
        .storage(replica_set[0].id)
        .execute_ref(&set2.validate.operation)
        .await;
    assert_eq!(resp, set2.validate.expected_response);

    let resp = ctx
        .storage(replica_set[1].id)
        .execute_ref(&set3.operation)
        .await;
    assert_eq!(resp, set3.expected_response);

    let resp = ctx
        .storage(replica_set[1].id)
        .execute_ref(&set3.validate.operation)
        .await;
    assert_eq!(resp, set3.validate.expected_response);

    let resp = ctx.coordinator.execute_ref(&set1.validate.operation).await;
    assert_eq!(&resp, &set1.validate.expected_response);

    let resp = ctx
        .storage(replica_set[0].id)
        .execute_ref(&set1.operation)
        .await;
    assert_eq!(resp, set1.expected_response);

    let resp = ctx
        .storage(replica_set[1].id)
        .execute_ref(&set1.operation)
        .await;
    assert_eq!(resp, set1.expected_response);
}
