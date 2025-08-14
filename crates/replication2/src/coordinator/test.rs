use {
    super::{hash, Response},
    crate::{coordinator, Coordinator},
    derive_more::derive::AsRef,
    std::{collections::HashSet, sync::Arc, time::Duration},
    tap::Tap,
    wcn_cluster::{
        keyspace::{self, ReplicaSet},
        migration,
        node_operator,
        smart_contract::{self, testing::FakeSmartContract},
        Client,
        Cluster,
        EncryptionKey,
        Node,
        NodeOperator,
        PeerId,
    },
    wcn_storage_api2::{
        self as storage_api,
        operation,
        testing::FakeStorage,
        Error,
        Factory,
        Namespace,
        Operation,
        RecordBorrowed,
        StorageApi,
    },
};

#[derive(AsRef, Clone)]
struct Config {
    #[as_ref]
    encryption_key: EncryptionKey,
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

impl wcn_cluster::Config for Config {
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

struct Context {
    config: Config,

    cluster_view: Arc<wcn_cluster::View<Config>>,

    coordinator: Coordinator<Config>,
    conn: coordinator::InboundConnection<Config>,
}

impl Context {
    async fn new() -> Self {
        let cfg = Config {
            encryption_key: wcn_cluster::testing::encryption_key(),
            smart_contract_registry: Default::default(),
            storage_registry: Default::default(),
        };

        let client_peer_id = PeerId::random();

        let cluster = Cluster::deploy(
            cfg.clone(),
            &cfg.smart_contract_registry
                .deployer(smart_contract::testing::signer(42)),
            wcn_cluster::Settings {
                max_node_operator_data_bytes: 1024,
            },
            (0..8)
                .map(|idx| {
                    wcn_cluster::testing::node_operator(idx as u8).tap_mut(|operator| {
                        if idx == 0 {
                            operator.clients = vec![Client {
                                peer_id: client_peer_id,
                                authorized_namespaces: [0].into_iter().collect(),
                            }]
                        }
                    })
                })
                .collect(),
        )
        .await
        .unwrap();

        let cluster_view = cluster.view();
        let coordinator = Coordinator::new(cfg.clone(), cluster);

        Self {
            config: cfg,
            cluster_view,
            conn: coordinator.new_inbound_connection(client_peer_id).unwrap(),
            coordinator,
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
            let operator = wcn_cluster::testing::node_operator(100 + n as u8);
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

fn namespace(operator_id: u8, idx: u8) -> Namespace {
    let operator_id = wcn_cluster::testing::node_operator(operator_id).id;
    format!("{operator_id}/{idx}").parse().unwrap()
}

fn set_test_case<'a>(key: &'a str, value: &'a str) -> WriteTestCase<'a> {
    let record = RecordBorrowed::new(value.as_bytes(), Duration::from_secs(60));

    let namespace = namespace(0, 0);

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
async fn inbound_connections_not_authorized_for_clients_not_in_cluster() {
    let ctx = Context::new().await;
    let err = ctx
        .coordinator
        .new_storage_api(PeerId::random())
        .err()
        .unwrap();

    assert_eq!(err, Error::unauthorized())
}

#[tokio::test]
async fn operations_not_authorized_for_unauthorized_namespaces() {
    let ctx = Context::new().await;

    let mut get = operation::GetBorrowed {
        namespace: namespace(0, 1),
        key: b"test",
        keyspace_version: None,
    };

    let res = ctx
        .conn
        .execute(operation::Borrowed::Get(get.clone()).into())
        .await;
    assert_eq!(res, Err(Error::unauthorized()));

    get.namespace = namespace(1, 0);

    let res = ctx.conn.execute(operation::Borrowed::Get(get).into()).await;
    assert_eq!(res, Err(Error::unauthorized()));
}

#[tokio::test]
async fn sets_operation_keyspace_version() {
    let ctx = Context::new().await;

    ctx.config
        .storage_registry
        .for_each(|_, storage| storage.expect_keyspace_version(0));

    let get = operation::GetBorrowed {
        namespace: namespace(0, 0),
        key: b"test",
        keyspace_version: None,
    };

    let res = ctx.conn.execute(operation::Borrowed::Get(get).into()).await;
    assert_eq!(res, Ok(operation::Output::Record(None)));
}

#[tokio::test]
async fn replicates_only_to_replica_set() {
    let ctx = Context::new().await;

    let set = set_test_case("a", "1");

    let resp = ctx.conn.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    let resp = ctx.conn.execute_ref(&set.validate.operation).await;
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

        let resp = ctx.conn.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        let resp = ctx.conn.execute_ref(&set.validate.operation).await;
        assert_eq!(&resp, &set.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();

        let resp = ctx.conn.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        let resp = ctx.conn.execute_ref(&set.validate.operation).await;
        assert_eq!(&resp, &set.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();

        let resp = ctx.conn.execute_ref(&set.operation).await;
        assert_eq!(resp, set.expected_response);

        ctx.storage(replica_set[2].id).break_();

        let resp = ctx.conn.execute_ref(&set.validate.operation).await;
        assert!(resp.is_err());
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");
        ctx.storage(replica_set[0].id).break_();
        ctx.storage(replica_set[1].id).break_();
        ctx.storage(replica_set[2].id).break_();

        let resp = ctx.conn.execute_ref(&set.operation).await;
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

        let resp = ctx.conn.execute_ref(&set1.operation).await;
        assert_eq!(resp, set1.expected_response);

        let resp = ctx
            .storage(replica_set[0].id)
            .execute_ref(&set2.operation)
            .await;
        assert_eq!(resp, set2.expected_response);

        let resp = ctx.conn.execute_ref(&set1.validate.operation).await;
        assert_eq!(&resp, &set1.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");

        let resp = ctx.conn.execute_ref(&set1.operation).await;
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

        let resp = ctx.conn.execute_ref(&set1.validate.operation).await;
        assert_eq!(&resp, &set1.validate.expected_response);
    }

    {
        let ctx = Context::new().await;

        let replica_set = ctx.primary_replica_set("a");

        let resp = ctx.conn.execute_ref(&set1.operation).await;
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

        let resp = ctx.conn.execute_ref(&set1.validate.operation).await;
        assert!(resp.is_err());
    }
}

#[tokio::test]
async fn replicates_to_both_replica_sets_during_migrations() {
    let mut ctx = Context::new().await;
    ctx.replace_all_operators().await;

    let set = set_test_case("a", "1");

    let resp = ctx.conn.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    let resp = ctx.conn.execute_ref(&set.validate.operation).await;
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

    let resp = ctx.conn.execute_ref(&set.operation).await;
    assert_eq!(resp, set.expected_response);

    ctx.storage(secondary_replica_set[2].id).break_();

    let resp = ctx.conn.execute_ref(&set2.operation).await;
    assert!(resp.is_err());
}

#[tokio::test]
async fn repairs_replicas_with_inconsistent_responses() {
    let ctx = Context::new().await;

    let set1 = set_test_case("a", "1");
    let set2 = set_test_case("a", "2");
    let set3 = set_test_case("a", "3");

    let replica_set = ctx.primary_replica_set("a");

    let resp = ctx.conn.execute_ref(&set1.operation).await;
    assert_eq!(resp, set1.expected_response);

    let resp = ctx.conn.execute_ref(&set1.validate.operation).await;
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

    let resp = ctx.conn.execute_ref(&set1.validate.operation).await;
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
