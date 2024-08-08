use {
    super::{keyspace, node, Nodes},
    crate::cluster::{self, ReplicaSet},
    futures::StreamExt,
    serde::{Deserialize, Serialize},
    std::{
        collections::HashSet,
        error::Error as StdError,
        hash::{BuildHasherDefault, DefaultHasher},
        ops::RangeInclusive,
    },
    tap::Tap,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    id: u64,
    region: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    unique_attr: Option<String>,
}

impl Node {
    fn new(id: u64, region: &'static str) -> Self {
        Self {
            id,
            region: region.to_string(),
            unique_attr: None,
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct Error(&'static str);

impl cluster::Node for Node {
    type Id = u64;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn can_add(&self, nodes: &Nodes<Self>) -> Result<(), impl StdError> {
        if self.unique_attr.is_none() {
            return Ok(());
        }

        if nodes.iter().any(|(_, n)| n.unique_attr == self.unique_attr) {
            return Err(Error("Not unique!"));
        }

        Ok(())
    }

    fn can_update(&self, nodes: &Nodes<Self>, new_state: &Self) -> Result<(), impl StdError> {
        if new_state.unique_attr.is_none() {
            return Ok(());
        }

        if nodes
            .iter()
            .any(|(_, n)| n.id != self.id && n.unique_attr == self.unique_attr)
        {
            return Err(Error("Not unique!"));
        }

        Ok(())
    }
}

type NodeState = node::State<Node>;

type Keyspace = keyspace::Sharded<3, BuildHasherDefault<DefaultHasher>, sharding::DefaultStrategy>;

type Cluster = crate::Cluster<Node, Keyspace>;

type ClusterSnapshot<'a> = super::Snapshot<'a, Node, Keyspace>;

#[test]
fn node_slot_map() {
    let mut nodes = node::SlotMap::new();
    nodes.remove(&1);

    assert!(nodes.insert(Node::new(1, "eu")).unwrap().is_none());
    assert_eq!(
        nodes.insert(Node::new(1, "ap")).unwrap(),
        Some(Node::new(1, "eu"))
    );
    assert_eq!(nodes.iter().count(), 1);

    assert_eq!(nodes.remove(&1), Some(Node::new(1, "ap")));
    assert_eq!(nodes.iter().count(), 0);

    assert!(nodes.insert(Node::new(1, "eu")).unwrap().is_none());

    assert_eq!(nodes.get(&1), Some(&Node::new(1, "eu")));
    assert_eq!(nodes.get_by_idx(0), Some(&Node::new(1, "eu")));

    assert_eq!(nodes.get(&2), None);
    assert_eq!(nodes.get_by_idx(1), None);

    assert!(nodes.insert(Node::new(2, "us")).unwrap().is_none());
    assert_eq!(nodes.get(&2), Some(&Node::new(2, "us")));
    assert_eq!(nodes.get_by_idx(1), Some(&Node::new(2, "us")));

    assert!(nodes.insert(Node::new(3, "ap")).unwrap().is_none());
    assert_eq!(nodes.get(&3), Some(&Node::new(3, "ap")));
    assert_eq!(nodes.get_by_idx(2), Some(&Node::new(3, "ap")));

    assert_eq!(nodes.remove(&2), Some(Node::new(2, "us")));
    assert_eq!(nodes.iter().collect::<Vec<_>>(), vec![
        (0, &Node::new(1, "eu")),
        (2, &Node::new(3, "ap")),
    ]);
    assert_eq!(nodes.get(&2), None);
    assert_eq!(nodes.get_by_idx(1), None);
    assert_eq!(nodes.get(&1), Some(&Node::new(1, "eu")));
    assert_eq!(nodes.get_by_idx(0), Some(&Node::new(1, "eu")));
    assert_eq!(nodes.get(&3), Some(&Node::new(3, "ap")));
    assert_eq!(nodes.get_by_idx(2), Some(&Node::new(3, "ap")));

    assert!(nodes.insert(Node::new(4, "eu")).unwrap().is_none());
    assert_eq!(nodes.iter().count(), 3);
    assert_eq!(nodes.get(&4), Some(&Node::new(4, "eu")));
    assert_eq!(nodes.get_by_idx(1), Some(&Node::new(4, "eu")));

    assert!(!nodes.contains(&2));
    assert!(nodes.contains(&4));

    assert_eq!(nodes.iter().collect::<Vec<_>>(), vec![
        (0, &Node::new(1, "eu")),
        (1, &Node::new(4, "eu")),
        (2, &Node::new(3, "ap")),
    ]);

    assert!(nodes.insert(Node::new(2, "eu")).unwrap().is_none());
    assert_eq!(nodes.iter().count(), 4);
    assert_eq!(nodes.iter().collect::<Vec<_>>(), vec![
        (0, &Node::new(1, "eu")),
        (1, &Node::new(4, "eu")),
        (2, &Node::new(3, "ap")),
        (3, &Node::new(2, "eu"))
    ]);

    for id in 5..=256 {
        assert!(nodes.insert(Node::new(id, "eu")).unwrap().is_none());
    }
    assert_eq!(nodes.iter().count(), 256);

    assert!(nodes.insert(Node::new(257, "eu")).is_err());
    assert_eq!(nodes.iter().count(), 256);

    assert_eq!(nodes.remove(&42), Some(Node::new(42, "eu")));
    assert_eq!(nodes.iter().count(), 255);

    assert_eq!(nodes.insert(Node::new(257, "eu")).unwrap(), None);
    assert_eq!(nodes.iter().count(), 256);
    assert_eq!(nodes.get(&257), Some(&Node::new(257, "eu")));
    assert_eq!(nodes.get_by_idx(41), Some(&Node::new(257, "eu")));

    assert!(nodes.insert(Node::new(258, "eu")).is_err());

    assert_eq!(
        nodes.insert(Node::new(1, "ap")).unwrap(),
        Some(Node::new(1, "eu"))
    );
}

#[test]
fn keyspace_migration_plan() {
    const SHARD_SIZE: u64 = ((u64::MAX as u128 + 1) / 8 as u128) as u64;

    fn keyranges(shards: [&[u64; 3]; 8]) -> [keyspace::Range<&[u64; 3]>; 8] {
        let mut idx = 0;
        shards.map(|replicas| keyspace::Range {
            keys: keyrange(idx).tap(|_| idx += 1),
            replicas,
        })
    }

    fn keyrange(shard_idx: u64) -> RangeInclusive<u64> {
        let start = shard_idx * SHARD_SIZE;
        RangeInclusive::new(start, start + (SHARD_SIZE - 1))
    }

    fn pending_range(shard_idx: u64, replicas: [u64; 3]) -> keyspace::Range<HashSet<u64>> {
        keyspace::Range {
            keys: keyrange(shard_idx),
            replicas: replicas.into_iter().collect(),
        }
    }

    let migration_plan =
        |old, new| keyspace::MigrationPlan::<Node>::new(keyranges(old), keyranges(new), 0);

    let shards = [
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
        &[1, 2, 3],
    ];

    assert!(migration_plan(shards, shards).unwrap().is_empty());

    let mut new_shards = shards;
    new_shards[0] = &[1, 2, 4];
    let plan = migration_plan(shards, new_shards).unwrap();
    assert_eq!(plan.pulling_nodes().count(), 1);
    assert_eq!(plan.pending_ranges(&4), vec![pending_range(0, [1, 2, 3])]);

    let shards = new_shards;
    new_shards[7] = &[5, 6, 7];
    let plan = migration_plan(shards, new_shards).unwrap();
    assert_eq!(plan.pulling_nodes().count(), 3);
    assert_eq!(plan.pending_ranges(&5), vec![pending_range(7, [1, 2, 3])]);
    assert_eq!(plan.pending_ranges(&6), vec![pending_range(7, [1, 2, 3])]);
    assert_eq!(plan.pending_ranges(&7), vec![pending_range(7, [1, 2, 3])]);

    let shards = new_shards;
    new_shards[3] = &[5, 2, 3];
    new_shards[4] = &[5, 2, 3];
    let plan = migration_plan(shards, new_shards).unwrap();
    assert_eq!(plan.pulling_nodes().count(), 1);
    assert_eq!(plan.pending_ranges(&5), vec![
        pending_range(3, [1, 2, 3]),
        pending_range(4, [1, 2, 3])
    ]);
}

#[test]
fn snapshot() {
    let mut cluster = Cluster::new();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [],
        "restarting_node": null,
        "keyspace": null,
        "migration": null,
        "version": 0,
    });
    assert_eq!(serialized, expected_json);

    cluster.add_node(Node::new(1, "eu")).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"}],
        "restarting_node": null,
        "keyspace": null,
        "migration": null,
        "version": 1,
    });
    assert_eq!(serialized, expected_json);

    cluster.add_node(Node::new(2, "us")).unwrap();
    cluster.add_node(Node::new(3, "ap")).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "us"},{"id": 3, "region": "ap"}],
        "restarting_node": null,
        "keyspace": { "version": 0 },
        "migration": null,
        "version": 3,
    });
    assert_eq!(serialized, expected_json);

    cluster.add_node(Node::new(4, "eu")).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "us"},{"id": 3, "region": "ap"}],
        "restarting_node": null,
        "keyspace": { "version": 0 },
        "migration": {
            "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "us"},{"id": 3, "region": "ap"},{"id": 4, "region": "eu"}],
            "pulling_nodes": [4],
            "keyspace": { "version": 1 },
        },
        "version": 4,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());

    cluster.complete_pull(&4, 1).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "us"},{"id": 3, "region": "ap"},{"id": 4, "region": "eu"}],
        "restarting_node": null,
        "keyspace": { "version": 1 },
        "migration": null,
        "version": 5,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());

    cluster.shutdown_node(&2).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "us"},{"id": 3, "region": "ap"},{"id": 4, "region": "eu"}],
        "restarting_node": 2,
        "keyspace": { "version": 1 },
        "migration": null,
        "version": 6,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());

    cluster.startup_node(Node::new(2, "ap")).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "ap"},{"id": 3, "region": "ap"},{"id": 4, "region": "eu"}],
        "restarting_node": null,
        "keyspace": { "version": 1 },
        "migration": null,
        "version": 7,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());

    cluster.decommission_node(&3).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "ap"},{"id": 3, "region": "ap"},{"id": 4, "region": "eu"}],
        "restarting_node": null,
        "keyspace": { "version": 1 },
        "migration": {
            "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "ap"},null,{"id": 4, "region": "eu"}],
            "pulling_nodes": [1, 2, 4],
            "keyspace": { "version": 2 },
        },
        "version": 8,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());

    cluster.complete_pull(&1, 2).unwrap();
    cluster.complete_pull(&2, 2).unwrap();
    cluster.complete_pull(&4, 2).unwrap();

    let serialized = serde_json::to_value(&cluster.snapshot()).unwrap();
    let expected_json = serde_json::json!({
        "nodes": [{"id": 1, "region": "eu"},{"id": 2, "region": "ap"},null,{"id": 4, "region": "eu"}],
        "restarting_node": null,
        "keyspace": { "version": 2 },
        "migration": null,
        "version": 11,
    });
    assert_eq!(serialized, expected_json);
    let deserialized: ClusterSnapshot = serde_json::from_value(serialized).unwrap();
    assert_eq!(cluster, Cluster::from_snapshot(deserialized).unwrap());
}

#[tokio::test]
async fn view() {
    let mut cluster = Cluster::new().into_viewable();

    let cluster_view = cluster.view();
    let mut updates = cluster_view.updates();
    assert!(futures::poll!(updates.next()).is_ready());
    assert!(!futures::poll!(updates.next()).is_ready());

    cluster
        .modify(|c| c.add_node(Node::new(1, "eu")).map(|_| true))
        .unwrap();
    assert!(futures::poll!(updates.next()).is_ready());
    assert!(!futures::poll!(updates.next()).is_ready());

    assert_eq!(cluster_view.cluster().as_ref(), cluster.inner());
    cluster_view.peek(|c| assert_eq!(c, cluster.inner()));
}

#[test]
fn cluster() {
    fn assert_replica_set<'a>(
        required_count: usize,
        nodes: impl IntoIterator<Item = u64>,
        set: super::Result<ReplicaSet<impl Iterator<Item = &'a Node>>>,
    ) {
        let set = set.unwrap();
        assert_eq!(set.required_count, required_count);
        let expected_nodes: HashSet<_> = nodes.into_iter().collect();
        let nodes: HashSet<_> = set.nodes.map(|n| n.id).collect();
        assert_eq!(nodes, expected_nodes);
    }

    let mut cluster = Cluster::new();
    assert_eq!(
        cluster.replica_set(42, false).err(),
        Some(cluster::Error::NotBootstrapped)
    );

    assert_eq!(cluster.add_node(Node::new(1, "eu")), Ok(()));
    assert_eq!(
        cluster.replica_set(42, false).err(),
        Some(cluster::Error::NotBootstrapped)
    );

    assert_eq!(cluster.add_node(Node::new(2, "us")), Ok(()));
    assert_eq!(
        cluster.replica_set(42, false).err(),
        Some(cluster::Error::NotBootstrapped)
    );

    assert_eq!(cluster.add_node(Node::new(3, "ap")), Ok(()));
    assert_eq!(cluster.replica_set(42, false).err(), None);

    let mut nodes = vec![Node::new(1, "eu"), Node::new(2, "us"), Node::new(3, "ap")];

    assert_eq!(&nodes, &cluster.nodes().cloned().collect::<Vec<_>>());

    assert_eq!(cluster.node(&1), Some(&Node::new(1, "eu")));
    assert_eq!(cluster.node(&2), Some(&Node::new(2, "us")));
    assert_eq!(cluster.node(&3), Some(&Node::new(3, "ap")));
    assert_eq!(cluster.node(&4), None);

    assert!(cluster.contains_node(&1));
    assert!(cluster.contains_node(&2));
    assert!(cluster.contains_node(&3));
    assert!(!cluster.contains_node(&4));

    assert_eq!(cluster.node_state(&1), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&2), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&3), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&4), None);

    assert_replica_set(2, [1, 2, 3], cluster.replica_set(42, false));
    assert_replica_set(2, [1, 2, 3], cluster.replica_set(42, true));

    // restart

    assert_eq!(cluster.version(), 3);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(cluster.shutdown_node(&4), Err(cluster::Error::UnknownNode));

    assert_eq!(cluster.version(), 3);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(cluster.shutdown_node(&1), Ok(true));

    assert_eq!(cluster.node_state(&1), Some(NodeState::Restarting));
    assert_eq!(cluster.version(), 4);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(cluster.shutdown_node(&1), Ok(false));

    assert_eq!(cluster.node_state(&1), Some(NodeState::Restarting));
    assert_eq!(cluster.version(), 4);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(
        cluster.shutdown_node(&2),
        Err(cluster::Error::AnotherNodeRestarting)
    );

    assert_eq!(cluster.node_state(&2), Some(NodeState::Normal));
    assert_eq!(cluster.version(), 4);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(
        cluster.startup_node(Node::new(4, "ap")),
        Err(cluster::Error::UnknownNode)
    );
    assert_eq!(cluster.version(), 4);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(
        cluster.startup_node(Node::new(2, "us")),
        Err(cluster::Error::NodeAlreadyStarted)
    );
    assert_eq!(cluster.version(), 4);
    assert_eq!(cluster.keyspace_version(), 0);

    nodes[0] = Node::new(1, "us");

    assert_eq!(cluster.startup_node(Node::new(1, "us")), Ok(()));
    assert_eq!(&nodes, &cluster.nodes().cloned().collect::<Vec<_>>());
    assert_eq!(cluster.node(&1), Some(&Node::new(1, "us")));
    assert_eq!(cluster.node_state(&1), Some(NodeState::Normal));
    assert_eq!(cluster.version(), 5);
    assert_eq!(cluster.keyspace_version(), 0);

    assert_eq!(
        cluster.startup_node(Node::new(1, "eu")),
        Err(cluster::Error::NodeAlreadyStarted)
    );
    assert_eq!(cluster.version(), 5);
    assert_eq!(cluster.keyspace_version(), 0);

    // node addition

    assert_eq!(
        cluster.add_node(Node::new(3, "eu")),
        Err(cluster::Error::NodeAlreadyExists)
    );

    let mut new_node = Node::new(4, "eu");
    new_node.unique_attr = Some("wc".to_string());
    nodes.push(new_node.clone());

    assert_eq!(cluster.add_node(new_node), Ok(()));
    assert_eq!(&nodes, &cluster.nodes().cloned().collect::<Vec<_>>());
    assert_eq!(cluster.node_state(&1), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&2), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&3), Some(NodeState::Normal));
    let migration_plan = match cluster.node_state(&4) {
        Some(node::State::Pulling(plan)) => plan,
        _ => panic!(),
    };
    assert_eq!(cluster.version(), 6);
    assert_eq!(cluster.keyspace_version(), 1);

    {
        let plan = migration_plan;
        let pending_ranges = plan.pending_ranges(&4);
        let key = *pending_ranges[0].keys.start();

        assert_replica_set(2, [1, 2, 3], cluster.replica_set(key, false));
        assert_replica_set(3, [1, 2, 3, 4], cluster.replica_set(key, true));
        assert_replica_set(2, [1, 2, 3], cluster.replica_set(key - 1, true));
    }

    assert_eq!(
        cluster.add_node(Node::new(5, "eu")),
        Err(cluster::Error::MigrationInProgress)
    );
    assert_eq!(cluster.version(), 6);
    assert_eq!(cluster.keyspace_version(), 1);

    assert_eq!(
        cluster.complete_pull(&5, 1),
        Err(cluster::Error::UnknownNode)
    );
    assert_eq!(cluster.version(), 6);
    assert_eq!(cluster.keyspace_version(), 1);

    assert_eq!(
        cluster.complete_pull(&4, 0),
        Err(cluster::Error::KeyspaceVersionMismatch)
    );
    assert_eq!(cluster.version(), 6);
    assert_eq!(cluster.keyspace_version(), 1);

    assert_eq!(cluster.complete_pull(&3, 1), Ok(false));
    assert_eq!(cluster.version(), 6);
    assert_eq!(cluster.keyspace_version(), 1);

    assert_eq!(
        cluster.shutdown_node(&1),
        Err(cluster::Error::MigrationInProgress)
    );

    assert_eq!(cluster.complete_pull(&4, 1), Ok(true));
    assert_eq!(cluster.version(), 7);
    assert_eq!(cluster.keyspace_version(), 1);
    assert_eq!(cluster.node_state(&1), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&2), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&3), Some(NodeState::Normal));
    assert_eq!(cluster.node_state(&4), Some(NodeState::Normal));

    assert_eq!(
        cluster.complete_pull(&4, 1),
        Err(cluster::Error::NoMigration)
    );
    assert_eq!(cluster.version(), 7);
    assert_eq!(cluster.keyspace_version(), 1);

    // node removal

    assert_eq!(
        cluster.decommission_node(&5),
        Err(cluster::Error::UnknownNode)
    );
    assert_eq!(cluster.version(), 7);
    assert_eq!(cluster.keyspace_version(), 1);

    assert_eq!(cluster.decommission_node(&2), Ok(()));
    assert_eq!(&nodes, &cluster.nodes().cloned().collect::<Vec<_>>());
    let migration_plan = match cluster.node_state(&1) {
        Some(node::State::Pulling(plan)) => plan,
        _ => panic!(),
    };
    assert_eq!(cluster.node_state(&2), Some(NodeState::Decommissioning));
    assert!(matches!(
        cluster.node_state(&3),
        Some(NodeState::Pulling(_))
    ));
    assert!(matches!(
        cluster.node_state(&4),
        Some(NodeState::Pulling(_))
    ));
    assert_eq!(cluster.version(), 8);
    assert_eq!(cluster.keyspace_version(), 2);

    {
        let plan = migration_plan;

        let key = *plan.pending_ranges(&1)[0].keys.start();
        assert_replica_set(2, [2, 3, 4], cluster.replica_set(key, false));
        assert_replica_set(3, [1, 2, 3, 4], cluster.replica_set(key, true));

        let key = *plan.pending_ranges(&3)[0].keys.start();
        assert_replica_set(2, [1, 2, 4], cluster.replica_set(key, false));
        assert_replica_set(3, [1, 2, 3, 4], cluster.replica_set(key, true));

        let key = *plan.pending_ranges(&4)[0].keys.start();
        assert_replica_set(2, [1, 2, 3], cluster.replica_set(key, false));
        assert_replica_set(3, [1, 2, 3, 4], cluster.replica_set(key, true));
    }

    assert_eq!(
        cluster.decommission_node(&3),
        Err(cluster::Error::MigrationInProgress)
    );
    assert_eq!(cluster.version(), 8);
    assert_eq!(cluster.keyspace_version(), 2);

    cluster.complete_pull(&1, 2).unwrap();
    cluster.complete_pull(&3, 2).unwrap();
    cluster.complete_pull(&4, 2).unwrap();

    assert_eq!(cluster.version(), 11);
    assert_eq!(cluster.keyspace_version(), 2);
}
