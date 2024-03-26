use {
    crate::{
        cluster::{
            keyspace::{hashring::Positioned, Keyspace, RingPosition},
            replication::{ConsistencyLevel, Strategy},
            NodeOperationMode,
        },
        network::HandleRequest,
        replication::{CoordinatorResponse, DispatchReplicated},
        test::{self, ReadAssert, ReadOutput, RemoveOutput, WriteOutput},
        PeerId,
        Storage,
    },
    futures::{stream, StreamExt},
    itertools::{Either, Itertools},
    std::{
        collections::HashMap,
        fmt::Debug,
        marker::PhantomData,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::time::sleep,
};

#[derive(Debug)]
pub struct ReplicationTestCase {
    pub seed: Option<u64>,
    pub nodes_count: usize,
    pub strategy: Strategy,
    pub coordinator_in_replica_set: bool,
}

impl ReplicationTestCase {
    pub fn new(
        nodes_count: usize,
        replication_factor: usize,
        consistency_level: ConsistencyLevel,
        coordinator_in_replica_set: bool,
    ) -> Self {
        // Seeded PRNG allows to obtain the same sequence of random keys for all
        // assertions that are executed against a given test case.
        let seed = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        );
        let strategy = Strategy::new(replication_factor, consistency_level);
        Self {
            seed,
            nodes_count,
            strategy,
            coordinator_in_replica_set,
        }
    }
}

/// Test suite for replicated operations.
/// Defines a set of tests that should be run against a cluster that has a
/// given set of trait bounds.
pub struct ReplicationTestSuite<C, SE, NE> {
    _phantom: PhantomData<(C, NE, SE)>,
}

impl<C, SE, NE> ReplicationTestSuite<C, SE, NE>
where
    C: test::Cluster,
    SE: Debug,
    NE: Debug,
    C::Node: HandleRequest<
            DispatchReplicated<C::ReadOperation>,
            Response = CoordinatorResponse<ReadOutput<C>, SE, NE>,
        > + HandleRequest<
            DispatchReplicated<C::WriteOperation>,
            Response = CoordinatorResponse<WriteOutput<C>, SE, NE>,
        > + HandleRequest<
            DispatchReplicated<C::RemoveOperation>,
            Response = CoordinatorResponse<RemoveOutput<C>, SE, NE>,
        >,
    C::Keyspace: Keyspace<Node = PeerId>,
    C::Storage: Storage<Positioned<C::ReadOperation>, Ok = ReadOutput<C>, Error = SE>
        + Storage<Positioned<C::WriteOperation>, Ok = WriteOutput<C>, Error = SE>
        + Storage<Positioned<C::RemoveOperation>, Ok = WriteOutput<C>, Error = SE>,
    ReadOutput<C>: Debug + PartialEq + Eq,
    WriteOutput<C>: Debug + PartialEq + Eq,
    RemoveOutput<C>: Debug + PartialEq + Eq,
{
    pub async fn string_read_repair(test_case: ReplicationTestCase) {
        assert!(
            test_case.nodes_count >= 3
                && test_case.strategy.consistency_level() == ConsistencyLevel::Quorum
        );

        // Init cluster.
        let cluster: C =
            test::Cluster::run(test_case.nodes_count, Some(test_case.strategy.clone())).await;

        for idx in 0..test_case.nodes_count {
            let node = cluster.node(idx).as_ref();
            node.wait_op_mode(node.id, NodeOperationMode::Normal).await;
        }

        // Generate write and read assertions.
        let write_assert = cluster.gen_write_assert(test_case.seed);
        let read_assert = cluster.gen_read_assert(test_case.seed);
        let empty_read_assert = cluster.gen_empty_read_assert(test_case.seed);
        let pos = cluster
            .keyspace()
            .key_position(write_assert.operation.as_ref());

        // Split nodes into two groups: replica set nodes and non-replica set nodes.
        let replicas = cluster.keyspace().replica_set(pos).replicas();
        let (rs_nodes, non_rs_nodes): (Vec<_>, Vec<_>) =
            (0..test_case.nodes_count).partition_map(|idx| {
                let node = cluster.node(idx);
                if replicas.contains(&node.as_ref().id) {
                    Either::Left(idx)
                } else {
                    Either::Right(idx)
                }
            });

        // All but a single replica node will get value (directly via storage).
        stream::iter(rs_nodes.clone())
            .skip(1)
            .map(|idx| {
                let op = write_assert.operation.clone();
                let node = cluster.node(idx);
                async move {
                    node.as_ref()
                        .storage()
                        .exec(Positioned {
                            position: pos,
                            inner: op,
                        })
                        .await
                        .expect("storage");
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;
        Self::assert_storage_read(&cluster, &empty_read_assert, rs_nodes[0], pos).await;
        for node_idx in rs_nodes.iter().skip(1) {
            Self::assert_storage_read(&cluster, &read_assert, *node_idx, pos).await;
        }

        // Select the coordinator node.
        // If coordinator is in the replica set, the node w/o value is selected.
        let coordinator = if test_case.coordinator_in_replica_set {
            rs_nodes[0]
        } else {
            non_rs_nodes[0]
        };

        // Make sure that selected coordinator node doesn't have the value.
        Self::assert_storage_read(&cluster, &empty_read_assert, coordinator, pos).await;

        // Send replicated requests to a node in replica set that does have the
        // value. This should return a value (from the requested node and from one of
        // its replica peers), but shouldn't result in a repair of the coordinator's
        // value.
        let output = cluster
            .node(rs_nodes[1])
            .handle_request(DispatchReplicated {
                operation: read_assert.operation.clone(),
            })
            .await
            .expect("coordinator")
            .expect("replica");
        assert_eq!(output, read_assert.expected_output, "{:?}", test_case);
        Self::assert_storage_read(&cluster, &empty_read_assert, coordinator, pos).await;

        // If the node coordinator is in the replica set, then not only the data is
        // returned but the value is also repaired (and available when requested
        // directly via storage of the coordinator).
        let output = cluster
            .node(coordinator)
            .handle_request(DispatchReplicated {
                operation: read_assert.operation.clone(),
            })
            .await
            .expect("coordinator")
            .expect("replica");
        assert_eq!(output, read_assert.expected_output, "{:?}", test_case);
        if test_case.coordinator_in_replica_set {
            Self::assert_storage_read(&cluster, &read_assert, coordinator, pos).await;
        } else {
            Self::assert_storage_read(&cluster, &empty_read_assert, coordinator, pos).await;
        }
    }

    pub async fn string_basic_ops(test_case: ReplicationTestCase) {
        // How many peers are targeted for the request.
        let replication_factor = test_case.strategy.replication_factor();

        // How many peers are required to respond with success for the request to be
        // considered successful.
        let required_replica_count = test_case.strategy.required_replica_count();

        // Init cluster.
        let cluster: C =
            test::Cluster::run(test_case.nodes_count, Some(test_case.strategy.clone())).await;

        for idx in 0..test_case.nodes_count {
            let node = cluster.node(idx).as_ref();
            node.wait_op_mode(node.id, NodeOperationMode::Normal).await;
        }

        let write_assert = cluster.gen_write_assert(test_case.seed);
        let pos = cluster
            .keyspace()
            .key_position(write_assert.operation.as_ref());

        tracing::info!("case: {:?}", test_case);

        let replicas = cluster.keyspace().replica_set(pos).replicas();

        // For test cases in which coordinator is in the replica set, the coordinator
        // node is selected from that set. Otherwise, it is selected from the remaining
        // nodes, which results in one additional remote request when dispatched.
        let coordinator = (0..test_case.nodes_count)
            .find_map(|idx| {
                let node = cluster.node(idx);
                if test_case.coordinator_in_replica_set == replicas.contains(&node.as_ref().id) {
                    Some(node)
                } else {
                    None
                }
            })
            .expect("coordinator can be selected");

        // Set data point by dispatching a replicated request.
        let output = coordinator
            .handle_request(DispatchReplicated {
                operation: write_assert.operation,
            })
            .await
            .expect("coordinator")
            .expect("replica");
        assert_eq!(output, write_assert.expected_output, "{:?}", test_case);

        // Make sure that at least consistency level of peers have the data.
        // Request nodes' storages directly, to avoid replicated dispatch.
        let read_assert = cluster.gen_read_assert(test_case.seed);
        let peers_data =
            Self::peers_data_match(&cluster, test_case.nodes_count, pos, &read_assert).await;
        let peers_with_data_count = peers_data.values().sum::<usize>();
        assert!(
            peers_with_data_count >= required_replica_count,
            "{:?}",
            test_case
        );

        // If peers with data count is less than replication factor, then allow some
        // time for data to propagate, and check all replicas.
        if peers_with_data_count < replication_factor {
            sleep(Duration::from_millis(200)).await;
            let peers_data =
                Self::peers_data_match(&cluster, test_case.nodes_count, pos, &read_assert).await;
            let peers_with_data_count = peers_data.values().sum::<usize>();
            assert_eq!(peers_with_data_count, replication_factor, "{:?}", test_case);

            // Make sure that data is NOT present on nodes that are not in the replica set.
            (0..test_case.nodes_count).for_each(|idx| {
                let node = cluster.node(idx);
                // Assert on nodes that are not in the replica set.
                if !replicas.contains(&node.as_ref().id) {
                    assert_eq!(peers_data.get(&node.as_ref().id).unwrap(), &0);
                }
            });
        }

        // Now check the data using replicated read.
        let output = coordinator
            .handle_request(DispatchReplicated {
                operation: read_assert.operation.clone(),
            })
            .await
            .expect("coordinator")
            .expect("replica");
        assert_eq!(output, read_assert.expected_output, "{:?}", test_case);

        // Delete replicated data.
        let remove_assert = cluster.gen_remove_assert(test_case.seed);
        let output = coordinator
            .handle_request(DispatchReplicated {
                operation: remove_assert.operation,
            })
            .await
            .expect("coordinator")
            .expect("replica");
        assert_eq!(output, remove_assert.expected_output, "{:?}", test_case);

        // Make sure that no data is present on any of the cluster peers.
        sleep(Duration::from_millis(200)).await;
        let peers_data =
            Self::peers_data_match(&cluster, test_case.nodes_count, pos, &read_assert).await;
        let peers_with_data_count = peers_data.values().sum::<usize>();
        assert_eq!(peers_with_data_count, 0, "{:?}", test_case);
    }

    async fn peers_data_match(
        cluster: &C,
        nodes_count: usize,
        pos: RingPosition,
        assert: &ReadAssert<C>,
    ) -> HashMap<PeerId, usize> {
        stream::iter(0..nodes_count)
            .map(|idx| {
                let op = assert.operation.clone();
                let expected_output = &assert.expected_output;
                let cluster_node = cluster.node(idx);
                async move {
                    let output = cluster_node
                        .as_ref()
                        .storage()
                        .exec(Positioned {
                            position: pos,
                            inner: op,
                        })
                        .await
                        .expect("storage");
                    (
                        cluster_node.as_ref().id,
                        (output == *expected_output) as usize,
                    )
                }
            })
            .buffer_unordered(10)
            .collect::<HashMap<_, _>>()
            .await
    }

    async fn assert_storage_read(
        cluster: &C,
        read_assert: &ReadAssert<C>,
        node_idx: usize,
        pos: RingPosition,
    ) {
        let output = cluster
            .node(node_idx)
            .as_ref()
            .storage()
            .exec(Positioned {
                position: pos,
                inner: read_assert.operation.clone(),
            })
            .await
            .expect("storage");
        assert_eq!(output, read_assert.expected_output);
    }
}
