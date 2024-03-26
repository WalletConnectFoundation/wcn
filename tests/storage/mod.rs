use {
    crate::{test_key, timestamp},
    ::network::PeerId,
    irn::{
        cluster::{keyspace::hashring::Positioned, replication::Strategy},
        migration::StoreHinted,
        network::SendRequest,
        replication::{ReplicaError, ReplicatableOperation, ReplicatedRequest},
        Storage as _,
    },
    irn_bin::{
        network::{self, rpc, MapRpc},
        storage::{Del, Get, GetExp, HDel, HGet, HGetExp, HSet, HSetExp, Set, SetExp, Storage},
        test_cluster::Cluster,
    },
    relay_rocks::{util::timestamp_micros, StorageError},
    relay_storage::keys::PositionedKey,
    std::fmt,
    test_utils::samples::sample_project_data_cache_item_ser,
};

mod namespaces;

async fn assert_storage_request<Op: ReplicatableOperation<Key = Vec<u8>> + MapRpc>(
    cluster: &Cluster,
    nodes: (usize, usize),
    operation: Op,
    expected: Result<Op::Output, ReplicaError<StorageError>>,
) where
    Result<Op::Output, ReplicaError<StorageError>>: Eq + fmt::Debug,
    network::Client:
        rpc::Send<Op::Rpc, PeerId, ReplicatedRequest<Op>, Ok = rpc::replica::Result<Op::Output>>,
{
    let node = cluster.peers[nodes.0].node();

    let req = ReplicatedRequest {
        key_position: test_key(operation.as_ref()).position(),
        operation,
        cluster_view_version: node.cluster().read().await.view().version(),
    };

    let result = node
        .network()
        .send_request(*cluster.peers[nodes.1].node().id(), req)
        .await
        .unwrap();
    assert_eq!(result, expected);
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn project_data_cache() {
    let cluster = Cluster::setup(2, Strategy::default()).await.unwrap();

    let (key, value) = sample_project_data_cache_item_ser("UUID1", "NAME1");

    let get = Get { key: key.clone() };

    // Make sure that key is not present on both nodes.
    assert_storage_request(&cluster, (1, 0), get.clone(), Ok(None)).await;
    assert_storage_request(&cluster, (0, 1), get.clone(), Ok(None)).await;

    // Set key on node 0. Make sure it is still not present on node 1.
    let mut set = Set {
        key: key.clone(),
        value: value.clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_storage_request(&cluster, (1, 0), set.clone(), Ok(())).await;
    let expected = Ok(Some(value.clone()));
    assert_storage_request(&cluster, (1, 0), get.clone(), expected).await;
    assert_storage_request(&cluster, (0, 1), get.clone(), Ok(None)).await;

    // Update key on node 0. Make sure it is still not present on node 1.
    let (_, value_updated) = sample_project_data_cache_item_ser("UUID1_UPD", "NAME1_UPD");

    set.value = value_updated.clone();
    set.version = timestamp_micros();
    assert_storage_request(&cluster, (1, 0), set, Ok(())).await;
    let expected = Ok(Some(value_updated.clone()));
    assert_storage_request(&cluster, (1, 0), get.clone(), expected).await;
    assert_storage_request(&cluster, (0, 1), get.clone(), Ok(None)).await;

    // Remove key on node 0. Both nodes should not have the key.

    assert_storage_request(&cluster, (1, 0), Del { key: key.clone() }, Ok(())).await;
    assert_storage_request(&cluster, (1, 0), get.clone(), Ok(None)).await;
    assert_storage_request(&cluster, (0, 1), get.clone(), Ok(None)).await;

    // Remove operation on non-existent key should return Ok.
    assert_storage_request(&cluster, (1, 0), Del { key: key.clone() }, Ok(())).await;
}

async fn assert_operation_hint<Op, Ok: fmt::Debug + Eq>(
    cluster: &Cluster,
    node: usize,
    operation: Op,
    expected: Result<Ok, StorageError>,
) where
    Storage: irn::Storage<StoreHinted<Positioned<Op>>, Ok = Ok, Error = StorageError>,
{
    let operation = Positioned {
        position: 0,
        inner: operation,
    };

    let got = cluster.peers[node]
        .node()
        .storage()
        .exec(StoreHinted { operation })
        .await;

    assert_eq!(expected, got);
}

async fn assert_storage_op<Op, Ok: fmt::Debug + Eq>(
    cluster: &Cluster,
    node: usize,
    operation: Op,
    expected: Result<Ok, StorageError>,
) where
    Storage: irn::Storage<Positioned<Op>, Ok = Ok, Error = StorageError>,
{
    let got = cluster.peers[node]
        .node()
        .storage()
        .exec(Positioned {
            position: 0,
            inner: operation,
        })
        .await;

    assert_eq!(expected, got);
}

// TODO: this should be unit tested in rocks
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn string_type_hinted_ops() {
    let cluster = Cluster::setup(2, Strategy::default()).await.unwrap();

    let keys = [
        "key1".as_bytes().to_vec(),
        "key2".as_bytes().to_vec(),
        "key3".as_bytes().to_vec(),
    ];
    let vals = [
        "value1".as_bytes().to_vec(),
        "value2".as_bytes().to_vec(),
        "value3".as_bytes().to_vec(),
    ];

    let expiration = timestamp(10);

    // Make sure that no data is present on any of two nodes.
    for key in &keys {
        assert_storage_op(&cluster, 0, Get { key: key.clone() }, Ok(None)).await;
        assert_storage_op(&cluster, 1, Get { key: key.clone() }, Ok(None)).await;
    }

    // Save hinted ops on node 0.

    // set(key=key1, value=value1)
    let mut set = Set {
        key: keys[0].clone(),
        value: vals[0].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key2, value=value2)
    set.key = keys[1].clone();
    set.value = vals[1].clone();
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key3, value=value3)
    set.key = keys[2].clone();
    set.value = vals[2].clone();
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // setexp(key=key2, expiration=5)
    let set_exp = SetExp {
        key: keys[1].clone(),
        expiration: Some(expiration),
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set_exp, Ok(())).await;

    // del(key=key1)
    let del = Del {
        key: keys[0].clone(),
    };
    assert_operation_hint(&cluster, 0, del, Ok(())).await;

    // Commit hinted ops.
    cluster.peers[0]
        .node()
        .storage()
        .db()
        .commit_hinted_ops(None, None)
        .unwrap();

    // Now data should be present on node 0 (but not on node 1).
    for (key, val) in keys.iter().zip(vals.iter()) {
        // Deleted key, must not be present.
        if key == &keys[0] {
            assert_storage_op(&cluster, 0, Get { key: key.clone() }, Ok(None)).await;
            continue;
        }

        // Assert value.
        let expected = Ok(Some(val.clone()));
        assert_storage_op(&cluster, 0, Get { key: key.clone() }, expected).await;

        // Assert expiration.
        let expected_expiration = if key == &keys[1] {
            Some(expiration)
        } else {
            None
        };

        let get_exp = GetExp { key: key.clone() };
        assert_storage_op(&cluster, 0, get_exp, Ok(expected_expiration)).await;

        // Node 1 must not have the data.
        assert_storage_op(&cluster, 1, Get { key: key.clone() }, Ok(None)).await;
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn map_type_hinted_ops() {
    let cluster = Cluster::setup(2, Strategy::default()).await.unwrap();

    let keys = [
        "key1".as_bytes().to_vec(),
        "key2".as_bytes().to_vec(),
        "key3".as_bytes().to_vec(),
    ];
    let fields = [
        "field1".as_bytes().to_vec(),
        "field2".as_bytes().to_vec(),
        "field3".as_bytes().to_vec(),
    ];
    let vals = [
        "value1".as_bytes().to_vec(),
        "value2".as_bytes().to_vec(),
        "value3".as_bytes().to_vec(),
    ];

    let expiration = timestamp(10);

    // Make sure that no data is present on any of two nodes.
    for (key, field) in keys.iter().zip(fields.iter()) {
        let hget = HGet {
            key: key.clone(),
            field: field.clone(),
        };

        assert_storage_op(&cluster, 0, hget.clone(), Ok(None)).await;
        assert_storage_op(&cluster, 1, hget.clone(), Ok(None)).await;
    }

    // Save hinted ops on node 0 (node 1 sends requests).

    // set(key=key1, field=field1, value=value1)
    let set = HSet {
        key: keys[0].clone(),
        field: fields[0].clone(),
        value: vals[0].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key1, field=field2, value=value2), another field to the same key
    let set = HSet {
        key: keys[0].clone(),
        field: fields[1].clone(),
        value: vals[1].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key1, field=field3, value=value2), another field to the same key
    let set = HSet {
        key: keys[0].clone(),
        field: fields[2].clone(),
        value: vals[1].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key2, field=field2, value=value2)
    let set = HSet {
        key: keys[1].clone(),
        field: fields[1].clone(),
        value: vals[1].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // set(key=key3, field=field3, value=value3)
    let set = HSet {
        key: keys[2].clone(),
        field: fields[2].clone(),
        value: vals[2].clone(),
        expiration: None,
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set.clone(), Ok(())).await;

    // setexp(key=key1, field=field2, expiration=5)
    let set_exp = HSetExp {
        key: keys[0].clone(),
        field: fields[1].clone(),
        expiration: Some(expiration),
        version: timestamp_micros(),
    };
    assert_operation_hint(&cluster, 0, set_exp, Ok(())).await;

    // del(key=key1, field=field1)
    let del = HDel {
        key: keys[0].clone(),
        field: fields[0].clone(),
    };
    assert_operation_hint(&cluster, 0, del, Ok(())).await;

    // Commit hinted ops.
    cluster.peers[0]
        .node()
        .storage()
        .db()
        .commit_hinted_ops(None, None)
        .unwrap();

    // Now data should be present on node 0 (but not on node 1).
    for (key, field, val) in itertools::multizip((&keys, &fields, &vals)) {
        let hget = HGet {
            key: key.clone(),
            field: field.clone(),
        };

        // Deleted pair, must not be present.
        if key == &keys[0] && field == &fields[0] {
            assert_storage_op(&cluster, 0, hget.clone(), Ok(None)).await;
            continue;
        }

        // Assert value.
        assert_storage_op(&cluster, 0, hget.clone(), Ok(Some(val.clone()))).await;

        // Assert expiration.
        let expected_expiration = if key == &keys[0] && field == &fields[1] {
            Some(expiration)
        } else {
            None
        };

        let hget_exp = HGetExp {
            key: key.clone(),
            field: field.clone(),
        };
        assert_storage_op(&cluster, 0, hget_exp, Ok(expected_expiration)).await;

        // Node 1 must not have the data.
        assert_storage_op(&cluster, 1, hget, Ok(None)).await;
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn cluster_version_mismatch() {
    let cluster = Cluster::setup(2, Strategy::default()).await.unwrap();

    let node = cluster.peers[0].node();

    let (key, _) = sample_project_data_cache_item_ser("UUID1", "NAME1");

    let req = ReplicatedRequest {
        key_position: test_key(&key).position(),
        operation: Get { key: key.clone() },
        cluster_view_version: u128::MAX,
    };

    let result = node
        .network()
        .send_request(*cluster.peers[1].node().id(), req)
        .await
        .unwrap();
    assert_eq!(result, Err(ReplicaError::ClusterViewVersionMismatch));
}
