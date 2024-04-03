use {
    crate::{test_key, Data},
    irn::cluster::{
        self,
        keyspace::{hashring, Keyspace},
        replication::{ConsistencyLevel, Strategy},
    },
    lib::{
        storage::{
            Del,
            Get,
            GetExp,
            HFields,
            HGet,
            HGetExp,
            HScan,
            HSet,
            HSetExp,
            HVals,
            Set,
            SetExp,
        },
        test_cluster::Cluster,
    },
    relay_rocks::{db::types::StringStorage, util::timestamp_micros},
    test_log::test,
};

async fn create_cluster(node_count: usize, strategy: Strategy) -> (Cluster, hashring::Keyspace) {
    let cluster = Cluster::setup(node_count, strategy.clone()).await.unwrap();

    // Register cluster nodes in the keyspace.
    let mut keyspace = hashring::Keyspace::new(strategy);
    cluster.peers.iter().for_each(|p| {
        keyspace
            .add_node(&cluster::Node::generate(*p.node().id()))
            .expect("Failed to add node to keyspace");
    });

    (cluster, keyspace)
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn string_type_basic_replication() {
    let (cluster, keyspace) = create_cluster(3, Strategy::new(3, ConsistencyLevel::All)).await;

    let (key, value) = crate::generate_kv_data_ser();
    let positioned_key = test_key(&key);
    let replica_set = keyspace.replica_set(positioned_key.position());

    // List of peers that are expected to act as replicas.
    let replicas = replica_set.replicas();
    assert_eq!(replicas.len(), 3);

    let node = cluster.peer(&replicas[0]).node();

    // Ensure there's no initial value.
    let response = node.storage().string().get(&positioned_key).await;
    assert_eq!(response, Ok(None));

    // Store the data.
    let expiration = chrono::Utc::now().timestamp() as u64 + 10;
    let response = node
        .dispatch_replicated(Set {
            key: key.clone(),
            value: value.clone(),
            expiration: Some(expiration),
            version: timestamp_micros(),
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    // Confirm the expiration timestamp.
    let response = node.dispatch_replicated(GetExp { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Some(expiration))));

    // Set expiration explicitly.
    let expiration = chrono::Utc::now().timestamp() as u64 + 25;
    let response = node
        .dispatch_replicated(SetExp {
            key: key.clone(),
            expiration: Some(expiration),
            version: timestamp_micros(),
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    // Confirm the expiration timestamp.
    let response = node.dispatch_replicated(GetExp { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Some(expiration))));

    // Delete the data.
    let response = node.dispatch_replicated(Del { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(())));

    // Confirm deletion.
    let response = node.dispatch_replicated(Get { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(None)));
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn string_type_modification_timestamp() {
    let (cluster, keyspace) = create_cluster(3, Strategy::new(3, ConsistencyLevel::All)).await;

    let (key, value1) = crate::generate_kv_data_ser();
    let (_, value2) = crate::generate_kv_data_ser();
    let positioned_key = test_key(&key);
    let replica_set = keyspace.replica_set(positioned_key.position());

    // List of peers that are expected to act as replicas.
    let replicas = replica_set.replicas();
    assert_eq!(replicas.len(), 3);

    let node = cluster.peer(&replicas[0]).node();

    // Ensure there's no initial value.
    let response = node.storage().string().get(&positioned_key).await;
    assert_eq!(response, Ok(None));

    let timestamp1 = timestamp_micros();
    let timestamp2 = timestamp_micros() + 1;

    // Add data.
    let response = node
        .dispatch_replicated(Set {
            key: key.clone(),
            value: value1.clone(),
            expiration: None,
            version: timestamp1,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node.dispatch_replicated(Get { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Some(value1.clone()))));

    // Update the data with higher timestamp value. It's expected to succeed.
    let response = node
        .dispatch_replicated(Set {
            key: key.clone(),
            value: value2.clone(),
            expiration: None,
            version: timestamp2,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node.dispatch_replicated(Get { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Some(value2.clone()))));

    // Update the data with lower timestamp value. It's expected to be ignored.
    let response = node
        .dispatch_replicated(Set {
            key: key.clone(),
            value: value1.clone(),
            expiration: None,
            version: timestamp1,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node.dispatch_replicated(Get { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Some(value2))));
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn map_type_basic_replication() {
    let (cluster, keyspace) = create_cluster(3, Strategy::new(3, ConsistencyLevel::All)).await;

    let mut data = crate::generate_data(1, 10);
    let (key, entries) = data.pop().unwrap();
    let key = crate::serialize(&key);
    let positioned_key = test_key(&key);
    let replica_set = keyspace.replica_set(positioned_key.position());

    // List of peers that are expected to act as replicas.
    let replicas = replica_set.replicas();
    assert_eq!(replicas.len(), 3);

    let node = cluster.peer(&replicas[0]).node();

    // Ensure there's no initial value.
    let response = node.dispatch_replicated(HFields { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Vec::new())));

    let base_timestamp = chrono::Utc::now().timestamp() as u64 + 10;

    for (i, entry) in entries.iter().enumerate() {
        let i = i as u64;
        let field = entry.to_hash();
        let value = crate::serialize(&entry);
        let expiration = base_timestamp + i;

        // Add entry to the set.
        let response = node
            .dispatch_replicated(HSet {
                key: key.clone(),
                field: field.clone(),
                value: value.clone(),
                expiration: Some(expiration),
                version: timestamp_micros(),
            })
            .await;
        assert_eq!(response, Ok(Ok(())));

        // Get expiration.
        let response = node
            .dispatch_replicated(HGetExp {
                key: key.clone(),
                field: field.clone(),
            })
            .await;
        assert!(matches!(response, Ok(Ok(Some(t))) if t == expiration ));

        let expiration = expiration + 10;

        // Update expiration.
        let response = node
            .dispatch_replicated(HSetExp {
                key: key.clone(),
                field: field.clone(),
                expiration: Some(expiration),
                version: timestamp_micros(),
            })
            .await;
        assert_eq!(response, Ok(Ok(())));

        // Get expiration.
        let response = node
            .dispatch_replicated(HGetExp {
                key: key.clone(),
                field: field.clone(),
            })
            .await;
        assert!(matches!(response, Ok(Ok(Some(t))) if t == expiration ));
    }

    // Fetch all fields.
    let fields = entries
        .iter()
        .map(|entry| entry.to_hash())
        .collect::<Vec<_>>();
    let response = node
        .dispatch_replicated(HFields { key: key.clone() })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(crate::sort_data(response), crate::sort_data(fields));

    // Fetch all values.
    let values = entries
        .into_iter()
        .map(|entry| crate::serialize(&entry))
        .collect::<Vec<_>>();
    let response = node
        .dispatch_replicated(HVals { key: key.clone() })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(crate::sort_data(response), crate::sort_data(values));

    // Scan for entries.
    let page = node
        .dispatch_replicated(HScan {
            key: key.clone(),
            count: 5,
            cursor: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.items.len(), 5);
    assert!(page.has_more);

    let page = node
        .dispatch_replicated(HScan {
            key: key.clone(),
            count: 5,
            cursor: Some(page.items.last().map(|t| t.0.clone()).unwrap()),
        })
        .await
        .unwrap()
        .unwrap();

    assert_eq!(page.items.len(), 5);
    assert!(!page.has_more);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn map_type_modification_timestamp() {
    let (cluster, keyspace) = create_cluster(3, Strategy::new(3, ConsistencyLevel::All)).await;

    let key = Data::generate_ser();
    let field = Data::generate_ser();
    let value1 = Data::generate_ser();
    let value2 = Data::generate_ser();

    let positioned_key = test_key(&key);
    let replica_set = keyspace.replica_set(positioned_key.position());

    // List of peers that are expected to act as replicas.
    let replicas = replica_set.replicas();
    assert_eq!(replicas.len(), 3);

    let node = cluster.peer(&replicas[0]).node();

    // Ensure there's no initial value.
    let response = node.dispatch_replicated(HFields { key: key.clone() }).await;
    assert_eq!(response, Ok(Ok(Vec::new())));

    let timestamp1 = timestamp_micros();
    let timestamp2 = timestamp_micros() + 1;

    // Add data.
    let response = node
        .dispatch_replicated(HSet {
            key: key.clone(),
            field: field.clone(),
            value: value1.clone(),
            expiration: None,
            version: timestamp1,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node
        .dispatch_replicated(HGet {
            key: key.clone(),
            field: field.clone(),
        })
        .await;
    assert_eq!(response, Ok(Ok(Some(value1.clone()))));

    // Update the data with higher timestamp value. It's expected to succeed.
    let response = node
        .dispatch_replicated(HSet {
            key: key.clone(),
            field: field.clone(),
            value: value2.clone(),
            expiration: None,
            version: timestamp2,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node
        .dispatch_replicated(HGet {
            key: key.clone(),
            field: field.clone(),
        })
        .await;
    assert_eq!(response, Ok(Ok(Some(value2.clone()))));

    // Update the data with lower timestamp value. It's expected to be ignored.
    let response = node
        .dispatch_replicated(HSet {
            key: key.clone(),
            field: field.clone(),
            value: value1.clone(),
            expiration: None,
            version: timestamp1,
        })
        .await;
    assert_eq!(response, Ok(Ok(())));

    let response = node
        .dispatch_replicated(HGet {
            key: key.clone(),
            field: field.clone(),
        })
        .await;
    assert_eq!(response, Ok(Ok(Some(value2.clone()))));
}
