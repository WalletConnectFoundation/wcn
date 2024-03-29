use {
    api::{namespace, Key},
    std::time::Duration,
    test_utils::samples::TestData,
};

#[tokio::test]
async fn test_namespaces() {
    let namespaces = (0..2)
        .map(|i| {
            let auth = namespace::Auth::from_secret(
                b"namespace_master_secret",
                format!("namespace{i}").as_bytes(),
            )
            .unwrap();

            let public_key = auth.public_key();

            (auth, public_key)
        })
        .collect::<Vec<_>>();

    let cluster = lib::test_cluster::Cluster::setup_with_opts(Default::default())
        .await
        .unwrap();

    let node_config = &cluster.peers[0].config;

    let config = api::client::Config {
        nodes: [(node_config.id.id, node_config.api_addr.clone())]
            .into_iter()
            .collect(),
        shadowing_nodes: [].into_iter().collect(),
        shadowing_factor: 0.0,
        request_timeout: Duration::from_secs(5),
        max_operation_time: Duration::from_secs(15),
        connection_timeout: Duration::from_secs(5),
        udp_socket_count: 3,
        namespaces: vec![],
    };

    // Client, authorized for the first namespace.
    let client0 = api::Client::new(api::client::Config {
        namespaces: vec![namespaces[0].0.clone()],
        ..config.clone()
    })
    .unwrap();
    let namespace0 = namespaces[0].1;

    // Client, authorized for the second namespace.
    let client1 = api::Client::new(api::client::Config {
        namespaces: vec![namespaces[1].0.clone()],
        ..config.clone()
    })
    .unwrap();
    let namespace1 = namespaces[1].1;

    let shared_key = Key {
        namespace: None,
        bytes: b"shared_key".to_vec(),
    };
    let shared_value = b"shared_value".to_vec();

    // Add shared data without a namespace. Validate that it's accessible by both
    // clients.
    client0
        .set(shared_key.clone(), shared_value.clone(), None)
        .await
        .unwrap();
    assert_eq!(
        client1.get(shared_key.clone()).await.unwrap(),
        Some(shared_value.clone())
    );

    // Validate that the shared data is inaccessible with namespaces set.
    let mut key = shared_key.clone();
    key.namespace = Some(namespaces[0].1);
    assert_eq!(client0.get(key.clone()).await.unwrap(), None);
    key.namespace = Some(namespaces[1].1);
    assert_eq!(client1.get(key.clone()).await.unwrap(), None);

    // Validate that data added into one namespace is inaccessible in others.
    let mut key = Key {
        namespace: Some(namespace0),
        bytes: b"key1".to_vec(),
    };
    let value = b"value2".to_vec();
    client0.set(key.clone(), value.clone(), None).await.unwrap();
    assert_eq!(client0.get(key.clone()).await.unwrap(), Some(value.clone()));
    key.namespace = Some(namespace1);
    assert_eq!(client1.get(key.clone()).await.unwrap(), None);

    // Validate that the client returns an access denied error trying to access
    // unauthorized namespace.
    assert!(matches!(
        client0.get(key.clone()).await,
        Err(api::client::Error::Api(api::Error::Unauthorized))
    ));

    // Validate map type.
    let keys = [
        TestData::new("key1"),
        TestData::new("key2"),
        TestData::new("key3"),
    ];
    let data = [
        (TestData::new("field1"), TestData::new("value1")),
        (TestData::new("field2"), TestData::new("value2")),
        (TestData::new("field3"), TestData::new("value3")),
    ];

    // Add a few maps filled with multiple entries to the private namespace.
    for key in &keys {
        for (field, value) in &data {
            let key = Key {
                namespace: Some(namespace0),
                bytes: key.0.clone(),
            };
            client0
                .hset(key, field.0.clone(), value.0.clone(), None)
                .await
                .unwrap();
        }
    }

    // Add a few maps filled with multiple entries to the shared namespace.
    for key in &keys {
        for (field, value) in &data {
            let key = Key {
                namespace: None,
                bytes: key.0.clone(),
            };
            client0
                .hset(key, field.0.clone(), value.0.clone(), None)
                .await
                .unwrap();
        }
    }

    // Validate private data.
    for key in &keys {
        let key = Key {
            namespace: Some(namespace0),
            bytes: key.0.clone(),
        };

        // Validate `hget`.
        for (field, value) in &data {
            let data = client0.hget(key.clone(), field.0.clone()).await.unwrap();
            assert_eq!(data, Some(value.0.clone()));
        }

        // Validate `hvals`.
        assert_eq!(
            client0.hvals(key.clone()).await.unwrap(),
            data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>()
        );

        // Validate `hscan`.
        let (values, _) = client0.hscan(key, 10, None).await.unwrap();
        assert_eq!(
            values,
            data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>()
        );
    }

    // Validate shared data.
    for key in &keys {
        let key = Key {
            namespace: Some(namespace0),
            bytes: key.0.clone(),
        };

        // Validate `hget`.
        for (field, value) in &data {
            let data = client0.hget(key.clone(), field.0.clone()).await.unwrap();
            assert_eq!(data, Some(value.0.clone()));
        }

        // Validate `hvals`.
        assert_eq!(
            client0.hvals(key.clone()).await.unwrap(),
            data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>()
        );

        // Validate `hscan`.
        let (values, _) = client0.hscan(key, 10, None).await.unwrap();
        assert_eq!(
            values,
            data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>()
        );
    }
}
