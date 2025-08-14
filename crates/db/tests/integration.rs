use {
    rand::prelude::*,
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        path::PathBuf,
        time::Duration,
    },
    tap::Pipe,
    wc::future::FutureExt,
    wcn_rpc::{client2::Connection, identity::Keypair, server2::ShutdownSignal},
    wcn_storage_api2::{
        operation::*,
        rpc::DatabaseApi,
        MapEntry,
        MapPage,
        Namespace,
        Record,
        RecordExpiration,
        RecordVersion,
        StorageApi,
    },
};

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e() {
    let _logger = wcn_logging::Logger::init(wcn_logging::LogFormat::Json, None, None);
    let shutdown_signal = ShutdownSignal::new();

    let db_port = find_available_port();
    let keypair = Keypair::generate_ed25519();
    let server_id = keypair.public().to_peer_id();
    let rocksdb_dir: PathBuf = format!("/tmp/wcn/test-node/{}", keypair.public().to_peer_id())
        .parse()
        .unwrap();

    let cfg = wcn_db::config::Config {
        keypair,
        primary_rpc_server_port: db_port,
        secondary_rpc_server_port: find_available_port(),
        metrics_server_port: 0,
        connection_timeout: Duration::from_secs(10),
        max_connections: 50,
        max_concurrent_rpcs: 500,
        max_connections_per_ip: 500,
        max_connection_rate_per_ip: 500,
        rocksdb_dir,
        rocksdb: Default::default(),
    };

    let db_handle = wcn_db::run(shutdown_signal.clone(), cfg)
        .unwrap()
        .pipe(tokio::spawn);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = wcn_storage_api2::rpc::client::database(wcn_rpc::client2::Config {
        keypair: Keypair::generate_ed25519(),
        connection_timeout: Duration::from_secs(10),
        reconnect_interval: Duration::from_secs(1),
        max_concurrent_rpcs: 100,
        priority: wcn_rpc::transport::Priority::High,
    })
    .unwrap();

    let client_conn = client
        .connect(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, db_port),
            &server_id,
            (),
        )
        .await
        .unwrap();

    test_string_ops(&client_conn).await;
    test_map_ops(&client_conn).await;
    test_namespaces(&client_conn).await;

    shutdown_signal.emit();

    db_handle
        .with_timeout(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
}

async fn test_string_ops(client: &Connection<DatabaseApi>) {
    let key = gen_data();
    let value = gen_data();
    let namespace = gen_ns();
    let keyspace_version = None;

    let get = Get {
        namespace,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(get.clone().into()).await,
        Ok(Output::Record(None))
    );

    let expiration = exp(120);
    let version = ver(1);

    let set = Set {
        namespace,
        key: key.clone(),
        record: Record {
            value: value.clone(),
            expiration,
            version,
        },
        keyspace_version,
    };
    assert_eq!(client.execute(set.into()).await, Ok(Output::none()));

    assert_eq!(
        client.execute(get.clone().into()).await.unwrap(),
        Output::Record(Some(Record {
            value: value.clone(),
            expiration,
            version
        }))
    );

    let expiration = exp(150);
    let version = ver(2);

    let set_exp = SetExp {
        namespace,
        key: key.clone(),
        expiration,
        version,
        keyspace_version,
    };
    assert_eq!(client.execute(set_exp.into()).await, Ok(Output::none()));

    let get_exp = GetExp {
        namespace,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(get_exp.into()).await,
        Ok(Output::Expiration(Some(expiration)))
    );

    let del = Del {
        namespace,
        key: key.clone(),
        keyspace_version,
        version: ver(3),
    };
    assert_eq!(client.execute(del.into()).await, Ok(Output::none()));
    assert_eq!(client.execute(get.into()).await, Ok(Output::Record(None)));
}

async fn test_map_ops(client: &Connection<DatabaseApi>) {
    let key = gen_data();
    let namespace = gen_ns();
    let expiration = exp(120);
    let version = ver(1);
    let keyspace_version = None;

    // Field-value pairs.
    let mut pairs = (0..5).map(|_| (gen_data(), gen_data())).collect::<Vec<_>>();
    pairs.sort_by_key(|(field, _)| field.clone());

    for (field, value) in &pairs {
        let hset = HSet {
            key: key.clone(),
            entry: MapEntry {
                field: field.clone(),
                record: Record {
                    value: value.clone(),
                    expiration,
                    version,
                },
            },
            namespace,
            keyspace_version,
        };

        assert_eq!(client.execute(hset.into()).await, Ok(Output::none()));
    }

    let (field, value) = pairs.pop().unwrap();
    let hget = HGet {
        namespace,
        key: key.clone(),
        field: field.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(hget.into()).await.unwrap(),
        Output::Record(Some(Record {
            value: value.clone(),
            expiration,
            version
        }))
    );

    {
        let expiration = exp(150);

        let hset_exp = HSetExp {
            namespace,
            key: key.clone(),
            field: field.clone(),
            expiration,
            version: ver(2),
            keyspace_version,
        };
        assert_eq!(client.execute(hset_exp.into()).await, Ok(Output::none()));

        let hget_exp = HGetExp {
            namespace,
            key: key.clone(),
            field: field.clone(),
            keyspace_version,
        };
        assert_eq!(
            client.execute(hget_exp.into()).await,
            Ok(Output::Expiration(Some(expiration)))
        );
    }

    let hdel = HDel {
        namespace,
        key: key.clone(),
        field: field.clone(),
        keyspace_version,
        version: ver(3),
    };
    assert_eq!(client.execute(hdel.into()).await, Ok(Output::none()));

    let hcard = HCard {
        namespace,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(hcard.into()).await,
        Ok(Output::Cardinality(4))
    );

    let hscan = HScan {
        namespace,
        key: key.clone(),
        count: 3,
        cursor: None,
        keyspace_version,
    };
    let entries = pairs
        .iter()
        .take(3)
        .map(|(field, value)| MapEntry {
            field: field.clone(),
            record: Record {
                value: value.clone(),
                expiration,
                version,
            },
        })
        .collect();

    let result = match client.execute(hscan.clone().into()).await.unwrap() {
        Output::MapPage(page) => page,
        _ => panic!("wrong output"),
    };
    assert_eq!(result, MapPage {
        entries,
        has_next: true,
    });

    let hscan = HScan {
        namespace,
        key: key.clone(),
        count: 1,
        cursor: result.next_page_cursor().map(ToOwned::to_owned),
        keyspace_version,
    };
    let entries = pairs
        .iter()
        .skip(3)
        .map(|(field, value)| MapEntry {
            field: field.clone(),
            record: Record {
                value: value.clone(),
                expiration,
                version,
            },
        })
        .collect();
    let result = match client.execute(hscan.into()).await.unwrap() {
        Output::MapPage(page) => page,
        _ => panic!("wrong output"),
    };
    assert_eq!(result, MapPage {
        entries,
        has_next: false,
    });
}

async fn test_namespaces(client: &Connection<DatabaseApi>) {
    let key = gen_data();
    let value1 = gen_data();
    let value2 = gen_data();
    let namespace1 = gen_ns();
    let namespace2 = gen_ns();
    let expiration = exp(120);
    let version = ver(1);
    let keyspace_version = None;

    assert_ne!(namespace1, namespace2);

    let get = Get {
        namespace: namespace1,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(get.clone().into()).await,
        Ok(Output::Record(None))
    );

    let get = Get {
        namespace: namespace2,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(client.execute(get.into()).await, Ok(Output::Record(None)));

    let set = Set {
        namespace: namespace1,
        key: key.clone(),
        record: Record {
            value: value1.clone(),
            expiration,
            version,
        },
        keyspace_version,
    };
    assert_eq!(client.execute(set.into()).await, Ok(Output::none()));

    let set = Set {
        namespace: namespace2,
        key: key.clone(),
        record: Record {
            value: value2.clone(),
            expiration,
            version,
        },
        keyspace_version,
    };
    assert_eq!(client.execute(set.into()).await, Ok(Output::none()));

    let get = Get {
        namespace: namespace1,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(get.into()).await.unwrap(),
        Output::Record(Some(Record {
            value: value1.clone(),
            expiration,
            version
        }))
    );

    let get = Get {
        namespace: namespace2,
        key: key.clone(),
        keyspace_version,
    };
    assert_eq!(
        client.execute(get.into()).await.unwrap(),
        Output::Record(Some(Record {
            value: value2.clone(),
            expiration,
            version
        }))
    );
}

fn exp(added: u64) -> RecordExpiration {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    RecordExpiration::from_unix_timestamp_secs(now + added)
}

fn ver(version: u64) -> RecordVersion {
    RecordVersion::from_unix_timestamp_micros(version)
}

fn gen_ns() -> Namespace {
    let mut rng = rand::rng();

    let mut bytes = [0; 21];
    rng.fill_bytes(&mut bytes);

    format!("{}/{}", const_hex::encode(&bytes[0..20]), bytes[20])
        .parse()
        .unwrap()
}

fn gen_data() -> Vec<u8> {
    let mut rng = rand::rng();
    let mut data = vec![0; 32];
    rng.fill_bytes(&mut data);
    data
}

fn find_available_port() -> u16 {
    use std::{
        net::UdpSocket,
        sync::atomic::{AtomicU16, Ordering},
    };

    static NEXT_PORT: AtomicU16 = AtomicU16::new(48100);

    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        assert!(port != u16::MAX, "failed to find a free port");

        if UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).is_ok() {
            return port;
        }
    }
}
