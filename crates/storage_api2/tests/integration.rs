use {
    futures::{
        stream::{self, BoxStream},
        Stream,
        StreamExt,
        TryStreamExt as _,
    },
    rand::{random, Rng},
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        ops::RangeInclusive,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tracing_subscriber::EnvFilter,
    wc::future::StaticFutureExt,
    wcn_rpc::{
        client2::{Api, Client, Connection},
        identity::Keypair,
        server2::ShutdownSignal,
        transport,
    },
    wcn_storage_api2::{
        operation,
        DataFrame,
        DataItem,
        DataType,
        KeyspaceVersion,
        MapEntry,
        MapPage,
        Namespace,
        Operation,
        Record,
        RecordExpiration,
        RecordVersion,
        Result,
        StorageApi,
    },
};

#[tokio::test]
async fn test_rpc() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    for _ in 0..10 {
        test_rpc_api(
            wcn_storage_api2::rpc::server::coordinator,
            wcn_storage_api2::rpc::client::coordinator,
        )
        .await;

        test_rpc_api(
            wcn_storage_api2::rpc::server::replica,
            wcn_storage_api2::rpc::client::replica,
        )
        .await;

        test_rpc_api(
            wcn_storage_api2::rpc::server::database,
            wcn_storage_api2::rpc::client::database,
        )
        .await;
    }
}

async fn test_rpc_api<API, S>(
    server: impl FnOnce(TestStorage) -> S,
    client: impl FnOnce(wcn_rpc::client2::Config) -> wcn_rpc::client2::Result<Client<API>>,
) where
    API: Api<ConnectionParameters = ()>,
    S: wcn_rpc::server2::Server,
    Connection<API>: StorageApi,
{
    let storage = TestStorage::default();

    let server_port = find_available_port();
    let server_keypair = Keypair::generate_ed25519();
    let server_peer_id = server_keypair.public().to_peer_id();
    let server_cfg = wcn_rpc::server2::Config {
        name: "test",
        port: server_port,
        keypair: server_keypair,
        connection_timeout: Duration::from_secs(10),
        max_connections: 1,
        max_connections_per_ip: 1,
        max_connection_rate_per_ip: 1,
        max_concurrent_rpcs: 10,
        priority: transport::Priority::High,
        shutdown_signal: ShutdownSignal::new(),
    };

    let server_handle = server(storage.clone()).serve(server_cfg).unwrap().spawn();

    let client_config = wcn_rpc::client2::Config {
        keypair: Keypair::generate_ed25519(),
        connection_timeout: Duration::from_secs(10),
        reconnect_interval: Duration::from_secs(1),
        max_concurrent_rpcs: 10,
        priority: transport::Priority::High,
    };

    let client = client(client_config).unwrap();

    let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, server_port);
    let client_conn = client
        .connect(server_addr, &server_peer_id, ())
        .await
        .unwrap();

    let ctx = &TestContext {
        storage,
        client_conn,
    };

    ctx.test_operations().await;
    ctx.test_pull_data().await;
    ctx.test_push_data().await;

    server_handle.abort();
}

struct TestContext<API: Api> {
    storage: TestStorage,
    client_conn: Connection<API>,
}

impl<API: Api> TestContext<API>
where
    Connection<API>: StorageApi,
{
    async fn test_operations(&self) {
        self.test_get().await;
        self.test_set().await;
        self.test_del().await;
        self.test_get_exp().await;
        self.test_set_exp().await;

        self.test_hget().await;
        self.test_hset().await;
        self.test_hdel().await;
        self.test_hget_exp().await;
        self.test_hset_exp().await;

        self.test_hcard().await;
        self.test_hscan().await;
    }

    async fn test_operation(
        &self,
        operation: impl Into<operation::Owned>,
        output: impl Into<operation::Output>,
    ) {
        let operation: Operation<'_> = operation.into().into();
        let result = Ok(output.into());

        let expect = (operation.clone(), result.clone());
        let _ = self.storage.expect.lock().unwrap().insert(expect);
        assert_eq!(self.client_conn.execute(operation).await, result);
    }

    async fn test_get(&self) {
        let op = operation::Get {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, opt(record)).await;
    }

    async fn test_set(&self) {
        let op = operation::Set {
            namespace: namespace(),
            key: bytes(),
            record: record(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_del(&self) {
        let op = operation::Del {
            namespace: namespace(),
            key: bytes(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_get_exp(&self) {
        let op = operation::GetExp {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, opt(record_expiration)).await;
    }

    async fn test_set_exp(&self) {
        let op = operation::SetExp {
            namespace: namespace(),
            key: bytes(),
            expiration: record_expiration(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_hget(&self) {
        let op = operation::HGet {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, opt(record)).await;
    }

    async fn test_hset(&self) {
        let op = operation::HSet {
            namespace: namespace(),
            key: bytes(),
            entry: map_entry(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_hdel(&self) {
        let op = operation::HDel {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_hget_exp(&self) {
        let op = operation::HGetExp {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, opt(record_expiration)).await;
    }

    async fn test_hset_exp(&self) {
        let op = operation::HSetExp {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            expiration: record_expiration(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, ()).await;
    }

    async fn test_hcard(&self) {
        let op = operation::HCard {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, random::<u64>()).await;
    }

    async fn test_hscan(&self) {
        let op = operation::HScan {
            namespace: namespace(),
            key: bytes(),
            count: random(),
            cursor: opt(bytes),
            keyspace_version: keyspace_version(),
        };

        self.test_operation(op, map_page()).await;
    }

    async fn test_pull_data(&self) {
        let keyrange = 10..=42;
        let keyspace_version = 1;
        let frame = DataFrame {
            data_type: DataType::Kv,
            key: vec![1, 1, 1],
            value: vec![2, 2, 2],
        };
        let items = [Ok(DataItem::Frame(frame)), Ok(DataItem::Done(1))];
        let items_st = stream::iter(items.clone()).boxed();

        let _ = self.storage.expect_pull_data.lock().unwrap().insert((
            keyrange.clone(),
            keyspace_version,
            items_st,
        ));

        let items_got: Vec<_> = self
            .client_conn
            .read_data(keyrange, keyspace_version)
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(items.as_slice(), items_got.as_slice())
    }

    async fn test_push_data(&self) {
        let frame = DataFrame {
            data_type: DataType::Kv,
            key: vec![1, 1, 1],
            value: vec![2, 2, 2],
        };
        let items = vec![DataItem::Frame(frame), DataItem::Done(1)];

        let _ = self
            .storage
            .expect_push_data
            .lock()
            .unwrap()
            .insert(items.clone());

        self.client_conn
            .write_data(stream::iter(items.into_iter()).map(Ok))
            .await
            .unwrap();
    }
}

fn namespace() -> Namespace {
    let operator_id = rand::random::<[u8; 20]>();
    let id = rand::random::<u8>();

    let operator_id = const_hex::encode(operator_id);
    format!("{operator_id}/{id}").parse().unwrap()
}

fn keyspace_version() -> Option<KeyspaceVersion> {
    if random() {
        Some(random())
    } else {
        None
    }
}

fn record() -> Record {
    Record {
        value: bytes(),
        expiration: record_expiration(),
        version: RecordVersion::now(),
    }
}

fn map_entry() -> MapEntry {
    MapEntry {
        field: bytes(),
        record: record(),
    }
}

fn map_page() -> MapPage {
    let mut rng = rand::thread_rng();

    let len = rng.gen_range(1..=1000);
    let mut buf = Vec::with_capacity(len);
    for _ in 0..len {
        buf.push(map_entry());
    }

    MapPage {
        entries: buf,
        has_next: random(),
    }
}

fn opt<T>(f: fn() -> T) -> Option<T> {
    if random() {
        Some(f())
    } else {
        None
    }
}

fn bytes() -> Vec<u8> {
    let mut rng = rand::thread_rng();

    let mut buf = vec![0u8; rng.gen_range(1..=4096)];
    rng.fill(&mut buf[..]);
    buf
}

fn record_expiration() -> RecordExpiration {
    let secs = rand::thread_rng().gen_range(30..=60 * 60 * 24 * 30);
    Duration::from_secs(secs).into()
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
#[allow(clippy::type_complexity)]
#[derive(Clone, Default)]
struct TestStorage {
    expect: Arc<Mutex<Option<(Operation<'static>, Result<operation::Output>)>>>,
    expect_pull_data: Arc<Mutex<Option<(RangeInclusive<u64>, u64, DataStream)>>>,
    expect_push_data: Arc<Mutex<Option<Vec<DataItem>>>>,
}

type DataStream = BoxStream<'static, Result<DataItem>>;

impl StorageApi for TestStorage {
    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        let expected = self.expect.lock().unwrap().take().unwrap();
        assert_eq!(operation, expected.0);
        expected.1
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> Result<impl Stream<Item = Result<DataItem>> + Send> {
        let expected = self.expect_pull_data.lock().unwrap().take().unwrap();
        assert_eq!(keyrange, expected.0);
        assert_eq!(keyspace_version, expected.1);

        Ok(expected.2)
    }

    async fn write_data(&self, stream: impl Stream<Item = Result<DataItem>> + Send) -> Result<()> {
        let expected = self.expect_push_data.lock().unwrap().take().unwrap();

        let got: Vec<_> = stream.try_collect().await?;
        assert_eq!(expected, got);

        Ok(())
    }
}
