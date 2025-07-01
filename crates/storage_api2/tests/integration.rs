use {
    rand::{random, Rng},
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tracing_subscriber::EnvFilter,
    wc::future::StaticFutureExt,
    wcn_rpc::{
        client2::{Api, Client, LoadBalancer},
        identity::Keypair,
        transport,
    },
    wcn_storage_api2::{
        operation,
        Bytes,
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
    LoadBalancer<API>: StorageApi,
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
        max_connections: 2,
        max_connections_per_ip: 2,
        max_connection_rate_per_ip: 2,
        max_concurrent_rpcs: 10,
        priority: transport::Priority::High,
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
    let client_conn1 = client
        .connect(server_addr, &server_peer_id, ())
        .await
        .unwrap();
    let client_conn2 = client
        .connect(server_addr, &server_peer_id, ())
        .await
        .unwrap();

    let ctx = &TestContext {
        storage,
        lb: LoadBalancer::new([client_conn1, client_conn2]),
    };

    ctx.test_operations().await;

    server_handle.abort();
}

struct TestContext<API: Api> {
    storage: TestStorage,
    lb: LoadBalancer<API>,
}

impl<API: Api> TestContext<API>
where
    LoadBalancer<API>: StorageApi,
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

    async fn test_get(&self) {
        let op = operation::Get {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(opt(record));

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::Get(&a), b));
        assert_eq!(self.lb.get(&op).await, res);
    }

    async fn test_set(&self) {
        let op = operation::Set {
            namespace: namespace(),
            key: bytes(),
            record: record(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::Set(&a), b));
        assert_eq!(self.lb.set(&op).await, res);
    }

    async fn test_del(&self) {
        let op = operation::Del {
            namespace: namespace(),
            key: bytes(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::Del(&a), b));
        assert_eq!(self.lb.del(&op).await, res);
    }

    async fn test_get_exp(&self) {
        let op = operation::GetExp {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(opt(record_expiration));

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::GetExp(&a), b));
        assert_eq!(self.lb.get_exp(&op).await, res);
    }

    async fn test_set_exp(&self) {
        let op = operation::SetExp {
            namespace: namespace(),
            key: bytes(),
            expiration: record_expiration(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::SetExp(&a), b));
        assert_eq!(self.lb.set_exp(&op).await, res);
    }

    async fn test_hget(&self) {
        let op = operation::HGet {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(opt(record));

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HGet(&a), b));
        assert_eq!(self.lb.hget(&op).await, res);
    }

    async fn test_hset(&self) {
        let op = operation::HSet {
            namespace: namespace(),
            key: bytes(),
            entry: map_entry(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HSet(&a), b));
        assert_eq!(self.lb.hset(&op).await, res);
    }

    async fn test_hdel(&self) {
        let op = operation::HDel {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            version: RecordVersion::now(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HDel(&a), b));
        assert_eq!(self.lb.hdel(&op).await, res);
    }

    async fn test_hget_exp(&self) {
        let op = operation::HGetExp {
            namespace: namespace(),
            key: bytes(),
            field: bytes(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(opt(record_expiration));

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HGetExp(&a), b));
        assert_eq!(self.lb.hget_exp(&op).await, res);
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

        let res = Ok(());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HSetExp(&a), b));
        assert_eq!(self.lb.hset_exp(&op).await, res);
    }

    async fn test_hcard(&self) {
        let op = operation::HCard {
            namespace: namespace(),
            key: bytes(),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(random::<u64>());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HCard(&a), b));
        assert_eq!(self.lb.hcard(&op).await, res);
    }

    async fn test_hscan(&self) {
        let op = operation::HScan {
            namespace: namespace(),
            key: bytes(),
            count: random(),
            cursor: opt(bytes),
            keyspace_version: keyspace_version(),
        };

        let res = Ok(map_page());

        self.storage
            .assert_next(&op, &res, |a, b| assert_eq!(Operation::HScan(&a), b));
        assert_eq!(self.lb.hscan(&op).await, res);
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

fn record() -> Record<'static> {
    Record {
        value: bytes(),
        expiration: record_expiration(),
        version: RecordVersion::now(),
    }
}

fn map_entry() -> MapEntry<'static> {
    MapEntry {
        field: bytes(),
        record: record(),
    }
}

fn map_page() -> MapPage<'static> {
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

fn bytes() -> Bytes<'static> {
    let mut rng = rand::thread_rng();

    let mut buf = vec![0u8; rng.gen_range(1..=4096)];
    rng.fill(&mut buf[..]);
    buf.into()
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

#[derive(Clone, Default)]
struct TestStorage {
    expect_fn: Arc<Mutex<Option<Box<dyn TestStorageFn>>>>,
}

pub trait TestStorageFn:
    for<'a> FnOnce(Operation<'a>) -> Result<operation::Output<'static>> + Send + 'static
{
}

impl<F> TestStorageFn for F where
    F: for<'a> FnOnce(Operation<'a>) -> Result<operation::Output<'static>> + Send + 'static
{
}

impl TestStorage {
    fn assert_next<Op, Out, F>(&self, op: &Op, result: &Result<Out>, assert_fn: F)
    where
        Op: Clone + Send + 'static,
        Out: Clone + Into<operation::Output<'static>>,
        F: for<'a> FnOnce(Op, Operation<'a>) + Send + 'static,
    {
        let op = op.clone();
        let res = result.clone().map(Into::into);

        let _ = self
            .expect_fn
            .lock()
            .unwrap()
            .insert(Box::new(move |operation| {
                assert_fn(op, operation);
                res
            }));
    }
}

impl StorageApi for TestStorage {
    async fn execute<'a>(
        &'a self,
        operation: impl Into<Operation<'a>> + Send + 'a,
    ) -> Result<operation::Output<'a>> {
        let expect_fn = self.expect_fn.lock().unwrap().take().unwrap();
        expect_fn(operation.into())
    }
}
