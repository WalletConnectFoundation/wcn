use {
    crate::{
        storage::{Del, Get, Set, Storage},
        Config,
        Consensus,
        Network,
        Node,
    },
    async_trait::async_trait,
    irn::{
        cluster::{
            keyspace::{hashring, Keyspace},
            replication::{ConsistencyLevel, Strategy},
            NodeOperationMode,
        },
        test::{ReadAssert, RemoveAssert, WriteAssert, WriteReadAssert},
        PeerId,
        ShutdownReason,
    },
    network::{Keypair, Multiaddr},
    rand::{prelude::StdRng, Rng, SeedableRng},
    relay_rocks::util::timestamp_micros,
    std::{
        collections::HashMap,
        sync::atomic::{AtomicU16, Ordering},
        time::Duration,
    },
    tempfile::TempDir,
    tokio::task::JoinHandle,
};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub num_bootstrap_nodes: usize,
    pub replication_strategy: Strategy,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_bootstrap_nodes: 6,
            replication_strategy: Strategy::new(6, ConsistencyLevel::All),
        }
    }
}

#[derive(Default)]
pub struct Cluster {
    pub peers: Vec<TestNode>,
    keyspace: hashring::Keyspace,
}

pub struct TestNode {
    inner: Node,
    handle: JoinHandle<()>,
    api_client: api::Client,
    pub config: Config,
    _guard: DirGuard,
}

impl TestNode {
    pub fn node(&self) -> &irn::Node<Consensus, Network, Storage> {
        self.inner.node()
    }

    pub async fn shutdown(self, reason: ShutdownReason) {
        // Send shutdown signal to node, urging it to release resources.
        let _ = self.inner.node().shutdown(reason);

        tokio::time::timeout(Duration::from_secs(10), async move {
            if let Err(err) = self.handle.await {
                tracing::error!(?err, "failed to join LocalPeer handle");
            }
        })
        .await
        .expect("timeout shutting down the TestNode");
    }
}

impl Cluster {
    /// Setup a bootstrap cluster.
    pub async fn setup(
        num_bootstrap_nodes: usize,
        replication_strategy: Strategy,
    ) -> anyhow::Result<Self> {
        Self::setup_with_opts(ClusterConfig {
            num_bootstrap_nodes,
            replication_strategy,
        })
        .await
    }

    pub async fn setup_with_opts(opts: ClusterConfig) -> anyhow::Result<Self> {
        let mut cluster = Self {
            peers: vec![],
            keyspace: hashring::Keyspace::new(opts.replication_strategy),
        };

        for cfg in TestConfig::bootstrap_nodes(opts.num_bootstrap_nodes) {
            cluster.add_node_inner(cfg).await?;
        }

        tracing::info!("Waiting for nodes to transition to `Normal`");
        tokio::time::timeout(Duration::from_secs(20), async {
            for node in &cluster.peers {
                for peer in &cluster.peers {
                    node.node()
                        .wait_op_mode(*peer.node().id(), NodeOperationMode::Normal)
                        .await;
                }
            }
        })
        .await
        .expect("Cluster setup timed out");

        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(cluster)
    }

    pub async fn add_node(&mut self, secret: Option<&str>, is_member: bool) -> anyhow::Result<()> {
        self.add_node_inner(TestConfig::new(secret, is_member, &self.peers))
            .await
    }

    async fn add_node_inner(&mut self, mut cfg: TestConfig) -> anyhow::Result<()> {
        cfg.inner.replication_strategy = self.keyspace.replication_strategy().clone();

        let (node, handle) = run_peer(&cfg.inner).await?;

        let api_client = api::Client::new(api::client::Config {
            nodes: [(cfg.inner.id.id, cfg.inner.api_addr.clone())]
                .into_iter()
                .collect(),
            shadowing_nodes: [].into_iter().collect(),
            shadowing_factor: 0.0,
            request_timeout: Duration::from_secs(5),
            max_operation_time: Duration::from_secs(15),
            connection_timeout: Duration::from_secs(5),
            udp_socket_count: 3,
            namespaces: vec![],
        })
        .unwrap();

        self.keyspace
            .add_node(&*node.inner)
            .expect("keyspace updated");
        self.peers.push(TestNode {
            inner: node,
            handle,
            api_client,
            config: cfg.inner,
            _guard: cfg.dir_guard,
        });
        Ok(())
    }

    // Returns the peer by `PeerId`.
    pub fn peer(&self, id: &PeerId) -> &Node {
        &self
            .peers
            .iter()
            .find(|p| p.inner.node().id() == id)
            .expect("node not found")
            .inner
    }

    /// Returns list of peers in the cluster.
    pub fn peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .map(|peer| *peer.inner.node().id())
            .collect()
    }

    pub fn irn_api_client(&self, idx: usize) -> &api::Client {
        &self.peers[idx].api_client
    }

    /// Returns the node operation mode of a node identified by `idx`.
    ///
    /// Cluster view of the node is used to determine the mode, i.e. it is
    /// node's own view of its operation mode. Returns `None` if the node is
    /// not found.
    pub async fn node_op_mode(&self, idx: usize) -> Option<NodeOperationMode> {
        let peer_id = &self.peers[idx].node().id();
        self.peers[idx]
            .inner
            .inner
            .cluster()
            .read()
            .await
            .node(peer_id)
            .map(|n| n.mode)
    }

    pub async fn node_keyspace(&self, idx: usize) -> hashring::Keyspace {
        self.peers[idx]
            .inner
            .inner
            .cluster()
            .read()
            .await
            .keyspace()
            .clone()
    }

    fn gen_key_value(seed: u64) -> (Vec<u8>, Vec<u8>) {
        let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
        let key = rng.gen::<[u8; 16]>().to_vec();
        let mut value = Vec::with_capacity(1024);
        for _ in 0..(1024 / 32) {
            let part = rng.gen::<[u8; 32]>();
            value.extend_from_slice(&part);
        }
        (key, value)
    }
}

struct TestConfig {
    inner: Config,
    dir_guard: DirGuard,
}

struct DirGuard {
    _raft: TempDir,
}

impl TestConfig {
    /// Creates a new config with a keypair and a random port.
    ///
    /// If keypair secret is not specified, a random keypair will be generated.
    fn new(secret: Option<&str>, is_member: bool, peers: &[TestNode]) -> Self {
        Self::partial(secret, is_member).complete(peers.iter().map(|p| &p.config), false)
    }

    fn partial(secret: Option<&str>, is_member: bool) -> Self {
        let raft_dir = TempDir::new().unwrap();
        let rocksdb_dir = format!(
            "/tmp/irn/rocksdb/{}",
            rand::Rng::gen::<u128>(&mut rand::thread_rng()),
        );

        let keypair = if let Some(secret) = secret {
            let secret: [u8; 32] = secret.as_bytes().try_into().unwrap();
            Keypair::ed25519_from_bytes(secret).unwrap()
        } else {
            Keypair::generate_ed25519()
        };
        let port = get_random_port();
        let addr: Multiaddr = format!("/ip4//udp/{port}/quic-v1")
            .parse()
            .unwrap();

        let port = get_random_port();
        let api_addr: Multiaddr = format!("/ip4//udp/{port}/quic-v1")
            .parse()
            .unwrap();

        let port = get_random_port();
        let metrics_addr = format!(":{port}");

        static GROUP: AtomicU16 = AtomicU16::new(1);

        let group = GROUP.fetch_add(1, Ordering::Relaxed);
        let id = PeerId::from_public_key(&keypair.public(), group);

        Self {
            inner: Config {
                id,
                keypair,
                addr,
                api_addr,
                metrics_addr,
                is_raft_member: is_member,
                known_peers: HashMap::new(),
                bootstrap_nodes: None,
                raft_dir: raft_dir.path().to_path_buf(),
                rocksdb_dir: rocksdb_dir.into(),
                rocksdb_num_batch_threads: 2,
                rocksdb_num_callback_threads: 2,
                replication_strategy: Strategy::default(),
                request_concurrency_limit: 10000,
                request_limiter_queue: 32768,
                network_connection_timeout: 15000,
                network_request_timeout: 15000,
                replication_request_timeout: 15000,
                warmup_delay: 10,
            },
            dir_guard: DirGuard { _raft: raft_dir },
        }
    }

    fn complete<'a>(
        mut self,
        peer_configs: impl Iterator<Item = &'a Config>,
        is_bootstrap: bool,
    ) -> Self {
        self.inner.known_peers = peer_configs.map(|c| (c.id, c.addr.clone())).collect();

        if is_bootstrap {
            self.inner.bootstrap_nodes = Some(self.inner.known_peers.clone());
        }

        self
    }

    fn bootstrap_nodes(n: usize) -> Vec<Self> {
        let partial_configs: Vec<_> = (0..n)
            .map(|i| {
                let secret = format!("{}{}", "0".repeat(32 - (i + 1).to_string().len()), i + 1);
                Self::partial(Some(&*secret), true)
            })
            .collect();
        let inner_configs: Vec<_> = partial_configs.iter().map(|c| c.inner.clone()).collect();

        partial_configs
            .into_iter()
            .map(|cfg| cfg.complete(inner_configs.iter(), true))
            .collect()
    }
}

async fn run_peer(cfg: &Config) -> anyhow::Result<(Node, JoinHandle<()>)> {
    let servers = Node::serve(cfg.clone()).await?;
    let node_handle = servers.handle.node().clone();

    let task_handle = tokio::spawn(servers);

    // Let the listeners activate, open sockets etc.
    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok((node_handle, task_handle))
}

// TODO: Move this to `utils-rs` repo maybe? It's being used by the relay tests
// as well.
pub fn get_random_port() -> u16 {
    use {
        once_cell::sync::Lazy,
        std::net::{Ipv4Addr, SocketAddrV4, TcpListener},
    };

    fn is_port_available(port: u16) -> bool {
        TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).is_ok()
    }

    // Base port should be random per-process so that there's no race condition when
    // running tests using multiple processes (e.g. `cargo nextest run`), and static
    // to eliminate collisions within the process.
    static NEXT_PORT: Lazy<AtomicU16> =
        Lazy::new(|| AtomicU16::new(rand::Rng::gen_range(&mut rand::thread_rng(), 9000, 65535)));

    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);

        if is_port_available(port) {
            return port;
        }
    }
}

#[async_trait]
impl irn::test::Cluster for Cluster {
    type Node = irn::Node<Consensus, Network, Storage>;

    type Consensus = Consensus;
    type Network = Network;
    type Storage = Storage;
    type Keyspace = hashring::Keyspace;

    type ReadOperation = Get;
    type WriteOperation = Set;
    type RemoveOperation = Del;

    async fn run(size: usize, strategy: Option<Strategy>) -> Self {
        Cluster::setup(size, strategy.unwrap_or_default())
            .await
            .unwrap()
    }

    async fn spinup_node(&mut self) {
        let mut n_try = 1;
        loop {
            match self.add_node(None, true).await {
                Ok(()) => return,
                Err(err) if n_try < 5 => {
                    tracing::warn!(?err, "TestCluster::add_node");
                    n_try += 1;
                    tokio::time::sleep(Duration::from_secs(n_try * 2)).await;
                    continue;
                }
                Err(err) => panic!("{err:?}"),
            };
        }
    }

    async fn decommission_node(&mut self, idx: usize) {
        self.keyspace
            .remove_node(&*self.peers[idx].inner.inner)
            .expect("keyspace updated");
        self.peers
            .remove(idx)
            .shutdown(ShutdownReason::Decommission)
            .await;
    }

    #[allow(clippy::diverging_sub_expression)] // false positive
    async fn restart_node(&mut self, _idx: usize) {
        todo!();
    }

    fn node(&self, idx: usize) -> &Self::Node {
        self.peers[idx].inner.node()
    }

    fn keyspace(&self) -> &Self::Keyspace {
        &self.keyspace
    }

    fn gen_write_read_assert(&self, seed: Option<u64>) -> WriteReadAssert<Self> {
        let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

        WriteReadAssert {
            write: Set {
                key: key.clone(),
                value: value.clone(),
                expiration: None,
                version: timestamp_micros(),
            },
            read: Get { key },
            expected_output: Some(value),
        }
    }

    fn gen_write_assert(&self, seed: Option<u64>) -> WriteAssert<Self> {
        let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

        WriteAssert {
            operation: Set {
                key,
                value,
                expiration: None,
                version: timestamp_micros(),
            },
            expected_output: (),
        }
    }

    fn gen_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self> {
        let (key, value) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

        ReadAssert {
            operation: Get { key },
            expected_output: Some(value),
        }
    }

    fn gen_empty_read_assert(&self, seed: Option<u64>) -> ReadAssert<Self> {
        let (key, _) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

        ReadAssert {
            operation: Get { key },
            expected_output: None,
        }
    }

    fn gen_remove_assert(&self, seed: Option<u64>) -> RemoveAssert<Self> {
        let (key, _) = Self::gen_key_value(seed.unwrap_or_else(rand::random));

        RemoveAssert {
            operation: Del { key },
            expected_output: (),
        }
    }
}
