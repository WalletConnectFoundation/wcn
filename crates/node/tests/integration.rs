use {
    admin_api::{ClusterView, NodeState},
    futures::{
        stream::{self, FuturesUnordered},
        FutureExt,
        StreamExt as _,
    },
    itertools::Itertools,
    libp2p::{Multiaddr, PeerId},
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
    rand::Rng,
    replication::storage,
    std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        sync::{
            atomic::{AtomicU16, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    },
    tap::Pipe,
    tokio::sync::oneshot,
    tracing_subscriber::EnvFilter,
    wcn::fsm::ShutdownReason,
    wcn_node::{cluster::NodeRegion, Config, RocksdbDatabaseConfig},
    wcn_rpc::{identity::Keypair, quic::socketaddr_to_multiaddr, PeerAddr},
};

static NEXT_PORT: AtomicU16 = AtomicU16::new(42100);
const NETWORK_ID: &str = "wcn_integration_tests";

fn next_port() -> u16 {
    use std::net::{Ipv4Addr, TcpListener, UdpSocket};

    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if port == u16::MAX {
            panic!("failed to find a free port");
        }

        let is_tcp_available = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok();
        let is_udp_available = UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).is_ok();

        if is_tcp_available && is_udp_available {
            return port;
        }
    }
}

fn authorized_client_keypair(n: u8) -> Keypair {
    let mut bytes: [u8; 32] = Default::default();
    bytes[0] = n;
    Keypair::ed25519_from_bytes(bytes).unwrap()
}

#[tokio::test]
async fn test_suite() {
    let mut cluster = TestCluster::bootstrap().await;

    cluster.test_pubsub().await;

    cluster.test_namespaces().await;

    // add 2 exta nodes, 5 total
    cluster.spawn_node().await;
    cluster.spawn_node().await;

    cluster.test_addr_change().await;

    cluster.test_replication_and_read_repairs().await;

    cluster.test_full_node_rotation().await;

    cluster.test_restart_then_decommission().await;

    cluster.test_scaling().await;

    cluster.test_client_auth().await;
}

struct NodeHandle {
    config: Config,
    thread_handle: thread::JoinHandle<()>,
    shutdown_tx: oneshot::Sender<ShutdownReason>,

    replica_api_server_addr: Multiaddr,
    client_api_server_addr: Multiaddr,
    admin_api_server_addr: Multiaddr,
}

impl NodeHandle {
    fn change_addr(&mut self) {
        self.config.raft_server_port = next_port();
        self.config.replica_api_server_port = next_port();
    }

    fn replica_api_addr(&self) -> PeerAddr {
        PeerAddr::new(self.config.id, self.replica_api_server_addr.clone())
    }

    fn client_api_addr(&self) -> PeerAddr {
        PeerAddr::new(self.config.id, self.client_api_server_addr.clone())
    }

    fn admin_api_addr(&self) -> PeerAddr {
        PeerAddr::new(self.config.id, self.admin_api_server_addr.clone())
    }
}

struct TestCluster {
    nodes: HashMap<PeerId, NodeHandle>,
    prometheus: PrometheusHandle,

    admin_api_client: admin_api::Client,

    version: u128,
    keyspace_version: u64,
}

impl TestCluster {
    async fn bootstrap() -> Self {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .with_env_filter(EnvFilter::from_default_env())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let prometheus = PrometheusBuilder::new()
            .install_recorder()
            .expect("install prometheus recorder");

        let node_configs: Vec<_> = (0..3).map(|_| new_node_config()).collect();
        let known_peers: HashMap<_, _> = node_configs
            .iter()
            .map(|c| (c.id, local_multiaddr(c.raft_server_port)))
            .collect();

        let mut nodes = HashMap::new();

        for mut cfg in node_configs {
            cfg.known_peers = known_peers.clone();
            cfg.bootstrap_nodes = Some(known_peers.keys().copied().collect());

            nodes.insert(cfg.id, spawn_node(cfg, prometheus.clone()));
        }

        let admin_addr = PeerAddr::new(PeerId::random(), local_multiaddr(0));

        let admin_api_client = admin_api::Client::new(
            admin_api::client::Config::new(admin_addr).with_keypair(authorized_client_keypair(0)),
        )
        .unwrap();

        let mut cluster = Self {
            nodes,
            prometheus,
            admin_api_client,
            version: 0,
            keyspace_version: 0,
        };

        cluster.wait_normal().await;

        cluster
    }

    async fn spawn_node(&mut self) -> PeerId {
        self.configure_and_spawn_node(|_| {}).await
    }

    async fn restart_node(&mut self, id: &PeerId) {
        let config = self.shutdown_node(id).await;
        self.configure_and_spawn_node(|c| *c = config).await;
    }

    async fn shutdown_node(&mut self, id: &PeerId) -> Config {
        let node = self.nodes.remove(id).unwrap();
        node.shutdown_tx.send(ShutdownReason::Restart).unwrap();
        node.thread_handle.join().unwrap();

        self.wait_node_state(id, NodeState::Restarting).await;
        node.config
    }

    async fn decommission_node(&mut self, id: &PeerId) {
        let node = self.nodes.remove(id).unwrap();
        self.admin_api_client.set_server_addr(node.admin_api_addr());

        self.admin_api_client
            .decommission_node(*id, false)
            .await
            .unwrap();

        self.wait_normal().await;
    }

    async fn configure_and_spawn_node(&mut self, f: impl FnOnce(&mut Config)) -> PeerId {
        let mut cfg = new_node_config();
        cfg.known_peers = self
            .nodes
            .values()
            .take(1)
            .map(|n| (n.config.id, local_multiaddr(n.config.raft_server_port)))
            .collect();

        f(&mut cfg);

        let id = cfg.id;

        self.nodes
            .insert(cfg.id, spawn_node(cfg, self.prometheus.clone()));

        self.wait_normal().await;
        id
    }

    async fn wait_normal(&mut self) {
        self.wait(|this, view| {
            for node_id in this.nodes.keys() {
                if view.nodes.get(node_id).map(|n| &n.state) != Some(&NodeState::Normal) {
                    return false;
                };
            }

            true
        })
        .await
    }

    async fn wait_node_state(&mut self, id: &PeerId, state: NodeState) {
        self.wait(|_, view| {
            view.nodes
                .get(id)
                .map(|n| n.state == state)
                .unwrap_or_default()
        })
        .await
    }

    async fn wait(&mut self, f: impl Fn(&mut Self, &ClusterView) -> bool) {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match self.is(&f).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(err) => tracing::error!(?err),
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn is(
        &mut self,
        f: impl FnOnce(&mut Self, &ClusterView) -> bool,
    ) -> anyhow::Result<bool> {
        let cluster_view = self.get_cluster_view().await?;
        tracing::info!(?cluster_view);

        if cluster_view.cluster_version <= self.version {
            return Ok(false);
        }

        self.version = cluster_view.cluster_version;

        if cluster_view.keyspace_version > self.keyspace_version {
            self.keyspace_version = cluster_view.keyspace_version;
        }

        Ok(f(self, &cluster_view))
    }

    async fn get_cluster_view(&self) -> Result<ClusterView, anyhow::Error> {
        let node = self.nodes.values().next().unwrap();

        let mut client = self.admin_api_client.clone();
        client.set_server_addr(node.admin_api_addr());

        Ok(client.get_cluster_view().await?)
    }

    fn gen_test_ops(&self) -> Operations {
        let mut rng = rand::thread_rng();
        let key = rng.gen::<u128>().to_be_bytes().to_vec();
        let value = rng.gen::<u128>().to_be_bytes().to_vec();
        let value2 = rng.gen::<u128>().to_be_bytes().to_vec();

        Operations {
            set: Set {
                key: key.clone(),
                value: value.clone(),
                expiration: Duration::from_secs(600),
            },
            get: Get { key: key.clone() },
            expected_output: Some(value),
            overwrite: Set {
                key,
                value: value2,
                expiration: Duration::from_secs(600),
            },
        }
    }

    async fn new_replication_driver(
        &self,
        f: impl FnOnce(&mut replication::Config),
    ) -> replication::Driver {
        let nodes = self
            .nodes
            .values()
            .map(|node| node.client_api_addr())
            .collect();

        let mut cfg = replication::Config::new(nodes).with_keypair(authorized_client_keypair(0));
        f(&mut cfg);

        replication::Driver::new(cfg).await.unwrap()
    }

    async fn new_client_api_client(&self) -> client_api::Client {
        let nodes: HashSet<_> = self
            .nodes
            .values()
            .map(|node| node.client_api_addr())
            .collect();

        client_api::Client::new(
            client_api::client::Config::new(nodes).with_keypair(authorized_client_keypair(0)),
        )
        .await
        .unwrap()
    }

    async fn new_storage_api_client(&self) -> storage_api::Client {
        let client_api = self.new_client_api_client().await;
        let token = client_api.auth_token();

        storage_api::Client::new(
            storage_api::client::Config::new(token).with_keypair(authorized_client_keypair(0)),
        )
        .unwrap()
    }

    async fn test_pubsub(&self) {
        let channel = b"channel";
        let payload = b"payload";
        let num_clients = 10;
        let num_messages = 100;
        let num_messages_total_per_client = num_clients * num_messages;

        let clients = (0..num_clients)
            .map(|n| {
                self.new_replication_driver(move |cfg| {
                    cfg.keypair = authorized_client_keypair(n as u8 % 2)
                })
            })
            .pipe(stream::iter)
            .buffer_unordered(num_clients)
            .collect::<Vec<_>>()
            .await;

        let mut subscriptions = stream::iter(&clients)
            .then(|c| async {
                c.subscribe([channel.to_vec()].into_iter().collect())
                    .await
                    .unwrap()
            })
            .collect::<Vec<_>>()
            .await;

        let tasks = FuturesUnordered::new();

        for client in &clients {
            tasks.push(
                async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;

                    for _ in 0..num_messages {
                        client
                            .publish(channel.to_vec(), payload.to_vec())
                            .await
                            .unwrap();
                    }
                }
                .boxed_local(),
            )
        }

        let num_received = Arc::new(AtomicUsize::new(0));

        for sub in subscriptions.iter_mut() {
            let num_received = num_received.clone();

            tasks.push(
                async move {
                    for _ in 0..num_messages_total_per_client {
                        let data = sub.next().await.unwrap().unwrap();
                        assert_eq!(&data.channel, channel);
                        assert_eq!(&data.message, payload);
                        num_received.fetch_add(1, Ordering::SeqCst);
                        // tracing::error!("receiving {i} {j}");
                    }
                }
                .boxed_local(),
            );
        }

        tasks.count().await;

        assert_eq!(
            num_received.load(Ordering::SeqCst),
            num_messages_total_per_client * num_clients
        );

        // Verify there's no more messages to receive.
        stream::iter(subscriptions)
            .for_each_concurrent(None, |mut subscriber| async move {
                let result = tokio::time::timeout(Duration::from_secs(3), subscriber.next()).await;

                // We should receive a timeout error, since there's no more messages.
                assert!(result.is_err());
            })
            .await;
    }

    async fn test_namespaces(&self) {
        tracing::info!("Namespaces");

        let namespaces = (0..2)
            .map(|i| {
                let auth = auth::Auth::from_secret(
                    b"namespace_master_secret",
                    format!("namespace{i}").as_bytes(),
                )
                .unwrap();

                let public_key = auth.public_key();

                (auth, public_key)
            })
            .collect::<Vec<_>>();

        let node = self.nodes.values().next().unwrap();

        // Client, authorized for the first namespace.
        let client0 = self
            .new_replication_driver(|cfg| {
                cfg.nodes.insert(node.client_api_addr());
                cfg.namespaces = vec![namespaces[0].0.clone()];
            })
            .await;
        let namespace0 = namespaces[0].1;

        // Client, authorized for the second namespace.
        let client1 = self
            .new_replication_driver(|cfg| {
                cfg.nodes.insert(node.client_api_addr());
                cfg.namespaces = vec![namespaces[1].0.clone()];
            })
            .await;
        let namespace1 = namespaces[1].1;

        let shared_entry = storage::Entry::new(
            storage::Key::shared(b"shared_key"),
            b"shared_value".to_vec(),
            Duration::from_secs(600),
        );

        // Add shared data without a namespace. Validate that it's accessible by both
        // clients.
        client0.set(shared_entry.clone()).await.unwrap();
        let record = client1.get(shared_entry.key.clone()).await.unwrap();
        assert_eq!(record.unwrap().value, shared_entry.value);

        // Validate that the shared data is inaccessible with namespaces set.
        let key = storage::Key::private(&namespaces[0].1, b"shared_key");
        assert_eq!(client0.get(key.clone()).await.unwrap(), None);

        let key = storage::Key::private(&namespaces[1].1, b"shared_key");
        assert_eq!(client1.get(key.clone()).await.unwrap(), None);

        // Validate that data added into one namespace is inaccessible in others.
        let private_entry = storage::Entry::new(
            storage::Key::private(&namespace0, b"key1"),
            b"value2".to_vec(),
            Duration::from_secs(600),
        );

        client0.set(private_entry.clone()).await.unwrap();
        let record = client0.get(private_entry.key.clone()).await.unwrap();
        assert_eq!(record.unwrap().value, private_entry.value);

        let key = storage::Key::private(&namespace1, b"key1");
        assert_eq!(client1.get(key.clone()).await.unwrap(), None);

        // Validate that the client returns an access denied error trying to access
        // unauthorized namespace.
        assert!(matches!(
            client0.get(key.clone()).await,
            Err(replication::Error::StorageApi(
                storage_api::client::Error::Unauthorized
            ))
        ));

        // Validate map type.
        let keys = [Data::generate(), Data::generate(), Data::generate()];
        let data = [
            (Data::generate(), Data::generate()),
            (Data::generate(), Data::generate()),
            (Data::generate(), Data::generate()),
        ];

        // Add a few maps filled with multiple entries to the private namespace.
        for key in &keys {
            for (field, value) in &data {
                let entry = storage::MapEntry::new(
                    storage::Key::private(&namespace0, key.0.clone()),
                    field.0.clone(),
                    value.0.clone(),
                    Duration::from_secs(600),
                );

                client0.hset(entry).await.unwrap();
            }
        }

        // Add a few maps filled with multiple entries to the shared namespace.
        for key in &keys {
            for (field, value) in &data {
                let entry = storage::MapEntry::new(
                    storage::Key::shared(key.0.clone()),
                    field.0.clone(),
                    value.0.clone(),
                    Duration::from_secs(600),
                );

                client0.hset(entry).await.unwrap();
            }
        }

        // Validate private data.
        for key in &keys {
            let key = storage::Key::private(&namespace0, key.0.clone());

            // Validate `hget`.
            for (field, value) in &data {
                let data = client0.hget(key.clone(), field.0.clone()).await.unwrap();
                assert_eq!(data.unwrap().value, value.0.clone());
            }

            // Validate `hvals`.
            assert_eq!(
                crate::sort_data(client0.hvals(key.clone()).await.unwrap()),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );

            // Validate `hscan`.
            let page = client0.hscan(key, 10, None).await.unwrap();
            assert_eq!(
                crate::sort_data(
                    page.records
                        .into_iter()
                        .map(|rec| (rec.field, rec.value))
                        .collect()
                ),
                crate::sort_data(
                    data.iter()
                        .map(|v| (v.0 .0.clone(), v.1 .0.clone()))
                        .collect::<Vec<_>>()
                )
            );
        }

        // Validate shared data.
        for key in &keys {
            let key = storage::Key::shared(key.0.clone());

            // Validate `hget`.
            for (field, value) in &data {
                let data = client0.hget(key.clone(), field.0.clone()).await.unwrap();
                assert_eq!(data.unwrap().value, value.0.clone());
            }

            // Validate `hvals`.
            assert_eq!(
                crate::sort_data(client0.hvals(key.clone()).await.unwrap()),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );

            // Validate `hscan`.
            let page = client0.hscan(key, 10, None).await.unwrap();
            assert_eq!(
                crate::sort_data(
                    page.records
                        .into_iter()
                        .map(|rec| (rec.field, rec.value))
                        .collect()
                ),
                crate::sort_data(
                    data.iter()
                        .map(|v| (v.0 .0.clone(), v.1 .0.clone()))
                        .collect::<Vec<_>>()
                )
            );
        }
    }

    async fn test_addr_change(&mut self) {
        tracing::info!("Address changes");

        for id in self.nodes.keys().copied().collect_vec() {
            self.nodes
                .get_mut(&id)
                .map(NodeHandle::change_addr)
                .unwrap();

            self.restart_node(&id).await;
        }
    }

    async fn test_replication_and_read_repairs(&mut self) {
        const REPLICATION_FACTOR: usize = 3;
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 100;

        tracing::info!("Replication and read repairs");

        let driver = &self.new_replication_driver(|_| {}).await;
        let storage_api_client = &self.new_storage_api_client().await;

        let restarting_node_id = self.nodes.keys().next().copied().unwrap();
        let restarting_node_cfg = self.shutdown_node(&restarting_node_id).await;

        let test_cases: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async {
                let ops = self.gen_test_ops();

                let set = ops.set.clone();

                driver
                    .set(storage::Entry::new(
                        storage::Key::shared(set.key),
                        set.value,
                        set.expiration,
                    ))
                    .await
                    .unwrap();

                ops
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        self.configure_and_spawn_node(|cfg| *cfg = restarting_node_cfg)
            .await;

        stream::iter(&test_cases)
            .map(|c| async {
                let key = storage::Key::shared(c.get.key.clone());

                // trigger read repair
                let record = driver.get(key).await.unwrap().unwrap();
                assert_eq!(record.value, c.set.value);
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let mismatches: usize = stream::iter(&test_cases)
            .map(|c| async {
                let key = storage::Key::shared(c.get.key.clone());

                // check that all replicas have the data
                let replicas: usize = stream::iter(self.nodes.values())
                    .map(|node| async {
                        let addr = node.replica_api_addr();
                        let storage = storage_api_client.remote_storage(&addr);
                        let output = storage.get(key.clone()).await.unwrap().map(|rec| rec.value);
                        usize::from(c.expected_output == output)
                    })
                    .buffer_unordered(self.nodes.len())
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .sum();

                usize::from(replicas != REPLICATION_FACTOR)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    /// Test of Theseus. If all nodes in the cluster are replaced
    /// ~~is it the same cluster?~~ does it still possess all the data?
    async fn test_full_node_rotation(&mut self) {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 10;

        tracing::info!("Full node rotation");

        let driver = &self.new_replication_driver(|_| {}).await;

        let this = &self;
        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async {
                let assert = this.gen_test_ops();
                let set = assert.set.clone();

                driver
                    .set(storage::Entry::new(
                        storage::Key::shared(set.key),
                        set.value,
                        set.expiration,
                    ))
                    .await
                    .unwrap();

                assert
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        for config in self.nodes.values().map(|n| n.config.clone()).collect_vec() {
            let is_raft_voter = config.bootstrap_nodes.is_some() || config.is_raft_voter;

            self.configure_and_spawn_node(|cfg| {
                cfg.is_raft_voter = is_raft_voter;
                cfg.region = config.region;
            })
            .await;

            self.decommission_node(&config.id).await;
        }

        let driver = &self.new_replication_driver(|_| {}).await;

        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = driver
                    .get(storage::Key::shared(assert.get.key))
                    .await
                    .unwrap()
                    .map(|rec| rec.value);

                usize::from(assert.expected_output != output)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    /// Restart a node then decommission the same node. We had an FSM bug
    /// related to this.
    async fn test_restart_then_decommission(&mut self) {
        tracing::info!("Restart then decommission");

        let node_id = self.spawn_node().await;
        self.restart_node(&node_id).await;
        self.decommission_node(&node_id).await;
    }

    /// Scale cluster 2x, then scale back down.
    async fn test_scaling(&mut self) {
        tracing::info!("Scaling");

        let n = self.nodes.len();
        let mut new_nodes = vec![];

        for _ in 0..n {
            new_nodes.push(self.spawn_node().await);
        }

        for id in new_nodes {
            self.decommission_node(&id).await
        }
    }

    async fn test_client_auth(&mut self) {
        let node_keypair = Keypair::generate_ed25519();
        let node_peer_id = PeerId::from_public_key(&node_keypair.public());

        let client_keypair = Keypair::generate_ed25519();
        let client_id = PeerId::from_public_key(&client_keypair.public());

        let node_id = self
            .configure_and_spawn_node(|cfg| {
                cfg.id = node_peer_id;
                cfg.keypair = node_keypair;
                cfg.network_id = NETWORK_ID.to_owned();
                cfg.authorized_clients = Some([client_id].into_iter().collect())
            })
            .await;

        let node_addr = self.nodes[&node_id].client_api_addr();

        let namespaces = (0..2)
            .map(|i| {
                let auth = auth::Auth::from_secret(
                    b"namespace_master_secret",
                    format!("namespace{i}").as_bytes(),
                )
                .unwrap();

                let public_key = auth.public_key();

                (auth, public_key)
            })
            .collect::<Vec<_>>();

        let client = new_client_api_client(|cfg| {
            cfg.keypair = client_keypair;
            cfg.nodes.insert(node_addr);
            cfg.namespaces = namespaces.iter().map(|(auth, _)| auth.clone()).collect();
        })
        .await;

        let token = client.auth_token().load_full();
        let claims = token.decode().unwrap();

        // Validate the auth token claims are correct.
        assert_eq!(claims.network_id(), NETWORK_ID);
        assert_eq!(claims.issuer_peer_id(), node_peer_id);
        assert_eq!(claims.client_peer_id(), client_id);
        assert_eq!(claims.purpose(), auth::token::Purpose::Storage);
        assert_eq!(
            claims.namespaces(),
            namespaces
                .iter()
                .map(|(_, public_key)| *public_key)
                .collect::<Vec<_>>()
        );
        assert!(!claims.is_expired());

        // Spawn a couple of nodes and make sure the cluster state is updated in the
        // client.
        let cluster1 = client.cluster().load_full();

        self.spawn_node().await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let cluster2 = client.cluster().load_full();
        assert_ne!(cluster1.version(), cluster2.version());
        assert_ne!(cluster1.keyspace_version(), cluster2.keyspace_version());

        self.spawn_node().await;
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let cluster3 = client.cluster().load_full();
        assert_ne!(cluster2.version(), cluster3.version());
        assert_ne!(cluster2.keyspace_version(), cluster3.keyspace_version());
    }
}

fn new_node_config() -> Config {
    static COUNTER: AtomicU16 = AtomicU16::new(0);

    let keypair = Keypair::generate_ed25519();
    let id = PeerId::from_public_key(&keypair.public());
    let dir: PathBuf = format!("/tmp/wcn/test-node/{}", id).parse().unwrap();

    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let region = match n % 3 {
        0 => NodeRegion::Eu,
        1 => NodeRegion::Us,
        2 => NodeRegion::Ap,
        _ => unreachable!(),
    };

    let authorized_clients: HashSet<_> =
        [authorized_client_keypair(0), authorized_client_keypair(1)]
            .into_iter()
            .map(|kp| kp.public().to_peer_id())
            .collect();

    Config {
        id,
        region,
        organization: "WalletConnect".to_string(),
        network_id: NETWORK_ID.to_string(),
        is_raft_voter: false,
        raft_server_port: next_port(),
        replica_api_server_port: next_port(),
        client_api_server_port: next_port(),
        admin_api_server_port: next_port(),
        migration_api_server_port: next_port(),
        client_api_max_concurrent_connections: 500,
        client_api_max_concurrent_rpcs: 10000,
        replica_api_max_concurrent_connections: 500,
        replica_api_max_concurrent_rpcs: 10000,
        coordinator_api_max_concurrent_connections: 500,
        coordinator_api_max_concurrent_rpcs: 10000,
        known_peers: HashMap::default(),
        bootstrap_nodes: None,
        raft_dir: dir.join("raft"),
        keypair,
        server_addr: [127, 0, 0, 1].into(),
        metrics_server_port: next_port(),
        rocksdb_dir: dir.join("rocksdb"),
        rocksdb: RocksdbDatabaseConfig {
            num_batch_threads: 1,
            num_callback_threads: 1,
            ..Default::default()
        },
        network_connection_timeout: 5000,
        network_request_timeout: 5000,

        request_concurrency_limit: 1000,
        request_limiter_queue: 1000,
        replication_request_timeout: 5000,
        warmup_delay: 5000,
        authorized_clients: Some(authorized_clients.clone()),
        authorized_admin_api_clients: authorized_clients,
        authorized_raft_candidates: None,
        eth_address: None,
        smart_contract: None,
    }
}

async fn new_client_api_client(
    f: impl FnOnce(&mut client_api::client::Config),
) -> client_api::Client {
    let mut config = client_api::client::Config::new([]);

    f(&mut config);

    client_api::Client::new(config).await.unwrap()
}

fn spawn_node(cfg: Config, prometheus: PrometheusHandle) -> NodeHandle {
    let (tx, rx) = oneshot::channel();
    let rx = rx.map(|res| res.unwrap_or(ShutdownReason::Decommission));

    let replica_api_server_addr = local_multiaddr(cfg.replica_api_server_port);
    let client_api_server_addr = local_multiaddr(cfg.client_api_server_port);
    let admin_api_server_addr = local_multiaddr(cfg.admin_api_server_port);

    let config = cfg.clone();
    let thread_handle = thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                wcn_node::run(rx, prometheus, &config)
                    .await
                    .map_err(|err| tracing::error!(?err, "wcn_node::run() failed"))
                    .unwrap()
                    .await;

                // Seems like `openraft` resolves the shutdown future before finishing all of
                // its spawned tasks. Not waiting here (sometimes!) causes it to fail to
                // write the latest FSM state to disk.
                tokio::time::sleep(Duration::from_secs(1)).await;
            });
    });

    NodeHandle {
        config: cfg,
        thread_handle,
        shutdown_tx: tx,
        replica_api_server_addr,
        client_api_server_addr,
        admin_api_server_addr,
    }
}

#[derive(Clone)]
pub struct Operations {
    pub set: Set,
    pub get: Get,
    pub expected_output: Option<storage_api::Value>,

    // for corrupting the data to validate read repairs
    pub overwrite: Set,
}

#[derive(Clone)]
pub struct Set {
    key: Vec<u8>,
    value: Vec<u8>,
    expiration: Duration,
}

#[derive(Clone)]
pub struct Get {
    key: Vec<u8>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Data(pub Vec<u8>);

impl Data {
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let data = rng.gen::<[u8; 32]>();
        Self(data.into())
    }
}

fn sort_data<T: Ord>(mut data: Vec<T>) -> Vec<T> {
    data.sort();
    data
}

fn local_multiaddr(port: u16) -> Multiaddr {
    socketaddr_to_multiaddr(([127, 0, 0, 1], port))
}
