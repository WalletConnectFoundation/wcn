use {
    admin_api::{ClusterView, NodeState},
    api::{auth, Multiaddr},
    futures::{
        stream::{self, FuturesUnordered},
        FutureExt,
        StreamExt as _,
    },
    irn::fsm::ShutdownReason,
    irn_node::{
        cluster::NodeRegion,
        network::{namespaced_key, rpc},
        storage::{self, Get, Set, Value},
        Config,
        RocksdbDatabaseConfig,
    },
    irn_rpc::{identity::Keypair, quic::socketaddr_to_multiaddr, transport::NoHandshake},
    itertools::Itertools,
    libp2p::PeerId,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle},
    rand::{seq::IteratorRandom as _, Rng},
    relay_rocks::util::{timestamp_micros, timestamp_secs},
    std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        sync::atomic::{AtomicU16, Ordering},
        thread,
        time::Duration,
    },
    tokio::sync::oneshot,
    tracing_subscriber::EnvFilter,
};

static NEXT_PORT: AtomicU16 = AtomicU16::new(42100);

fn next_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

fn authorized_client_keypair() -> Keypair {
    Keypair::ed25519_from_bytes(
        base64::decode("N++7piqpE9cGyaMZmNXKeTwhL5Uk89XsUyCGKQP3TcA=").unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn test_suite() {
    let mut cluster = TestCluster::bootstrap().await;

    cluster.test_pubsub().await;

    cluster.test_namespaces().await;

    cluster.test_coordinator_authorization().await;

    // add 2 exta nodes, 5 total
    cluster.spawn_node().await;
    cluster.spawn_node().await;

    cluster.test_addr_change().await;

    cluster.test_replication_and_read_repairs().await;

    cluster.test_full_node_rotation().await;

    cluster.test_restart_then_decommission().await;

    cluster.test_scaling().await;
}

struct NodeHandle {
    config: Config,
    thread_handle: thread::JoinHandle<()>,
    shutdown_tx: oneshot::Sender<ShutdownReason>,

    /// Coordinator API client to make coordinator requests _to_ this node.
    coordinator_api_client: api::Client,

    /// Replica API client to make replica requests _from_ this node to other
    /// nodes.
    replica_api_client: irn_rpc::quic::Client,

    replica_api_server_addr: Multiaddr,
    coordinator_api_server_addr: Multiaddr,
    admin_api_server_addr: Multiaddr,
}

impl NodeHandle {
    async fn replica_get(
        &self,
        mut operation: storage::Get,
        keyspace_version: u64,
    ) -> Option<Value> {
        operation.key = namespaced_key(api::Key {
            namespace: None,
            bytes: operation.key,
        });

        let addr = &self.replica_api_server_addr;

        let req = rpc::replica::Request {
            operation,
            keyspace_version,
        };

        rpc::replica::Get::send(&self.replica_api_client, addr, req)
            .await
            .unwrap()
            .unwrap()
            .map(|out| out.0)
    }

    async fn replica_set(&self, mut operation: storage::Set, keyspace_version: u64) {
        operation.key = namespaced_key(api::Key {
            namespace: None,
            bytes: operation.key,
        });

        let addr = &self.replica_api_server_addr;

        let req = rpc::replica::Request {
            operation,
            keyspace_version,
        };

        rpc::replica::Set::send(&self.replica_api_client, addr, req)
            .await
            .unwrap()
            .unwrap()
    }

    fn change_addr(&mut self) {
        self.config.raft_server_port = next_port();
        self.config.replica_api_server_port = next_port();
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

        let client = admin_api::Client::new(
            admin_api::client::Config::new(local_multiaddr(0))
                .with_keypair(authorized_client_keypair()),
        )
        .unwrap();

        let mut cluster = Self {
            nodes,
            prometheus,
            admin_api_client: client,
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
        let node = self.nodes.remove(id).unwrap();
        node.shutdown_tx.send(ShutdownReason::Restart).unwrap();
        node.thread_handle.join().unwrap();

        self.wait_node_state(id, NodeState::Restarting).await;

        self.configure_and_spawn_node(|c| *c = node.config).await;
    }

    async fn decommission_node(&mut self, id: &PeerId) {
        let node = self.nodes.remove(id).unwrap();
        self.admin_api_client
            .set_server_addr(node.admin_api_server_addr);

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
        client.set_server_addr(node.admin_api_server_addr.clone());

        Ok(client.get_cluster_view().await?)
    }

    fn random_node(&self) -> &NodeHandle {
        self.nodes.values().choose(&mut rand::thread_rng()).unwrap()
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
                expiration: timestamp_secs() + 600,
                version: timestamp_micros(),
            },
            get: Get { key: key.clone() },
            expected_output: Some(value),
            overwrite: Set {
                key,
                value: value2,
                expiration: timestamp_secs() + 600,
                version: timestamp_micros(),
            },
        }
    }

    async fn test_pubsub(&self) {
        tracing::info!("PubSub");

        let channel1 = b"channel1";
        let channel2 = b"channel2";
        let payload1 = b"payload1";
        let payload2 = b"payload2";

        let clients = self
            .nodes
            .values()
            .map(|n| n.coordinator_api_client.clone())
            .collect_vec();

        // Subscribe 2 groups of clients to different channels.
        let mut subscriptions1 = stream::iter(&clients)
            .then(|c| {
                c.subscribe([channel1.to_vec()].into_iter().collect())
                    .map(Box::pin)
            })
            .collect::<Vec<_>>()
            .await;
        let mut subscriptions2 = stream::iter(&clients)
            .then(|c| {
                c.subscribe([channel2.to_vec()].into_iter().collect())
                    .map(Box::pin)
            })
            .collect::<Vec<_>>()
            .await;

        let tasks = FuturesUnordered::new();

        // Publish a message to each channel.
        // Subscribers need to be polled in order to connect to the IRN backend, so we
        // wait a bit before publishing.
        tasks.push(
            async {
                let publisher = &clients[0];

                tokio::time::sleep(Duration::from_secs(3)).await;

                publisher
                    .publish(channel1.to_vec(), payload1.to_vec())
                    .await
                    .unwrap();
                publisher
                    .publish(channel2.to_vec(), payload2.to_vec())
                    .await
                    .unwrap();
            }
            .boxed_local(),
        );

        // All subscriber groups should receive their messages.
        for sub in subscriptions1.iter_mut() {
            tasks.push(
                async {
                    let data = sub.next().await.unwrap();

                    assert_eq!(&data.channel, channel1);
                    assert_eq!(&data.payload, payload1);
                }
                .boxed_local(),
            )
        }

        for sub in subscriptions2.iter_mut() {
            tasks.push(
                async {
                    let data = sub.next().await.unwrap();

                    assert_eq!(&data.channel, channel2);
                    assert_eq!(&data.payload, payload2);
                }
                .boxed_local(),
            )
        }

        // Await all tasks.
        tasks.count().await;

        // Verify there's no more messages to receive.
        stream::iter(subscriptions1)
            .chain(stream::iter(subscriptions2))
            .for_each_concurrent(None, |mut subscriber| async move {
                let result = tokio::time::timeout(Duration::from_secs(3), subscriber.next()).await;

                // We should receive a timeout error, since there's no more messages.
                assert!(result.is_err());
            })
            .await;
    }

    async fn test_namespaces(&self) {
        tracing::info!("Namespaces");

        let expiration = timestamp_secs() + 600;

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
        let client0 = new_coordinator_api_client(|c| {
            c.nodes
                .insert(node.config.id, node.coordinator_api_server_addr.clone());
            c.namespaces = vec![namespaces[0].0.clone()];
        });
        let namespace0 = namespaces[0].1;

        // Client, authorized for the second namespace.
        let client1 = new_coordinator_api_client(|c| {
            c.nodes
                .insert(node.config.id, node.coordinator_api_server_addr.clone());
            c.namespaces = vec![namespaces[1].0.clone()];
        });
        let namespace1 = namespaces[1].1;

        let shared_key = api::Key {
            namespace: None,
            bytes: b"shared_key".to_vec(),
        };
        let shared_value = b"shared_value".to_vec();

        // Add shared data without a namespace. Validate that it's accessible by both
        // clients.
        client0
            .set(shared_key.clone(), shared_value.clone(), expiration)
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
        let mut key = api::Key {
            namespace: Some(namespace0),
            bytes: b"key1".to_vec(),
        };
        let value = b"value2".to_vec();
        client0
            .set(key.clone(), value.clone(), expiration)
            .await
            .unwrap();
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
        let keys = [Data::generate(), Data::generate(), Data::generate()];
        let data = [
            (Data::generate(), Data::generate()),
            (Data::generate(), Data::generate()),
            (Data::generate(), Data::generate()),
        ];

        // Add a few maps filled with multiple entries to the private namespace.
        for key in &keys {
            for (field, value) in &data {
                let key = api::Key {
                    namespace: Some(namespace0),
                    bytes: key.0.clone(),
                };
                client0
                    .hset(key, field.0.clone(), value.0.clone(), expiration)
                    .await
                    .unwrap();
            }
        }

        // Add a few maps filled with multiple entries to the shared namespace.
        for key in &keys {
            for (field, value) in &data {
                let key = api::Key {
                    namespace: None,
                    bytes: key.0.clone(),
                };
                client0
                    .hset(key, field.0.clone(), value.0.clone(), expiration)
                    .await
                    .unwrap();
            }
        }

        // Validate private data.
        for key in &keys {
            let key = api::Key {
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
                crate::sort_data(client0.hvals(key.clone()).await.unwrap()),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );

            // Validate `hscan`.
            let (values, _) = client0.hscan(key, 10, None).await.unwrap();
            assert_eq!(
                crate::sort_data(values),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );
        }

        // Validate shared data.
        for key in &keys {
            let key = api::Key {
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
                crate::sort_data(client0.hvals(key.clone()).await.unwrap()),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );

            // Validate `hscan`.
            let (values, _) = client0.hscan(key, 10, None).await.unwrap();
            assert_eq!(
                crate::sort_data(values),
                crate::sort_data(data.iter().map(|v| v.1 .0.clone()).collect::<Vec<_>>())
            );
        }
    }

    async fn test_coordinator_authorization(&mut self) {
        tracing::info!("Authorization");

        let ops = self.gen_test_ops();

        let client_keypair = Keypair::generate_ed25519();
        let client_id = PeerId::from_public_key(&client_keypair.public());

        // bootup a new node with authorization enabled
        let node_id = self
            .configure_and_spawn_node(|cfg| {
                cfg.authorized_clients = Some([client_id].into_iter().collect())
            })
            .await;

        let node = self.nodes.get(&node_id).unwrap();

        let resp = node
            .coordinator_api_client
            .get(api::Key {
                namespace: None,
                bytes: ops.get.key.clone(),
            })
            .await;

        // TODO: shouldn't be an internal error
        assert_eq!(
            resp,
            Err(api::client::Error::Api(api::Error::Internal(
                api::InternalError {
                    code: "other".into(),
                    message: "Err(Unauthorized)".to_string(),
                }
            )))
        );

        let authorized_client = new_coordinator_api_client(|c| {
            c.keypair = client_keypair;
            c.nodes
                .insert(node_id, node.coordinator_api_server_addr.clone());
        });

        let resp = authorized_client
            .get(api::Key {
                namespace: None,
                bytes: ops.get.key,
            })
            .await;

        assert_eq!(resp, Ok(None));

        let _ = node;
        self.decommission_node(&node_id).await;
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

    async fn test_replication_and_read_repairs(&self) {
        const REPLICATION_FACTOR: usize = 3;
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 100;

        tracing::info!("Replication and read repairs");

        let this = &self;
        let test_cases: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async move {
                let ops = self.gen_test_ops();

                let set = ops.set.clone();
                let key = api::Key {
                    namespace: None,
                    bytes: set.key,
                };

                this.random_node()
                    .coordinator_api_client
                    .set(key, set.value, set.expiration)
                    .await
                    .unwrap();

                ops
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let this = &self;
        let mismatches: usize = stream::iter(test_cases)
            .map(|c| async move {
                // find one replica and break it
                for n in this.nodes.values() {
                    let output = n.replica_get(c.get.clone(), self.keyspace_version).await;

                    if c.expected_output == output {
                        n.replica_set(c.overwrite.clone(), self.keyspace_version)
                            .await;

                        break;
                    }
                }

                // make replicated request via each coordinator to surely trigger a read repair
                let coordinators: usize = stream::iter(this.nodes.values())
                    .map(|n| async {
                        let key = api::Key {
                            namespace: None,
                            bytes: c.get.key.clone(),
                        };

                        let output = n.coordinator_api_client.get(key).await.unwrap();
                        usize::from(c.expected_output == output)
                    })
                    .buffer_unordered(this.nodes.len())
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .sum();

                if coordinators != this.nodes.len() {
                    return 1;
                }

                // check that all replicas have the data
                let replicas: usize = stream::iter(this.nodes.values())
                    .map(|n| async {
                        let output = n.replica_get(c.get.clone(), self.keyspace_version).await;
                        usize::from(c.expected_output == output)
                    })
                    .buffer_unordered(this.nodes.len())
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

        let this = &self;
        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async {
                let assert = this.gen_test_ops();
                let set = assert.set.clone();

                let key = api::Key {
                    namespace: None,
                    bytes: set.key,
                };

                self.random_node()
                    .coordinator_api_client
                    .set(key, set.value, set.expiration)
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

        let node = &self.random_node();
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let key = api::Key {
                    namespace: None,
                    bytes: assert.get.key,
                };

                let output = node.coordinator_api_client.get(key).await.unwrap();

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
}

fn new_node_config() -> Config {
    static COUNTER: AtomicU16 = AtomicU16::new(0);

    let keypair = Keypair::generate_ed25519();
    let id = PeerId::from_public_key(&keypair.public());
    let dir: PathBuf = format!("/tmp/irn/test-node/{}", id).parse().unwrap();

    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let region = match n % 3 {
        0 => NodeRegion::Eu,
        1 => NodeRegion::Us,
        2 => NodeRegion::Ap,
        _ => unreachable!(),
    };

    let authorized_admin_api_client = authorized_client_keypair().public().to_peer_id();

    Config {
        id,
        region,
        organization: "WalletConnect".to_string(),
        is_raft_voter: false,
        raft_server_port: next_port(),
        replica_api_server_port: next_port(),
        coordinator_api_server_port: next_port(),
        client_api_server_port: next_port(),
        admin_api_server_port: next_port(),
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
        authorized_clients: None,
        authorized_admin_api_clients: [authorized_admin_api_client].into_iter().collect(),
        authorized_raft_candidates: None,
        eth_address: None,
        smart_contract: None,
    }
}

fn new_coordinator_api_client(f: impl FnOnce(&mut api::client::Config)) -> api::Client {
    let mut client_config = api::client::Config {
        keypair: Keypair::generate_ed25519(),
        nodes: HashMap::new(),
        shadowing_nodes: HashMap::new(),
        shadowing_factor: 0.0,
        shadowing_default_namespace: None,
        request_timeout: Duration::from_secs(5),
        max_operation_time: Duration::from_secs(10),
        connection_timeout: Duration::from_secs(3),
        udp_socket_count: 1,
        namespaces: Vec::new(),
    };

    f(&mut client_config);

    api::Client::new(client_config).unwrap()
}

fn spawn_node(cfg: Config, prometheus: PrometheusHandle) -> NodeHandle {
    let (tx, rx) = oneshot::channel();
    let rx = rx.map(|res| res.unwrap_or(ShutdownReason::Decommission));

    let replica_api_server_addr = local_multiaddr(cfg.replica_api_server_port);
    let coordinator_api_server_addr = local_multiaddr(cfg.coordinator_api_server_port);
    let admin_api_server_addr = local_multiaddr(cfg.admin_api_server_port);

    let coordinator_api_client = new_coordinator_api_client(|c| {
        c.nodes.insert(cfg.id, coordinator_api_server_addr.clone());
    });

    let client_config = irn_rpc::client::Config {
        keypair: cfg.keypair.clone(),
        known_peers: HashSet::new(),
        handshake: NoHandshake,
        connection_timeout: Duration::from_secs(5),
    };

    let replica_api_client = irn_rpc::quic::Client::new(client_config).unwrap();

    let config = cfg.clone();
    let thread_handle = thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                irn_node::run(rx, prometheus, &config)
                    .await
                    .map_err(|err| tracing::error!(?err, "irn_node::run() failed"))
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
        coordinator_api_client,
        replica_api_client,
        replica_api_server_addr,
        coordinator_api_server_addr,
        admin_api_server_addr,
    }
}

pub struct Operations {
    pub set: Set,
    pub get: Get,
    pub expected_output: Option<Value>,

    // for corrupting the data to validate read repairs
    pub overwrite: Set,
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

fn sort_data(mut data: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    data.sort();
    data
}

fn local_multiaddr(port: u16) -> Multiaddr {
    socketaddr_to_multiaddr(([127, 0, 0, 1], port))
}
