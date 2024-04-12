use {
    api::{namespace, Key, Multiaddr},
    futures::{
        stream::{self, FuturesUnordered},
        FutureExt,
        StreamExt,
    },
    irn::{
        cluster::{self, replication::Strategy},
        test::{self, NodeIdentity},
        NodeOpts,
        PeerId,
        ShutdownReason,
    },
    itertools::Itertools,
    lib::{
        storage::{Get, Set, Storage},
        Config,
        Consensus,
        Network,
    },
    rand::Rng,
    relay_rocks::util::timestamp_micros,
    std::{collections::HashMap, path::PathBuf, time::Duration},
    tokio::{runtime, sync::oneshot},
    tracing_subscriber::EnvFilter,
};

struct Context {
    max_raft_members: usize,
    nodes: HashMap<PeerId, NodeContext>,
}

struct NodeContext {
    runtime: runtime::Handle,
    runtime_shutdown_tx: oneshot::Sender<()>,
    runtime_thread: std::thread::JoinHandle<()>,

    consensus: Consensus,
    config: Config,
}

impl test::Context for Context {
    type Consensus = Consensus;
    type Network = Network;
    type Storage = Storage;

    type ReadOperation = Get;
    type WriteOperation = Set;

    async fn init_deps(
        &mut self,
        idt: NodeIdentity,
        peers: HashMap<PeerId, Multiaddr>,
        is_bootstrap: bool,
    ) -> test::Dependencies<Self::Consensus, Self::Network, Self::Storage> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_handle = runtime.handle().clone();

        let (runtime_shutdown_tx, rx) = oneshot::channel();
        let runtime_thread = std::thread::spawn(move || {
            runtime.block_on(async move {
                let _ = rx.await;
            });
        });

        let is_raft_member = self.nodes.len() < self.max_raft_members;

        let (cfg, deps) = runtime_handle
            .clone()
            .spawn(async move {
                let dir: PathBuf = format!("/tmp/irn/test-node/{}", idt.peer_id)
                    .parse()
                    .unwrap();

                let cfg = Config {
                    id: idt.peer_id,
                    addr: idt.addr,
                    is_raft_member,
                    known_peers: peers.clone(),
                    bootstrap_nodes: is_bootstrap.then(|| peers.clone()),
                    raft_dir: dir.join("raft"),
                    keypair: idt.keypair.clone(),
                    api_addr: idt.api_addr.clone(),
                    metrics_addr: String::new(),
                    rocksdb_dir: dir.join("rocksdb"),
                    rocksdb_num_batch_threads: 1,
                    rocksdb_num_callback_threads: 1,
                    network_connection_timeout: 5000,
                    network_request_timeout: 5000,

                    // these are being overwritten by the test machinery
                    replication_strategy: Default::default(),
                    request_concurrency_limit: 1000,
                    request_limiter_queue: 1000,
                    replication_request_timeout: 5000,
                    warmup_delay: 5000,
                };

                let storage = Storage::new(&cfg).unwrap();

                let network = Network::new(&cfg).unwrap();

                let consensus = Consensus::new(&cfg, network.clone()).await.unwrap();

                consensus.init(&cfg).await.unwrap();

                (cfg, test::Dependencies {
                    consensus,
                    network,
                    storage,
                })
            })
            .await
            .unwrap();

        self.nodes.insert(cfg.id, NodeContext {
            runtime: runtime_handle,
            runtime_shutdown_tx,
            runtime_thread,
            consensus: deps.consensus.clone(),
            config: cfg,
        });

        deps
    }

    fn gen_test_ops() -> test::Operations<Self> {
        let mut rng = rand::thread_rng();
        let key = rng.gen::<u128>().to_be_bytes().to_vec();
        let value = rng.gen::<u128>().to_be_bytes().to_vec();
        let value2 = rng.gen::<u128>().to_be_bytes().to_vec();

        test::Operations {
            write: Set {
                key: key.clone(),
                value: value.clone(),
                expiration: None,
                version: timestamp_micros(),
            },
            read: Get { key: key.clone() },
            expected_output: Some(value),
            overwrite: Set {
                key,
                value: value2,
                expiration: None,
                version: timestamp_micros(),
            },
        }
    }

    async fn pre_bootup(&mut self, idt: &test::NodeIdentity, node: &test::Node<Self>) {
        let node_ctx = self.nodes.get_mut(&idt.peer_id).unwrap();
        let _guard = node_ctx.runtime.enter();
        Network::spawn_servers(&node_ctx.config, node.clone()).unwrap();
    }

    async fn post_shutdown(&mut self, node: &test::NodeHandle<Self>, reason: ShutdownReason) {
        let node_ctx = self.nodes.remove(node.id()).unwrap();
        node_ctx
            .runtime
            .spawn(async move { node_ctx.consensus.shutdown(reason).await })
            .await
            .unwrap();

        node_ctx.runtime_shutdown_tx.send(()).unwrap();
        node_ctx.runtime_thread.join().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_suite() {
    const NUM_BOOTNODES: usize = 5;

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let ctx = Context {
        max_raft_members: NUM_BOOTNODES + 1,
        nodes: HashMap::new(),
    };

    let mut cluster = test::Cluster::new(ctx, test::ClusterConfig {
        num_bootnodes: NUM_BOOTNODES,
        num_groups: 3,
        node_opts: NodeOpts {
            replication_strategy: Strategy::new(3, cluster::replication::ConsistencyLevel::Quorum),
            replication_request_timeout: Duration::from_secs(15),
            replication_concurrency_limit: 1000,
            replication_request_queue: 4096,
            warmup_delay: Duration::from_secs(1),
        },
    })
    .await;

    cluster.run_test_suite().await;

    pubsub(&cluster).await;
    namespaces(&cluster).await;
}

async fn pubsub(cluster: &test::Cluster<Context>) {
    let channel1 = b"channel1";
    let channel2 = b"channel2";
    let payload1 = b"payload1";
    let payload2 = b"payload2";

    let clients = cluster
        .nodes()
        .values()
        .map(|n| {
            api::Client::new(api::client::Config {
                nodes: [(n.identity().peer_id.id, n.identity().api_addr.clone())]
                    .into_iter()
                    .collect(),
                shadowing_nodes: Default::default(),
                shadowing_factor: Default::default(),
                request_timeout: Duration::from_secs(3),
                max_operation_time: Duration::from_secs(5),
                connection_timeout: Duration::from_secs(1),
                udp_socket_count: 2,
                namespaces: vec![],
            })
            .unwrap()
        })
        .collect_vec();

    // Subscribe 2 groups of clients to different channels.
    let mut subscriptions1 = stream::iter(&clients)
        .then(|c| async move {
            c.subscribe([channel1.to_vec()].into_iter().collect())
                .await
                .boxed()
        })
        .collect::<Vec<_>>()
        .await;
    let mut subscriptions2 = stream::iter(&clients)
        .then(|c| async move {
            c.subscribe([channel2.to_vec()].into_iter().collect())
                .await
                .boxed()
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

async fn namespaces(cluster: &test::Cluster<Context>) {
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

    let node = &cluster.nodes().values().next().unwrap();

    let config = api::client::Config {
        nodes: [(node.identity().peer_id.id, node.identity().api_addr.clone())]
            .into_iter()
            .collect(),
        shadowing_nodes: Default::default(),
        shadowing_factor: Default::default(),
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
    let keys = [Data::generate(), Data::generate(), Data::generate()];
    let data = [
        (Data::generate(), Data::generate()),
        (Data::generate(), Data::generate()),
        (Data::generate(), Data::generate()),
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
