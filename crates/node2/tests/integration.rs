use {
    alloy::{
        node_bindings::{Anvil, AnvilInstance},
        signers::local::PrivateKeySigner,
    },
    derive_more::derive::AsRef,
    futures::{stream, StreamExt},
    metrics_exporter_prometheus::PrometheusRecorder,
    std::{
        net::{Ipv4Addr, SocketAddrV4},
        thread,
        time::Duration,
    },
    tap::Pipe as _,
    tracing_subscriber::EnvFilter,
    wcn_client::{ClientBuilder, PeerAddr},
    wcn_cluster::{
        node_operator,
        smart_contract::{
            self,
            evm::{self, RpcProvider},
            Read,
            Signer,
        },
        Cluster,
        EncryptionKey,
        Settings,
    },
    wcn_rpc::{identity::Keypair, server2::ShutdownSignal},
};

#[derive(AsRef, Clone, Copy)]
struct DeploymentConfig {
    #[as_ref]
    encryption_key: EncryptionKey,
}

impl wcn_cluster::Config for DeploymentConfig {
    type SmartContract = evm::SmartContract;
    type KeyspaceShards = ();
    type Node = wcn_cluster::Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: wcn_cluster::Node) -> Self::Node {
        node
    }
}

#[tokio::test]
async fn test_suite() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Spin up a local Anvil instance automatically
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(31337)
        .try_spawn()
        .unwrap();

    let settings = Settings {
        max_node_operator_data_bytes: 4096,
    };

    // Use Anvil's first key for deployment - convert PrivateKeySigner to our Signer
    let private_key_signer: PrivateKeySigner = anvil.keys().last().unwrap().clone().into();
    let signer =
        Signer::try_from_private_key(&format!("{:#x}", private_key_signer.to_bytes())).unwrap();

    let provider = provider(signer, &anvil).await;

    let mut operators: Vec<_> = (1..=5).map(|n| NodeOperator::new(n, &anvil)).collect();
    let operators_on_chain = operators.iter().map(NodeOperator::on_chain).collect();

    let cfg = DeploymentConfig {
        encryption_key: wcn_cluster::testing::encryption_key(),
    };

    let cluster = Cluster::deploy(cfg, &provider, settings, operators_on_chain)
        .await
        .unwrap();

    let contract_address = cluster.smart_contract().address().unwrap();

    operators
        .iter_mut()
        .flat_map(|operator| operator.nodes.as_mut_slice())
        .for_each(|node| node.config.smart_contract_address = contract_address);

    stream::iter(&mut operators)
        .for_each_concurrent(5, NodeOperator::deploy)
        .await;

    let bootstrap_node = &operators[1].nodes[0];
    let bootstrap_node = PeerAddr {
        id: bootstrap_node.config.keypair.public().to_peer_id(),
        addr: SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            bootstrap_node.config.primary_rpc_server_port,
        ),
    };

    let client = wcn_client::Client::new(wcn_client::Config {
        keypair: operators[0].clients[0].keypair.clone(),
        connection_timeout: Duration::from_secs(1),
        operation_timeout: Duration::from_secs(2),
        reconnect_interval: Duration::from_millis(100),
        max_concurrent_rpcs: 5000,
        nodes: vec![bootstrap_node],
        metrics_tag: "mainnet",
    })
    .await
    .unwrap()
    .build();

    let namespace = format!("{}/0", operators[0].signer.address())
        .parse()
        .unwrap();

    client
        .set(namespace, b"foo", b"bar", Duration::from_secs(60))
        .await
        .unwrap();

    let res = client.get(namespace, b"foo").await.unwrap();
    assert_eq!(res, Some(b"bar".into()));
}

struct NodeOperator {
    signer: smart_contract::Signer,
    name: node_operator::Name,
    database: Database,
    nodes: Vec<Node>,
    clients: Vec<Client>,
}

struct Client {
    keypair: Keypair,
    authorized_namespaces: Vec<u8>,
}

impl Client {
    fn on_chain(&self) -> wcn_cluster::Client {
        wcn_cluster::Client {
            peer_id: self.keypair.public().to_peer_id(),
            authorized_namespaces: self.authorized_namespaces.clone().into(),
        }
    }
}

struct Database {
    config: wcn_db::Config,
    shutdown_signal: ShutdownSignal,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl Database {
    fn deploy(&mut self) {
        let fut = wcn_db::run(self.shutdown_signal.clone(), self.config.clone()).unwrap();

        self.thread_handle = Some(thread::spawn(move || {
            let _guard = tracing::info_span!("database").entered();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(fut);
        }));
    }
}

struct Node {
    operator_id: node_operator::Id,
    config: wcn_node::Config,
    _prometheus_recorder: PrometheusRecorder,
    _shutdown_signal: ShutdownSignal,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl Node {
    async fn deploy(&mut self) {
        let operator_id = self.operator_id;
        let fut = wcn_node::run(self.config.clone());

        let (tx, rx) = std::sync::mpsc::channel::<wcn_node::Result<()>>();

        self.thread_handle = Some(thread::spawn(move || {
            let _guard = tracing::info_span!("node", %operator_id).entered();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    match fut.await {
                        Ok(fut) => {
                            let _ = tx.send(Ok(()));
                            fut.await
                        }
                        Err(err) => tx.send(Err(err)).pipe(drop),
                    }
                });
        }));

        tokio::task::spawn_blocking(move || rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn on_chain(&self) -> wcn_cluster::Node {
        wcn_cluster::Node {
            peer_id: self.config.keypair.public().to_peer_id(),
            ipv4_addr: Ipv4Addr::LOCALHOST,
            primary_port: self.config.primary_rpc_server_port,
            secondary_port: self.config.secondary_rpc_server_port,
        }
    }
}

impl NodeOperator {
    fn new(id: u8, anvil: &AnvilInstance) -> NodeOperator {
        // dummy value, we don't know the address at this point yet
        let smart_contract_address = "0xF85FA2ce74D0b65756E14377f0359BB13E229ECE"
            .parse()
            .unwrap();

        let smart_contract_signer = anvil.keys()[id as usize].to_bytes().pipe(|bytes| {
            smart_contract::Signer::try_from_private_key(&const_hex::encode(bytes)).unwrap()
        });

        let operator_id = *smart_contract_signer.address();

        let rpc_provider_url = anvil.endpoint_url().to_string().replace("http://", "ws://");

        let db_keypair = Keypair::generate_ed25519();
        let db_peer_id = db_keypair.public().to_peer_id();
        let db_shutdown_signal = ShutdownSignal::new();

        let db_config = wcn_db::Config {
            keypair: db_keypair,
            primary_rpc_server_port: find_available_port(),
            secondary_rpc_server_port: find_available_port(),
            metrics_server_port: find_available_port(),
            connection_timeout: Duration::from_secs(1),
            max_connections: 100,
            max_connections_per_ip: 100,
            max_connection_rate_per_ip: 100,
            max_concurrent_rpcs: 100,
            rocksdb_dir: format!("/tmp/wcn_db/{db_peer_id}").parse().unwrap(),
            rocksdb: Default::default(),
        };

        let database = Database {
            config: db_config.clone(),
            shutdown_signal: db_shutdown_signal,
            thread_handle: None,
        };

        let nodes = (0..=1)
            .map(|n| {
                let keypair = Keypair::generate_ed25519();
                let shutdown_signal = ShutdownSignal::new();

                let prometheus_recorder =
                    metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();

                let config = wcn_node::Config {
                    keypair,
                    primary_rpc_server_port: find_available_port(),
                    secondary_rpc_server_port: find_available_port(),
                    metrics_server_port: find_available_port(),
                    database_rpc_server_address: Ipv4Addr::LOCALHOST,
                    database_peer_id: db_peer_id,
                    database_primary_rpc_server_port: db_config.primary_rpc_server_port,
                    database_secondary_rpc_server_port: db_config.secondary_rpc_server_port,
                    smart_contract_address,
                    smart_contract_signer: (n == 0).then_some(smart_contract_signer.clone()),
                    smart_contract_encryption_key: wcn_cluster::testing::encryption_key(),
                    rpc_provider_url: rpc_provider_url.clone().parse().unwrap(),
                    shutdown_signal: shutdown_signal.clone(),
                    prometheus_handle: prometheus_recorder.handle(),
                };

                Node {
                    operator_id,
                    config,
                    _prometheus_recorder: prometheus_recorder,
                    _shutdown_signal: shutdown_signal,
                    thread_handle: None,
                }
            })
            .collect();

        Self {
            signer: smart_contract_signer,
            name: node_operator::Name::new(format!("operator{id}")).unwrap(),
            database,
            nodes,
            clients: vec![Client {
                keypair: Keypair::generate_ed25519(),
                authorized_namespaces: vec![0, 1],
            }],
        }
    }

    async fn deploy(&mut self) {
        self.database.deploy();

        self.nodes
            .iter_mut()
            .pipe(stream::iter)
            .for_each_concurrent(10, Node::deploy)
            .await;
    }

    fn on_chain(&self) -> wcn_cluster::NodeOperator {
        wcn_cluster::NodeOperator::new(
            *self.signer.address(),
            self.name.clone(),
            self.nodes.iter().map(Node::on_chain).collect(),
            self.clients.iter().map(Client::on_chain).collect(),
        )
        .unwrap()
    }
}

async fn provider(signer: Signer, anvil: &AnvilInstance) -> RpcProvider {
    let ws_url = anvil.endpoint_url().to_string().replace("http://", "ws://");
    RpcProvider::new(ws_url.parse().unwrap(), signer)
        .await
        .unwrap()
}

fn find_available_port() -> u16 {
    use std::{
        net::{TcpListener, UdpSocket},
        sync::atomic::{AtomicU16, Ordering},
    };

    static NEXT_PORT: AtomicU16 = AtomicU16::new(48100);

    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        assert!(port != u16::MAX, "failed to find a free port");

        let is_udp_available = UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).is_ok();
        let is_tcp_available = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok();

        if is_udp_available && is_tcp_available {
            return port;
        }
    }
}
