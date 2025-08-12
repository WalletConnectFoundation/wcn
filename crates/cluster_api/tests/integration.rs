use {
    alloy::{
        node_bindings::{Anvil, AnvilInstance},
        signers::local::PrivateKeySigner,
    },
    futures::{StreamExt as _, TryStreamExt},
    std::{net::Ipv4Addr, time::Duration},
    tap::{Pipe, Tap as _},
    wcn_cluster::{
        Cluster,
        Event,
        Node,
        NodeOperator,
        node_operator,
        smart_contract::{
            self,
            Signer,
            evm::{self, RpcProvider},
        },
        testing,
    },
    wcn_cluster_api::{ClusterApi, Read, rpc::client::ApiConfig},
    wcn_rpc::{
        identity::Keypair,
        server2::{Server, ShutdownSignal},
    },
};

#[tokio::test]
async fn test_rpc() {
    let _logger = logging::Logger::init(logging::LogFormat::Json, None, None);
    let port = find_available_port();

    // Spin up a local Anvil instance automatically
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(31337)
        .try_spawn()
        .unwrap();

    let cfg = Config;

    let settings = wcn_cluster::Settings {
        max_node_operator_data_bytes: 4096,
    };

    // Use Anvil's first key for deployment - convert PrivateKeySigner to our Signer
    let private_key_signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let private_key_signer = format!("{:#x}", private_key_signer.to_bytes());
    let signer = smart_contract::Signer::try_from_private_key(&private_key_signer).unwrap();

    let provider = provider(signer, &anvil).await;
    let operators = (1..=5).map(|n| new_node_operator(n, &anvil)).collect();

    let cluster = Cluster::deploy(cfg, &provider, settings, operators)
        .await
        .unwrap();
    let smart_contract = cluster.smart_contract().clone();

    let server_keypair = Keypair::generate_ed25519();
    let server_peer_id = server_keypair.public().to_peer_id();

    let server = wcn_cluster_api::rpc::server::new(smart_contract.clone())
        .serve(wcn_rpc::server2::Config {
            name: "test_cluster_api",
            port,
            keypair: server_keypair,
            connection_timeout: Duration::from_secs(10),
            max_connections: 100,
            max_connections_per_ip: 100,
            max_connection_rate_per_ip: 100,
            max_concurrent_rpcs: 500,
            priority: wcn_rpc::transport::Priority::High,
            shutdown_signal: ShutdownSignal::new(),
        })
        .unwrap()
        .pipe(tokio::spawn);

    let client_keypair = Keypair::generate_ed25519();
    let client = wcn_cluster_api::rpc::client::new(wcn_rpc::client2::Config {
        keypair: client_keypair,
        connection_timeout: Duration::from_secs(10),
        reconnect_interval: Duration::from_secs(1),
        max_concurrent_rpcs: 500,
        priority: wcn_rpc::transport::Priority::High,
        api: ApiConfig {
            rpc_timeout: Duration::from_secs(5),
        },
    })
    .unwrap();

    let client_conn = client
        .connect(
            format!("127.0.0.1:{port}").parse().unwrap(),
            &server_peer_id,
            (),
        )
        .await
        .unwrap();

    let address = client_conn.address().await.unwrap();
    assert_eq!(address, smart_contract.address());

    let cluster_view = client_conn.cluster_view().await.unwrap();
    assert_eq!(cluster_view.cluster_version, 1);

    let events = client_conn.events().await.unwrap();

    for id in 6..=8 {
        cluster
            .add_node_operator(new_node_operator(id, &anvil))
            .await
            .unwrap();
    }

    let events = events.take(3).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(events.len(), 3);

    for evt in events {
        assert!(matches!(evt, Event::NodeOperatorAdded(_)));
    }

    server.abort();
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

#[derive(Clone, Copy)]
struct Config;

impl wcn_cluster::Config for Config {
    type SmartContract = evm::SmartContract;
    type KeyspaceShards = ();
    type Node = Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: Node) -> Self::Node {
        node
    }
}

fn new_node_operator(n: u8, anvil: &AnvilInstance) -> NodeOperator {
    testing::node_operator(0).tap_mut(|op| op.id = operator_id(n, anvil))
}

fn operator_id(n: u8, anvil: &AnvilInstance) -> node_operator::Id {
    *new_signer(n, anvil).address()
}

fn new_signer(n: u8, anvil: &AnvilInstance) -> Signer {
    let private_key_signer: PrivateKeySigner = anvil.keys()[n as usize].clone().into();
    let private_key = &format!("{:#x}", private_key_signer.to_bytes());
    Signer::try_from_private_key(private_key).unwrap()
}

async fn provider(signer: Signer, anvil: &AnvilInstance) -> RpcProvider {
    let ws_url = anvil.endpoint_url().to_string().replace("http://", "ws://");
    RpcProvider::new(ws_url.parse().unwrap(), signer)
        .await
        .unwrap()
}
