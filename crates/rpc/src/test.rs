use {
    crate::{
        client::{self, AnyPeer},
        id as rpc_id,
        identity::Keypair,
        quic,
        server::{self, ClientConnectionInfo},
        tcp,
        transport::{BiDirectionalStream, NoHandshake, PostcardCodec, Read, Write},
        Acceptor,
        AcceptorConfig,
        Connector,
        Id as RpcId,
        Multiaddr,
        PeerId,
        Server,
        ServerName,
    },
    futures::{lock::Mutex, Future, SinkExt, StreamExt},
    std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration},
    tap::Pipe,
};

const RPC_SERVER_NAME: ServerName = ServerName::new("test_server");

type UnaryRpc = crate::Unary<{ rpc_id(b"test_unary") }, String, String>;
type StreamingRpc = crate::Streaming<{ rpc_id(b"test_streaming") }, String, String>;
type OneshotRpc = crate::Oneshot<{ rpc_id(b"test_oneshot") }, u8>;

#[derive(Clone, Debug)]
pub struct Node {
    received_messages: Arc<Mutex<HashSet<u8>>>,
    rpc_server_config: server::Config,
}

impl crate::Server for Node {
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.rpc_server_config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream<impl Read, impl Write>,
        _: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match id {
                UnaryRpc::ID => {
                    UnaryRpc::handle(stream, |req| async move {
                        assert_eq!(req, "ping".to_string());
                        Ok("pong".to_string())
                    })
                    .await
                }
                StreamingRpc::ID => {
                    StreamingRpc::handle(stream, |mut rx, mut tx| async move {
                        let mut count = 0u8;

                        while let Some(res) = rx.next().await {
                            let req = res.unwrap();
                            assert_eq!(req, "ping".to_string());
                            tx.send(&Ok("pong".to_string())).await.unwrap();
                            count += 1;
                        }

                        assert_eq!(count, 3);
                        Ok(())
                    })
                    .await
                }
                OneshotRpc::ID => {
                    OneshotRpc::handle(stream, |msg| async move {
                        self.received_messages.lock().await.insert(msg);
                    })
                    .await
                }

                _ => unreachable!(),
            }
            .unwrap()
        }
    }
}

trait Transport {
    type Connector: Connector;
    type Acceptor: Acceptor;

    fn multiaddr(n: usize) -> Multiaddr;
    fn connector(keypair: Keypair) -> Self::Connector;
    fn acceptor(keypair: Keypair, addr: Multiaddr, port: u16) -> Self::Acceptor;
}

struct Quic;

impl Transport for Quic {
    type Connector = quic::Connector;
    type Acceptor = quic::Acceptor;

    fn multiaddr(n: usize) -> Multiaddr {
        Multiaddr::from_str(&format!("/ip4/127.0.0.1/udp/300{n}/quic-v1")).unwrap()
    }

    fn connector(keypair: Keypair) -> Self::Connector {
        quic::Connector::new(keypair).unwrap()
    }

    fn acceptor(keypair: Keypair, addr: Multiaddr, _port: u16) -> Self::Acceptor {
        quic::Acceptor::new(quic::AcceptorConfig {
            addr,
            keypair,
            max_concurrent_streams: 100,
        })
        .unwrap()
    }
}

struct Tcp;

impl Transport for Tcp {
    type Connector = tcp::Connector;
    type Acceptor = tcp::Acceptor;

    fn multiaddr(n: usize) -> Multiaddr {
        Multiaddr::from_str(&format!("/ip4/127.0.0.1/tcp/300{n}")).unwrap()
    }

    fn connector(keypair: Keypair) -> Self::Connector {
        tcp::Connector::new(keypair).unwrap()
    }

    fn acceptor(keypair: Keypair, _addr: Multiaddr, port: u16) -> Self::Acceptor {
        tcp::Acceptor::new(tcp::AcceptorConfig { port, keypair }).unwrap()
    }
}

#[tokio::test]
async fn suite() {
    // Left here on purpose, uncomment to quickly debug.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    tracing::info!("Testing QUIC transport");
    test_transport::<Quic>().await;

    tracing::info!("Testing TCP transport");
    test_transport::<Tcp>().await;
}

async fn test_transport<T: Transport>() {
    let gen_peer = |n: usize| {
        let keypair = Keypair::generate_ed25519();
        (
            PeerId::from_public_key(&keypair.public()),
            T::multiaddr(n),
            3000u16 + n as u16,
            keypair,
        )
    };

    let peers = [gen_peer(0), gen_peer(1), gen_peer(2)];

    let mut clients = Vec::with_capacity(3);

    let mut nodes = Vec::new();

    for (id, addr, port, keypair) in &peers {
        let client_config = client::Config {
            known_peers: peers
                .iter()
                .filter_map(|p| (&p.0 != id).then_some(p.1.clone()))
                .collect(),
            handshake: NoHandshake,
            connection_timeout: Duration::from_secs(15),
            server_name: RPC_SERVER_NAME,
        };

        let client_socket = T::connector(keypair.clone());
        let client = client::new(client_socket, client_config);

        let server_config = server::Config {
            name: &RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        clients.push(client.clone());

        let node = Node {
            received_messages: Arc::new(Mutex::new(HashSet::new())),
            rpc_server_config: server_config,
        };
        nodes.push(node.clone());

        let acceptor = T::acceptor(keypair.clone(), addr.clone(), *port);

        let acceptor_config = AcceptorConfig {
            max_concurrent_connections: 100,
            max_concurrent_streams: 100,
        };

        node.serve(acceptor, acceptor_config).pipe(tokio::spawn);
    }

    // wait a bit for sockets opening
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for (i, ((local_id, ..), client)) in peers.iter().zip(&clients).enumerate() {
        for (remote_id, remote_addr, _, _) in &peers {
            if local_id == remote_id {
                continue;
            }

            let to = remote_addr;

            // unary

            let res = UnaryRpc::send(client, to, &"ping".to_string()).await;
            assert_eq!(res.unwrap(), "pong".to_string());

            let res = UnaryRpc::send(client, &AnyPeer, &"ping".to_string()).await;
            assert_eq!(res.unwrap(), "pong".to_string());

            // streaming

            StreamingRpc::send(client, to, &|mut tx, mut rx| async move {
                for _ in 0..3 {
                    tx.send(&"ping".to_string()).await?;
                    assert_eq!(rx.recv_message().await?, Ok("pong".to_string()));
                }
                Ok(())
            })
            .await
            .unwrap();

            // oneshot

            OneshotRpc::send(client, to, &(i as u8)).await.unwrap();
        }
    }

    // wait a bit for peers to receive the broadcast
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // assert that all oneshot messages have reached all peers

    for (i, peer) in nodes.iter().enumerate() {
        let expected: HashSet<u8> = (0..=(nodes.len() - 1) as u8)
            .filter(|m| *m != i as u8)
            .collect();
        let received = peer.received_messages.lock().await;
        assert_eq!(&*received, &expected);
    }
}
