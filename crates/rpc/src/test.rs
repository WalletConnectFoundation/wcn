use {
    crate::{
        client::{self, AnyPeer},
        id as rpc_id,
        identity::Keypair,
        quic,
        server::{self, ClientConnectionInfo},
        transport::{self, BiDirectionalStream, NoHandshake, PostcardCodec},
        Id as RpcId,
        Multiaddr,
        PeerAddr,
        PeerId,
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
        stream: BiDirectionalStream,
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

#[tokio::test]
async fn suite() {
    // Left here on purpose, uncomment to quickly debug.

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let gen_peer = |n: usize| {
        let keypair = Keypair::generate_ed25519();
        (
            PeerId::from_public_key(&keypair.public()),
            Multiaddr::from_str(&format!("/ip4/127.0.0.1/udp/300{n}/quic-v1")).unwrap(),
            keypair,
        )
    };

    let peers = [gen_peer(0), gen_peer(1), gen_peer(2)];

    let mut clients = Vec::with_capacity(3);

    let mut nodes = Vec::new();

    for (id, addr, keypair) in &peers {
        let client_config = client::Config {
            keypair: keypair.clone(),
            known_peers: peers
                .iter()
                .filter_map(|peer| (&peer.0 != id).then_some(PeerAddr::new(peer.0, peer.1.clone())))
                .collect(),
            handshake: NoHandshake,
            connection_timeout: Duration::from_secs(15),
            server_name: RPC_SERVER_NAME,
            priority: transport::Priority::High,
        };

        let client = quic::Client::new(client_config).expect("Client::new");

        let server_config = server::Config {
            name: RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        let quic_server_config = quic::server::Config {
            name: "test_server",
            addr: addr.clone(),
            keypair: keypair.clone(),
            max_connections: 500,
            max_connections_per_ip: 100,
            max_connection_rate_per_ip: 100,
            max_streams: 10000,
            priority: transport::Priority::High,
        };

        clients.push(client.clone());

        let node = Node {
            received_messages: Arc::new(Mutex::new(HashSet::new())),
            rpc_server_config: server_config,
        };
        nodes.push(node.clone());

        quic::server::run(node, quic_server_config)
            .expect("run_server")
            .pipe(tokio::spawn);
    }

    // wait a bit for sockets opening
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for (i, ((local_id, ..), client)) in peers.iter().zip(&clients).enumerate() {
        for (remote_id, remote_addr, _) in &peers {
            if local_id == remote_id {
                continue;
            }

            let to = PeerAddr::new(*remote_id, remote_addr.clone());

            // unary

            let res = UnaryRpc::send(client, &to, &"ping".to_string()).await;
            assert_eq!(res, Ok("pong".to_string()));

            let res = UnaryRpc::send(client, &AnyPeer, &"ping".to_string()).await;
            assert_eq!(res, Ok("pong".to_string()));

            // streaming

            StreamingRpc::send(client, &to, &|mut tx, mut rx| async move {
                for _ in 0..3 {
                    tx.send(&"ping".to_string()).await?;
                    assert_eq!(rx.recv_message().await?, Ok("pong".to_string()));
                }
                Ok(())
            })
            .await
            .unwrap();

            // oneshot

            OneshotRpc::send(client, &to, &(i as u8)).await.unwrap();
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

#[test]
fn peer_addr() {
    let encoded =
        "12D3KooWDJrGKPuU1vJLBZv2UXfcZvdBprUgAkjvkUET7q2PzwPp-/ip4/127.0.0.1/udp/3011/quic-v1";
    let decoded = PeerAddr::from_str(encoded).unwrap();

    assert_eq!(
        decoded,
        PeerAddr::new(
            "12D3KooWDJrGKPuU1vJLBZv2UXfcZvdBprUgAkjvkUET7q2PzwPp"
                .parse()
                .unwrap(),
            "/ip4/127.0.0.1/udp/3011/quic-v1".parse().unwrap(),
        )
    );

    assert_eq!(encoded, &decoded.to_string());
}
