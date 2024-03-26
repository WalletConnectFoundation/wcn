use {
    super::{identity::Keypair, Multiaddr, PeerId},
    crate::{
        inbound,
        inbound::ConnectionInfo,
        rpc::{self, AnyPeer},
        BiDirectionalStream,
        Client,
        ClientConfig,
        NoHandshake,
        Rpc,
        ServerConfig,
    },
    futures::{lock::Mutex, Future, SinkExt, StreamExt},
    std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration},
    wc::future::StaticFutureExt,
};

type UnaryRpc = rpc::Unary<{ rpc::id(b"test_unary") }, String, String>;
type StreamingRpc = rpc::Streaming<{ rpc::id(b"test_streaming") }, String, String>;
type OneshotRpc = rpc::Oneshot<{ rpc::id(b"test_oneshot") }, u8>;

#[derive(Clone, Debug)]
pub struct Node {
    received_messages: Arc<Mutex<HashSet<u8>>>,
}

impl inbound::RpcHandler for Node {
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        _: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        async move {
            match id {
                UnaryRpc::ID => {
                    UnaryRpc::handle(stream, |req| async move {
                        assert_eq!(req, "ping".to_string());
                        "pong".to_string()
                    })
                    .await
                }
                StreamingRpc::ID => {
                    StreamingRpc::handle(stream, |mut rx, mut tx| async move {
                        let mut count = 0u8;

                        while let Some(res) = rx.next().await {
                            let req = res.unwrap();
                            assert_eq!(req, "ping".to_string());
                            tx.send("pong".to_string()).await.unwrap();
                            count += 1;
                        }

                        Ok(assert_eq!(count, 3))
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
        let client_config = ClientConfig {
            keypair: keypair.clone(),
            known_peers: peers
                .iter()
                .filter_map(|p| (&p.0 != id).then_some((p.0, p.1.clone())))
                .collect(),
            handshake: NoHandshake,
            connection_timeout: Duration::from_secs(15),
        };

        let client = Client::new(client_config).expect("Client::new");

        let server_config = ServerConfig {
            addr: addr.clone(),
            keypair: keypair.clone(),
        };

        for (remote_id, remote_addr, ..) in &peers {
            if id != remote_id {
                client
                    .register_peer(*remote_id, remote_addr.clone())
                    .await
                    .unwrap();
            }
        }

        clients.push(client.clone());

        let node = Node {
            received_messages: Arc::new(Mutex::new(HashSet::new())),
        };
        nodes.push(node.clone());

        crate::run_server(server_config, NoHandshake, node)
            .expect("run_server")
            .spawn("server");
    }

    // wait a bit for sockets opening
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for (i, ((local_id, ..), client)) in peers.iter().zip(&clients).enumerate() {
        for (remote_id, ..) in &peers {
            if local_id == remote_id {
                continue;
            }

            // unary

            let res = UnaryRpc::send(client, *remote_id, "ping".to_string()).await;
            assert_eq!(res, Ok("pong".to_string()));

            let res = UnaryRpc::send(client, AnyPeer, "ping".to_string()).await;
            assert_eq!(res, Ok("pong".to_string()));

            // streaming

            StreamingRpc::send(client, *remote_id, |mut tx, mut rx| async move {
                for _ in 0..3 {
                    tx.send("ping".to_string()).await?;
                    assert_eq!(rx.recv_message().await?, "pong".to_string());
                }
                tx.finish().await;
                Ok(())
            })
            .await
            .unwrap();
        }

        // pubsub broadcast

        OneshotRpc::broadcast(client, i as u8).await;
    }

    // wait a bit for peers to receive the broadcast
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // assert that all broadcasted messages have reached all peers

    for (i, peer) in nodes.iter().enumerate() {
        let expected: HashSet<u8> = (0..=(nodes.len() - 1) as u8)
            .filter(|m| *m != i as u8)
            .collect();
        let received = peer.received_messages.lock().await;
        assert_eq!(&*received, &expected);
    }
}
