use {
    crate::{HandshakeRequest, HandshakeResponse},
    futures_util::{Future, SinkExt},
    irn_rpc::{
        quic,
        transport::{self, PendingConnection},
        PeerId,
    },
    std::{collections::HashSet, io, sync::Arc, time::Duration},
    wc::future::FutureExt,
};

/// Runs an RPC server using the provided [`RpcHandler`].
pub fn run(
    server: impl irn_rpc::Server<Handshake = Handshake>,
    config: irn_rpc::quic::server::Config,
) -> Result<impl Future<Output = ()>, quic::Error> {
    quic::server::run(server, config)
}

/// Server part of the [`network::Handshake`].
#[derive(Clone)]
pub struct Handshake;

#[derive(Clone, Debug)]
pub struct HandshakeData {
    pub namespaces: Arc<HashSet<auth::PublicKey>>,
}

impl transport::Handshake for Handshake {
    type Ok = HandshakeData;
    type Err = transport::Error;

    fn handle(
        &self,
        _peer_id: PeerId,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .initiate_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let auth_nonce = auth::Nonce::generate();

            tx.send(&HandshakeRequest { auth_nonce }).await?;

            let resp = rx
                .recv_message()
                .with_timeout(Duration::from_secs(2))
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;

            let namespaces = resp
                .namespaces
                .into_iter()
                .map(|auth| {
                    auth.namespace
                        .verify(auth_nonce.as_ref(), &auth.signature)
                        .map(|_| auth.namespace)
                        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
                })
                .collect::<Result<_, _>>()?;

            Ok(HandshakeData {
                namespaces: Arc::new(namespaces),
            })
        }
    }
}
