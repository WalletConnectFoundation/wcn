pub use network::{inbound::RpcHandler, ServerConfig as Config};
use {
    crate::{namespace, HandshakeRequest, HandshakeResponse},
    futures_util::{Future, SinkExt},
    std::{collections::HashSet, io, sync::Arc, time::Duration},
    wc::future::FutureExt,
};

/// Runs an RPC server using the provided [`RpcHandler`].
pub fn run(
    cfg: Config,
    rpc_handler: impl RpcHandler<HandshakeData>,
) -> Result<impl Future<Output = ()>, network::Error> {
    network::run_server(cfg, Handshake, rpc_handler)
}

/// Server part of the [`network::Handshake`].
#[derive(Clone)]
struct Handshake;

#[derive(Clone, Debug)]
pub struct HandshakeData {
    pub namespaces: Arc<HashSet<namespace::PublicKey>>,
}

impl network::Handshake for Handshake {
    type Ok = HandshakeData;
    type Err = super::HandshakeError;

    fn handle(
        &self,
        conn: network::PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .initiate_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let auth_nonce = namespace::Nonce::generate();

            tx.send(HandshakeRequest { auth_nonce }).await?;

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
