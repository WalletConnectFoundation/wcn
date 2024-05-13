use {
    crate::{rpc, BiDirectionalStream, Handshake, PendingConnection, METRICS},
    futures::{Future, TryFutureExt},
    libp2p::PeerId,
    std::{convert::Infallible, io, net::SocketAddr},
    tokio::io::AsyncReadExt,
    wc::future::{FutureExt as _, StaticFutureExt},
};

#[derive(Debug, Clone)]
pub struct ConnectionInfo<H = ()> {
    pub remote_address: SocketAddr,
    pub peer_id: PeerId,
    pub handshake_data: H,
}

/// Handler of inbound RPCs.
pub trait RpcHandler<H = ()>: Clone + Send + Sync + 'static {
    /// Handles an inbound RPC.
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<H>,
    ) -> impl Future<Output = ()> + Send;
}

pub(super) async fn handle_connections<H: Handshake>(
    endpoint: quinn::Endpoint,
    handshake: H,
    rpc_handler: impl RpcHandler<H::Ok>,
) {
    while let Some(connecting) = endpoint.accept().await {
        ConnectionHandler {
            rpc_handler: rpc_handler.clone(),
            handshake: handshake.clone(),
        }
        .handle(connecting)
        .map_err(|err| tracing::warn!(?err, "Inbound connection handler failed"))
        .spawn("quic_inbound_connection_handler");
    }
}

#[derive(Debug)]
struct ConnectionHandler<S, H> {
    rpc_handler: S,
    handshake: H,
}

impl<S, H> ConnectionHandler<S, H>
where
    S: RpcHandler<H::Ok>,
    H: Handshake,
{
    async fn handle(
        self,
        connecting: quinn::Connecting,
    ) -> Result<(), ConnectionHandlerError<H::Err>> {
        use ConnectionHandlerError as E;

        let conn = connecting.await?;

        let identity = conn.peer_identity().ok_or(E::MissingPeerIdentity)?;
        let certificate = identity
            .downcast::<Vec<rustls::Certificate>>()
            .map_err(|_| E::DowncastPeerIdentity)?
            .into_iter()
            .next()
            .ok_or(E::MissingTlsCertificate)?;

        let conn_info = ConnectionInfo {
            remote_address: conn.remote_address(),
            peer_id: libp2p_tls::certificate::parse(&certificate)
                .map_err(E::ParseTlsCertificate)?
                .peer_id(),
            handshake_data: self
                .handshake
                .handle(PendingConnection(conn.clone()))
                .await
                .map_err(E::Handshake)?,
        };

        loop {
            let (tx, mut rx) = conn
                .accept_bi()
                .with_metrics(METRICS.with_name("accept_bi"))
                .await?;

            let local_peer = self.rpc_handler.clone();
            let conn_info = conn_info.clone();
            async move {
                let rpc_id = match rx.read_u128().await {
                    Ok(id) => id,
                    Err(err) => return tracing::warn!(?err, "Failed to read inbound RPC ID"),
                };

                let stream = BiDirectionalStream::new(tx, rx);
                local_peer.handle_rpc(rpc_id, stream, &conn_info).await
            }
            .with_metrics(METRICS.with_name("inbound_stream_handler"))
            .spawn("quic_inbound_stream_handler");
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionHandlerError<H = Infallible> {
    #[error("Inbound connection failed: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("Missing peer identity")]
    MissingPeerIdentity,

    #[error("Failed to downcast peer identity")]
    DowncastPeerIdentity,

    #[error("Missing TLS certificate")]
    MissingTlsCertificate,

    #[error("Failed to parse TLS certificate: {0:?}")]
    ParseTlsCertificate(libp2p_tls::certificate::ParseError),

    #[error("Handshake failed: {0:?}")]
    Handshake(H),

    #[error("Failed to read RpcId: {0:?}")]
    ReadRpcId(#[from] io::Error),
}
