use {
    crate::{rpc, BiDirectionalStream, Handshake, PendingConnection},
    futures::{Future, TryFutureExt},
    libp2p::PeerId,
    std::{convert::Infallible, io, net::SocketAddr},
    tap::Pipe,
    tokio::io::AsyncReadExt,
    wc::future_metrics::{future_name, FutureExt},
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
        .with_metrics(const { &future_name("quic_inbound_connection_handler") })
        .pipe(tokio::spawn);
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
        use ConnectionHandlerError as Error;

        let conn = connecting.await?;

        let identity = conn.peer_identity().ok_or(Error::MissingPeerIdentity)?;
        let certificate = identity
            .downcast::<Vec<rustls::Certificate>>()
            .map_err(|_| Error::DowncastPeerIdentity)?
            .into_iter()
            .next()
            .ok_or(Error::MissingTlsCertificate)?;

        let conn_info = ConnectionInfo {
            remote_address: conn.remote_address(),
            peer_id: libp2p_tls::certificate::parse(&certificate)
                .map_err(Error::ParseTlsCertificate)?
                .peer_id(),
            handshake_data: self
                .handshake
                .handle(PendingConnection(conn.clone()))
                .await
                .map_err(Error::Handshake)?,
        };

        loop {
            let (tx, mut rx) = conn
                .accept_bi()
                .with_metrics(const { &future_name("quic_accept_bi") })
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
            .with_metrics(const { &future_name("irn_network_inbound_stream_handler") })
            .pipe(tokio::spawn);
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
