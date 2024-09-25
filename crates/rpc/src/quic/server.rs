use {
    super::Error,
    crate::{
        server::{Config, ConnectionInfo},
        transport::{BiDirectionalStream, Handshake, PendingConnection},
        Server,
    },
    futures::TryFutureExt as _,
    std::{convert::Infallible, future::Future, io, net::SocketAddr, sync::Arc, time::Duration},
    tap::Pipe as _,
    tokio::io::AsyncReadExt as _,
    wc::{
        future::FutureExt as _,
        metrics::{future_metrics, FutureExt as _},
    },
};

/// Runs the [`rpc::Server`].
pub fn run<H: Handshake>(
    server: impl Server<H>,
    cfg: Config,
    handshake: H,
) -> Result<impl Future<Output = ()>, Error> {
    let transport_config = super::new_quinn_transport_config();

    let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_tls_config));
    server_config.transport = transport_config.clone();
    server_config.migration(false);

    let socket_addr = match super::multiaddr_to_socketaddr(&cfg.addr)? {
        SocketAddr::V4(v4) => SocketAddr::new([0, 0, 0, 0].into(), v4.port()),
        SocketAddr::V6(v6) => SocketAddr::new([0, 0, 0, 0, 0, 0, 0, 0].into(), v6.port()),
    };

    let endpoint = super::new_quinn_endpoint(
        socket_addr,
        &cfg.keypair,
        transport_config,
        Some(server_config),
    )?;

    Ok(handle_connections(server, endpoint, handshake))
}

pub(super) async fn handle_connections<H: Handshake>(
    server: impl Server<H>,
    endpoint: quinn::Endpoint,
    handshake: H,
) {
    while let Some(connecting) = endpoint.accept().await {
        ConnectionHandler {
            server: server.clone(),
            handshake: handshake.clone(),
        }
        .handle(connecting)
        .map_err(|err| tracing::warn!(?err, "Inbound connection handler failed"))
        .with_metrics(future_metrics!("quic_inbound_connection_handler"))
        .pipe(tokio::spawn);
    }
}

#[derive(Debug)]
struct ConnectionHandler<S, H> {
    server: S,
    handshake: H,
}

impl<S, H> ConnectionHandler<S, H>
where
    S: Server<H>,
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

        let peer_id = libp2p_tls::certificate::parse(&certificate)
            .map_err(Error::ParseTlsCertificate)?
            .peer_id();

        // Unwrap is safe here, addr doesn't contain another `PeerId` as we've just
        // built it from `SocketAddr`.
        let remote_address = super::socketaddr_to_multiaddr(conn.remote_address())
            .with_p2p(peer_id)
            .unwrap();

        let conn_info = ConnectionInfo {
            peer_id,
            remote_address,
            handshake_data: self
                .handshake
                .handle(PendingConnection(conn.clone()))
                .await
                .map_err(Error::Handshake)?,
        };

        loop {
            let (tx, mut rx) = conn
                .accept_bi()
                .with_metrics(future_metrics!("quic_accept_bi"))
                .await?;

            let local_peer = self.server.clone();
            let conn_info = conn_info.clone();
            async move {
                let rpc_id = match read_rpc_id(&mut rx).await {
                    Ok(id) => id,
                    Err(err) => return tracing::warn!(%err, "Failed to read inbound RPC ID"),
                };

                let stream = BiDirectionalStream::new(tx, rx);
                local_peer.handle_rpc(rpc_id, stream, &conn_info).await
            }
            .with_metrics(future_metrics!("irn_network_inbound_stream_handler"))
            .pipe(tokio::spawn);
        }
    }
}

async fn read_rpc_id(rx: &mut quinn::RecvStream) -> Result<crate::Id, String> {
    rx.read_u128()
        .with_timeout(Duration::from_millis(500))
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("{err:?}"))
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
