use {
    super::Error,
    crate::{
        server::{Config, ConnectionInfo},
        transport::{BiDirectionalStream, Handshake, PendingConnection},
        Server,
    },
    futures::TryFutureExt as _,
    std::{convert::Infallible, future::Future, io, net::SocketAddr, sync::Arc, time::Duration},
    tap::{Pipe as _, TapOptional},
    tokio::{
        io::AsyncReadExt as _,
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    wc::{
        future::FutureExt as _,
        metrics::{self, future_metrics, FutureExt as _, StringLabel},
    },
};

/// Runs the [`rpc::Server`].
pub fn run<H: Handshake>(
    server: impl Server<H>,
    cfg: Config,
    handshake: H,
) -> Result<impl Future<Output = ()>, Error> {
    let transport_config = super::new_quinn_transport_config(cfg.max_concurrent_rpcs);

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

    Ok(async move {
        let conn_permits = Arc::new(Semaphore::new(cfg.max_concurrent_connections as usize));
        let stream_permits = Arc::new(Semaphore::new(cfg.max_concurrent_rpcs as usize));

        while let Some(connecting) = endpoint.accept().await {
            let Ok(permit) = conn_permits.clone().try_acquire_owned() else {
                metrics::counter!("irn_rpc_server_connections_dropped", StringLabel<"server_name"> => cfg.name)
                    .increment(1);

                continue;
            };

            metrics::counter!("irn_rpc_server_connections", StringLabel<"server_name"> => cfg.name)
                .increment(1);

            ConnectionHandler {
                server_name: cfg.name,
                server: server.clone(),
                handshake: handshake.clone(),
                stream_concurrency_limiter: stream_permits.clone(),
                _connection_permit: permit,
            }
            .handle(connecting)
            .map_err(|err| tracing::warn!(?err, "Inbound connection handler failed"))
            .with_metrics(future_metrics!("quic_inbound_connection_handler"))
            .pipe(tokio::spawn);
        }
    })
}

#[derive(Debug)]
struct ConnectionHandler<S, H> {
    server_name: &'static str,
    server: S,
    handshake: H,

    stream_concurrency_limiter: Arc<Semaphore>,
    _connection_permit: OwnedSemaphorePermit,
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

        let conn = connecting
            .with_timeout(Duration::from_millis(1000))
            .await
            .map_err(|_| {
                metrics::counter!("irn_rpc_server_connection_timeout", StringLabel<"server_name"> => self.server_name)
                    .increment(1);

                Error::ConnectionTimeout
            })??;

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

        // Wait for the client to send a protocol version being used, if it timeouts
        // then client doesn't support versioning yet.
        let protocol_version = read_protocol_version::<H::Err>(&conn)
            .with_timeout(Duration::from_millis(500))
            .await
            .ok()
            .transpose()?;

        let conn_info = ConnectionInfo {
            peer_id,
            remote_address,
            handshake_data: self
                .handshake
                .handle(PendingConnection(conn.clone()))
                .with_timeout(Duration::from_millis(1000))
                .await
                .map_err(|_| {
                    metrics::counter!("irn_rpc_server_handshake_timeout", StringLabel<"server_name"> => self.server_name)
                        .increment(1);

                    Error::ConnectionTimeout
                })?
                .map_err(Error::Handshake)?,
        };

        loop {
            let (tx, mut rx) = conn.accept_bi().await?;

            let Some(stream_permit) = self.acquire_stream_permit().await else {
                // Over the allowed capacity, so just drop the stream. Do this instead of
                // awaiting a permit to become available, so that the server doesn't lag behind
                // the client, and also to keep streams from accumulating in quic internals.
                continue;
            };

            let local_peer = self.server.clone();
            let conn_info = conn_info.clone();

            async move {
                let _permit = stream_permit;

                let rpc_id = match read_rpc_id(&mut rx).await {
                    Ok(id) => id,
                    Err(err) => return tracing::warn!(%err, "Failed to read inbound RPC ID"),
                };

                let mut stream = BiDirectionalStream::new(tx, rx);
                stream.wrap_result = protocol_version.is_some();
                local_peer.handle_rpc(rpc_id, stream, &conn_info).await
            }
            .with_metrics(future_metrics!("irn_network_inbound_stream_handler"))
            .pipe(tokio::spawn);
        }
    }

    async fn acquire_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!("irn_rpc_server_available_permits", StringLabel<"server_name"> => self.server_name)
            .set(self.stream_concurrency_limiter.available_permits() as f64);

        self.stream_concurrency_limiter
            .clone()
            .try_acquire_owned()
            .ok()
            .tap_none(|| {
                metrics::counter!("irn_rpc_server_throttled_streams", StringLabel<"server_name"> => self.server_name)
                    .increment(1);
            })
    }
}

async fn read_protocol_version<H>(
    conn: &quinn::Connection,
) -> Result<u32, ConnectionHandlerError<H>> {
    let mut rx = conn.accept_uni().await?;
    Ok(rx.read_u32().await?)
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

    #[error("Connection timeout")]
    ConnectionTimeout,
}
