use {
    super::{ConnectionHeader, Error},
    crate::{
        self as rpc,
        quic,
        server::ConnectionInfo,
        transport::{self, BiDirectionalStream, Handshake, PendingConnection},
        ServerName,
    },
    derive_more::derive::Deref,
    // filter::{Filter, Permit, RejectionReason},
    futures::{FutureExt, SinkExt as _, TryFutureExt as _},
    libp2p::{identity::Keypair, Multiaddr},
    quinn::crypto::rustls::QuicServerConfig,
    std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration},
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

// mod filter;

/// QUIC RPC server config.
pub struct Config {
    /// Name of the server. For metrics purposes only.
    pub name: &'static str,

    /// [`Multiaddr`] to bind the server to.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Maximum global number of concurrent connections.
    pub max_connections: u32,

    /// Maximum number of concurrent connections per client IP address.
    pub max_connections_per_ip: u32,

    /// Maximum number of connections accepted per client IP address per second.
    pub max_connection_rate_per_ip: u32,

    /// Maximum number of concurrent streams.
    pub max_streams: u32,

    /// [`transport::Priority`] of the server.
    pub priority: transport::Priority,
}

/// Runs the provided [`rpc::Server`] using QUIC protocol.
pub fn run(rpc_server: impl rpc::Server, cfg: Config) -> Result<impl Future<Output = ()>, Error> {
    multiplex((rpc_server,), cfg)
}

/// Runs multiple [`rpc::Server`]s on top of a single QUIC server.
///
/// `rpc_servers` argument is expected to be a tuple of [`rpc::Server`] impls.
pub fn multiplex<S>(rpc_servers: S, cfg: Config) -> Result<impl Future<Output = ()>, Error>
where
    S: Send + Sync + 'static,
    Server<S>: Multiplexer,
{
    // let filter = Filter::new(&cfg)?;
    let server = Server::new(rpc_servers, cfg)?;
    Ok(server.serve())
}

/// QUIC server.
#[derive(Clone, Debug, Deref)]
pub struct Server<S>(#[deref] Arc<ServerInner<S>>);

#[derive(Debug)]
pub struct ServerInner<S> {
    name: &'static str,
    endpoint: quinn::Endpoint,
    rpc_servers: S,
    stream_semaphore: Arc<Semaphore>,
}

impl<S> Server<S>
where
    S: Send + Sync + 'static,
    Self: Multiplexer,
{
    pub fn new(rpc_servers: S, cfg: Config) -> Result<Self, quic::Error> {
        let transport_config = super::new_quinn_transport_config(cfg.max_streams);
        let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair)
            .map_err(|err| Error::Tls(err.to_string()))?;
        let server_tls_config = QuicServerConfig::try_from(server_tls_config)
            .map_err(|err| Error::Tls(err.to_string()))?;
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
            cfg.priority,
        )?;

        Ok(Self(Arc::new(ServerInner {
            name: cfg.name,
            endpoint,
            rpc_servers,
            stream_semaphore: Arc::new(Semaphore::new(cfg.max_streams as usize)),
        })))
    }

    async fn serve(self) {
        while let Some(incoming) = self.endpoint.accept().await {
            // match filter.try_acquire_permit(&incoming) {
            //     Ok(permit) =>
            match incoming.accept() {
                Ok(connecting) => self.accept_connection(connecting),

                Err(err) => tracing::warn!(?err, "failed to accept incoming connection"),
            }

            //     Err(err) => {
            //         if err == RejectionReason::AddressNotValidated {
            //             // Signal the client to retry with validated address.
            //             let _ = incoming.retry();
            //         } else {
            //             tracing::debug!(
            //                 server_name = self.name,
            //                 reason = err.as_str(),
            //                 remote_addr = ?incoming.remote_address().ip(),
            //                 "inbound connection dropped"
            //             );

            //             metrics::counter!(
            //                 "wcn_rpc_quic_server_connections_dropped",
            //                 EnumLabel<"reason", RejectionReason> => err,
            //                 StringLabel<"server_name"> => self.name
            //             )
            //             .increment(1);

            //             // Calling `ignore()` instead of dropping avoids
            // sending a response.             incoming.ignore();
            //         }
            //     }
            // };
        }
    }

    fn accept_connection(&self, connecting: quinn::Connecting) {
        let this = self.clone();

        async move {
            let conn = connecting
                .with_timeout(Duration::from_millis(1000))
                .await
                .map_err(|_| ConnectionError::Timeout)??;

            let header = read_connection_header(&conn)
                .with_timeout(Duration::from_millis(500))
                .await
                .map_err(|_| ConnectionError::ReadHeaderTimeout)??;

            // let _permit = permit;

            this.route_connection(header.server_name, conn).await
        }
        .map_err(|err| tracing::warn!(?err, "Inbound connection handler failed"))
        .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_connection"))
        .pipe(tokio::spawn);
    }

    async fn handle_connection<R: rpc::Server>(
        &self,
        conn: quinn::Connection,
        rpc_server: &R,
    ) -> Result<(), ConnectionError> {
        use ConnectionError as Error;

        let cfg = rpc_server.config();
        let server_name = cfg.name.as_str();

        let peer_id = quic::connection_peer_id(&conn)?;

        // Unwrap is safe here, addr doesn't contain another `PeerId` as we've just
        // built it from `SocketAddr`.
        let remote_address = quic::socketaddr_to_multiaddr(conn.remote_address())
            .with_p2p(peer_id)
            .unwrap();

        let conn_info = ConnectionInfo {
            peer_id,
            remote_address,
            handshake_data: cfg
                .handshake
                .handle(peer_id, PendingConnection(conn.clone()))
                .with_timeout(Duration::from_millis(1000))
                .await
                .map_err(|_| {
                    metrics::counter!("wcn_rpc_quic_server_handshake_timeout", StringLabel<"server_name"> => server_name)
                        .increment(1);

                    Error::Timeout
                })?
                .map_err(|err| Error::Handshake(err.to_string()))?,
            storage: Default::default()
        };

        loop {
            let (tx, mut rx) = conn.accept_bi().await?;

            let Some(stream_permit) = self.acquire_stream_permit() else {
                static THROTTLED_RESULT: &crate::Result<()> = &Err(crate::Error::THROTTLED);

                let (_, mut tx) =
                    BiDirectionalStream::new(tx, rx).upgrade::<(), crate::Result<()>, R::Codec>();

                // The send buffer is large enough to write the whole response.
                tx.send(THROTTLED_RESULT).now_or_never();

                continue;
            };

            let rpc_server = rpc_server.clone();
            let conn_info = conn_info.clone();

            async move {
                let _permit = stream_permit;

                let rpc_id = match read_rpc_id(&mut rx).await {
                    Ok(id) => id,
                    Err(err) => return tracing::warn!(%err, "Failed to read inbound RPC ID"),
                };

                let stream = BiDirectionalStream::new(tx, rx);
                rpc_server.handle_rpc(rpc_id, stream, &conn_info).await
            }
            .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_stream"))
            .pipe(tokio::spawn);
        }
    }

    fn acquire_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!("wcn_rpc_quic_server_available_stream_permits", StringLabel<"server_name"> => self.name)
            .set(self.stream_semaphore.available_permits() as f64);

        self.stream_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .tap_none(|| {
                metrics::counter!("wcn_rpc_quic_server_throttled_streams", StringLabel<"server_name"> => self.name)
                    .increment(1);
            })
    }
}

async fn read_connection_header(
    conn: &quinn::Connection,
) -> Result<ConnectionHeader, ConnectionError> {
    let mut rx = conn.accept_uni().await?;

    let protocol_version = rx.read_u32().await?;

    let server_name = match protocol_version {
        0 => None,
        super::PROTOCOL_VERSION => {
            let mut buf = [0; 16];
            rx.read_exact(&mut buf).await?;
            Some(ServerName(buf))
        }
        ver => return Err(ConnectionError::UnsupportedProtocolVersion(ver)),
    };

    Ok(ConnectionHeader { server_name })
}

async fn read_rpc_id(rx: &mut quinn::RecvStream) -> Result<crate::Id, String> {
    rx.read_u128()
        .with_timeout(Duration::from_millis(500))
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("{err:?}"))
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Quinn: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error(transparent)]
    ExtractPeerId(#[from] super::ExtractPeerIdError),

    #[error("Handshake failed: {0:?}")]
    Handshake(String),

    #[error("Failed to read RpcId: {0:?}")]
    ReadRpcId(#[from] io::Error),

    #[error("Timeout")]
    Timeout,

    #[error("Unsupported protocol version")]
    UnsupportedProtocolVersion(u32),

    #[error("Read Header timeout")]
    ReadHeaderTimeout,

    #[error("Failed to read ConnectionHeader: {0:?}")]
    ReadHeader(#[from] quinn::ReadExactError),

    #[error("Unknown Rpc server")]
    UnknownRpcServer,
}

pub trait Multiplexer: Clone + Sized {
    fn route_connection(
        &self,
        server_name: Option<ServerName>,
        conn: quinn::Connection,
    ) -> impl Future<Output = Result<(), ConnectionError>> + Send;
}

impl<A> Multiplexer for Server<(A,)>
where
    A: rpc::Server,
{
    fn route_connection(
        &self,
        server_name: Option<ServerName>,
        conn: quinn::Connection,
    ) -> impl Future<Output = Result<(), ConnectionError>> + Send {
        async move {
            let Some(server_name) = server_name else {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            };

            if self.rpc_servers.0.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            }

            Err(ConnectionError::UnknownRpcServer)
        }
    }
}

impl<A, B> Multiplexer for Server<(A, B)>
where
    A: rpc::Server,
    B: rpc::Server,
{
    fn route_connection(
        &self,
        server_name: Option<ServerName>,
        conn: quinn::Connection,
    ) -> impl Future<Output = Result<(), ConnectionError>> + Send {
        async move {
            let Some(server_name) = server_name else {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            };

            if self.rpc_servers.0.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            }

            if self.rpc_servers.1.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.1).await;
            }

            Err(ConnectionError::UnknownRpcServer)
        }
    }
}

impl<A, B, C> Multiplexer for Server<(A, B, C)>
where
    A: rpc::Server,
    B: rpc::Server,
    C: rpc::Server,
{
    fn route_connection(
        &self,
        server_name: Option<ServerName>,
        conn: quinn::Connection,
    ) -> impl Future<Output = Result<(), ConnectionError>> + Send {
        async move {
            let Some(server_name) = server_name else {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            };

            if self.rpc_servers.0.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.0).await;
            }

            if self.rpc_servers.1.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.1).await;
            }

            if self.rpc_servers.2.config().name == server_name {
                return self.handle_connection(conn, &self.rpc_servers.2).await;
            }

            Err(ConnectionError::UnknownRpcServer)
        }
    }
}
