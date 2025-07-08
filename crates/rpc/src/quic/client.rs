use {
    super::{ConnectionHeader, ExtractPeerIdError, InvalidMultiaddrError, PROTOCOL_VERSION},
    crate::{
        client::{self, AnyPeer, Config, Result},
        transport::{self, BiDirectionalStream, Handshake, NoHandshake, PendingConnection},
        Id as RpcId,
        PeerAddr,
        ServerName,
    },
    backoff::ExponentialBackoffBuilder,
    derivative::Derivative,
    derive_more::From,
    futures::{
        future::{BoxFuture, Shared},
        FutureExt as _,
        TryFutureExt as _,
    },
    indexmap::IndexMap,
    libp2p::{Multiaddr, PeerId},
    std::{
        future::Future,
        io,
        net::SocketAddr,
        sync::{Arc, PoisonError},
        time::Duration,
    },
    tokio::{io::AsyncWriteExt as _, sync::RwLock},
    wc::{
        future::FutureExt as _,
        metrics::{self, StringLabel},
    },
};

/// QUIC RPC client.
#[derive(Clone, Debug)]
pub struct Client<H = NoHandshake> {
    peer_id: PeerId,
    endpoint: quinn::Endpoint,
    handshake: H,

    server_name: ServerName,

    connection_handlers: Arc<RwLock<Arc<OutboundConnectionHandlers<H>>>>,
    connection_timeout: Duration,
}

impl<H> client::Marker for Client<H> {}

impl<H: Handshake> crate::Client for Client<H> {
    fn server_name(&self) -> &ServerName {
        &self.server_name
    }

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok>(
        &'a self,
        peer: &'a PeerAddr,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        self.establish_stream(peer, rpc_id)
            .map_err(Into::into)
            .and_then(move |stream| f(stream).map_err(Into::into))
    }
}

impl<H: Handshake> crate::Client<AnyPeer> for Client<H> {
    fn server_name(&self) -> &ServerName {
        &self.server_name
    }

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok>(
        &'a self,
        _: &'a AnyPeer,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        self.establish_stream_any(rpc_id)
            .map_err(Into::into)
            .and_then(move |stream| f(stream).map_err(Into::into))
    }
}

impl From<EstablishStreamError> for client::Error {
    fn from(err: EstablishStreamError) -> Self {
        Self::Transport(transport::Error::Other(format!(
            "Connection handler: {err:?}"
        )))
    }
}

type OutboundConnectionHandlers<H> = IndexMap<Multiaddr, ConnectionHandler<H>>;

impl<H: Handshake> Client<H> {
    /// Builds a new [`Client`] using the provided [`Config`].
    pub fn new(cfg: Config<H>) -> Result<Client<H>, super::Error> {
        let transport_config = super::new_quinn_transport_config(64u32 * 1024);
        let socket_addr = SocketAddr::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0);
        let endpoint = super::new_quinn_endpoint(
            socket_addr,
            &cfg.keypair,
            transport_config,
            None,
            cfg.priority,
        )?;

        let local_peer_id = cfg.keypair.public().to_peer_id();

        let handlers = cfg
            .known_peers
            .into_iter()
            .map(|peer| {
                super::multiaddr_to_socketaddr(&peer.addr).map(|socketaddr| {
                    let handler = ConnectionHandler::new(
                        peer.id,
                        socketaddr,
                        cfg.server_name,
                        endpoint.clone(),
                        cfg.handshake.clone(),
                        cfg.connection_timeout,
                    );

                    (peer.addr, handler)
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Client {
            peer_id: local_peer_id,
            endpoint,
            handshake: cfg.handshake,
            server_name: cfg.server_name,
            connection_handlers: Arc::new(RwLock::new(Arc::new(handlers))),
            connection_timeout: cfg.connection_timeout,
        })
    }

    /// [`PeerId`] of this [`Client`].
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

#[derive(Clone, Debug)]
pub(super) struct ConnectionHandler<H> {
    inner: Arc<std::sync::RwLock<ConnectionHandlerInner>>,
    handshake: H,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ConnectionHandlerInner {
    peer_id: PeerId,
    addr: SocketAddr,
    server_name: ServerName,
    endpoint: quinn::Endpoint,

    #[derivative(Debug = "ignore")]
    connection: Connection,
    connection_timeout: Duration,
}

type Connection = Shared<BoxFuture<'static, quinn::Connection>>;

// TODO: This should return a result, as one possible error here is invalid peer
// ID. Currently, if the peer ID is invalid, the connection will be retried.
fn new_connection<H: Handshake>(
    peer_id: PeerId,
    addr: SocketAddr,
    server_name: ServerName,
    endpoint: quinn::Endpoint,
    handshake: H,
    timeout: Duration,
) -> Connection {
    // We want to reconnect as fast as possible, otherwise it may lead to a lot of
    // lost requests, especially on cluster startup.
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_interval(Duration::from_millis(100))
        .with_max_elapsed_time(None)
        .build();

    let connect = move || {
        let endpoint = endpoint.clone();
        let handshake = handshake.clone();

        async move {
            // `libp2p_tls` uses this "l" placeholder as server_name.
            let conn = endpoint
                .connect(addr, "l")?
                .with_timeout(timeout)
                .await
                .map_err(|_| ConnectionError::Timeout)??;

            let actual_peer_id = super::connection_peer_id(&conn)?;

            if actual_peer_id != peer_id {
                return Err(ConnectionError::Handshake(format!(
                    "Invalid peer ID: expected: {peer_id} actual: {actual_peer_id}"
                )));
            }

            write_connection_header(&conn, ConnectionHeader {
                server_name: Some(server_name),
            })
            .await?;

            handshake
                .handle(peer_id, PendingConnection(conn.clone()))
                .await
                .map_err(|e| ConnectionError::Handshake(format!("{e:?}")))?;

            Ok(conn)
        }
        .map_err(backoff::Error::transient)
    };

    backoff::future::retry_notify(backoff, connect, move |err: ConnectionError, _| {
        tracing::debug!(?err, "failed to connect");
        metrics::counter!(
            "wcn_network_connection_failures",
            StringLabel<"kind"> => err.as_metrics_label(),
            StringLabel<"addr", SocketAddr> => &addr
        )
        .increment(1);
    })
    .map(move |res| {
        tracing::info!(%addr, "connection established");
        // we explicitly set `max_elapsed_time` to `None`
        res.unwrap()
    })
    .boxed()
    .shared()
}

impl<H: Handshake> ConnectionHandler<H> {
    pub(super) fn new(
        peer_id: PeerId,
        addr: SocketAddr,
        server_name: ServerName,
        endpoint: quinn::Endpoint,
        handshake: H,
        connection_timeout: Duration,
    ) -> Self {
        let inner = ConnectionHandlerInner {
            peer_id,
            addr,
            server_name,
            endpoint: endpoint.clone(),
            connection: new_connection(
                peer_id,
                addr,
                server_name,
                endpoint,
                handshake.clone(),
                connection_timeout,
            ),
            connection_timeout,
        };

        Self {
            inner: Arc::new(std::sync::RwLock::new(inner)),
            handshake,
        }
    }

    async fn establish_stream(
        &self,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream, EstablishStreamError> {
        let fut = self.inner.write()?.connection.clone();
        let conn = fut.await;

        let (mut tx, rx) = match conn.open_bi().await {
            Ok(bi) => bi,
            Err(_) => self
                .reconnect(conn.stable_id())?
                .await
                .open_bi()
                .await
                .map_err(|err| EstablishStreamError::Connection(err.into()))?,
        };

        tx.write_u128(rpc_id)
            .await
            .map_err(ConnectionError::WriteRpcId)?;

        Ok(BiDirectionalStream::new(tx, rx))
    }

    async fn try_establish_stream(&self, rpc_id: RpcId) -> Option<BiDirectionalStream> {
        let conn = self
            .inner
            .try_read()
            .ok()?
            .connection
            .clone()
            .now_or_never()?;

        let (mut tx, rx) = match conn.open_bi().await {
            Ok(bi) => bi,
            Err(_) => {
                // we don't need to await this future, it's shared
                drop(self.reconnect(conn.stable_id()));
                return None;
            }
        };

        tx.write_u128(rpc_id).await.map_err(|e| e.kind()).ok()?;

        Some(BiDirectionalStream::new(tx, rx))
    }

    /// Replaces the current connection with a new one.
    ///
    /// No-op if the current connection id doesn't match the provided one,
    /// meaning the connection was already replaced.
    fn reconnect(&self, prev_connection_id: usize) -> Result<Connection, EstablishStreamError> {
        let mut this = self.inner.write()?;

        if this
            .connection
            .peek()
            .filter(|conn| conn.stable_id() == prev_connection_id)
            .is_some()
        {
            metrics::counter!("wcn_network_reconnects").increment(1);
            this.connection = new_connection(
                this.peer_id,
                this.addr,
                this.server_name,
                this.endpoint.clone(),
                self.handshake.clone(),
                this.connection_timeout,
            );
        };

        Ok(this.connection.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),

    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),

    #[error(transparent)]
    ExtractPeerId(#[from] ExtractPeerIdError),

    #[error("Failed to write protocol version: {0}")]
    WriteProtocolVersion(io::Error),

    #[error("Failed to write server name: {0}")]
    WriteServerName(quinn::WriteError),

    #[error("Failed to write RpcId: {0}")]
    WriteRpcId(io::Error),

    #[error("Handshake error: {0}")]
    Handshake(String),

    #[error("Timeout establishing outbound connection")]
    Timeout,
}

impl ConnectionError {
    fn as_metrics_label(&self) -> &'static str {
        match self {
            Self::Connect(_) => "quinn_connect",
            Self::Connection(_) => "quinn_connection",
            Self::ExtractPeerId(_) => "extract_peer_id",
            Self::WriteProtocolVersion(_) => "write_protocol_version",
            Self::WriteServerName(_) => "write_server_name",
            Self::WriteRpcId(_) => "write_rpc_id",
            Self::Handshake(_) => "handshake",
            Self::Timeout => "timeout",
        }
    }
}

#[derive(Debug, From, thiserror::Error)]
pub enum EstablishStreamError {
    #[error("Invalid Multiaddr")]
    InvalidMultiaddr(InvalidMultiaddrError),

    #[error("There's no healthy connections to any peer at the moment")]
    NoAvailablePeers,

    #[error("Failed to establish outbound connection: {0}")]
    Connection(#[from(forward)] ConnectionError),

    #[error("Timeout establishing stream")]
    Timeout,

    #[error("RNG failed")]
    Rng,

    #[error("Poisoned lock")]
    Lock,
}

impl<G> From<PoisonError<G>> for EstablishStreamError {
    fn from(_: PoisonError<G>) -> Self {
        Self::Lock
    }
}

impl<H: Handshake> Client<H> {
    /// Establishes a [`BiDirectionalStream`] with the requested remote peer.
    pub async fn establish_stream(
        &self,
        peer: &PeerAddr,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream, EstablishStreamError> {
        let handlers = self.connection_handlers.read().await;
        let handler = if let Some(handler) = handlers.get(&peer.addr) {
            handler.clone()
        } else {
            let handler = ConnectionHandler::new(
                peer.id,
                super::multiaddr_to_socketaddr(&peer.addr)?,
                self.server_name,
                self.endpoint.clone(),
                self.handshake.clone(),
                self.connection_timeout,
            );

            drop(handlers);
            let mut handlers = self.connection_handlers.write().await;
            if let Some(handler) = handlers.get(&peer.addr) {
                handler.clone()
            } else {
                // ad-hoc "copy-on-write" behaviour, the map changes infrequently and we don't
                // want to clone it in the hot path.
                let mut new_handlers = (**handlers).clone();
                new_handlers.insert(peer.addr.clone(), handler.clone());
                *handlers = Arc::new(new_handlers);
                handler
            }
        };

        handler
            .establish_stream(rpc_id)
            .with_timeout(Duration::from_secs(5))
            .await
            .map_err(|_| EstablishStreamError::Timeout)?
    }

    /// Establishes a [`BiDirectionalStream`] with one of the remote peers.
    ///
    /// Tries to spread the load equally and to minimize the latency by skipping
    /// broken connections early.
    pub async fn establish_stream_any(
        &self,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream, EstablishStreamError> {
        use rand::{Rng, SeedableRng};

        let handlers = self.connection_handlers.read().await.clone();
        let len = handlers.len();
        let mut rng = rand::rngs::SmallRng::from_rng(&mut rand::thread_rng())
            .map_err(|_| EstablishStreamError::Rng)?;
        let mut n: usize = rng.gen();

        // fast run, skipping broken connections
        for _ in 0..len {
            let idx = n % len;
            if let Some(stream) = handlers[idx].try_establish_stream(rpc_id).await {
                return Ok(stream);
            }
            n += 1;
        }

        // slow run, waiting for reconnects
        for _ in 0..len {
            let idx = n % len;
            if let Ok(stream) = handlers[idx].establish_stream(rpc_id).await {
                return Ok(stream);
            }
            n += 1;
        }

        Err(EstablishStreamError::NoAvailablePeers)
    }
}

async fn write_connection_header(
    conn: &quinn::Connection,
    header: ConnectionHeader,
) -> Result<(), ConnectionError> {
    let mut tx = conn.open_uni().await?;
    tx.write_u32(PROTOCOL_VERSION)
        .await
        .map_err(ConnectionError::WriteProtocolVersion)?;

    if let Some(server_name) = &header.server_name {
        tx.write_all(&server_name.0)
            .await
            .map_err(ConnectionError::WriteServerName)?;
    }

    Ok(())
}
