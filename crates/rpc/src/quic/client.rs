use {
    super::InvalidMultiaddrError,
    crate::{
        client::{self, AnyPeer, Config, Result},
        transport::{self, BiDirectionalStream, Handshake, NoHandshake, PendingConnection},
        Id as RpcId,
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
    wc::future::FutureExt as _,
};

/// QUIC RPC client.
#[derive(Clone, Debug)]
pub struct Client<H = NoHandshake> {
    peer_id: PeerId,
    endpoint: quinn::Endpoint,
    handshake: H,

    connection_handlers: Arc<RwLock<Arc<OutboundConnectionHandlers<H>>>>,
    connection_timeout: Duration,
}

impl<H> client::Marker for Client<H> {}

impl<H: Handshake> crate::Client for Client<H> {
    fn send_rpc<Fut: Future<Output = Result<Ok>> + Send, Ok>(
        &self,
        addr: &Multiaddr,
        rpc_id: RpcId,
        f: impl FnOnce(BiDirectionalStream) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send {
        self.establish_stream(addr, rpc_id)
            .map_err(Into::into)
            .and_then(|stream| f(stream).map_err(Into::into))
    }
}

impl<H: Handshake> crate::Client<AnyPeer> for Client<H> {
    fn send_rpc<Fut: Future<Output = Result<Ok>> + Send, Ok>(
        &self,
        _: &AnyPeer,
        rpc_id: RpcId,
        f: impl FnOnce(BiDirectionalStream) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send {
        self.establish_stream_any(rpc_id)
            .map_err(Into::into)
            .and_then(|stream| f(stream).map_err(Into::into))
    }
}

impl From<ConnectionHandlerError> for client::Error {
    fn from(err: ConnectionHandlerError) -> Self {
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
        let endpoint =
            super::new_quinn_endpoint(socket_addr, &cfg.keypair, transport_config, None)?;

        let local_peer_id = cfg.keypair.public().to_peer_id();

        let handlers = cfg
            .known_peers
            .into_iter()
            .map(|multiaddr| {
                super::multiaddr_to_socketaddr(&multiaddr).map(|socketaddr| {
                    let handler = ConnectionHandler::new(
                        socketaddr,
                        endpoint.clone(),
                        cfg.handshake.clone(),
                        cfg.connection_timeout,
                    );
                    (multiaddr, handler)
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Client {
            peer_id: local_peer_id,
            endpoint,
            handshake: cfg.handshake,
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
    addr: SocketAddr,
    endpoint: quinn::Endpoint,

    #[derivative(Debug = "ignore")]
    connection: Connection,
    connection_timeout: Duration,
}

type Connection = Shared<BoxFuture<'static, quinn::Connection>>;

fn new_connection<H: Handshake>(
    addr: SocketAddr,
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
                .connect(addr, "l")
                .map_err(|e| e.to_string())?
                .with_timeout(timeout)
                .await
                .map_err(|_| "timeout".to_owned())?
                .map_err(|e| e.to_string())?;

            handshake
                .handle(PendingConnection(conn.clone()))
                .await
                .map_err(|e| format!("handshake error: {e:?}"))?;

            Ok(conn)
        }
        .map_err(backoff::Error::transient)
    };

    backoff::future::retry_notify(backoff, connect, |err: String, _| {
        tracing::debug!(?err, "failed to connect");
        metrics::counter!("irn_network_connection_failures").increment(1);
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
        addr: SocketAddr,
        endpoint: quinn::Endpoint,
        handshake: H,
        connection_timeout: Duration,
    ) -> Self {
        let inner = ConnectionHandlerInner {
            addr,
            endpoint: endpoint.clone(),
            connection: new_connection(addr, endpoint, handshake.clone(), connection_timeout),
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
    ) -> Result<BiDirectionalStream, ConnectionHandlerError> {
        let fut = self.inner.write()?.connection.clone();
        let conn = fut.await;

        let (mut tx, rx) = match conn.open_bi().await {
            Ok(bi) => bi,
            Err(_) => self
                .reconnect(conn.stable_id())?
                .await
                .open_bi()
                .await
                .map_err(|err| ConnectionHandlerError::Connection(err.into()))?,
        };

        tx.write_u128(rpc_id).await.map_err(|e| e.kind())?;

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
    fn reconnect(&self, prev_connection_id: usize) -> Result<Connection, ConnectionHandlerError> {
        let mut this = self.inner.write()?;

        if this
            .connection
            .peek()
            .filter(|conn| conn.stable_id() == prev_connection_id)
            .is_some()
        {
            metrics::counter!("irn_network_reconnects").increment(1);
            this.connection = new_connection(
                this.addr,
                this.endpoint.clone(),
                self.handshake.clone(),
                this.connection_timeout,
            );
        };

        Ok(this.connection.clone())
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum ConnectionError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),

    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
}

#[derive(Clone, Debug, From, thiserror::Error, Eq, PartialEq)]
pub enum ConnectionHandlerError {
    #[error("Invalid Multiaddr")]
    InvalidMultiaddr(InvalidMultiaddrError),

    #[error("There's no healthy connections to any peer at the moment")]
    NoAvailablePeers,

    #[error("Failed to establish outbound connection: {0}")]
    Connection(#[from(forward)] ConnectionError),

    #[error("Timeout establishing outbound connection")]
    ConnectionTimeout,

    #[error("Failed to write RpcId: {0:?}")]
    WriteRpcId(io::ErrorKind),

    #[error("RNG failed")]
    Rng,

    #[error("Poisoned lock")]
    Lock,
}

impl<G> From<PoisonError<G>> for ConnectionHandlerError {
    fn from(_: PoisonError<G>) -> Self {
        Self::Lock
    }
}

impl<H: Handshake> Client<H> {
    /// Establishes a [`BiDirectionalStream`] with the requested remote peer.
    pub async fn establish_stream(
        &self,
        multiaddr: &Multiaddr,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream, ConnectionHandlerError> {
        let handlers = self.connection_handlers.read().await;
        let handler = if let Some(handler) = handlers.get(multiaddr) {
            handler.clone()
        } else {
            let handler = ConnectionHandler::new(
                super::multiaddr_to_socketaddr(multiaddr)?,
                self.endpoint.clone(),
                self.handshake.clone(),
                self.connection_timeout,
            );

            drop(handlers);
            let mut handlers = self.connection_handlers.write().await;
            if let Some(handler) = handlers.get(multiaddr) {
                handler.clone()
            } else {
                // ad-hoc "copy-on-write" behaviour, the map changes infrequently and we don't
                // want to clone it in the hot path.
                let mut new_handlers = (**handlers).clone();
                new_handlers.insert(multiaddr.clone(), handler.clone());
                *handlers = Arc::new(new_handlers);
                handler
            }
        };

        handler
            .establish_stream(rpc_id)
            .with_timeout(Duration::from_secs(5))
            .await
            .map_err(|_| ConnectionHandlerError::ConnectionTimeout)?
            .map_err(Into::into)
    }

    /// Establishes a [`BiDirectionalStream`] with one of the remote peers.
    ///
    /// Tries to spread the load equally and to minimize the latency by skipping
    /// broken connections early.
    pub async fn establish_stream_any(
        &self,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream, ConnectionHandlerError> {
        use rand::{Rng, SeedableRng};

        let handlers = self.connection_handlers.read().await.clone();
        let len = handlers.len();
        let mut rng = rand::rngs::SmallRng::from_rng(&mut rand::thread_rng())
            .map_err(|_| ConnectionHandlerError::Rng)?;
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

        Err(ConnectionHandlerError::NoAvailablePeers)
    }
}
