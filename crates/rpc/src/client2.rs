use {
    crate::{
        self as rpc,
        quic::{self},
        transport::{self, BiDirectionalStream, Codec, RecvStream, SendStream},
        Message,
        RpcImpl,
        RpcV2,
    },
    derive_where::derive_where,
    futures::{
        sink::SinkMapErr,
        stream::MapErr,
        FutureExt,
        Sink,
        SinkExt,
        Stream,
        StreamExt as _,
        TryStreamExt,
    },
    libp2p::{identity, PeerId},
    std::{
        future::Future,
        io,
        marker::PhantomData,
        net::{SocketAddr, SocketAddrV4},
        sync::Arc,
        time::Duration,
    },
    tap::TapFallible,
    tokio::{
        io::AsyncWriteExt,
        sync::{watch, Mutex},
    },
    wc::future::FutureExt as _,
};

/// Client-specific part of an RPC [`Api`](super::Api).
pub trait Api: super::Api {
    /// [`ConnectionHandler`] of this [`Api`].
    type ConnectionHandler: ConnectionHandler;

    /// [`RpcHandler`] of this [`Api`].
    type RpcHandler: Clone + Send + Sync + 'static;
}

/// Handler of newly established outbound [`Connection`]s.
///
/// Every time a new outbound [`Connection`] gets established it's being
/// passed into a [`ConnectionHandler`].
pub trait ConnectionHandler: Clone + Send + Sync + 'static {
    /// [`Api`] that uses this [`ConnectionHandler`].
    type Api: Api;

    /// Error of this [`ConnectionHandler`].
    type Error;

    /// Handles the provided outbound [`Connection`].
    fn handle_connection(
        &self,
        conn: &mut Connection<Self::Api>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Handler of a specific type of [`Outbound`] RPCs.
pub trait RpcHandler<RPC, Args>: Clone + Send + Sync + 'static
where
    RPC: RpcV2,
{
    /// [`Result`] of this [`RpcHandler`].
    type Result;

    /// Handles the provided [`Outbound`] RPC.
    fn handle_rpc(
        &self,
        rpc: &mut Outbound<RPC>,
        args: Args,
    ) -> impl Future<Output = Self::Result> + Send;
}

/// [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Connection timeout.
    pub connection_timeout: Duration,

    /// Highest allowed frequency of connection retries.
    pub reconnect_interval: Duration,

    /// Maximum number of concurrent RPCs.
    pub max_concurrent_rpcs: u32,

    /// [`transport::Priority`] of the client.
    pub priority: transport::Priority,
}

/// RPC client responsible for establishing outbound [`Connection`]s to remote
/// peers.
#[derive(Clone)]
pub struct Client<API: Api> {
    config: Arc<Config>,
    endpoint: quinn::Endpoint,
    connection_handler: API::ConnectionHandler,
    rpc_handler: API::RpcHandler,
}

impl<API: Api> Client<API> {
    /// Creates a new RPC [`Client`].
    pub fn new(
        connection_handler: API::ConnectionHandler,
        rpc_handler: API::RpcHandler,
        cfg: Config,
    ) -> Result<Self, Error> {
        let transport_config = quic::new_quinn_transport_config(cfg.max_concurrent_rpcs);
        let socket_addr = SocketAddr::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0);
        let endpoint = quic::new_quinn_endpoint(
            socket_addr,
            &cfg.keypair,
            transport_config,
            None,
            cfg.priority,
        )
        .map_err(Error::new)?;

        Ok(Client {
            config: Arc::new(cfg),
            endpoint,
            connection_handler,
            rpc_handler,
        })
    }

    /// Establishes a new outbound [`Connection`].
    pub fn connect(&self, addr: SocketAddrV4, peer_id: PeerId) -> Connection<API> {
        let (tx, rx) = watch::channel(None);

        let conn = Connection {
            inner: Arc::new(ConnectionInner {
                client: self.clone(),
                remote_addr: addr,
                remote_peer_id: peer_id,
                watch_rx: rx,
                watch_tx: Arc::new(tokio::sync::Mutex::new(tx)),
            }),
        };

        conn.reconnect();
        conn
    }
}

/// Outboud RPC of a specific type.
pub struct Outbound<RPC: RpcV2> {
    send: SinkMapErr<SendStream<RPC::Request, RPC::Codec>, fn(transport::Error) -> Error>,
    recv: MapErr<RecvStream<RPC::Response, RPC::Codec>, fn(transport::Error) -> Error>,
}

impl<RPC: RpcV2> Outbound<RPC> {
    /// Returns [`Sink`] of outbound requests.
    pub fn sink(&mut self) -> &mut impl Sink<RPC::Request, Error = Error> {
        &mut self.send
    }

    /// Returns [`Stream`] of inbound responses.
    pub fn stream(&mut self) -> &mut impl Stream<Item = Result<RPC::Response>> {
        &mut self.recv
    }
}

/// Outbound connection.
///
/// Existence of an instance of this type doesn't guarantee that the actual
/// network connection is already established (or will ever be established).
///
/// Reconnects are being handled automatically in the background.
#[derive(Clone)]
pub struct Connection<API: Api> {
    inner: Arc<ConnectionInner<API>>,
}

struct ConnectionInner<API: Api> {
    client: Client<API>,

    remote_addr: SocketAddrV4,
    remote_peer_id: PeerId,

    watch_rx: watch::Receiver<Option<quinn::Connection>>,
    watch_tx: Arc<Mutex<watch::Sender<Option<quinn::Connection>>>>,
}

impl<API: Api> Connection<API> {
    /// Returns [`SocketAddrV4`] of the remote peer.
    pub fn peer_addr(&self) -> &SocketAddrV4 {
        &self.inner.remote_addr
    }

    /// Returns [`PeerId`] of the remote peer.
    pub fn peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Indicates whether this [`Connection`] is currently open.
    pub fn is_open(&self) -> bool {
        self.inner
            .watch_rx
            .borrow()
            .as_ref()
            .map(|conn| conn.close_reason().is_some())
            .unwrap_or_default()
    }

    /// Waits for this [`Connection`] to become open.
    ///
    /// IMPORTANT: This future may never resolve! Make sure that you use a
    /// timeout.
    pub async fn wait_open(&self) {
        let mut watch_rx = self.inner.watch_rx.clone();
        match watch_rx.borrow_and_update().as_ref() {
            Some(conn) if conn.close_reason().is_none() => return,
            _ => {}
        }

        drop(watch_rx.changed().await)
    }

    /// Sends a new [`Outbound`] RPC over this [`Connection`].
    pub fn send<RPC: RpcV2>(&self) -> Result<Outbound<RPC>> {
        (|| {
            let opt = self.inner.watch_rx.borrow();
            let Some(conn) = opt.as_ref() else {
                return Err(ErrorInner::NotConnected);
            };

            // `open_bi` only blocks if there are too many outbound streams.
            let (mut tx, rx) = match conn.open_bi().now_or_never() {
                Some(Ok(stream)) => stream,
                Some(Err(err)) => return Err(err.into()),
                None => return Err(ErrorInner::TooManyConcurrentRpcs),
            };

            // This can only block if send buffer is full.
            tx.write_u8(RPC::ID)
                .now_or_never()
                .ok_or_else(|| ErrorInner::SendBufferFull)?;

            let (recv, send) = BiDirectionalStream::new(tx, rx)
                .upgrade::<RPC::Response, RPC::Request, RPC::Codec>();

            Ok(Outbound {
                send: SinkExt::<&RPC::Request>::sink_map_err(send, |err: transport::Error| {
                    Error::new(err)
                }),
                recv: recv.map_err(Error::new),
            })
        })()
        .map_err(Error::new)
        .tap_err(|err| {
            if err.is_connection_error() {
                self.reconnect();
            }
        })
    }

    fn reconnect(&self) {
        // If we can't acquire the lock then reconnection is already in progress.
        let Ok(watch_tx) = self.inner.watch_tx.clone().try_lock_owned() else {
            return;
        };

        let this = self.inner.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.client.config.reconnect_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if let Ok(conn) = this.clone().new_quic_connection().await {
                    watch_tx.send(Some(conn));
                    return;
                }
            }
        });
    }
}

impl<API: Api> ConnectionInner<API> {
    async fn new_quic_connection(self: Arc<Self>) -> Result<quinn::Connection> {
        let timeout = self.client.config.connection_timeout;

        async move {
            // `libp2p_tls` uses this "l" placeholder as server_name.
            let conn = self
                .client
                .endpoint
                .connect(self.remote_addr.into(), "l")?
                .await?;

            let peer_id = quic::connection_peer_id(&conn)
                .map_err(|err| ErrorInner::ExtractPeerId(err.to_string()))?;

            if peer_id != self.remote_peer_id {
                tracing::warn!(
                    expected = ?self.remote_peer_id,
                    got = ?peer_id,
                    addr = ?self.remote_addr,
                    "Wrong PeerId"
                );

                return Err(ErrorInner::WrongPeerId(peer_id));
            }

            // write connection header
            let mut tx = conn.open_uni().await?;
            tx.write_u32(super::PROTOCOL_VERSION).await?;
            tx.write_all(&API::NAME.0).await?;

            Ok(conn)
        }
        .with_timeout(timeout)
        .await
        .map_err(|_| ErrorInner::Timeout)?
        .map_err(Error::new)
    }
}

impl<const ID: u8, K, API, Req, Resp, C> RpcImpl<ID, K, API, Req, Resp, C>
where
    K: Send + Sync + 'static,
    API: Api,
    Req: Message,
    Resp: Message,
    C: Codec,
{
    /// Sends this RPC over the provided [`Connection`].
    pub fn send<Args, Res>(
        conn: &Connection<API>,
        args: Args,
    ) -> Result<impl Future<Output = Res> + '_>
    where
        API::RpcHandler: RpcHandler<Self, Args, Result = Res>,
        Args: 'static,
    {
        let mut rpc = conn.send::<Self>()?;
        let handler = &conn.inner.client.rpc_handler;
        Ok(async move { handler.handle_rpc(&mut rpc, args).await })
    }
}

impl<const ID: u8, API, Req, Resp, C> Outbound<rpc::UnaryV2<ID, API, Req, Resp, C>>
where
    API: Api,
    Req: Message,
    Resp: Message,
    C: Codec,
{
    /// Handles this [`Outbound`] [`rpc::UnaryV2`].
    pub async fn handle(&mut self, req: Req) -> Result<Resp> {
        self.sink().send(req).await?;
        self.stream()
            .next()
            .await
            .ok_or_else(|| Error::new(transport::Error::StreamFinished))?
    }
}

/// Default implementation of [`RpcHandler`].
#[derive_where(Clone, Debug, Default; S)]
pub struct DefaultRpcHandler<Err, S = ()> {
    state: S,
    _marker: PhantomData<Err>,
}

impl<Err, S> DefaultRpcHandler<Err, S> {
    /// Creates a new [`DefaultRpcHandler`].
    pub fn new(state: S) -> Self {
        Self {
            state,
            _marker: PhantomData,
        }
    }
}

impl<const ID: u8, API, Req, Resp, C, Err, S> RpcHandler<rpc::UnaryV2<ID, API, Req, Resp, C>, Req>
    for DefaultRpcHandler<Err, S>
where
    API: Api,
    Req: Message,
    Resp: Message,
    C: Codec,
    Err: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    Error: Into<Err>,
{
    type Result = Result<Resp, Err>;

    async fn handle_rpc(
        &self,
        rpc: &mut Outbound<rpc::UnaryV2<ID, API, Req, Resp, C>>,
        req: Req,
    ) -> Self::Result {
        rpc.handle(req).await.map_err(Into::into)
    }
}

/// RPC [`Client`] error.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(ErrorInner);

/// RPC [`Client`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    fn new(err: impl Into<ErrorInner>) -> Self {
        Self(err.into())
    }

    /// Indicates whether this [`Error`] is caused by the
    /// [`OutboundConnection`] being closed / broken.
    ///
    /// Such errors may be fixed by a reconnect. Consider
    /// (waiting)[OutboundConnection::wait_open] for the [`OutboundConnection`]
    /// to become open again.
    pub fn is_connection_error(&self) -> bool {
        match &self.0 {
            ErrorInner::NotConnected
            | ErrorInner::Quic(_)
            | ErrorInner::ExtractPeerId(_)
            | ErrorInner::WrongPeerId(_)
            | ErrorInner::Connect(_)
            | ErrorInner::Connection(_)
            | ErrorInner::Io(_)
            | ErrorInner::Write(_)
            | ErrorInner::Timeout
            | ErrorInner::Transport(_) => true,

            ErrorInner::TooManyConcurrentRpcs | ErrorInner::SendBufferFull => false,
        }
    }
}

impl From<ErrorInner> for Error {
    fn from(err: ErrorInner) -> Self {
        Self::new(err)
    }
}

#[derive(Debug, thiserror::Error)]
enum ErrorInner {
    #[error("Not connected")]
    NotConnected,

    #[error("QUIC: {0}")]
    Quic(#[from] quic::Error),

    #[error("Failed to extract PeerId")]
    ExtractPeerId(String),

    #[error("Wrong PeerId: {0}")]
    WrongPeerId(PeerId),

    #[error("Connect: {0:?}")]
    Connect(#[from] quinn::ConnectError),

    #[error("Connection: {0:?}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("IO: {0:?}")]
    Io(#[from] io::Error),

    #[error("Write: {0:?}")]
    Write(#[from] quinn::WriteError),

    #[error("Too many concurrent RPCs")]
    TooManyConcurrentRpcs,

    #[error("Send buffer is full")]
    SendBufferFull,

    #[error("Timeout")]
    Timeout,

    #[error("Transport: {0}")]
    Transport(#[from] transport::Error),
}
