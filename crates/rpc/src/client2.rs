use {
    crate::{
        self as rpc,
        quic::{self},
        transport::{self, BiDirectionalStream, Codec},
        Message,
        RpcImpl,
        RpcV2,
    },
    arc_swap::ArcSwap,
    derive_where::derive_where,
    futures::{FutureExt, Sink, SinkExt, Stream, StreamExt as _},
    libp2p::{identity, PeerId},
    std::{
        future::Future,
        io,
        marker::PhantomData,
        net::{SocketAddr, SocketAddrV4},
        sync::Arc,
        time::Duration,
    },
    tokio::io::AsyncWriteExt,
    wc::future::FutureExt as _,
};

/// Client-specific part of an RPC [`Api`](super::Api).
pub trait Api: super::Api {
    /// [`OutboundConnectionHandler`] of this [`Api`].
    type OutboundConnectionHandler: OutboundConnectionHandler;

    /// [`OutboundRpcHandler`] of this [`Api`].
    type OutboundRpcHandler: Clone + Send + Sync + 'static;
}

/// Handler of newly established [`OutboundConnection`]s.
///
/// Every time a new [`OutboundConnection`] gets established it's being passed
/// into an [`OutboundConnectionHandler`].
pub trait OutboundConnectionHandler: Clone + Send + Sync + 'static {
    /// [`Api`] that uses this [`OutboundConnectionHandler`].
    type Api: Api;

    /// Error of this [`OutboundConnectionHandler`].
    type Error;

    /// Handles the provided [`OutboundConnection`].
    fn handle_connection(
        &self,
        conn: &mut OutboundConnection<Self::Api>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Handler of a specific type of [`Outbound`] RPCs.
pub trait OutboundRpcHandler<RPC, Args>: Clone + Send + Sync + 'static
where
    RPC: RpcV2,
{
    /// [`Result`] of this [`OutboundRpcHandler`].
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

/// RPC client responsible for establishing [`OutboundConnection`]s to remote
/// peers.
#[derive(Clone)]
pub struct Client<API: Api> {
    config: Arc<Config>,
    endpoint: quinn::Endpoint,
    connection_handler: API::OutboundConnectionHandler,
    rpc_handler: API::OutboundRpcHandler,
}

impl<API: Api> Client<API> {
    /// Creates a new RPC [`Client`].
    pub fn new(
        connection_handler: API::OutboundConnectionHandler,
        rpc_handler: API::OutboundRpcHandler,
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
        .map_err(|err| Error::Transport(err.to_string()))?;

        Ok(Client {
            config: Arc::new(cfg),
            endpoint,
            connection_handler,
            rpc_handler,
        })
    }

    /// Establishes a new [`OutboundConnection`].
    pub fn connect(&self, addr: SocketAddrV4, peer_id: PeerId) -> OutboundConnection<API> {
        let conn = OutboundConnection {
            inner: Arc::new(OutboundConnectionInner {
                client: self.clone(),
                remote_addr: addr,
                remote_peer_id: peer_id,
                quinn: Arc::new(ArcSwap::new(Arc::new(None))),
                mutex: Arc::new(tokio::sync::Mutex::new(())),
            }),
        };

        conn.reconnect();
        conn
    }
}

/// Error of [`Client::new`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Transport: {_0}")]
    Transport(String),
}

/// Outboud RPC of a specific type.
pub struct Outbound<RPC: RpcV2> {
    send: transport::SendStream<RPC::Request, RPC::Codec>,
    recv: transport::RecvStream<RPC::Response, RPC::Codec>,
}

impl<RPC: RpcV2> Outbound<RPC> {
    /// Returns [`Sink`] of outbound requests.
    pub fn sink(&mut self) -> &mut impl Sink<RPC::Request, Error = transport::Error> {
        &mut self.send
    }

    /// Returns [`Stream`] of inbound responses.
    pub fn stream(&mut self) -> &mut impl Stream<Item = transport::Result<RPC::Response>> {
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
pub struct OutboundConnection<API: Api> {
    inner: Arc<OutboundConnectionInner<API>>,
}

struct OutboundConnectionInner<API: Api> {
    client: Client<API>,

    remote_addr: SocketAddrV4,
    remote_peer_id: PeerId,

    quinn: Arc<ArcSwap<Option<quinn::Connection>>>,

    mutex: Arc<tokio::sync::Mutex<()>>,
}

impl<API: Api> OutboundConnection<API> {
    /// Returns [`SocketAddrV4`] of the remote peer.
    pub fn peer_addr(&self) -> &SocketAddrV4 {
        &self.inner.remote_addr
    }

    /// Returns [`PeerId`] of the remote peer.
    pub fn peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Indicates whether this [`OutboundConnection`] is currently open.
    pub fn is_open(&self) -> bool {
        (**self.inner.quinn.load())
            .as_ref()
            .map(|conn| conn.close_reason().is_some())
            .unwrap_or_default()
    }

    /// Sends a new [`Outbound`] RPC over this [`OutboundConnection`].
    pub fn send<RPC: RpcV2>(&self) -> SendRpcResult<Outbound<RPC>> {
        let inner = self.inner.quinn.load();
        let Some(conn) = inner.as_ref() else {
            return Err(SendRpcError::ConnectionClosed);
        };

        // `open_bi` only blocks if there are too many outbound streams.
        let (mut tx, rx) = match conn.open_bi().now_or_never() {
            Some(Ok(stream)) => stream,
            Some(Err(_)) => return Err(SendRpcError::ConnectionClosed),
            None => return Err(SendRpcError::TooManyConcurrentRpcs),
        };

        // Stream buffer is large enough to fit `u8` without blocking.
        tx.write_u8(RPC::ID)
            .now_or_never()
            .unwrap()
            .map_err(|_| SendRpcError::ConnectionClosed)?;

        let (recv, send) =
            BiDirectionalStream::new(tx, rx).upgrade::<RPC::Response, RPC::Request, RPC::Codec>();

        Ok(Outbound { send, recv })
    }

    fn reconnect(&self) {
        // If we can't acquire the lock then reconnection is already in progress.
        let Ok(mutex_guard) = self.inner.mutex.clone().try_lock_owned() else {
            return;
        };

        let this = self.inner.clone();

        tokio::spawn(async move {
            let _mutex_guard = mutex_guard;

            let mut interval = tokio::time::interval(this.client.config.reconnect_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match this.clone().new_quic_connection().await {
                    Ok(conn) => {
                        this.quinn.store(Arc::new(Some(conn)));
                        return;
                    }
                    Err(err @ ConnectionError::WrongPeerId(_)) => {
                        tracing::warn!("outbound connection failed: {err}")
                    }
                    Err(err) => {
                        tracing::debug!("outbound connection failed: {err}")
                    }
                }
            }
        });
    }
}

impl<API: Api> OutboundConnectionInner<API> {
    async fn new_quic_connection(self: Arc<Self>) -> Result<quinn::Connection, ConnectionError> {
        let timeout = self.client.config.connection_timeout;

        async move {
            // `libp2p_tls` uses this "l" placeholder as server_name.
            let conn = self
                .client
                .endpoint
                .connect(self.remote_addr.into(), "l")?
                .await?;

            let peer_id = quic::connection_peer_id(&conn)
                .map_err(|err| ConnectionError::ExtractPeerId(err.to_string()))?;

            if peer_id != self.remote_peer_id {
                return Err(ConnectionError::WrongPeerId(peer_id));
            }

            // write connection header
            let mut tx = conn.open_uni().await?;
            tx.write_u32(super::PROTOCOL_VERSION).await?;
            tx.write_all(&API::NAME.0).await?;

            Ok(conn)
        }
        .with_timeout(timeout)
        .await
        .map_err(|_| ConnectionError::Timeout)?
    }
}

/// Error of sending an [`Outbound`] RPC.
#[derive(Debug, thiserror::Error)]
pub enum SendRpcError {
    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Too many concurrent RPCs")]
    TooManyConcurrentRpcs,
}

/// Result of sending an [`Outbound`] RPC.
pub type SendRpcResult<T> = Result<T, SendRpcError>;

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
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

    #[error("Timeout")]
    Timeout,
}

impl<const ID: u8, K, API, Req, Resp, C> RpcImpl<ID, K, API, Req, Resp, C>
where
    K: Send + Sync + 'static,
    API: Api,
    Req: Message,
    Resp: Message,
    C: Codec,
{
    /// Sends this RPC over the provided [`OutboundConnection`].
    pub fn send<Args, Res>(
        conn: &OutboundConnection<API>,
        args: Args,
    ) -> SendRpcResult<impl Future<Output = Res> + '_>
    where
        API::OutboundRpcHandler: OutboundRpcHandler<Self, Args, Result = Res>,
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
    pub async fn handle(&mut self, req: Req) -> transport::Result<Resp> {
        self.sink().send(req).await?;
        self.stream()
            .next()
            .await
            .ok_or_else(|| transport::Error::StreamFinished)?
    }
}

/// Default implementation of [`OutboundRpcHandler`].
#[derive_where(Clone, Debug, Default; S)]
pub struct DefaultOutboundRpcHandler<Err, S = ()> {
    state: S,
    _marker: PhantomData<Err>,
}

impl<Err, S> DefaultOutboundRpcHandler<Err, S> {
    /// Creates a new [`DefaultOutboundRpcHandler`].
    pub fn new(state: S) -> Self {
        Self {
            state,
            _marker: PhantomData,
        }
    }
}

impl<const ID: u8, API, Req, Resp, C, Err, S>
    OutboundRpcHandler<rpc::UnaryV2<ID, API, Req, Resp, C>, Req>
    for DefaultOutboundRpcHandler<Err, S>
where
    API: Api,
    Req: Message,
    Resp: Message,
    C: Codec,
    Err: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    transport::Error: Into<Err>,
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
