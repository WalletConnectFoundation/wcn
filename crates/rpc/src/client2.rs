use {
    crate::{
        quic::{self},
        transport,
        transport2::{self, BiDirectionalStream, RecvStream, SendStream},
        BorrowedRequest,
        ConnectionStatusCode,
        RpcV2,
        UnaryRpc,
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
        net::{SocketAddr, SocketAddrV4},
        sync::{
            atomic::{self, AtomicUsize},
            Arc,
        },
        time::Duration,
    },
    strum::{EnumDiscriminants, IntoDiscriminant, IntoStaticStr},
    tap::TapFallible as _,
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt},
        sync::{watch, Mutex},
    },
    wc::{
        future::FutureExt as _,
        metrics::{self, enum_ordinalize::Ordinalize, EnumLabel, StringLabel},
    },
};

// TODO: metrics, timeouts

/// Client-specific part of an RPC [Api][`super::Api`].
pub trait Api: super::Api + Sized {
    /// Outbound [`Connection`] parameters.
    type ConnectionParameters: Clone + Send + Sync + 'static;

    /// Implementor of [`HandleConnection`] of this RPC [`Api`].
    type ConnectionHandler: HandleConnection<Self>;

    //// Implementor of [`HandleRpc`] for the RPCs of this RPC [`Api`].
    type RpcHandler: Send + Sync + 'static;
}

/// Handler of newly established outbound [`Connection`]s.
pub trait HandleConnection<API: Api>: Clone + Send + Sync + 'static {
    /// Creates a new instance of [`Api::RpcHandler`].
    ///
    /// Each outbound [`Connection`] gets a separate RPC handler.
    fn new_rpc_handler(&self) -> API::RpcHandler;

    /// Handles the provided outbound [`Connection`].
    fn handle_connection(
        &self,
        conn: &Connection<API>,
        params: &API::ConnectionParameters,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Handler of [`Outbound`] RPCs.
pub trait HandleRpc<RPC: RpcV2>: Send + Sync + 'static {
    type Input<'a>: Send + Sync + 'a;
    type Output;

    fn handle_rpc<'a>(
        &'a self,
        rpc: Outbound<RPC>,
        input: &'a Self::Input<'a>,
    ) -> impl Future<Output = Result<Self::Output>> + Send + 'a;
}

type RpcHandlerInput<'a, H, RPC> = <H as HandleRpc<RPC>>::Input<'a>;
type RpcHandlerOutput<H, RPC> = <H as HandleRpc<RPC>>::Output;

/// RPC [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Timeout of establishing an outbound connection.
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
#[derive_where(Clone)]
pub struct Client<API: Api> {
    config: Arc<Config>,
    endpoint: quinn::Endpoint,

    connection_handler: API::ConnectionHandler,
}

impl<API: Api> Client<API> {
    /// Creates a new RPC [`Client`].
    pub fn new(cfg: Config, connection_handler: API::ConnectionHandler) -> Result<Self, Error> {
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
        })
    }

    /// Establishes a new outbound [`Connection`].
    pub async fn connect(
        &self,
        addr: SocketAddrV4,
        peer_id: &PeerId,
        params: API::ConnectionParameters,
    ) -> Result<Connection<API>> {
        async {
            // `libp2p_tls` uses this "l" placeholder as server_name.
            let conn = self.endpoint.connect(addr.into(), "l")?.await?;

            let remote_peer_id = quic::connection_peer_id(&conn)?;

            if *peer_id != remote_peer_id {
                tracing::warn!(
                    expected = ?peer_id,
                    got = ?&remote_peer_id,
                    addr = ?addr,
                    "Wrong PeerId"
                );

                return Err(ErrorInner::WrongPeerId(remote_peer_id));
            }

            // handshake
            let (mut tx, mut rx) = conn.open_bi().await?;
            tx.write_u32(super::PROTOCOL_VERSION).await?;
            tx.write_all(&API::NAME.0).await?;
            check_connection_status(rx.read_i32().await?)?;

            let conn = self.new_connection_inner(addr, peer_id, params, Some(conn));

            // we just created the `Connection`, the lock can't be locked
            // NOTE: by holding this guard here we are also making sure that
            // `ConnectionHandler::handle_connection` won't get into infinite recursion by
            // trying to reconnect
            let guard = conn.inner.watch_tx.try_lock().unwrap();
            let params = &guard.1;

            self.connection_handler
                .handle_connection(&conn, params)
                .await
                .map_err(|err| err.0)?;

            drop(guard);

            tracing::info!(
                api = %API::NAME,
                addr = %conn.remote_peer_addr(),
                peer_id = %conn.remote_peer_id(),
                "Connection established"
            );

            Ok(conn)
        }
        .with_timeout(self.config.connection_timeout)
        .await
        .map_err(|_| ErrorInner::Timeout)?
        .map_err(Error::new)
    }

    /// Creates a new outbound [`Connection`] without waiting for it to be
    /// established.
    pub fn new_connection(
        &self,
        addr: SocketAddrV4,
        peer_id: &PeerId,
        params: API::ConnectionParameters,
    ) -> Connection<API> {
        let conn = self.new_connection_inner(addr, peer_id, params, None);
        conn.reconnect();
        conn
    }

    fn new_connection_inner(
        &self,
        addr: SocketAddrV4,
        peer_id: &PeerId,
        params: API::ConnectionParameters,
        quic: Option<quinn::Connection>,
    ) -> Connection<API> {
        let (tx, rx) = watch::channel(quic);

        Connection {
            inner: Arc::new(ConnectionInner {
                client: self.clone(),
                remote_addr: addr,
                remote_peer_id: *peer_id,
                watch_rx: rx,
                watch_tx: Arc::new(tokio::sync::Mutex::new((tx, params))),
                rpc_handler: self.connection_handler.new_rpc_handler(),
            }),
        }
    }
}

/// Default implementation of [`ConnectionHandler`].
///
/// No-op, doesn't do anything with the [`Connection`].
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionHandler;

impl<API> HandleConnection<API> for ConnectionHandler
where
    API: Api<ConnectionParameters = (), ConnectionHandler = Self, RpcHandler = RpcHandler>,
{
    fn new_rpc_handler(&self) -> <API as Api>::RpcHandler {
        RpcHandler
    }

    async fn handle_connection(&self, _conn: &Connection<API>, _params: &()) -> Result<()> {
        Ok(())
    }
}

/// Outbound RPC of a specific type.
pub struct Outbound<RPC: RpcV2> {
    #[allow(clippy::type_complexity)]
    send: SinkMapErr<SendStream<RPC::Codec>, fn(transport2::Error) -> Error>,

    #[allow(clippy::type_complexity)]
    recv: MapErr<RecvStream<RPC::Response, RPC::Codec>, fn(transport2::Error) -> Error>,
}

impl<RPC: RpcV2> Outbound<RPC> {
    /// Returns mutable references to the underlying request/response streams.
    pub fn streams_mut(
        &mut self,
    ) -> (
        &mut (impl for<'a, 'b> Sink<&'a BorrowedRequest<'b, RPC>, Error = Error> + 'static),
        &mut impl Stream<Item = Result<RPC::Response>>,
    ) {
        (&mut self.send, &mut self.recv)
    }
}

/// Outbound connection.
///
/// Existence of an instance of this type doesn't guarantee that the actual
/// network connection is already established (or will ever be established).
#[derive_where(Clone)]
pub struct Connection<API: Api> {
    inner: Arc<ConnectionInner<API>>,
}

type ConnectionMutex<Params> = Mutex<(watch::Sender<Option<quinn::Connection>>, Params)>;

struct ConnectionInner<API: Api> {
    client: Client<API>,

    remote_addr: SocketAddrV4,
    remote_peer_id: PeerId,

    watch_rx: watch::Receiver<Option<quinn::Connection>>,
    watch_tx: Arc<ConnectionMutex<API::ConnectionParameters>>,

    rpc_handler: API::RpcHandler,
}

impl<API: Api> Connection<API> {
    /// Returns [`SocketAddrV4`] of the remote peer.
    pub fn remote_peer_addr(&self) -> &SocketAddrV4 {
        &self.inner.remote_addr
    }

    /// Returns [`PeerId`] of the remote peer.
    pub fn remote_peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Indicates whether this [`Connection`] is closed.
    pub fn is_closed(&self) -> bool {
        self.inner
            .watch_rx
            .borrow()
            .as_ref()
            .map(|conn| conn.close_reason().is_some())
            .unwrap_or(true)
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

        self.reconnect();

        drop(watch_rx.changed().await)
    }

    /// Sends the provided RPC over this [`Connection`].
    pub fn send<'a, RPC: RpcV2>(
        &'a self,
        input: &'a RpcHandlerInput<'a, API::RpcHandler, RPC>,
    ) -> Result<impl Future<Output = Result<RpcHandlerOutput<API::RpcHandler, RPC>>> + Send + 'a>
    where
        API::RpcHandler: HandleRpc<RPC>,
    {
        let rpc = self.new_outbound_rpc::<RPC>().tap_err(|err| {
            if err.requires_reconnect() {
                self.reconnect();
            }
        })?;

        Ok(async move {
            self.inner
                .rpc_handler
                .handle_rpc(rpc, input)
                .await
                .tap_err(|err| {
                    if err.0.requires_reconnect() {
                        self.reconnect();
                    }
                })
        })
    }

    fn new_outbound_rpc<RPC: RpcV2>(&self) -> Result<Outbound<RPC>, ErrorInner> {
        let quic = self.inner.watch_rx.borrow();
        let Some(conn) = quic.as_ref() else {
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
            .ok_or_else(|| ErrorInner::SendBufferFull)??;

        let (recv, send) = BiDirectionalStream::new(tx, rx).upgrade::<RPC::Response, RPC::Codec>();

        Ok(Outbound {
            send: SinkExt::<&RPC::Request>::sink_map_err(send, |err: transport2::Error| {
                Error::new(err)
            }),
            recv: recv.map_err(Error::new),
        })
    }

    fn reconnect(&self) {
        // If we can't acquire the lock then reconnection is already in progress.
        let Ok(guard) = self.inner.watch_tx.clone().try_lock_owned() else {
            return;
        };

        let this = self.inner.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.client.config.reconnect_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let res = this
                    .client
                    .connect(this.remote_addr, &this.remote_peer_id, guard.1.clone())
                    .await;

                match res {
                    Ok(conn) => {
                        let quic = conn.inner.watch_rx.borrow();

                        // should always be `Some`, as we just established the connection
                        let _ = guard.0.send(Some(quic.as_ref().unwrap().clone()));
                        return;
                    }
                    Err(err) => {
                        metrics::counter!(
                            "wcn_rpc_client_connection_errors",
                            StringLabel<"remote_addr", SocketAddrV4> => &this.remote_addr,
                            StringLabel<"remote_peer_id", PeerId> => &this.remote_peer_id,
                            EnumLabel<"kind", ErrorKind> => err.0.discriminant()
                        )
                        .increment(1);
                    }
                }
            }
        });
    }
}

/// Default implementation of [`HandleRpc`].
///
/// Automatically implements [`HandleRpc`] for all [`UnaryRpc`]s.
///
/// You'll need to provide a manual implementation of [`HandleRpc`] for your
/// custom RPCs.
#[derive(Clone, Copy, Debug, Default)]
pub struct RpcHandler;

impl<RPC: UnaryRpc> HandleRpc<RPC> for RpcHandler {
    type Input<'a> = BorrowedRequest<'a, RPC>;
    type Output = RPC::Response;

    async fn handle_rpc<'a>(
        &'a self,
        mut rpc: Outbound<RPC>,
        req: &'a BorrowedRequest<'a, RPC>,
    ) -> Result<RPC::Response> {
        let (tx, rx) = rpc.streams_mut();
        tx.send(req).await?;
        rx.next()
            .await
            .ok_or_else(|| Error::new(transport2::Error::StreamFinished))?
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
}

impl From<ErrorInner> for Error {
    fn from(err: ErrorInner) -> Self {
        Self::new(err)
    }
}

#[derive(Debug, thiserror::Error, EnumDiscriminants)]
#[strum_discriminants(name(ErrorKind))]
#[strum_discriminants(derive(Ordinalize, IntoStaticStr))]
enum ErrorInner {
    #[error("Not connected")]
    NotConnected,

    #[error("QUIC: {0}")]
    Quic(#[from] quic::Error),

    #[error(transparent)]
    ExtractPeerId(#[from] quic::ExtractPeerIdError),

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
    Transport(#[from] transport2::Error),

    #[error("Unknown ConnectionStatusCode({0})")]
    UnknownConnectionStatusCode(i32),

    #[error("Unsupported protocol")]
    UnsupportedProtocol,

    #[error("Unknown API")]
    UnknownApi,

    #[error("Unauthorized")]
    Unauthorized,
}

impl ErrorInner {
    fn requires_reconnect(&self) -> bool {
        match self {
            ErrorInner::NotConnected
            | ErrorInner::Quic(_)
            | ErrorInner::ExtractPeerId(_)
            | ErrorInner::WrongPeerId(_)
            | ErrorInner::Connect(_)
            | ErrorInner::Connection(_)
            | ErrorInner::Io(_)
            | ErrorInner::Write(_)
            | ErrorInner::Timeout
            | ErrorInner::Transport(_)
            | ErrorInner::UnknownConnectionStatusCode(_)
            | ErrorInner::UnsupportedProtocol
            | ErrorInner::UnknownApi
            | ErrorInner::Unauthorized => true,

            ErrorInner::TooManyConcurrentRpcs | ErrorInner::SendBufferFull => false,
        }
    }
}

fn check_connection_status(code: i32) -> Result<(), ErrorInner> {
    let code = ConnectionStatusCode::try_from(code)
        .map_err(|err| ErrorInner::UnknownConnectionStatusCode(err.input))?;

    Err(match code {
        ConnectionStatusCode::Ok => return Ok(()),
        ConnectionStatusCode::UnsupportedProtocol => ErrorInner::UnsupportedProtocol,
        ConnectionStatusCode::UnknownApi => ErrorInner::UnknownApi,
        ConnectionStatusCode::Unauthorized => ErrorInner::Unauthorized,
    })
}

impl metrics::Enum for ErrorKind {
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

// TODO: Vec<Connection> Load Balancer

/// [`Connection`] Load balancer.
///
/// Load balances [`Connection::send`] across a list of [`Connection`]s using a
/// round-robin strategy.
pub struct LoadBalancer<API: Api> {
    /// List of [`Connections`] of this [`LoadBalancer`].
    ///
    /// Can be safely modified at any point.
    pub connections: Vec<Connection<API>>,

    counter: AtomicUsize,
}

impl<API: Api> LoadBalancer<API> {
    /// Creates a new [`LoadBalancer`].
    pub fn new(connections: impl IntoIterator<Item = Connection<API>>) -> Self {
        Self {
            connections: connections.into_iter().collect(),
            // overflows and starts from `0`
            counter: usize::MAX.into(),
        }
    }

    /// Sends the provided RPC using this [`LoadBalancer`].
    ///
    /// Doesn't automatically retry errors, you can call this fn again if you
    /// want to retry.
    pub fn send<'a, RPC: RpcV2>(
        &'a self,
        input: &'a RpcHandlerInput<'a, API::RpcHandler, RPC>,
    ) -> Result<impl Future<Output = Result<RpcHandlerOutput<API::RpcHandler, RPC>>> + Send + 'a>
    where
        API::RpcHandler: HandleRpc<RPC>,
    {
        let n = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        self.connections[n % self.connections.len()].send(input)
    }
}
