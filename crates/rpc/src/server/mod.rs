use {
    super::ConnectionHeader,
    crate::{
        self as rpc,
        transport::{
            BiDirectionalStream,
            Codec,
            NoHandshake,
            Read,
            RecvStream,
            SendStream,
            StreamError,
            Write,
        },
        Id as RpcId,
        Message,
        Result as RpcResult,
        ServerName,
    },
    futures::{Future, FutureExt, SinkExt as _, TryFutureExt as _},
    libp2p::{Multiaddr, PeerId},
    std::{io, sync::Arc, time::Duration},
    tap::{Pipe as _, TapOptional},
    tokio::{
        io::AsyncReadExt,
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    wc::{
        future::FutureExt as _,
        metrics::{self, future_metrics, FutureExt as _, StringLabel},
    },
};

pub mod middleware;

/// Server config.
#[derive(Clone, Debug)]
pub struct Config<H = NoHandshake> {
    /// Name of the server.
    pub name: &'static ServerName,

    /// [`Handshake`] implementation of the server.
    pub handshake: H,
}

/// [`Acceptor`] config.
#[derive(Clone, Copy, Debug)]
pub struct AcceptorConfig {
    /// Maximum allowed amount of concurrent connections.
    pub max_concurrent_connections: u32,

    /// Maximum allowed amount of concurrent streams.
    pub max_concurrent_streams: u32,
}

/// Server part of an application layer handshake.
pub trait Handshake: Clone + Send + Sync + 'static {
    type Data: Clone + Send + Sync + 'static;

    /// Handles the handshake.
    fn handle(
        &self,
        peer_id: &PeerId,
        conn: &mut impl InboundConnection,
    ) -> impl Future<Output = InboundConnectionResult<Self::Data>> + Send;
}

pub type HandshakeData<H> = <H as Handshake>::Data;

impl Handshake for NoHandshake {
    type Data = ();

    fn handle(
        &self,
        _peer_id: &PeerId,
        _conn: &mut impl InboundConnection,
    ) -> impl Future<Output = InboundConnectionResult<Self::Data>> + Send {
        async { Ok(()) }
    }
}

/// Info about an inbound connection.
#[derive(Debug, Clone)]
pub struct ConnectionInfo<H, S> {
    /// [`PeerId`] of the remote peer.
    pub peer_id: PeerId,

    /// [`Multiaddr`] of the remote peer.
    pub remote_address: Multiaddr,

    /// Handshake data.
    pub handshake_data: H,

    /// Local data storage for stateful connections.
    pub storage: S,
}

pub type ClientConnectionInfo<S> =
    ConnectionInfo<HandshakeData<<S as Server>::Handshake>, <S as Server>::ConnectionData>;

/// RPC server.
pub trait Server: Clone + Send + Sync + 'static {
    /// [`Handshake`] implementation of this RPC [`Server`].
    type Handshake: Handshake;

    /// Local data storage for stateful connections.
    type ConnectionData: Default + Clone + Send + Sync;

    /// Serialization codec.
    type Codec: Codec;

    /// Returns [`Config`] of this [`Server`].
    fn config(&self) -> &Config<Self::Handshake>;

    /// Handles an inbound RPC.
    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream<impl Read, impl Write>,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a;

    /// Runs this [`Server`] using the provided [`Acceptor`].
    fn serve(
        self,
        acceptor: impl Acceptor,
        cfg: AcceptorConfig,
    ) -> impl Future<Output = ()> + Send {
        multiplex((self,), acceptor, cfg)
    }
}

/// RPC [`Server`] error.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum Error {
    /// Conection error.
    #[error(transparent)]
    Connection(#[from] InboundConnectionError),
}

impl From<StreamError> for Error {
    fn from(err: StreamError) -> Self {
        Error::Connection(err.into())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Connection(err.into())
    }
}

/// RPC [`Server`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<const ID: RpcId, Req, Resp, C> super::Unary<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message,
    C: Codec,
{
    pub async fn handle<F, Fut>(
        stream: BiDirectionalStream<impl Read, impl Write>,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce(Req) -> Fut,
        Fut: Future<Output = RpcResult<Resp>>,
    {
        let (mut rx, mut tx) = stream.upgrade::<Req, RpcResult<Resp>, C>();
        let req = rx.recv_message().await?;
        let resp = f(req).await;
        tx.send(&resp).await?;
        Ok(())
    }
}

impl<const ID: RpcId, Req, Resp, C> super::Streaming<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message,
    C: Codec,
{
    pub async fn handle<R: Read, W: Write, F, Fut>(
        stream: BiDirectionalStream<R, W>,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce(RecvStream<R, Req, C>, SendStream<W, RpcResult<Resp>, C>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let (rx, tx) = stream.upgrade();
        f(rx, tx).await
    }
}

impl<const ID: RpcId, Msg, C> super::Oneshot<ID, Msg, C>
where
    Msg: Message,
    C: Codec,
{
    pub async fn handle<F, Fut>(
        stream: BiDirectionalStream<impl Read, impl Write>,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce(Msg) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (mut rx, _) = stream.upgrade::<Msg, (), C>();
        let req = rx.recv_message().await?;
        f(req).await;
        Ok(())
    }
}

/// Transport responsible for accepting [`InboundConnection`]s.
pub trait Acceptor: Send + Sync + 'static {
    /// Returns [`Multiaddr`] this [`Acceptor`] is bound to.
    fn address(&self) -> &Multiaddr;

    /// Accepts an [`InboundConnection`].
    fn accept(
        &self,
    ) -> impl Future<
        Output = Option<
            impl Future<Output = InboundConnectionResult<impl InboundConnection>> + Send + 'static,
        >,
    > + Send;
}

/// [`InboundConnection`] error.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum InboundConnectionError {
    #[error("IO: {0}")]
    IO(io::ErrorKind),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("Timeout accepting inbound connection")]
    ConnectionTimeout,

    #[error("Handshake error: {0}")]
    Handshake(String),

    #[error("Handshake timeout")]
    HandshakeTimeout,

    #[error("Unknown Rpc server")]
    UnknownRpcServer,

    #[error("Unsupported protocol version")]
    UnsupportedProtocolVersion(u32),

    #[error("Codec: {0}")]
    Codec(String),

    #[error("Failed to extract PeerId: {0}")]
    ExtractPeerId(String),

    #[error("{kind}: {details}")]
    Other { kind: &'static str, details: String },
}

impl InboundConnectionError {
    pub fn other(kind: &'static str) -> Self {
        Self::Other {
            kind,
            details: String::new(),
        }
    }
}

impl From<io::Error> for InboundConnectionError {
    fn from(err: io::Error) -> Self {
        InboundConnectionError::IO(err.kind())
    }
}

impl From<StreamError> for InboundConnectionError {
    fn from(err: StreamError) -> Self {
        match err {
            StreamError::IO(kind) => Self::IO(kind),
            StreamError::Finished => Self::StreamFinished,
            StreamError::Codec(err) => Self::Codec(err),
            StreamError::Other(err) => Self::Other {
                kind: "stream",
                details: err,
            },
        }
    }
}

pub type InboundConnectionResult<T> = Result<T, InboundConnectionError>;

/// Inbound connection.
pub trait InboundConnection: Send + 'static {
    /// Async [`Read`].
    type Read: Read;

    /// Async [`Write`].
    type Write: Write;

    /// Returns [`ConnectionHeader`] of this [`Connection`].
    fn header(&self) -> &ConnectionHeader;

    /// Returns [`PeerId`] and [`Multiaddr`] of the peer connected via this
    /// [`Connection`].
    fn peer_info(&self) -> InboundConnectionResult<(PeerId, Multiaddr)>;

    /// Accepts an inbound [`BiDirectionalStream`].
    fn accept_stream(
        &mut self,
    ) -> impl Future<Output = InboundConnectionResult<(Self::Read, Self::Write)>> + Send;
}

/// Runs multiple [`rpc::Server`]s using a single [`Acceptor`] transport.
///
/// `rpc_servers` argument is expected to be a tuple of [`rpc::Server`] impls.
pub fn multiplex<A, S>(
    rpc_servers: S,
    acceptor: A,
    cfg: AcceptorConfig,
) -> impl Future<Output = ()> + Send
where
    A: Acceptor,
    S: Send + Sync + 'static,
    Multiplexer<A, S>: ConnectionHandler,
{
    Multiplexer::new(acceptor, rpc_servers, cfg).serve()
}

/// RPC [`Server`] multiplexer.
#[derive(Debug)]
pub struct Multiplexer<T, S>(Arc<Inner<T, S>>);

impl<T, S> Clone for Multiplexer<T, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug)]
struct Inner<C, S> {
    acceptor: C,
    rpc_servers: S,

    connection_permits: Arc<Semaphore>,
    stream_permits: Arc<Semaphore>,
}

impl<C, S> Multiplexer<C, S>
where
    C: Acceptor,
    S: Send + Sync + 'static,
    Self: ConnectionHandler,
{
    fn new(acceptor: C, rpc_servers: S, cfg: AcceptorConfig) -> Self {
        Self(Arc::new(Inner {
            acceptor,
            rpc_servers,
            connection_permits: Arc::new(Semaphore::new(cfg.max_concurrent_connections as usize)),
            stream_permits: Arc::new(Semaphore::new(cfg.max_concurrent_streams as usize)),
        }))
    }

    async fn serve(self) {
        while let Some(fut) = self.0.acceptor.accept().await {
            self.accept_connection(fut)
        }
    }

    fn accept_connection(
        &self,
        fut: impl Future<Output = InboundConnectionResult<impl InboundConnection>> + Send + 'static,
    ) {
        let Ok(conn_permit) = self.0.connection_permits.clone().try_acquire_owned() else {
            metrics::counter!("wcn_rpc_server_connections_dropped").increment(1);
            return;
        };

        let this = self.clone();

        async move {
            let conn = fut
                .with_timeout(Duration::from_millis(2000))
                .await
                .map_err(|_| InboundConnectionError::ConnectionTimeout)??;

            let header = conn.header();

            let _conn_permit = conn_permit;

            ConnectionHandler::handle_connection(&this, header.server_name, conn).await
        }
        .map_err(|err| tracing::warn!(?err, "Inbound connection handler failed"))
        .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_connection"))
        .pipe(tokio::spawn);
    }

    async fn handle_connection<R: rpc::Server>(
        &self,
        mut conn: impl InboundConnection,
        rpc_server: &R,
    ) -> Result<(), InboundConnectionError> {
        use InboundConnectionError as Error;

        let cfg = rpc_server.config();

        let (peer_id, remote_address) = conn.peer_info()?;

        let remote_address = match remote_address.with_p2p(peer_id) {
            Ok(addr) => addr,
            Err(addr) => addr,
        };

        let conn_info = ConnectionInfo {
            peer_id,
            remote_address,
            handshake_data: cfg
                .handshake
                .handle(&peer_id, &mut conn)
                .with_timeout(Duration::from_millis(1000))
                .await
                .map_err(|_| Error::HandshakeTimeout)??,
            storage: Default::default(),
        };

        loop {
            let (mut rx, tx) = conn.accept_stream().await?;

            let Some(stream_permit) = self.acquire_stream_permit() else {
                static THROTTLED_RESULT: &crate::Result<()> = &Err(crate::Error::THROTTLED);

                let (_, mut tx) =
                    BiDirectionalStream::new(rx, tx).upgrade::<(), crate::Result<()>, R::Codec>();

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

                rpc_server
                    .handle_rpc(rpc_id, BiDirectionalStream::new(rx, tx), &conn_info)
                    .await
            }
            .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_stream"))
            .pipe(tokio::spawn);
        }
    }

    fn acquire_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!("wcn_rpc_server_available_stream_permits", StringLabel<"server_addr", Multiaddr> => self.0.acceptor.address())
            .set(self.0.stream_permits.available_permits() as f64);

        self.0.stream_permits
            .clone()
            .try_acquire_owned()
            .ok()
            .tap_none(|| {
                metrics::counter!("wcn_rpc_server_throttled_streams", StringLabel<"server_addr", Multiaddr> => &self.0.acceptor.address())
                    .increment(1);
            })
    }
}

impl ConnectionHeader {
    pub async fn read(rx: &mut impl Read) -> Result<Self, InboundConnectionError> {
        let protocol_version = rx.read_u32().await?;

        let (protocol_version, server_name) = match protocol_version {
            0 => (0, None),
            super::PROTOCOL_VERSION => {
                let mut buf = [0; 16];
                rx.read_exact(&mut buf).await?;
                (super::PROTOCOL_VERSION, Some(ServerName(buf)))
            }
            ver => return Err(InboundConnectionError::UnsupportedProtocolVersion(ver)),
        };

        Ok(ConnectionHeader {
            protocol_version,
            server_name,
        })
    }
}

async fn read_rpc_id(rx: &mut impl Read) -> Result<crate::Id, String> {
    rx.read_u128()
        .with_timeout(Duration::from_millis(500))
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("{err:?}"))
}

pub trait ConnectionHandler: Clone + Sized {
    fn handle_connection(
        &self,
        server_name: Option<ServerName>,
        conn: impl InboundConnection,
    ) -> impl Future<Output = Result<(), InboundConnectionError>> + Send;
}

impl<T, A> ConnectionHandler for Multiplexer<T, (A,)>
where
    T: Acceptor,
    A: rpc::Server,
{
    fn handle_connection(
        &self,
        server_name: Option<ServerName>,
        conn: impl InboundConnection,
    ) -> impl Future<Output = Result<(), InboundConnectionError>> + Send {
        async move {
            let Some(server_name) = server_name else {
                return self.handle_connection(conn, &self.0.rpc_servers.0).await;
            };

            if self.0.rpc_servers.0.config().name == &server_name {
                return self.handle_connection(conn, &self.0.rpc_servers.0).await;
            }

            Err(InboundConnectionError::UnknownRpcServer)
        }
    }
}

impl<T, A, B> ConnectionHandler for Multiplexer<T, (A, B)>
where
    T: Acceptor,
    A: rpc::Server,
    B: rpc::Server,
{
    fn handle_connection(
        &self,
        server_name: Option<ServerName>,
        conn: impl InboundConnection,
    ) -> impl Future<Output = Result<(), InboundConnectionError>> + Send {
        async move {
            let Some(server_name) = server_name else {
                return self.handle_connection(conn, &self.0.rpc_servers.0).await;
            };

            if self.0.rpc_servers.0.config().name == &server_name {
                return self.handle_connection(conn, &self.0.rpc_servers.0).await;
            }

            if self.0.rpc_servers.1.config().name == &server_name {
                return self.handle_connection(conn, &self.0.rpc_servers.1).await;
            }

            Err(InboundConnectionError::UnknownRpcServer)
        }
    }
}
