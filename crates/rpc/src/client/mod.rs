use {
    super::{ConnectionHeader, PROTOCOL_VERSION},
    crate::{
        kind,
        transport::{self, Codec, NoHandshake, Read, RecvStream, SendStream, StreamError, Write},
        Error as RpcError,
        ForceSendFuture,
        Id as RpcId,
        Message,
        Rpc,
        ServerName,
    },
    backoff::ExponentialBackoffBuilder,
    derivative::Derivative,
    derive_more::{derive::Display, From},
    futures::{
        future::{BoxFuture, Shared},
        Future,
        FutureExt as _,
        SinkExt as _,
        TryFutureExt as _,
    },
    indexmap::IndexMap,
    libp2p::Multiaddr,
    std::{
        collections::HashSet,
        io,
        sync::{Arc, PoisonError},
        time::Duration,
    },
    tokio::{io::AsyncWriteExt as _, sync::RwLock},
    wc::{
        future::FutureExt as _,
        metrics::{self, StringLabel},
    },
};

pub mod middleware;

/// [`Client`] config.
#[derive(Clone)]
pub struct Config<H = NoHandshake> {
    /// Known remote peer [`Multiaddr`]s.
    pub known_peers: HashSet<Multiaddr>,

    /// [`Handshake`] implementation to use for connection establishment.
    pub handshake: H,

    /// Connection timeout.
    pub connection_timeout: Duration,

    /// Name of the RPC server this client is going to be connecting to.
    pub server_name: ServerName,
}

pub trait Transport: Clone + Send + Sync + 'static {
    type Connection: Connection;

    fn establish_connection(
        &self,
        multiaddr: &Multiaddr,
        header: ConnectionHeader,
    ) -> impl Future<Output = TransportResult<Self::Connection>> + Send;
}

/// [`Transport`] error.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("IO: {0}")]
    IO(io::ErrorKind),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("Invalid Multiaddr")]
    InvalidMultiaddr,

    #[error("Timeout establishing outbound connection")]
    ConnectionTimeout,

    #[error("Handshake error: {0}")]
    Handshake(String),

    #[error("Codec: {_0}")]
    Codec(String),

    #[error("{kind}: {details}")]
    Other { kind: &'static str, details: String },
}

impl From<io::Error> for TransportError {
    fn from(err: io::Error) -> Self {
        TransportError::IO(err.kind())
    }
}

impl From<StreamError> for TransportError {
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

pub type TransportResult<T> = Result<T, TransportError>;

/// Outbound connection.
pub trait Connection: Clone + Send + Sync + 'static {
    type Read: Read;
    type Write: Write;

    /// Returns unique ID of this [`Connection`].
    fn id(&self) -> usize;

    /// Accepts an inbound [`BiDirectionalStream`].
    fn establish_stream(
        &self,
    ) -> impl Future<Output = TransportResult<(Self::Read, Self::Write)>> + Send;
}

pub trait Handshake: Clone + Send + Sync + 'static {
    type Data: Clone + Send + Sync + 'static;

    fn handle(
        &self,
        conn: &impl Connection,
    ) -> impl Future<Output = TransportResult<Self::Data>> + Send;
}

impl Handshake for NoHandshake {
    type Data = ();

    fn handle(
        &self,
        _conn: &impl Connection,
    ) -> impl Future<Output = TransportResult<Self::Data>> + Send {
        async { Ok(()) }
    }
}

type ConnectionRead<T> = <<T as Transport>::Connection as Connection>::Read;
type ConnectionWrite<T> = <<T as Transport>::Connection as Connection>::Write;

type BiDirectionalStream<T> = transport::BiDirectionalStream<ConnectionRead<T>, ConnectionWrite<T>>;

/// RPC client.
pub trait Client<A: Sync = Multiaddr>: Send + Sync {
    type Transport: Transport;

    /// Sends an outbound RPC.
    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok: Send>(
        &'a self,
        addr: &'a A,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<Self::Transport>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a;

    /// Sends an unary RPC.
    fn send_unary<'a, RPC: Rpc<Kind = kind::Unary>>(
        &'a self,
        addr: &'a A,
        request: &'a RPC::Request,
    ) -> impl Future<Output = Result<RPC::Response>> + Send + 'a {
        async move {
            self.send_rpc(addr, RPC::ID, &move |stream| async move {
                let (mut rx, mut tx) = stream.upgrade::<RpcResult<RPC>, RPC::Request, RPC::Codec>();
                tx.send(request).await?;

                Ok(rx.recv_message().await??)
            })
            .force_send_impl()
            .await
        }
    }

    /// Sends a streaming RPC.
    fn send_streaming<'a, RPC: Rpc<Kind = kind::Streaming>, Fut, Ok: Send>(
        &'a self,
        addr: &'a A,
        f: &'a (impl Fn(
            SendStream<ConnectionWrite<Self::Transport>, RPC::Request, RPC::Codec>,
            RecvStream<ConnectionRead<Self::Transport>, RpcResult<RPC>, RPC::Codec>,
        ) -> Fut
                 + Send
                 + Sync
                 + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a
    where
        Fut: Future<Output = Result<Ok>> + Send,
    {
        async move {
            self.send_rpc(addr, RPC::ID, &move |stream| async move {
                let (rx, tx) = stream.upgrade::<RpcResult<RPC>, RPC::Request, RPC::Codec>();
                f(tx, rx).await
            })
            .force_send_impl()
            .await
        }
    }

    /// Sends a oneshot RPC.
    fn send_oneshot<'a, RPC: Rpc<Kind = kind::Oneshot>>(
        &'a self,
        addr: &'a A,
        msg: &'a RPC::Request,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async move {
            self.send_rpc(addr, RPC::ID, &move |stream| async move {
                let (_, mut tx) = stream.upgrade::<RPC::Response, RPC::Request, RPC::Codec>();
                tx.send(msg).await?;
                Ok(())
            })
            .force_send_impl()
            .await
        }
    }
}

/// Marker trait that should accompany [`Client`] impls in order to blanket impl
/// the middleware extension traits.
pub trait Marker {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("There's no healthy connections to any peer at the moment")]
    NoAvailablePeers,

    #[error("Poisoned lock")]
    Lock,

    #[error("RNG failed")]
    Rng,

    // Transport error.
    #[error(transparent)]
    Transport(#[from] TransportError),

    /// RPC error.
    #[error("{source} RPC: {error:?}")]
    Rpc {
        #[source]
        error: RpcError,
        source: RpcErrorSource,
    },
}

impl From<StreamError> for Error {
    fn from(err: StreamError) -> Self {
        Error::Transport(err.into())
    }
}

impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        Self::Rpc {
            error,
            source: RpcErrorSource::Server,
        }
    }
}

impl Error {
    /// Creates a new client-side [`RpcError`] with the provided error code.
    pub fn rpc(code: &'static str) -> Self {
        RpcError::new(code).into()
    }
}

/// The source of an [`RpcError`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Display)]
pub enum RpcErrorSource {
    /// Client-side error.
    Client,

    /// Server-side error.
    Server,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Transport(err.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

type RpcResult<RPC> = Result<<RPC as Rpc>::Response, crate::Error>;

impl<const ID: RpcId, Req, Resp> super::Unary<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub async fn send<A: Sync>(client: &impl Client<A>, addr: &A, req: &Req) -> Result<Resp> {
        client.send_unary::<Self>(addr, req).await
    }
}

impl<const ID: RpcId, Req, Resp, C> super::Streaming<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message,
    C: Codec,
{
    pub fn send<'a, A: Sync, T: Transport, F, Fut, Ok: Send>(
        client: &'a impl Client<A, Transport = T>,
        addr: &'a A,
        f: &'a F,
    ) -> impl Future<Output = Result<Ok>> + Send + 'a
    where
        F: Fn(
                SendStream<ConnectionWrite<T>, Req, C>,
                RecvStream<ConnectionRead<T>, RpcResult<Self>, C>,
            ) -> Fut
            + Send
            + Sync
            + 'a,
        Fut: Future<Output = Result<Ok>> + Send,
    {
        client.send_streaming::<Self, _, _>(addr, f)
    }
}

impl<const ID: RpcId, Msg> super::Oneshot<ID, Msg>
where
    Msg: Message,
{
    pub fn send<'a, A: Sync>(
        client: &'a impl Client<A>,
        addr: &'a A,
        msg: &'a Msg,
    ) -> impl Future<Output = Result<()>> + 'a {
        client.send_oneshot::<Self>(addr, msg)
    }
}

/// Arbitrary remote peer.
pub struct AnyPeer;

/// Base [`Client`] impl.
#[derive(Clone, Debug)]
pub struct ClientImpl<T: Transport, H = NoHandshake> {
    // peer_id: PeerId,
    transport: T,
    handshake: H,

    server_name: ServerName,

    connection_handlers: Arc<RwLock<Arc<OutboundConnectionHandlers<T, H>>>>,
    connection_timeout: Duration,
}

impl<T: Transport, H> Marker for ClientImpl<T, H> {}

impl<T: Transport, H: Handshake> Client for ClientImpl<T, H> {
    type Transport = T;

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok>(
        &'a self,
        addr: &'a Multiaddr,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<T>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        self.establish_stream(addr, rpc_id)
            .map_err(Into::into)
            .and_then(move |stream| f(stream).map_err(Into::into))
    }
}

impl<T: Transport, H: Handshake> Client<AnyPeer> for ClientImpl<T, H> {
    type Transport = T;

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok>(
        &'a self,
        _: &'a AnyPeer,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<T>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        self.establish_stream_any(rpc_id)
            .map_err(Into::into)
            .and_then(move |stream| f(stream).map_err(Into::into))
    }
}

// impl From<EstablishStreamError> for client::Error {
//     fn from(err: EstablishStreamError) -> Self {
//         Self::Transport(transport::Error::Other(format!(
//             "Connection handler: {err:?}"
//         )))
//     }
// }

type OutboundConnectionHandlers<T, H> = IndexMap<Multiaddr, ConnectionHandler<T, H>>;

/// Builds a new [`Client`] using the provided [`Config`].
pub fn new<T: Transport, H: Handshake>(transport: T, cfg: Config<H>) -> ClientImpl<T, H> {
    let handlers = cfg
        .known_peers
        .into_iter()
        .map(|multiaddr| {
            let handler = ConnectionHandler::new(
                multiaddr.clone(),
                cfg.server_name,
                transport.clone(),
                cfg.handshake.clone(),
                cfg.connection_timeout,
            );
            (multiaddr, handler)
        })
        .collect();

    ClientImpl {
        // peer_id: local_peer_id,
        transport,
        handshake: cfg.handshake,
        server_name: cfg.server_name,
        connection_handlers: Arc::new(RwLock::new(Arc::new(handlers))),
        connection_timeout: cfg.connection_timeout,
    }
}

#[derive(Clone, Debug)]
pub(super) struct ConnectionHandler<T: Transport, H> {
    inner: Arc<std::sync::RwLock<ConnectionHandlerInner<T>>>,
    handshake: H,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ConnectionHandlerInner<T: Transport> {
    addr: Multiaddr,
    server_name: ServerName,
    transport: T,

    #[derivative(Debug = "ignore")]
    connection: SharedConnection<T>,
    connection_timeout: Duration,
}

type SharedConnection<T> = Shared<BoxFuture<'static, <T as Transport>::Connection>>;

fn new_connection<T: Transport, H: Handshake>(
    multiaddr: Multiaddr,
    server_name: ServerName,
    transport: T,
    handshake: H,
    timeout: Duration,
) -> SharedConnection<T> {
    // We want to reconnect as fast as possible, otherwise it may lead to a lot of
    // lost requests, especially on cluster startup.
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_interval(Duration::from_millis(100))
        .with_max_elapsed_time(None)
        .build();

    async move {
        let transport = &transport;
        let handshake = &handshake;
        let multiaddr = &multiaddr;

        let connect = || {
            async move {
                let header = ConnectionHeader {
                    protocol_version: PROTOCOL_VERSION,
                    server_name: Some(server_name),
                };

                let conn = transport
                    .establish_connection(multiaddr, header)
                    .with_timeout(timeout)
                    .await
                    .map_err(|_| TransportError::ConnectionTimeout)??;

                handshake
                    .handle(&conn)
                    .await
                    .map_err(|e| TransportError::Handshake(format!("{e:?}")))?;

                Ok(conn)
            }
            .map_err(backoff::Error::transient)
        };

        backoff::future::retry_notify(backoff, connect, move |err: TransportError, _| {
            tracing::debug!(?err, "failed to connect");
            metrics::counter!(
                "wcn_network_connection_failures",
                StringLabel<"kind"> => err.kind(),
                StringLabel<"addr", Multiaddr> => multiaddr
            )
            .increment(1);
        })
        .map(move |res| {
            tracing::info!(%multiaddr, "connection established");
            // we explicitly set `max_elapsed_time` to `None`
            res.unwrap()
        })
        .await
    }
    .boxed()
    .shared()
}

impl<T: Transport, H: Handshake> ConnectionHandler<T, H> {
    pub(super) fn new(
        addr: Multiaddr,
        server_name: ServerName,
        transport: T,
        handshake: H,
        connection_timeout: Duration,
    ) -> Self {
        let inner = ConnectionHandlerInner {
            addr: addr.clone(),
            server_name,
            transport: transport.clone(),
            connection: new_connection(
                addr,
                server_name,
                transport,
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

    async fn establish_stream(&self, rpc_id: RpcId) -> Result<BiDirectionalStream<T>> {
        let fut = self.inner.write()?.connection.clone();
        let conn = fut.await;

        let (rx, mut tx) = match conn.establish_stream().await {
            Ok(stream) => stream,
            Err(_) => self.reconnect(conn.id())?.await.establish_stream().await?,
        };

        tx.write_u128(rpc_id).await?;

        Ok(BiDirectionalStream::<T>::new(rx, tx))
    }

    async fn try_establish_stream(&self, rpc_id: RpcId) -> Option<BiDirectionalStream<T>> {
        let conn = self
            .inner
            .try_read()
            .ok()?
            .connection
            .clone()
            .now_or_never()?;

        let (rx, mut tx) = match conn.establish_stream().await {
            Ok(stream) => stream,
            Err(_) => {
                // we don't need to await this future, it's shared
                drop(self.reconnect(conn.id()));
                return None;
            }
        };

        tx.write_u128(rpc_id).await.map_err(|e| e.kind()).ok()?;

        Some(BiDirectionalStream::<T>::new(rx, tx))
    }

    /// Replaces the current connection with a new one.
    ///
    /// No-op if the current connection id doesn't match the provided one,
    /// meaning the connection was already replaced.
    fn reconnect(&self, prev_connection_id: usize) -> Result<SharedConnection<T>> {
        let mut this = self.inner.write()?;

        if this
            .connection
            .peek()
            .filter(|conn| conn.id() == prev_connection_id)
            .is_some()
        {
            metrics::counter!("wcn_network_reconnects").increment(1);
            this.connection = new_connection(
                this.addr.clone(),
                this.server_name,
                this.transport.clone(),
                self.handshake.clone(),
                this.connection_timeout,
            );
        };

        Ok(this.connection.clone())
    }
}

impl<G> From<PoisonError<G>> for Error {
    fn from(_: PoisonError<G>) -> Self {
        Self::Lock
    }
}

impl TransportError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::IO(_) => "io",
            Self::StreamFinished => "stream_finished",
            Self::InvalidMultiaddr => "invalid_multiaddr",
            Self::ConnectionTimeout => "connection_timeout",
            Self::Handshake(_) => "handshake",
            Self::Codec(_) => "codec",
            Self::Other { kind, .. } => kind,
        }
    }
}

impl<T: Transport, H: Handshake> ClientImpl<T, H> {
    /// Establishes a [`BiDirectionalStream`] with the requested remote peer.
    pub async fn establish_stream(
        &self,
        multiaddr: &Multiaddr,
        rpc_id: RpcId,
    ) -> Result<BiDirectionalStream<T>> {
        let handlers = self.connection_handlers.read().await;
        let handler = if let Some(handler) = handlers.get(multiaddr) {
            handler.clone()
        } else {
            let handler = ConnectionHandler::new(
                multiaddr.clone(),
                self.server_name,
                self.transport.clone(),
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
            .map_err(|_| TransportError::ConnectionTimeout)?
    }

    /// Establishes a [`BiDirectionalStream`] with one of the remote peers.
    ///
    /// Tries to spread the load equally and to minimize the latency by skipping
    /// broken connections early.
    pub async fn establish_stream_any(&self, rpc_id: RpcId) -> Result<BiDirectionalStream<T>> {
        use rand::{Rng, SeedableRng};

        let handlers = self.connection_handlers.read().await.clone();
        let len = handlers.len();
        let mut rng =
            rand::rngs::SmallRng::from_rng(&mut rand::thread_rng()).map_err(|_| Error::Rng)?;
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

        Err(Error::NoAvailablePeers)
    }
}

impl ConnectionHeader {
    pub async fn write(&self, tx: &mut impl Write) -> TransportResult<()> {
        tx.write_u32(PROTOCOL_VERSION).await?;

        if let Some(server_name) = &self.server_name {
            tx.write_all(&server_name.0).await?;
        }

        Ok(())
    }
}
