use {
    crate::{
        self as rpc,
        connection_filter::{self, Filter, RejectionReason},
        metrics::FallibleResponse,
        quic::{self},
        transport::{self, BiDirectionalStream, RecvStream, SendStream, Serializer},
        ApiName,
        BorrowedMessage,
        ConnectionStatus,
        Rpc,
    },
    derive_where::derive_where,
    futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt as _, TryStream as _},
    futures_concurrency::future::Race as _,
    libp2p_identity::PeerId,
    pin_project::pin_project,
    quinn::crypto::rustls::{self, QuicServerConfig},
    std::{
        error::Error as StdError,
        future::Future,
        io,
        net::IpAddr,
        pin::{pin, Pin},
        sync::Arc,
        task::{self, ready, Poll},
        time::Duration,
    },
    strum::{EnumDiscriminants, IntoDiscriminant as _, IntoStaticStr},
    tap::{Pipe as _, TapFallible as _},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    tokio_serde::Deserializer as _,
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt},
        metrics::{
            self,
            enum_ordinalize::Ordinalize,
            future_metrics,
            Enum as _,
            EnumLabel,
            FutureExt as _,
            StringLabel,
        },
    },
};

/// Server-specific part of an RPC [Api][`super::Api`].
pub trait Api: super::Api {
    /// [`Inbound`] RPC handler.
    type RpcHandler: Clone + Send + Sync + 'static;

    /// Handles the provided inbound [`PendingConnection`].
    fn handle_connection(
        conn: PendingConnection<'_, Self>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Converts this [`Api`] into [`Server`].
    fn into_server(self) -> impl Server {
        new(self)
    }

    /// Converts this [`Api`] into [`Server`] and multiplexes it with another
    /// [`Api`].
    fn multiplex(self, api: impl Api) -> impl Server {
        self.into_server().multiplex(api)
    }
}

/// RPC server config.
#[derive(Clone, Debug)]
pub struct Config {
    /// Name of the server. For metrics purposes only.
    pub name: &'static str,

    /// [`Multiaddr`] to bind the server to.
    pub port: u16,

    /// [`identity::Keypair`] of the server.
    pub keypair: libp2p_identity::Keypair,

    /// Timeout of establishing an inbound connection.
    pub connection_timeout: Duration,

    /// Maximum global number of concurrent connections.
    pub max_connections: u32,

    /// Maximum number of concurrent connections per client IP address.
    pub max_connections_per_ip: u32,

    /// Maximum number of connections accepted per client IP address per second.
    pub max_connection_rate_per_ip: u32,

    /// Maximum number of concurrent RPCs.
    pub max_concurrent_rpcs: u32,

    /// [`transport::Priority`] of the server.
    pub priority: transport::Priority,

    /// [`ShutdownSignal`] to use.
    pub shutdown_signal: ShutdownSignal,
}

/// Creates a new RPC [`Api`] server.
pub fn new(api: impl Api) -> impl Server {
    ApiServer { api }
}

/// RPC server.
pub trait Server: Sized + Send + Sync + 'static
where
    Self: sealed::ConnectionRouter,
{
    /// Multiplexes `self` with another [`Api`].
    fn multiplex(self, api_server: impl Api) -> impl Server {
        Multiplexer {
            head: api_server.into_server(),
            tail: self,
        }
    }

    /// Runs this RPC [`Server`]
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()> + Send> {
        let transport_config = quic::new_quinn_transport_config(cfg.max_concurrent_rpcs);
        let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair).map_err(Error::new)?;
        let server_tls_config =
            QuicServerConfig::try_from(server_tls_config).map_err(Error::new)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_tls_config));
        server_config.transport = transport_config.clone();
        server_config.migration(false);

        let endpoint = quic::new_quinn_endpoint(
            ([0, 0, 0, 0], cfg.port).into(),
            &cfg.keypair,
            transport_config,
            Some(server_config),
            cfg.priority,
        )
        .map_err(Error::new)?;

        let connection_filter = Filter::new(&connection_filter::Config {
            max_connections: cfg.max_connections,
            max_connections_per_ip: cfg.max_connections_per_ip,
            max_connection_rate_per_ip: cfg.max_connection_rate_per_ip,
        })
        .map_err(Error::new)?;

        let rpc_semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_rpcs as usize));

        tracing::info!(port = cfg.port, server_name = cfg.name, "Serving");

        let accept_conn_fut = accept_connections(
            cfg.clone(),
            endpoint.clone(),
            connection_filter,
            rpc_semaphore,
            self,
        );

        Ok(async move {
            (accept_conn_fut, cfg.shutdown_signal.wait()).race().await;

            tracing::info!(port = cfg.port, server_name = cfg.name, "Shutting down");

            endpoint.wait_idle().await;

            tracing::info!(port = cfg.port, server_name = cfg.name, "Shut down");
        })
    }
}

/// Handle for managing graceful shutdown of [`Server`]s.
#[derive(Clone, Debug, Default)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    /// Creates a new [`ShutdownSignal`].
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Emits this [`ShutdownSignal`].
    pub fn emit(&self) {
        self.token.cancel();
    }

    /// Waits for this [`ShutdownSignal`] to be [emitted][`Self::emit`].
    pub async fn wait(&self) {
        self.token.cancelled().await
    }

    /// Indicates whether this [`ShutdownSignal`] has been
    /// [emitted][`Self::emit`].
    pub fn is_emitted(&self) -> bool {
        self.token.is_cancelled()
    }
}

mod sealed {
    use super::*;

    pub trait ConnectionRouter: Clone + Send + Sync + 'static {
        fn contains_api(&self, api_name: &ApiName) -> bool;

        /// Routes [`Connection`] to the [`Api`] connection handler.
        fn route_connection(
            &self,
            api_name: &ApiName,
            conn: PendingConnection<'_>,
        ) -> impl Future<Output = Result<()>> + Send;
    }
}

#[derive_where(Clone)]
struct ApiServer<API: Api> {
    api: API,
}

#[derive(Clone)]
struct Multiplexer<A, B> {
    head: A,
    tail: B,
}

impl<API: Api> sealed::ConnectionRouter for ApiServer<API> {
    fn contains_api(&self, api_name: &ApiName) -> bool {
        api_name == &API::NAME
    }

    async fn route_connection(
        &self,
        api_name: &ApiName,
        conn: PendingConnection<'_>,
    ) -> Result<()> {
        if !self.contains_api(api_name) {
            return Err(ErrorInner::UnknownApi(*api_name).into());
        }

        API::handle_connection(PendingConnection {
            api: self.api.clone(),
            inner: conn.inner,
            status_tx: conn.status_tx,
        })
        .await
    }
}

impl<A, B> sealed::ConnectionRouter for Multiplexer<A, B>
where
    A: sealed::ConnectionRouter,
    B: sealed::ConnectionRouter,
{
    fn contains_api(&self, api_name: &ApiName) -> bool {
        self.head.contains_api(api_name) || self.tail.contains_api(api_name)
    }

    async fn route_connection(
        &self,
        api_name: &ApiName,
        conn: PendingConnection<'_>,
    ) -> Result<()> {
        if self.head.contains_api(api_name) {
            self.head.route_connection(api_name, conn).await
        } else {
            self.tail.route_connection(api_name, conn).await
        }
    }
}

impl<R: sealed::ConnectionRouter> Server for R {}

async fn accept_connections<R: sealed::ConnectionRouter>(
    config: Config,
    endpoint: quinn::Endpoint,
    connection_filter: Filter,
    rpc_semaphore: Arc<Semaphore>,
    router: R,
) {
    while let Some(incoming) = endpoint.accept().await {
        match connection_filter.try_acquire_permit(&incoming) {
            Ok(permit) => match incoming.accept() {
                Ok(connecting) => accept_connection(
                    config.connection_timeout,
                    config.name,
                    connecting,
                    permit,
                    rpc_semaphore.clone(),
                    router.clone(),
                    config.shutdown_signal.clone(),
                ),

                Err(err) => tracing::warn!(?err, "failed to accept incoming connection"),
            },

            Err(err) => {
                if err == connection_filter::RejectionReason::AddressNotValidated {
                    // Signal the client to retry with validated address.
                    let _ = incoming.retry();
                } else {
                    tracing::debug!(
                        server_name = config.name,
                        reason = err.as_str(),
                        remote_addr = ?incoming.remote_address().ip(),
                        "inbound connection dropped"
                    );

                    metrics::counter!(
                        "wcn_rpc_quic_server_connections_dropped",
                        EnumLabel<"reason", RejectionReason> => err,
                        StringLabel<"server_name"> => config.name
                    )
                    .increment(1);

                    // Calling `ignore()` instead of dropping avoids sending a response.
                    incoming.ignore();
                }
            }
        };
    }
}

fn accept_connection<R: sealed::ConnectionRouter>(
    timeout: Duration,
    server_name: &'static str,
    connecting: quinn::Connecting,
    permit: connection_filter::Permit,
    rpc_semaphore: Arc<Semaphore>,
    router: R,
    shutdown_signal: ShutdownSignal,
) {
    async move {
        let (api_name, conn, mut tx) = async move {
            let conn = connecting.await?;
            let remote_peer_id = quic::connection_peer_id(&conn)?;

            let (mut tx, mut rx) = conn.accept_bi().await?;

            let protocol_version = rx.read_u32().await?;

            let api_name = match protocol_version {
                super::PROTOCOL_VERSION => {
                    let mut buf = [0; 16];
                    rx.read_exact(&mut buf).await?;
                    ApiName(buf)
                }
                ver => {
                    tx.write_i32(ConnectionStatus::UnsupportedProtocol.code())
                        .await?;
                    return Err(ErrorInner::UnsupportedProtocolVersion(ver));
                }
            };

            let conn = ConnectionInner {
                api_name,
                server_name,
                _permit: permit,
                remote_peer_id,
                rpc_semaphore,
                quic: conn,
                shutdown_signal: shutdown_signal.clone(),
            };

            Ok((api_name, conn, tx))
        }
        .with_timeout(timeout)
        .map_err(|_| ErrorInner::ConnectionTimeout)
        .await?
        .map_err(Error::new)?;

        let conn = PendingConnection {
            api: (),
            inner: Arc::new(conn),
            status_tx: &mut tx,
        };

        let res = router.route_connection(&api_name, conn).await;

        if res.as_ref().err().is_some_and(Error::is_unknown_api) {
            tx.write_i32(ConnectionStatus::UnknownApi.code())
                .await
                .map_err(Error::new)?
        }

        res
    }
    .map_err(|err| tracing::debug!(?err, "Inbound connection handler failed"))
    .with_metrics(future_metrics!("wcn_rpc_server_inbound_connection"))
    .pipe(tokio::spawn);
}

/// Inbound connection that is not yet accepting inbound RPCs.
pub struct PendingConnection<'a, API = ()> {
    api: API,
    inner: Arc<ConnectionInner>,
    status_tx: &'a mut quinn::SendStream,
}

impl<API: Api> PendingConnection<'_, API> {
    /// Returns [`Api`] of this [`PendingConnection`].
    pub fn api(&self) -> &API {
        &self.api
    }

    /// Returns [`PeerId`] of the remote peer.
    pub fn remote_peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Accept this [`PendingConnection`] notifying the client that it's now
    /// ready to accept [`InboundRpc`]s.
    pub async fn accept(self, rpc_handler: API::RpcHandler) -> Result<Connection<API>> {
        self.status_tx
            .write_i32(ConnectionStatus::Ok.code())
            .await
            .map_err(Error::new)?;

        Ok(Connection {
            api: self.api,
            rpc_handler,
            inner: self.inner,
        })
    }

    /// Rejects this [`PendingConnection`] notifying the client that it won't be
    /// accepting [`InboundRpc`]s.
    ///
    /// Application layer logic is expected to define its own mapping for
    /// `reason` codes.
    pub async fn reject(self, reason: u8) -> Result<()> {
        self.status_tx
            .write_i32(ConnectionStatus::Rejected { reason }.code())
            .await
            .map_err(Error::new)
    }
}

/// Inbound connection.
#[derive(Clone)]
pub struct Connection<API: Api> {
    api: API,
    rpc_handler: API::RpcHandler,
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    server_name: &'static str,
    api_name: ApiName,

    remote_peer_id: PeerId,

    _permit: connection_filter::Permit,
    rpc_semaphore: Arc<Semaphore>,

    quic: quinn::Connection,

    shutdown_signal: ShutdownSignal,
}

/// Inbound RPC with yet undefined type.
pub struct InboundRpc<API: Api> {
    id: API::RpcId,
    stream: BiDirectionalStream,
    permit: OwnedSemaphorePermit,
    shutdown_signal: ShutdownSignal,
    conn: Connection<API>,
}

impl<API: Api> InboundRpc<API> {
    /// Handles this RPC using the provided request handler.
    pub async fn handle_request<RPC: Rpc>(
        self,
        handler: impl AsyncFnOnce(API::RpcHandler, RPC::Request) -> RPC::Response,
    ) -> Result<()> {
        self.handle_unary::<RPC>(|api, req, responder: Responder<_>| async {
            let resp = handler(api, req).await;
            responder.respond(&resp).await
        })
        .await
    }

    /// Handles this RPC using the provided Unary RPC handler.
    pub async fn handle_unary<RPC: Rpc>(
        self,
        handler: impl AsyncFnOnce(API::RpcHandler, RPC::Request, Responder<RPC>) -> Result<()>,
    ) -> Result<()> {
        self.handle::<RPC>(|api, mut rpc: Inbound<RPC>| async {
            let req = StreamExt::next(&mut pin!(&mut rpc.request_stream))
                .await
                .ok_or_else(|| Error::new(ErrorInner::StreamFinished))??;

            let responder = Responder { rpc };

            handler(api, req, responder).await
        })
        .await
    }

    /// Handles this RPC using the provided handler.
    pub async fn handle<RPC: Rpc>(
        self,
        handler: impl AsyncFnOnce(API::RpcHandler, Inbound<RPC>) -> Result<()>,
    ) -> Result<()> {
        let id = self.id;
        let server_name = self.conn.inner.server_name;
        let remote_addr = self.conn.remote_peer_addr();

        let future_metrics = future_metrics!("wcn_rpc_server_inbound_rpc",
            StringLabel<"server_name"> => server_name,
            StringLabel<"remote_addr", IpAddr> => &remote_addr,
            StringLabel<"api", ApiName> => &API::NAME,
            StringLabel<"rpc"> => id.into()
        );

        let err_counter = |err: &Error| {
            metrics::counter!(
                "wcn_rpc_server_inbound_rpc_errors",
                EnumLabel<"kind", ErrorKind> => err.0.discriminant(),
                StringLabel<"server_name"> => server_name,
                StringLabel<"remote_addr", IpAddr> => &remote_addr,
                StringLabel<"api", ApiName> => &API::NAME,
                StringLabel<"rpc"> => id.into()
            )
        };

        let (api, rpc_handler, rpc) = self.upgrade();

        async {
            if let Some(timeout) = api.rpc_timeout(id) {
                handler(rpc_handler, rpc)
                    .with_timeout(timeout)
                    .await
                    .map_err(|_| Error::timeout())?
            } else {
                handler(rpc_handler, rpc).await
            }
        }
        .with_metrics(future_metrics)
        .await
        .tap_err(|err| err_counter(err).increment(1))
    }

    /// Upgrades this untyped [`InboundRpc`] into a typed one.
    ///
    /// Caller is expected to ensure that the [`InboundRpc::id()`] is correct.
    fn upgrade<RPC: Rpc>(self) -> (API, API::RpcHandler, Inbound<RPC>) {
        if cfg!(debug_assertions) {
            let id: u8 = self.id.into();
            assert_eq!(id, RPC::ID);
        }

        let rpc = Inbound {
            request_stream: RequestStream {
                recv_stream: self.stream.rx,
                codec: Default::default(),
            },
            response_sink: ResponseSink {
                send_stream: self.stream.tx,
                codec: Default::default(),
                conn: self.conn.inner,
                rpc_name: self.id.into(),
            },
            _permit: self.permit,
            shutdown_signal: self.shutdown_signal,
        };

        (self.conn.api, self.conn.rpc_handler, rpc)
    }
}

impl<API: Api> InboundRpc<API> {
    /// Returns ID of this [`InboundRpc`].
    pub fn id(&self) -> API::RpcId {
        self.id
    }
}

/// Inbound RPC of a specific type.
pub struct Inbound<RPC: Rpc> {
    pub request_stream: RequestStream<RPC>,
    pub response_sink: ResponseSink<RPC>,

    /// [`ShutdownSignal`] of the RPC [`Server`].
    pub shutdown_signal: ShutdownSignal,

    _permit: OwnedSemaphorePermit,
}

/// Unary RPC responder.
pub struct Responder<RPC: Rpc> {
    rpc: Inbound<RPC>,
}

impl<RPC: Rpc> Responder<RPC> {
    /// Sends a response.
    pub fn respond<'a, T>(
        mut self,
        response: &'a T,
    ) -> impl Future<Output = Result<()>> + Send + use<'a, T, RPC>
    where
        T: BorrowedMessage<Owned = RPC::Response> + FallibleResponse,
        RPC::Codec: Serializer<T>,
    {
        async move { self.rpc.response_sink.send(response).await }
    }
}

impl<API: Api> Connection<API> {
    /// Returns [`Api`] of this [`Connection`].
    pub fn api(&self) -> &API {
        &self.api
    }

    /// Returns [`PeerId`] of the remote peer.
    pub fn remote_peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Returns [`IpAddr`] of the remote peer.
    pub fn remote_peer_addr(&self) -> IpAddr {
        self.inner.quic.remote_address().ip()
    }

    /// Handles this [`Connection`] by handling all [`InboundRpc`] using the
    /// provided `handler_fn`.
    pub async fn handle<Fut>(&self, handler_fn: fn(InboundRpc<API>) -> Fut) -> Result<()>
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let fut = async {
            loop {
                let rpc = self.accept_rpc().await?;
                async move { handler_fn(rpc).await }.spawn();
            }
        };

        // Stop accepting new inbound RPCs when server is shutting down.
        (fut, self.inner.shutdown_signal.wait().map(Ok))
            .race()
            .await
    }

    /// Handles the next [`InboundRpc`] using the provided `handler_fn`.
    pub async fn handle_rpc<F>(&self, f: impl FnOnce(InboundRpc<API>) -> F) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send,
    {
        let rpc = self.accept_rpc().await?;
        f(rpc).await
    }

    /// Accepts the next [`InboundRpc`].
    async fn accept_rpc(&self) -> Result<InboundRpc<API>> {
        loop {
            let (tx, mut rx) = self.inner.quic.accept_bi().await.map_err(Error::new)?;

            let Some(permit) = self.acquire_stream_permit() else {
                metrics::counter!(
                    "wcn_rpc_server_rpcs_dropped",
                    StringLabel<"server_name"> => self.inner.server_name
                )
                .increment(1);
                continue;
            };

            // when we receive a stream there's always at least some data in it
            let id = rx
                .read_u8()
                .now_or_never()
                .ok_or_else(|| ErrorInner::ReadRpcId)?
                .map_err(Error::new)?;

            let id = id.try_into().map_err(|_| ErrorInner::UnknownRpcId(id))?;

            return Ok(InboundRpc {
                id,
                stream: BiDirectionalStream::new(tx, rx),
                permit,
                shutdown_signal: self.inner.shutdown_signal.clone(),
                conn: self.clone(),
            });
        }
    }

    fn acquire_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!("wcn_rpc_server_available_rpc_permits", StringLabel<"server_name"> => self.inner.server_name)
            .set(self.inner.rpc_semaphore.available_permits() as f64);

        self.inner.rpc_semaphore.clone().try_acquire_owned().ok()
    }
}

/// RPC [`Server`] error.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(ErrorInner);

/// RPC [`Server`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    fn new(err: impl Into<ErrorInner>) -> Self {
        Self(err.into())
    }

    fn codec(err: impl StdError) -> Self {
        Self::new(ErrorInner::Codec(err.to_string()))
    }

    fn io(err: io::Error) -> Self {
        Self::new(ErrorInner::Io(err))
    }

    fn timeout() -> Self {
        Self::new(ErrorInner::Timeout)
    }

    fn is_unknown_api(&self) -> bool {
        matches!(self.0, ErrorInner::UnknownApi(_))
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
    #[error("Failed to generate TLS certificate: {0:?}")]
    GenCertificate(#[from] libp2p_tls::certificate::GenError),

    #[error("quinn::rustls: {0}")]
    Rustls(#[from] rustls::NoInitialCipherSuite),

    #[error("QUIC: {0}")]
    Quic(#[from] quic::Error),

    #[error(transparent)]
    ExtractPeerId(#[from] quic::ExtractPeerIdError),

    #[error("Connection: {0:?}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("Timeout establishing inbound connection")]
    ConnectionTimeout,

    #[error("IO: {0:?}")]
    Io(#[from] io::Error),

    #[error("Failed to read ConnectionHeader: {0:?}")]
    ReadHeader(#[from] quinn::ReadExactError),

    #[error("Unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u32),

    #[error("Unknown API: {0}")]
    UnknownApi(rpc::ApiName),

    #[error("Codec: {0}")]
    Codec(String),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("Failed to read RPC ID without blocking")]
    ReadRpcId,

    #[error("Unknown RPC ID: {0}")]
    UnknownRpcId(u8),

    #[error("Timeout")]
    Timeout,
}

impl metrics::Enum for ErrorKind {
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// [`Stream`] of inbound RPC requests.
#[pin_project(project = RequestStreamProj)]
pub struct RequestStream<RPC: Rpc> {
    #[pin]
    recv_stream: RecvStream,
    #[pin]
    codec: RPC::Codec,
}

impl<RPC: Rpc> Stream for RequestStream<RPC> {
    type Item = Result<RPC::Request>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let bytes = match ready!(self.as_mut().project().recv_stream.try_poll_next(cx)) {
            Some(res) => res.map_err(Error::io)?,
            None => return Poll::Ready(None),
        };

        let codec = self.as_mut().project().codec;

        Poll::Ready(Some(codec.deserialize(&bytes).map_err(Error::codec)))
    }
}

impl<RPC: Rpc> RequestStream<RPC> {
    /// Tries to receive the next RPC request.
    pub async fn try_next(&mut self) -> Result<RPC::Request> {
        self.next()
            .await
            .ok_or_else(|| Error::new(ErrorInner::StreamFinished))?
    }
}

/// [`Sink`] for outbound RPC requests.
#[pin_project(project = RequestSinkProj)]
pub struct ResponseSink<RPC: Rpc> {
    #[pin]
    send_stream: SendStream,
    #[pin]
    codec: RPC::Codec,

    conn: Arc<ConnectionInner>,
    rpc_name: &'static str,
}

impl<RPC: Rpc, T> Sink<&T> for ResponseSink<RPC>
where
    T: BorrowedMessage<Owned = RPC::Response> + FallibleResponse,
    RPC::Codec: Serializer<T>,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().send_stream.poll_ready(cx).map_err(Error::io)
    }

    fn start_send(mut self: Pin<&mut Self>, item: &T) -> Result<(), Self::Error> {
        let bytes = tokio_serde::Serializer::serialize(self.as_mut().project().codec, item)
            .map_err(Error::codec)?;

        self.as_mut()
            .project()
            .send_stream
            .start_send(bytes)
            .map_err(Error::io)?;

        if let Some(kind) = item.error_kind() {
            metrics::counter!("wcn_rpc_server_error_responses",
                StringLabel<"remote_addr", IpAddr> => &self.conn.quic.remote_address().ip(),
                StringLabel<"server_name"> => self.conn.server_name,
                StringLabel<"api", ApiName> => &self.conn.api_name,
                StringLabel<"rpc"> => self.rpc_name,
                StringLabel<"kind"> => kind
            )
            .increment(1);
        }

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().send_stream.poll_flush(cx).map_err(Error::io)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().project().send_stream.poll_flush(cx)).map_err(Error::io)?;
        self.project().send_stream.poll_close(cx).map_err(Error::io)
    }
}

impl<RPC: Rpc> ResponseSink<RPC> {
    /// Waits until this [`ResponseSink`] is closed.
    pub async fn wait_closed(&mut self) {
        self.send_stream.get_mut().stopped().map(drop).await
    }

    /// Sends all RPC responses from the provided [`Stream`] into this
    /// [`ResponseSink`].
    pub async fn send_all(&mut self, stream: impl Stream<Item = RPC::Response>) -> Result<()> {
        let mut stream = pin!(stream);

        // The loop is to make sure that we stop and drop the RPC as soon as the
        // `SendStream` is no longer available.
        // TODO: this is not ideal, as we aren't doing `Sink::send` concurrently
        // with `Stream::next`
        loop {
            tokio::select! {
                _ = self.wait_closed() => break Err(ErrorInner::StreamFinished.into()),

                data = stream.next() => match data {
                    Some(data) => self.send(&data).await?,
                    None => break Ok(()),
                }
            };
        }
    }
}
