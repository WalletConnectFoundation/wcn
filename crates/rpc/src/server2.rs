use {
    crate::{
        self as rpc,
        quic::{
            self,
            server::filter::{self, Filter, RejectionReason},
        },
        transport,
        transport2::{self, BiDirectionalStream, RecvStream, SendStream},
        Api,
        ApiName,
        BorrowedResponse,
        ConnectionStatusCode,
        RpcV2,
        ServerName,
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
        StreamExt,
        TryFutureExt as _,
        TryStreamExt as _,
    },
    libp2p::{identity, PeerId},
    quinn::crypto::rustls::{self, QuicServerConfig},
    std::{future::Future, io, marker::PhantomData, sync::Arc, time::Duration},
    tap::Pipe as _,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    wc::{
        future::{FutureExt as _, StaticFutureExt},
        metrics::{self, future_metrics, Enum as _, EnumLabel, FutureExt as _, StringLabel},
    },
};

// TODO: Authorization, metrics, timeouts

/// Handler of newly established inbound [`Connection`]s.
pub trait HandleConnection: Clone + Send + Sync + 'static {
    /// RPC [`Api`] the connections of which are being handled.
    type Api: Api;

    /// Handles the provided inbound [`Connection`].
    fn handle_connection(
        &self,
        conn: Connection<'_, Self::Api>,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Handler of [`Inbound`] RPCs.
pub trait HandleRpc<RPC: RpcV2>: Send + Sync {
    /// Handles the provided [`Inbound`] RPC.
    fn handle_rpc<'a>(
        &'a self,
        rpc: &'a mut Inbound<RPC>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

// /// [`HandleRpc`] specialization for [`UnaryRpc`]s.
// pub trait HandleUnaryRpc<RPC: UnaryRpc>: Send + Sync {
//     /// Handles the provided RPC request.
//     fn handle_unary_rpc(
//         &self,
//         request: RPC::Request,
//         responder: Responder<'_, RPC>,
//     ) -> impl Future<Output = Result<()>> + Send;
// }

/// [`HandleRpc`] specialization for [`UnaryRpc`]s.
pub trait HandleRequest<RPC: UnaryRpc>: Send + Sync {
    fn handle_request(
        &self,
        request: RPC::Request,
    ) -> impl Future<Output = RPC::Response> + Send + '_;
}

// /// [`UnaryRpc`] responder.
// pub trait Responder<RPC: UnaryRpc>: Send {
//     /// Sends an RPC response.
//     fn respond(self, response: &BorrowedResponse<RPC>) -> impl Future<Output
// = Result<()>> + Send; }

// struct ResponderImpl<RPC: UnaryRpc> {
//     sink: SinkMapErr<SendStream<RPC::Codec>, fn(transport2::Error) -> Error>,
// }

// impl<'a, RPC: UnaryRpc> Responder<RPC> for ResponderImpl<'a, RPC> {
//     fn respond(self, response: &BorrowedResponse<RPC>) -> impl Future<Output
// = Result<()>> + Send {         self.sink.send(response)
//     }
// }

impl<RPC, H> HandleRpc<RPC> for H
where
    RPC: UnaryRpc,
    H: HandleRequest<RPC>,
{
    fn handle_rpc<'a>(
        &'a self,
        rpc: &'a mut Inbound<RPC>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async {
            let req = rpc
                .recv
                .next()
                .await
                .ok_or_else(|| Error::new(transport2::Error::StreamFinished))??;

            let resp = self.handle_request(req).await;

            rpc.send.send(&resp).await?;

            Ok(())
        }
    }
}

/// RPC server config.
pub struct Config {
    /// Name of the server. For metrics purposes only.
    pub name: &'static str,

    /// [`Multiaddr`] to bind the server to.
    pub port: u16,

    /// [`identity::Keypair`] of the server.
    pub keypair: identity::Keypair,

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
}

/// Creates a new RPC [`Api`] server.
pub fn new(connection_handler: impl HandleConnection) -> impl Server {
    ApiServer { connection_handler }
}

/// RPC server.
pub trait Server: Sized + Send + Sync + 'static
where
    Self: sealed::ConnectionRouter,
{
    /// Multiplexes `self` with another [`Server`].
    fn multiplex(self, api_server: impl Server) -> impl Server {
        Multiplexer {
            head: api_server,
            tail: self,
        }
    }

    /// Runs this RPC [`Server`]
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()>> {
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

        let connection_filter = Filter::new(&filter::Config {
            max_connections: cfg.max_connections,
            max_connections_per_ip: cfg.max_connections_per_ip,
            max_connection_rate_per_ip: cfg.max_connection_rate_per_ip,
        })
        .map_err(Error::new)?;

        let rpc_semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_rpcs as usize));

        Ok(accept_connections(
            cfg,
            endpoint,
            connection_filter,
            rpc_semaphore,
            self,
        ))
    }
}

mod sealed {
    use super::*;

    pub trait ConnectionRouter: Clone + Send + Sync + 'static {
        /// Routes [`Connection`] to the [`Api`] connection handler.
        fn route_connection(
            &self,
            api_name: &ApiName,
            conn: Connection<'_>,
        ) -> impl Future<Output = Option<Result<()>>> + Send;
    }
}

#[derive_where(Clone)]
struct ApiServer<H: HandleConnection> {
    connection_handler: H,
}

#[derive(Clone)]
struct Multiplexer<A, B> {
    head: A,
    tail: B,
}

impl<H: HandleConnection> sealed::ConnectionRouter for ApiServer<H> {
    async fn route_connection(
        &self,
        api_name: &ApiName,
        conn: Connection<'_>,
    ) -> Option<Result<()>> {
        if api_name != &H::Api::NAME {
            return None;
        }

        Some(
            self.connection_handler
                .handle_connection(conn.specify_api())
                .await,
        )
    }
}

impl<A, B> sealed::ConnectionRouter for Multiplexer<A, B>
where
    A: sealed::ConnectionRouter,
    B: sealed::ConnectionRouter,
{
    async fn route_connection(
        &self,
        api_name: &ApiName,
        conn: Connection<'_>,
    ) -> Option<Result<()>> {
        match self.head.route_connection(api_name, conn).await {
            opt @ Some(_) => opt,
            None => self.tail.route_connection(api_name, conn).await,
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
                ),

                Err(err) => tracing::warn!(?err, "failed to accept incoming connection"),
            },

            Err(err) => {
                if err == filter::RejectionReason::AddressNotValidated {
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
    permit: filter::Permit,
    rpc_semaphore: Arc<Semaphore>,
    router: R,
) {
    use ConnectionStatusCode as Status;

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
                    ServerName(buf)
                }
                ver => {
                    tx.write_i32(Status::UnsupportedProtocol as i32).await?;
                    return Err(ErrorInner::UnsupportedProtocolVersion(ver));
                }
            };

            let conn = ConnectionInner {
                server_name,
                _permit: permit,
                remote_peer_id,
                rpc_semaphore,
                quic: conn,
            };

            Ok((api_name, conn, tx))
        }
        .with_timeout(timeout)
        .map_err(|_| ErrorInner::ConnectionTimeout)
        .await?
        .map_err(Error::new)?;

        let conn = Connection {
            inner: &conn,
            _marker: PhantomData,
        };

        match router.route_connection(&api_name, conn).await {
            Some(res) => res,
            None => {
                tx.write_i32(ConnectionStatusCode::UnknownApi as i32)
                    .await
                    .map_err(Error::new)?;

                Err(ErrorInner::UnknownApi(api_name).into())
            }
        }
    }
    .map_err(|err| tracing::debug!(?err, "Inbound connection handler failed"))
    .with_metrics(future_metrics!("wcn_rpc_server_inbound_connection"))
    .pipe(tokio::spawn);
}

/// Inbound connection.
#[derive(Clone, Copy)]
pub struct Connection<'a, API = ()> {
    inner: &'a ConnectionInner,
    _marker: PhantomData<API>,
}

impl<'a> Connection<'a> {
    fn specify_api<API>(self) -> Connection<'a, API> {
        Connection {
            inner: self.inner,
            _marker: PhantomData,
        }
    }
}

struct ConnectionInner {
    server_name: &'static str,

    remote_peer_id: PeerId,

    _permit: filter::Permit,
    rpc_semaphore: Arc<Semaphore>,

    quic: quinn::Connection,
}

impl<'a, API: Api> From<&'a mut ConnectionInner> for Connection<'a, API> {
    fn from(inner: &'a mut ConnectionInner) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

/// Inbound RPC with yet undefined type.
pub struct InboundRpc<API: Api> {
    id: API::RpcId,
    stream: BiDirectionalStream,
    permit: OwnedSemaphorePermit,
}

impl<API: Api> InboundRpc<API> {
    /// Handles this RPC using the provided handler.
    pub fn handle<RPC: RpcV2>(
        self,
        handler: &impl HandleRpc<RPC>,
    ) -> impl Future<Output = Result<()>> + Send + '_ {
        async move { handler.handle_rpc(&mut self.upgrade()).await }
            .with_metrics(future_metrics!("wcn_rpc_server_rpc"))
    }

    /// Upgrades this untyped [`InboundRpc`] into a typed one.
    ///
    /// Caller is expected to ensure that the [`InboundRpc::id()`] is correct.
    fn upgrade<RPC: RpcV2>(self) -> Inbound<RPC> {
        if cfg!(debug_assertions) {
            let id: u8 = self.id.into();
            assert_eq!(id, RPC::ID);
        }

        let (recv, send) = self.stream.upgrade();

        Inbound {
            send: SinkExt::<&RPC::Response>::sink_map_err(send, |err: transport2::Error| {
                Error::new(err)
            }),
            recv: recv.map_err(Error::new),
            _permit: self.permit,
        }
    }
}

impl<API: super::Api> InboundRpc<API> {
    /// Returns ID of this [`InboundRpc`].
    pub fn id(&self) -> API::RpcId {
        self.id
    }
}

/// Inbound RPC of a specific type.
pub struct Inbound<RPC: RpcV2> {
    #[allow(clippy::type_complexity)]
    recv: MapErr<RecvStream<RPC::Request, RPC::Codec>, fn(transport2::Error) -> Error>,

    #[allow(clippy::type_complexity)]
    send: SinkMapErr<SendStream<RPC::Codec>, fn(transport2::Error) -> Error>,

    _permit: OwnedSemaphorePermit,
}

impl<RPC: RpcV2> Inbound<RPC> {
    /// Returns mutable references to the underlying request/response streams.
    pub fn streams_mut(
        &mut self,
    ) -> (
        &mut impl Stream<Item = Result<RPC::Request>>,
        &mut (impl for<'a, 'b> Sink<&'a BorrowedResponse<'b, RPC>, Error = Error> + 'static),
    ) {
        (&mut self.recv, &mut self.send)
    }
}

pub struct Responder<'a, RPC: UnaryRpc> {
    rpc: &'a mut Inbound<RPC>,
}

impl<'a, RPC: UnaryRpc> Responder<'a, RPC> {
    pub fn respond<'r>(
        self,
        response: &'r BorrowedResponse<'r, RPC>,
    ) -> impl Future<Output = Result<()>> + Send + 'r
    where
        'a: 'r,
    {
        self.rpc.send.send(response)
    }
}

impl<API: Api> Connection<'_, API> {
    /// Returns [`PeerId`] of the remote peer.
    pub fn remote_peer_id(&self) -> &PeerId {
        &self.inner.remote_peer_id
    }

    /// Handles this [`Connection`] by handling all [`InboundRpc`] using the
    /// provided `handler_fn`.
    pub async fn handle<H, Fut>(
        &self,
        rpc_handler: &H,
        handler_fn: fn(InboundRpc<API>, H) -> Fut,
    ) -> Result<()>
    where
        H: Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        loop {
            let rpc = self.accept_rpc().await?;
            let handler = rpc_handler.clone();

            async move { handler_fn(rpc, handler).await }.spawn();
        }
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
    pub async fn accept_rpc(&self) -> Result<InboundRpc<API>> {
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
}

impl From<ErrorInner> for Error {
    fn from(err: ErrorInner) -> Self {
        Self::new(err)
    }
}

#[derive(Debug, thiserror::Error)]
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

    #[error("Transport: {0}")]
    Transport(#[from] transport2::Error),

    #[error("Failed to read RPC ID without blocking")]
    ReadRpcId,

    #[error("Unknown RPC ID: {0}")]
    UnknownRpcId(u8),
}
