use {
    crate::{
        self as rpc,
        quic::{
            self,
            server::{
                filter::{self, Filter, RejectionReason},
                read_connection_header,
            },
        },
        transport::{self, BiDirectionalStream, RecvStream, SendStream},
        ApiName,
        RpcV2,
    },
    futures::{future::MapErr, sink::SinkMapErr, Sink, Stream, TryFutureExt as _},
    libp2p::{identity::Keypair, PeerId},
    quinn::crypto::rustls::{self, QuicServerConfig},
    sealed::ConnectionRouter,
    std::{future::Future, io, marker::PhantomData, sync::Arc, time::Duration},
    tokio::sync::{OwnedSemaphorePermit, Semaphore},
    wc::{
        future::FutureExt as _,
        metrics::{self, future_metrics, Enum as _, EnumLabel, StringLabel},
    },
};

/// Server-specific part of an RPC [`Api`](super::Api).
pub trait Api: super::Api {
    /// [`InboundConnectionHandler`] of this [`Api`].
    type InboundConnectionHandler: InboundConnectionHandler<Api = Self>;
}

/// Handler of newly established [`InboundConnection`]s.
///
/// Every time a new [`InboundConnection`] gets established it's being passed
/// into an [`InboundConnectionHandler`].
pub trait InboundConnectionHandler: Clone + Send + Sync + 'static {
    type Api;

    fn handle(
        &self,
        conn: &mut InboundConnection<Self::Api>,
    ) -> impl Future<Output = InboundConnectionHandlerResult> + Send;
}

/// Handler of a specific type of [`Inbound`] RPCs.
pub trait InboundRpcHandler<RPC>
where
    RPC: RpcV2,
{
    /// Handles the provided [`Outbound`] RPC.
    fn handle_rpc(&self, rpc: &mut Inbound<RPC>) -> impl Future<Output = Result<()>> + Send;
}

pub struct Config {
    /// Name of the server. For metrics purposes only.
    pub name: &'static str,

    /// [`Multiaddr`] to bind the server to.
    pub port: u16,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

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

/// Serves a single RPC API on the specified port.
pub fn serve(
    cfg: Config,
    connection_handler: impl InboundConnectionHandler,
) -> Result<impl Future<Output = ()>> {
    multiplex(cfg, (connection_handler,))
}

/// Serves multiple RPC APIs on the specified port.
///
/// `connection_handlers` is expected to be a tuple of
/// [`InboundConnectionHandler`]s.
pub fn multiplex<H>(cfg: Config, connection_handlers: H) -> Result<impl Future<Output = ()>>
where
    H: sealed::ConnectionRouter,
{
    let transport_config = quic::new_quinn_transport_config(cfg.max_concurrent_rpcs);
    let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair).map_err(Error::new)?;
    let server_tls_config = QuicServerConfig::try_from(server_tls_config).map_err(Error::new)?;
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
        connection_handlers,
    ))
}

struct Server {
    config: Config,
    endpoint: quinn::Endpoint,

    connection_filter: Filter,
    rpc_semaphore: Arc<Semaphore>,
}

async fn accept_connections<H: sealed::ConnectionRouter>(
    config: Config,
    endpoint: quinn::Endpoint,
    connection_filter: Filter,
    rpc_semaphore: Arc<Semaphore>,
    handlers: H,
) {
    while let Some(incoming) = endpoint.accept().await {
        match connection_filter.try_acquire_permit(&incoming) {
            Ok(permit) => match incoming.accept() {
                Ok(connecting) => accept_connection(
                    config.name,
                    connecting,
                    permit,
                    rpc_semaphore.clone(),
                    handlers.clone(),
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

fn accept_connection<R: ConnectionRouter>(
    server_name: &'static str,
    connecting: quinn::Connecting,
    permit: filter::Permit,
    rpc_semaphore: Arc<Semaphore>,
    router: R,
) {
    async move {
        let conn = connecting
            .with_timeout(Duration::from_millis(1000))
            .await
            .map_err(|_| ErrorInner::ConnectionTimeout)?
            .map_err(Error::new)?;

        conn.accept_uni().await.map_err(Error::new)?;

        let protocol_version = rx.read_u32().await?;

        let server_name = match protocol_version {
            super::PROTOCOL_VERSION => {
                let mut buf = [0; 16];
                rx.read_exact(&mut buf).await?;
                ServerName(buf)
            }
            ver => return Err(ConnectionError::UnsupportedProtocolVersion(ver)),
        };

        let header = read_connection_header(&conn)
            .with_timeout(Duration::from_millis(500))
            .await
            .map_err(|_| ErrorInner::ReadConnectionHeaderTimeout)??;

        let conn = InboundConnection {
            server_name,
            permit,
            rpc_semaphore,
            inner: conn,
            _marker: PhantomData,
        };

        router.route_connection(header.server_name, conn).await
    }
    .map_err(|err| tracing::debug!(?err, "Inbound connection handler failed"))
    .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_connection"))
    .pipe(tokio::spawn);
}

pub struct InboundConnection<API = ()> {
    server_name: &'static str,

    permit: filter::Permit,
    rpc_semaphore: Arc<Semaphore>,

    inner: quinn::Connection,

    _marker: PhantomData<API>,
}

/// Inbound RPC of a specific type.
pub struct Inbound<RPC: RpcV2> {
    recv: MapErr<RecvStream<RPC::Request, RPC::Codec>, fn(transport::Error) -> Error>,
    send: SinkMapErr<SendStream<RPC::Response, RPC::Codec>, fn(transport::Error) -> Error>,
}

impl<RPC: RpcV2> Inbound<RPC> {
    /// Returns [`Stream`] of inbound requests.
    pub fn stream(&mut self) -> &mut impl Stream<Item = Result<RPC::Request>> {
        &mut self.recv
    }

    /// Returns [`Sink`] of outbound responses.
    pub fn sink(&mut self) -> &mut impl Sink<RPC::Response, Error = Error> {
        &mut self.send
    }
}

impl InboundConnection {
    fn set_api<API>(self) -> InboundConnection<API> {
        InboundConnection {
            server_name: self.server_name,
            permit: self.permit,
            rpc_semaphore: self.rpc_semaphore,
            inner: self.inner,
            _marker: (),
        }
    }
}

impl<API: rpc::Api> InboundConnection<API> {
    pub fn peer_id(&self) -> PeerId {
        todo!()
    }

    pub async fn handle<F: Future<Output = RpcHandlerResult>>(
        &self,
        rpc_handler: impl Fn(InboundRpc<API::RpcId>) -> F,
    ) -> InboundConnectionHandlerResult {
        loop {
            match self.handle_rpc(rpc_handler).await {
                Ok(fut) => tokio::spawn(fut),
                Err(RpcHandlerError::TooManyRpc) => continue,
                Err(err) => return Err(err),
            };
        }
    }

    pub async fn handle_rpc<F: Future<Output = RpcHandlerResult>>(
        &self,
        rpc_handler: impl Fn(InboundRpc<API::RpcId>) -> F,
    ) -> InboundConnectionHandlerResult<impl Future<Output = RpcHandlerResult>> {
        let (tx, mut rx) = self.inner.accept_bi().await?;

        let Some(permit) = self.acquire_stream_permit() else {
            metrics::counter!(
                "wcn_rpc_quic_server_streams_dropped",
                StringLabel<"server_name"> => self.server_name
            )
            .increment(1);
            return Err(RpcHandlerError::TooManyRpc);
        };

        async move {
            let _permit = permit;

            let rpc = InboundRpc {
                id: None,
                stream: BiDirectionalStream::new(tx, rx),
            };

            rpc_handler.handle(&mut rpc).await
        }
        .with_metrics(future_metrics!("wcn_rpc_quic_server_inbound_stream"))
    }

    fn acquire_stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        metrics::gauge!("wcn_rpc_quic_server_available_stream_permits", StringLabel<"server_name"> => self.server_name)
            .set(self.stream_semaphore.available_permits() as f64);

        self.rpc_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .tap_none(|| {
                metrics::counter!("wcn_rpc_quic_server_throttled_streams", StringLabel<"server_name"> => self.server_name)
                    .increment(1);
            })
    }
}

async fn read_rpc_id(stream: &mut BiDirectionalStream) -> Result<u8, ReadRpcIdError> {
    stream
        .rx
        .read_u8()
        .with_timeout(Duration::from_millis(500))
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("{err:?}"))
}

mod sealed {
    use super::*;

    pub trait ConnectionRouter: Clone {
        fn route_connection(
            &self,
            api_name: ApiName,
            connection: InboundConnection,
        ) -> impl Future<Output = Result<()>>;
    }
}

impl<A> ConnectionRouter for (A,)
where
    A: InboundConnectionHandler,
{
    async fn route_connection(
        &self,
        api_name: ApiName,
        conn: InboundConnection,
    ) -> Result<(), InboundConnectionHandlerError> {
        async move {
            match &api_name {
                A::Api::NAME => self.0.handle_connection(&mut conn.set_api()),
                _ => Err(InboundConnectionHandlerError::UnknownApi(api_name)),
            }
        }
    }
}

impl<A, B> ConnectionRouter for (A, B)
where
    A: InboundConnectionHandler,
    B: InboundConnectionHandler,
{
    async fn route_connection(
        &self,
        api_name: ApiName,
        conn: InboundConnection,
    ) -> Result<(), InboundConnectionHandlerError> {
        async move {
            match &api_name {
                A::Api::NAME => self.0.handle_connection(&mut conn.set_api()),
                B::Api::NAME => self.1.handle_connection(&mut conn.set_api()),
                _ => Err(InboundConnectionHandlerError::UnknownApi(api_name)),
            }
        }
    }
}

impl<A, B, C> ConnectionRouter for (A, B, C)
where
    A: InboundConnectionHandler,
    B: InboundConnectionHandler,
    C: InboundConnectionHandler,
{
    async fn route_connection(
        &self,
        api_name: ApiName,
        conn: InboundConnection,
    ) -> Result<(), InboundConnectionHandlerError> {
        async move {
            match &api_name {
                A::Api::NAME => self.0.handle_connection(&mut conn.set_api()),
                B::Api::NAME => self.1.handle_connection(&mut conn.set_api()),
                C::Api::NAME => self.1.handle_connection(&mut conn.set_api()),
                _ => Err(InboundConnectionHandlerError::UnknownApi(api_name)),
            }
        }
    }
}

// #[derive(Debug, thiserror::Error)]
// pub enum Error {
//     #[error("Transport: {0}")]
//     Transport(String),

//     #[error("Crypto: {0}")]
//     Crypto(String),

//     #[error("Config: {0}")]
//     Config(String),
// }

#[derive(Debug, thiserror::Error)]
pub enum InboundConnectionHandlerError {
    #[error("Transport: {0}")]
    Transport(String),

    #[error("Timeout establishing inbound connection")]
    ConnectionTimeout,

    #[error("Timeout reading inbound connection header")]
    ReadConnectionHeaderTimeout,

    #[error("Unknown API: {0}")]
    UnknownApi(rpc::ApiName),
}

pub type InboundConnectionHandlerResult<T = ()> = Result<T, InboundConnectionHandlerError>;

#[derive(Debug, thiserror::Error)]
pub enum RpcHandlerError {
    #[error("Too many concurrent RPCs")]
    TooManyRpc,

    #[error("Read RPC ID: {0}")]
    ReadRpcId(#[from] ReadRpcIdError),

    #[error(transparent)]
    UnknownRpc(#[from] UnknownRpcError),

    #[error("Transport: {0}")]
    Transport(String),
}

pub type RpcHandlerResult<T = ()> = Result<T, RpcHandlerError>;

#[derive(Debug, thiserror::Error)]
pub enum ReadRpcIdError {
    #[error("IO: {0:?}")]
    IO(io::Error),

    #[error("Timeout")]
    Timeout,
}

#[derive(Debug, thiserror::Error)]
#[error("Unknown RPC (ID: {0})")]
pub struct UnknownRpcError(pub u8);

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

#[derive(Debug, thiserror::Error)]
enum ErrorInner {
    #[error("Failed to generate TLS certificate: {0:?}")]
    GenCertificate(#[from] libp2p_tls::certificate::GenError),

    #[error("quinn::rustls: {0}")]
    Rustls(#[from] rustls::NoInitialCipherSuite),

    #[error("QUIC: {0}")]
    Quic(#[from] quic::Error),

    #[error("Connection: {0:?}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("Timeout establishing inbound connection")]
    ConnectionTimeout,

    #[error("Timeout reading inbound connection header")]
    ReadConnectionHeaderTimeout,

    #[error("Unknown API: {0}")]
    UnknownApi(rpc::ApiName),

    #[error("Transport: {0}")]
    Transport(#[from] transport::Error),
}
