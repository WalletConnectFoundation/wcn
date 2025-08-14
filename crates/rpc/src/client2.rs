use {
    crate::{
        metrics::{rpc_id_label, FallibleResponse},
        quic::{self},
        transport,
        transport2::{BiDirectionalStream, RecvStream, SendStream, Serializer},
        ApiName,
        BorrowedMessage,
        CodecV2,
        ConnectionStatus,
        MessageV2,
        RpcImpl,
        RpcV2,
    },
    derive_where::derive_where,
    futures::{FutureExt, Sink, SinkExt, Stream, StreamExt as _, TryStream as _},
    libp2p::{identity, PeerId},
    pin_project::pin_project,
    std::{
        error::Error as StdError,
        future::Future,
        io,
        marker::PhantomData,
        net::{SocketAddr, SocketAddrV4},
        pin::{pin, Pin},
        sync::Arc,
        task::{self, ready, Poll},
        time::Duration,
    },
    strum::{EnumDiscriminants, IntoDiscriminant, IntoStaticStr},
    tap::{Pipe as _, TapFallible as _},
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt},
        sync::{watch, Mutex},
    },
    tokio_serde::Deserializer,
    tracing::Instrument,
    wc::{
        future::FutureExt as _,
        metrics::{
            self,
            enum_ordinalize::Ordinalize,
            future_metrics,
            EnumLabel,
            FutureExt as _,
            StringLabel,
        },
    },
};

/// Client-specific part of an RPC [Api][`super::Api`].
pub trait Api: super::Api + Sized {
    /// Outbound [`Connection`] parameters.
    type ConnectionParameters: Clone + Send + Sync + 'static;

    /// Handles the provided outbound [`Connection`].
    fn handle_connection(
        _conn: &Connection<Self>,
        _params: &Self::ConnectionParameters,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Tries to convert this [`Api`] into [`Client`].
    fn try_into_client(self, config: Config) -> Result<Client<Self>, Error> {
        Client::new(config, self)
    }
}

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

    api: API,
}

impl<API: Api> Client<API> {
    /// Creates a new RPC [`Client`].
    pub fn new(cfg: Config, api: API) -> Result<Self, Error> {
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
            api,
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

            API::handle_connection(&conn, params)
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
        conn.reconnect(self.config.reconnect_interval);
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
            }),
        }
    }
}

impl<const ID: u8, Req, Resp, C> RpcImpl<ID, Req, Resp, C>
where
    Req: MessageV2,
    Resp: MessageV2 + FallibleResponse,
    C: CodecV2<Req> + CodecV2<Resp>,
{
    /// Sends the request over the provided [`Connection`] and waits for a
    /// response.
    pub async fn send_request<API: Api, R>(conn: &Connection<API>, req: R) -> Result<Resp>
    where
        R: BorrowedMessage<Owned = Req>,
        C: Serializer<R>,
    {
        conn.send_request::<Self, _>(req).await
    }

    /// Sends this RPC over the provided [`Connection`].
    pub async fn send<API: Api, Out>(
        conn: &Connection<API>,
        f: impl AsyncFnOnce(Outbound<API, Self>) -> Result<Out>,
    ) -> Result<Out> {
        conn.send::<Self, _>(f).await
    }
}

/// Outbound RPC of a specific type.
pub struct Outbound<API: Api, RPC: RpcV2> {
    pub request_sink: RequestSink<API, RPC>,
    pub response_stream: ResponseStream<API, RPC>,
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

        self.reconnect(self.inner.client.config.reconnect_interval);

        drop(watch_rx.changed().await)
    }

    /// Sends the request over this [`Connection`] and waits for a
    /// response.
    pub async fn send_request<RPC: RpcV2, R>(&self, req: R) -> Result<RPC::Response>
    where
        R: BorrowedMessage<Owned = RPC::Request>,
        RPC::Codec: Serializer<R>,
    {
        self.send::<RPC, _>(|rpc: Outbound<API, _>| async {
            pin!(rpc.request_sink).send(req).await?;
            pin!(rpc.response_stream)
                .next()
                .await
                .ok_or_else(|| Error::new(ErrorInner::StreamFinished))?
        })
        .await
    }

    /// Sends the provided RPC over this [`Connection`].
    pub async fn send<RPC: RpcV2, Out>(
        &self,
        f: impl AsyncFnOnce(Outbound<API, RPC>) -> Result<Out>,
    ) -> Result<Out> {
        let future_metrics = future_metrics!("wcn_rpc_client_outbound_rpc",
            StringLabel<"remote_addr", SocketAddrV4> => self.remote_peer_addr(),
            StringLabel<"api", ApiName> => &API::NAME,
            StringLabel<"rpc"> => rpc_id_label::<API>(RPC::ID)
        );

        let err_counter = |err: &Error| {
            metrics::counter!(
                "wcn_rpc_client_outbound_rpc_errors",
                EnumLabel<"kind", ErrorKind> => err.0.discriminant(),
                StringLabel<"remote_addr", SocketAddrV4> => self.remote_peer_addr(),
                StringLabel<"api", ApiName> => &API::NAME,
                StringLabel<"rpc"> => rpc_id_label::<API>(RPC::ID)
            )
        };

        async {
            let rpc_id = RPC::ID
                .try_into()
                .map_err(|_| Error::new(ErrorInner::InvalidRpcId(RPC::ID)))?;

            let rpc = self.send_::<RPC>()?;
            let fut = f(rpc);

            if let Some(timeout) = self.inner.client.api.rpc_timeout(rpc_id) {
                fut.with_timeout(timeout)
                    .await
                    .map_err(|_| Error::new(ErrorInner::Timeout))?
            } else {
                fut.await
            }
        }
        .with_metrics(future_metrics)
        .await
        .tap_err(|err| err_counter(err).increment(1))
    }

    /// Sends the provided RPC over this [`Connection`].
    fn send_<RPC: RpcV2>(&self) -> Result<Outbound<API, RPC>> {
        self.new_outbound_rpc::<RPC>()
            .tap_err(|err| {
                if let Some(interval) = err.requires_reconnect(self.config().reconnect_interval) {
                    self.reconnect(interval);
                }
            })
            .map_err(Into::into)
    }

    fn new_outbound_rpc<RPC: RpcV2>(&self) -> Result<Outbound<API, RPC>, ErrorInner> {
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

        let stream = BiDirectionalStream::new(tx, rx);

        Ok(Outbound {
            request_sink: RequestSink {
                send_stream: stream.tx,
                codec: Default::default(),
                _marker: PhantomData,
            },
            response_stream: ResponseStream {
                recv_stream: stream.rx,
                codec: Default::default(),
                conn: self.clone(),
            },
        })
    }

    fn reconnect(&self, interval: Duration) {
        // If we can't acquire the lock then reconnection is already in progress.
        let Ok(guard) = self.inner.watch_tx.clone().try_lock_owned() else {
            return;
        };

        let this = self.inner.clone();

        async move {
            let mut interval = tokio::time::interval(interval);
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
        }
        .instrument(tracing::debug_span!("reconnect", api = %API::NAME))
        .pipe(tokio::spawn);
    }

    fn config(&self) -> &Config {
        &self.inner.client.config
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

    fn codec(err: impl StdError) -> Self {
        Self::new(ErrorInner::Codec(err.to_string()))
    }

    fn io(err: io::Error) -> Self {
        Self::new(ErrorInner::Io(err))
    }

    fn wrong_response(err: impl StdError) -> Self {
        Self::new(ErrorInner::WrongResponse(format!("{err:?}")))
    }

    /// If this [`Error`] represents an outbound [`Connection`] being
    /// rejected by the server, returns the reason.
    pub fn connection_rejection_reason(&self) -> Option<u8> {
        match self.0 {
            ErrorInner::ConnectionRejected { reason } => Some(reason),
            _ => None,
        }
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

    #[error("Codec: {0}")]
    Codec(String),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("Wrong response: {_0}")]
    WrongResponse(String),

    #[error("Unknown ConnectionStatusCode({0})")]
    UnknownConnectionStatusCode(i32),

    #[error("Unsupported protocol")]
    UnsupportedProtocol,

    #[error("Unknown API")]
    UnknownApi,

    #[error("Invalid RPC ID: {0}")]
    InvalidRpcId(u8),

    #[error("Connection rejected (reason: {reason})")]
    ConnectionRejected { reason: u8 },
}

impl ErrorInner {
    /// If reconnect is required returns the reconnect interval to use.
    fn requires_reconnect(&self, configured_interval: Duration) -> Option<Duration> {
        match self {
            ErrorInner::NotConnected
            | ErrorInner::Quic(_)
            | ErrorInner::Connect(_)
            | ErrorInner::Connection(_)
            | ErrorInner::Io(_)
            | ErrorInner::Write(_)
            | ErrorInner::Timeout => Some(configured_interval),

            // These errors are theoretically transient, so we want to retry them.
            // However we don't want to retry them too aggressively.
            // So if the configured retry interval is small we override it.
            ErrorInner::ExtractPeerId(_)
            | ErrorInner::WrongPeerId(_)
            | ErrorInner::UnknownConnectionStatusCode(_)
            | ErrorInner::UnsupportedProtocol
            | ErrorInner::UnknownApi
            | ErrorInner::ConnectionRejected { .. } => {
                Some(Duration::from_secs(1).max(configured_interval))
            }

            // These errors don't break the connection.
            ErrorInner::Codec(_)
            | ErrorInner::WrongResponse(_)
            | ErrorInner::StreamFinished
            | ErrorInner::TooManyConcurrentRpcs
            | ErrorInner::InvalidRpcId(_)
            | ErrorInner::SendBufferFull => None,
        }
    }
}

fn check_connection_status(code: i32) -> Result<(), ErrorInner> {
    let code = ConnectionStatus::try_from(code)
        .ok_or_else(|| ErrorInner::UnknownConnectionStatusCode(code))?;

    Err(match code {
        ConnectionStatus::Ok => return Ok(()),
        ConnectionStatus::UnsupportedProtocol => ErrorInner::UnsupportedProtocol,
        ConnectionStatus::UnknownApi => ErrorInner::UnknownApi,
        ConnectionStatus::Unauthorized => ErrorInner::UnknownApi,
        ConnectionStatus::Rejected { reason } => ErrorInner::ConnectionRejected { reason },
    })
}

impl metrics::Enum for ErrorKind {
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// [`Sink`] for outbound RPC requests.
#[pin_project(project = RequestSinkProj)]
pub struct RequestSink<API: Api, RPC: RpcV2> {
    #[pin]
    send_stream: SendStream,
    #[pin]
    codec: RPC::Codec,

    _marker: PhantomData<API>,
}

impl<API: Api, RPC: RpcV2, T> Sink<T> for RequestSink<API, RPC>
where
    T: BorrowedMessage<Owned = RPC::Request>,
    RPC::Codec: Serializer<T>,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().send_stream.poll_ready(cx).map_err(Error::io)
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let bytes = tokio_serde::Serializer::serialize(self.as_mut().project().codec, &item)
            .map_err(Error::codec)?;

        self.as_mut()
            .project()
            .send_stream
            .start_send(bytes)
            .map_err(Error::io)
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

/// [`Stream`] of inbound RPC responses.
#[pin_project(project = ResponseStreamProj)]
pub struct ResponseStream<API: Api, RPC: RpcV2> {
    #[pin]
    recv_stream: RecvStream,
    #[pin]
    codec: RPC::Codec,

    conn: Connection<API>,
}

impl<API: Api, RPC: RpcV2> Stream for ResponseStream<API, RPC> {
    type Item = Result<RPC::Response>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let bytes = match ready!(self.as_mut().project().recv_stream.try_poll_next(cx)) {
            Some(res) => res.map_err(Error::io)?,
            None => return Poll::Ready(None),
        };

        let codec = self.as_mut().project().codec;

        let res: Result<RPC::Response> = codec.deserialize(&bytes).map_err(Error::codec);

        if let Some(kind) = res.as_ref().ok().and_then(|res| res.error_kind()) {
            metrics::counter!("wcn_rpc_client_error_responses",
                StringLabel<"remote_addr", SocketAddrV4> => self.conn.remote_peer_addr(),
                StringLabel<"api", ApiName> => &API::NAME,
                StringLabel<"rpc"> => rpc_id_label::<API>(RPC::ID),
                StringLabel<"kind"> => kind
            )
            .increment(1);
        }

        Poll::Ready(Some(res))
    }
}

impl<API: Api, RPC: RpcV2> ResponseStream<API, RPC> {
    /// Tries to receive the next RPC response and downcasts it to the specified
    /// type `T`.
    pub async fn try_next_downcast<T>(&mut self) -> Result<T>
    where
        RPC::Response: TryInto<T, Error: StdError>,
    {
        self.next()
            .await
            .ok_or_else(|| Error::new(ErrorInner::StreamFinished))??
            .try_into()
            .map_err(Error::wrong_response)
    }

    /// Downcasts the RPC response type to `T`.
    pub fn map_downcast<T>(self) -> impl Stream<Item = Result<T>> + Send + 'static
    where
        RPC::Response: TryInto<T, Error: StdError>,
    {
        self.map(|res| match res {
            Ok(resp) => resp.try_into().map_err(Error::wrong_response),
            Err(err) => Err(err),
        })
    }
}
