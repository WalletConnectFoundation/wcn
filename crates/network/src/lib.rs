#![allow(async_fn_in_trait)]
#![allow(clippy::manual_async_fn)]

use {
    futures::{Future, Sink, StreamExt as _},
    indexmap::IndexMap,
    libp2p::identity,
    libp2p_tls::certificate,
    pin_project::pin_project,
    quinn::VarInt,
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        convert::Infallible,
        io,
        net::{SocketAddr, UdpSocket},
        pin::Pin,
        sync::{Arc, PoisonError},
        task,
        time::Duration,
    },
    tokio::sync::RwLock,
    tokio_serde::Framed,
    tokio_serde_postcard::SymmetricalPostcard,
    tokio_stream::Stream,
    tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
    wc::metrics::{Lazy, OtelTaskMetricsRecorder},
};
pub use {
    libp2p::{identity::Keypair, multihash::Multihash, Multiaddr, PeerId},
    quinn::{ConnectionError, WriteError},
    rpc::Rpc,
};

pub mod inbound;
pub mod outbound;

pub mod pubsub;
pub mod rpc;

#[cfg(test)]
mod test;

static METRICS: Lazy<OtelTaskMetricsRecorder> =
    Lazy::new(|| OtelTaskMetricsRecorder::new("irn_network"));

/// Message transimitted over the network.
pub trait Message: Serialize + for<'de> Deserialize<'de> + Unpin + Send {}
impl<M> Message for M where M: Serialize + for<'de> Deserialize<'de> + Unpin + Send {}

/// [`Client`] config.
#[derive(Clone)]
pub struct ClientConfig<H = NoHandshake> {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Known remote peer [`Multiaddr`]s.
    pub known_peers: HashMap<PeerId, Multiaddr>,

    /// [`Handshake`] implementation to use for connection establishment.
    pub handshake: H,

    /// Connection timeout.
    pub connection_timeout: Duration,
}

type OutboundConnectionHandlers<H> = IndexMap<PeerId, outbound::ConnectionHandler<H>>;

/// Network client.
#[derive(Clone, Debug)]
pub struct Client<H = NoHandshake> {
    peer_id: PeerId,
    endpoint: quinn::Endpoint,
    handshake: H,

    connection_handlers: Arc<RwLock<Arc<OutboundConnectionHandlers<H>>>>,
    connection_timeout: Duration,
}

impl<H> AsRef<Self> for Client<H> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<H: Handshake> Client<H> {
    /// Builds a new [`Client`] using the provided [`ClientConfig`].
    pub fn new(cfg: ClientConfig<H>) -> Result<Client<H>, Error> {
        let transport_config = new_quinn_transport_config();
        let socket_addr = SocketAddr::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0);
        let endpoint = new_quinn_endpoint(socket_addr, &cfg.keypair, transport_config, None)?;

        let local_peer_id = cfg.keypair.public().to_peer_id();

        let handlers = cfg
            .known_peers
            .into_iter()
            .filter_map(|(id, addr)| {
                (id != local_peer_id).then(|| multiaddr_to_socketaddr(addr).map(|a| (id, a)))
            })
            .map(|res| {
                res.map(|(id, addr)| {
                    let handler = outbound::ConnectionHandler::new(
                        addr,
                        endpoint.clone(),
                        cfg.handshake.clone(),
                        cfg.connection_timeout,
                    );
                    (id, handler)
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

    /// Registers a new peer.
    pub async fn register_peer(
        &self,
        id: PeerId,
        addr: Multiaddr,
    ) -> Result<(), RegisterPeerError> {
        if self.peer_id == id {
            return Ok(());
        }

        let addr = multiaddr_to_socketaddr(addr)?;

        self.connection_handlers_mut(|handlers| {
            handlers
                .entry(id)
                .or_insert_with(|| {
                    outbound::ConnectionHandler::new(
                        addr,
                        self.endpoint.clone(),
                        self.handshake.clone(),
                        self.connection_timeout,
                    )
                })
                .set_addr(addr)
        })
        .await?;

        Ok(())
    }

    /// Unregisters a peer.
    pub async fn unregister_peer(&self, id: PeerId) {
        self.connection_handlers_mut(|handlers| drop(handlers.shift_remove(&id)))
            .await;
    }

    // ad-hoc "copy-on-write" behaviour, the map changes infrequently and we don't
    // want to clone it in the hot path.
    async fn connection_handlers_mut<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut IndexMap<PeerId, outbound::ConnectionHandler<H>>) -> T,
    {
        let mut handlers = self.connection_handlers.write().await;
        let mut new = (**handlers).clone();
        let out = f(&mut new);
        *handlers = Arc::new(new);
        out
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterPeerError {
    #[error(transparent)]
    InvalidMultiaddr(#[from] InvalidMultiaddrError),

    #[error("Poisoned lock")]
    Lock,
}

impl<G> From<PoisonError<G>> for RegisterPeerError {
    fn from(_: PoisonError<G>) -> Self {
        Self::Lock
    }
}

/// Server config.
#[derive(Clone)]
pub struct ServerConfig {
    /// [`Multiaddr`] of the server.
    pub addr: Multiaddr,

    /// [`identity::Keypair`] of the server.
    pub keypair: identity::Keypair,
}

/// Runs an RPC server using the provided [`inbound::RpcHandler`].
pub fn run_server<H: Handshake>(
    cfg: ServerConfig,
    handshake: H,
    inbound_rpc_handler: impl inbound::RpcHandler<H::Ok>,
) -> Result<impl Future<Output = ()>, Error> {
    let transport_config = new_quinn_transport_config();

    let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_tls_config));
    server_config.transport = transport_config.clone();
    server_config.migration(false);

    let socket_addr = match multiaddr_to_socketaddr(cfg.addr.clone())? {
        SocketAddr::V4(v4) => SocketAddr::new([0, 0, 0, 0].into(), v4.port()),
        SocketAddr::V6(v6) => SocketAddr::new([0, 0, 0, 0, 0, 0, 0, 0].into(), v6.port()),
    };

    let endpoint = new_quinn_endpoint(
        socket_addr,
        &cfg.keypair,
        transport_config,
        Some(server_config),
    )?;

    Ok(inbound::handle_connections(
        endpoint,
        handshake,
        inbound_rpc_handler,
    ))
}

/// Error of [`Client::new`] and [`run_server`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to generate a TLS certificate: {0}")]
    Tls(#[from] certificate::GenError),

    #[error("failed to create, configure or bind a UDP socket")]
    Socket(#[from] io::Error),

    #[error(transparent)]
    InvalidMultiaddr(#[from] InvalidMultiaddrError),
}

#[derive(Debug, thiserror::Error)]
#[error("{0}: invalid QUIC Multiaddr")]
pub struct InvalidMultiaddrError(Multiaddr);

fn new_quinn_transport_config() -> Arc<quinn::TransportConfig> {
    const STREAM_WINDOW: u32 = 16 * 1024 * 1024; // 16 MiB

    // Our tests are too slow and connections get dropped because of missing keep
    // alive messages. Setting idle timeout higher for debug builds.
    let max_idle_timeout_ms = if cfg!(debug_assertions) { 5000 } else { 200 };

    let mut transport = quinn::TransportConfig::default();
    // Disable uni-directional streams.
    transport
            .max_concurrent_uni_streams(0u32.into())
            .max_concurrent_bidi_streams((128u32 * 1024).into())
            // Disable datagrams.
            .datagram_receive_buffer_size(None)
            .keep_alive_interval(Some(Duration::from_millis(100)))
            .max_idle_timeout(Some(VarInt::from_u32(max_idle_timeout_ms).into()))
            .allow_spin(false)
            .receive_window(VarInt::MAX)
            .stream_receive_window(STREAM_WINDOW.into())
            .send_window(8 * STREAM_WINDOW as u64);

    Arc::new(transport)
}

fn new_quinn_endpoint(
    socket_addr: SocketAddr,
    keypair: &Keypair,
    transport_config: Arc<quinn::TransportConfig>,
    server_config: Option<quinn::ServerConfig>,
) -> Result<quinn::Endpoint, Error> {
    let client_tls_config = libp2p_tls::make_client_config(keypair, None)?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(client_tls_config));
    client_config.transport_config(transport_config);

    let socket = new_udp_socket(socket_addr).map_err(Error::Socket)?;

    let mut endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        server_config,
        socket,
        Arc::new(quinn::TokioRuntime),
    )?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn new_udp_socket(addr: SocketAddr) -> io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    tracing::info!(udp_send_buffer_size = socket.send_buffer_size()?);
    tracing::info!(udp_recv_buffer_size = socket.recv_buffer_size()?);
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

/// Untyped bi-directional stream.
pub struct BiDirectionalStream {
    rx: RawRecvStream,
    tx: RawSendStream,
}

impl AsRef<BiDirectionalStream> for BiDirectionalStream {
    fn as_ref(&self) -> &BiDirectionalStream {
        self
    }
}

type RawSendStream = FramedWrite<quinn::SendStream, LengthDelimitedCodec>;
type RawRecvStream = FramedRead<quinn::RecvStream, LengthDelimitedCodec>;

impl BiDirectionalStream {
    fn new(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        Self {
            tx: FramedWrite::new(tx, LengthDelimitedCodec::new()),
            rx: FramedRead::new(rx, LengthDelimitedCodec::new()),
        }
    }

    fn upgrade<I, O>(self) -> (RecvStream<I>, SendStream<O>) {
        (
            RecvStream(Framed::new(self.rx, SymmetricalPostcard::default())),
            SendStream(Framed::new(self.tx, SymmetricalPostcard::default())),
        )
    }
}

/// [`Stream`] of outbound [`Message`]s.
#[pin_project]
pub struct SendStream<T>(#[pin] Framed<RawSendStream, T, T, SymmetricalPostcard<T>>);

impl<T: Serialize> Sink<T> for SendStream<T> {
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.project().0.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_close(cx)
    }
}

impl<T: Serialize + Unpin> SendStream<T> {
    /// Changes the type of this [`SendStream`].
    pub fn transmute<M>(self) -> SendStream<M> {
        SendStream(Framed::new(
            self.0.into_inner(),
            SymmetricalPostcard::default(),
        ))
    }

    /// Shut down the send stream gracefully.
    /// Completes when the peer has acknowledged all sent data.
    ///
    /// It's only required to call this if the [`RecvStream`] counterpart on the
    /// other side expects the [`Stream`] to be finished -- meaning to
    /// return `Poll::Ready(None)`.
    pub async fn finish(self) {
        let _ = self.0.into_inner().into_inner().finish().await;
    }
}

/// [`Stream`] of inbound [`Message`]s.
#[pin_project]
pub struct RecvStream<T>(#[pin] Framed<RawRecvStream, T, T, SymmetricalPostcard<T>>);

impl<T: for<'de> Deserialize<'de>> Stream for RecvStream<T> {
    type Item = io::Result<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<T> RecvStream<T>
where
    T: for<'de> Deserialize<'de> + Unpin,
{
    /// Tries to receive the next message from this [`RecvStream`].
    pub async fn recv_message(&mut self) -> Result<T, rpc::Error> {
        self.next()
            .await
            .ok_or(rpc::Error::StreamFinished)?
            .map_err(|e| rpc::Error::IO(e.kind()))
    }

    /// Changes the type of this [`RecvStream`].
    pub fn transmute<M>(self) -> RecvStream<M> {
        RecvStream(Framed::new(
            self.0.into_inner(),
            SymmetricalPostcard::default(),
        ))
    }
}

fn multiaddr_to_socketaddr(addr: Multiaddr) -> Result<SocketAddr, InvalidMultiaddrError> {
    try_multiaddr_to_socketaddr(&addr).ok_or_else(|| InvalidMultiaddrError(addr))
}

pub fn socketaddr_to_multiaddr(addr: SocketAddr) -> Multiaddr {
    use libp2p::multiaddr::Protocol;

    let mut result = Multiaddr::from(addr.ip());
    result.push(Protocol::Udp(addr.port()));
    result.push(Protocol::QuicV1);
    result
}

/// Tries to turn a QUIC multiaddress into a UDP [`SocketAddr`]. Returns None if
/// the format of the multiaddr is wrong.
fn try_multiaddr_to_socketaddr(addr: &Multiaddr) -> Option<SocketAddr> {
    use libp2p::multiaddr::Protocol;

    let mut iter = addr.iter();
    let proto1 = iter.next()?;
    let proto2 = iter.next()?;
    let proto3 = iter.next()?;

    match proto3 {
        Protocol::Quic | Protocol::QuicV1 => {}
        _ => return None,
    };

    Some(match (proto1, proto2) {
        (Protocol::Ip4(ip), Protocol::Udp(port)) => SocketAddr::new(ip.into(), port),
        (Protocol::Ip6(ip), Protocol::Udp(port)) => SocketAddr::new(ip.into(), port),
        _ => return None,
    })
}

/// Extension trait wrapping [`Client`]s or [`BiDirectionalStream`]s with
/// [`Metered`].
pub trait MeteredExt<T>: Sized {
    /// Wraps `Self` with [`Metered`].
    fn metered(self) -> Metered<Self> {
        Metered { inner: self }
    }
}

impl<C: AsRef<Client<H>>, H> MeteredExt<Client<H>> for C {}
impl<S: AsRef<BiDirectionalStream>> MeteredExt<BiDirectionalStream> for S {}

/// Metered [`Client`] or [`BiDirectionalStream`], enabling outbound and inbound
/// RPC metrics respectively.
#[derive(Clone, Debug)]
pub struct Metered<T> {
    inner: T,
}

impl<C, H> AsRef<Client<H>> for Metered<C>
where
    C: AsRef<Client<H>>,
{
    fn as_ref(&self) -> &Client<H> {
        self.inner.as_ref()
    }
}

impl<S> AsRef<BiDirectionalStream> for Metered<S>
where
    S: AsRef<BiDirectionalStream>,
{
    fn as_ref(&self) -> &BiDirectionalStream {
        self.inner.as_ref()
    }
}

/// Extension trait wrapping [`Client`]s or [`BiDirectionalStream`]s with
/// [`WithTimeouts`].
pub trait WithTimeoutsExt<T>: Sized {
    /// Wraps `Self` with [`WithTimeouts`].
    fn with_timeouts(self, timeouts: rpc::Timeouts) -> WithTimeouts<Self> {
        WithTimeouts {
            inner: self,
            timeouts,
        }
    }
}

impl<C: AsRef<Client<H>>, H> WithTimeoutsExt<Client<H>> for C {}
impl<S: AsRef<BiDirectionalStream>> WithTimeoutsExt<BiDirectionalStream> for S {}

/// [`Client`] or [`BiDirectionalStream`] with configured RPC timeouts.
#[derive(Clone, Debug)]
pub struct WithTimeouts<T> {
    inner: T,
    timeouts: rpc::Timeouts,
}

impl<C, H> AsRef<Client<H>> for WithTimeouts<C>
where
    C: AsRef<Client<H>>,
{
    fn as_ref(&self) -> &Client<H> {
        self.inner.as_ref()
    }
}

impl<S> AsRef<BiDirectionalStream> for WithTimeouts<S>
where
    S: AsRef<BiDirectionalStream>,
{
    fn as_ref(&self) -> &BiDirectionalStream {
        self.inner.as_ref()
    }
}

/// Connection state before the [`Handshake`].
pub struct PendingConnection(quinn::Connection);

impl PendingConnection {
    /// Initiates the [`Handshake`].
    pub async fn initiate_handshake<Req, Resp>(
        &self,
    ) -> Result<(RecvStream<Resp>, SendStream<Req>), ConnectionError> {
        let (tx, rx) = self.0.open_bi().await?;
        Ok(BiDirectionalStream::new(tx, rx).upgrade())
    }

    /// Accepts the [`Handshake`].
    pub async fn accept_handshake<Req, Resp>(
        &self,
    ) -> Result<(RecvStream<Req>, SendStream<Resp>), ConnectionError> {
        let (tx, rx) = self.0.accept_bi().await?;
        Ok(BiDirectionalStream::new(tx, rx).upgrade())
    }
}

/// Application layer protocol specific handshake.
pub trait Handshake: Clone + Send + Sync + 'static {
    type Ok: Clone + Send + Sync + 'static;
    type Err: std::error::Error + Send;

    fn handle(
        &self,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send;
}

/// No-op [`Handshake`] implementation.
#[derive(Clone, Debug, Default)]
pub struct NoHandshake;

impl Handshake for NoHandshake {
    type Ok = ();
    type Err = Infallible;

    fn handle(
        &self,
        _: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async { Ok(()) }
    }
}
