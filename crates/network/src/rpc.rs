pub use kind::Kind;
use {
    crate::{
        outbound::{self, ConnectionHandlerError},
        BiDirectionalStream,
        Client,
        Handshake,
        Message,
        Metered,
        RecvStream,
        SendStream,
        WithTimeouts,
    },
    futures::{Future, SinkExt},
    libp2p::{Multiaddr, PeerId},
    std::{
        io,
        marker::{self, PhantomData},
        time::Duration,
    },
    wc::{future::FutureExt, future_metrics::FutureExt as _},
};

pub mod kind {
    /// RPC kind.
    pub trait Kind {
        /// Name of this RPC kind.
        const NAME: &'static str;
    }

    /// Unary (request-response) RPC.
    pub struct Unary;

    impl Kind for Unary {
        const NAME: &'static str = "unary";
    }

    /// RPC with bi-directional streaming.
    pub struct Streaming;

    impl Kind for Streaming {
        const NAME: &'static str = "streaming";
    }

    /// "Fire and forget" RPC, which sends a request and doesn't wait for any
    /// response.
    pub struct Oneshot;

    impl Kind for Oneshot {
        const NAME: &'static str = "oneshot";
    }
}

/// RPC identifier derived from a UTF-8 string of up to 16 bytes.
pub type Id = u128;

/// Builds [`Id`] from a byte slice.
///
/// Intended to be used in const contexts using `b"my_string"` notation.
///
/// # Panics
///
/// If the provided slice is larger than 16-bytes.
pub const fn id(bytes: &[u8]) -> Id {
    assert!(
        bytes.len() <= 16,
        "rpc::Id should be no longer than 16 bytes"
    );

    // loops aren't supported in const fns
    const fn copy(idx: usize, src: &[u8], mut dst: [u8; 16]) -> [u8; 16] {
        if idx == src.len() {
            return dst;
        }

        dst[idx] = src[idx];
        copy(idx + 1, src, dst)
    }

    u128::from_be_bytes(copy(0, bytes, [0u8; 16]))
}

/// RPC name derived from [`Id`].
pub struct Name([u8; 16]);

impl Name {
    /// Derives [`Name`] from the provided [`Id`].
    pub fn new(id: Id) -> Self {
        Self(id.to_be_bytes())
    }

    /// Returns UTF-8 representation of this [`Name`].
    pub fn as_str(&self) -> &str {
        match std::str::from_utf8(&self.0) {
            Ok(s) => s,
            Err(_) => "invalid",
        }
    }
}

/// RPC name derived from [`Id`] in comp time.
pub struct ConstName<const RPC_ID: Id>;

impl<const RPC_ID: Id> ConstName<RPC_ID> {
    pub const BYTES: [u8; 16] = RPC_ID.to_be_bytes();
    pub const STRING: &'static str = Self::as_str();

    /// Returns string representation of this [`Name`].
    const fn as_str() -> &'static str {
        match std::str::from_utf8(&Self::BYTES) {
            Ok(s) => s,
            Err(_) => "invalid",
        }
    }
}

/// Remote procedure call.
pub trait Rpc {
    /// [`RpcId`] of this [`Rpc`].
    const ID: Id;

    /// Name of this [`Rpc`].
    const NAME: &'static str;

    /// [`kind`] of this [`Rpc`].
    type Kind: Kind;
}

/// Send [`Rpc`] to a remote peer.
pub trait Send<Rpc, To, Args>: Sync {
    type Ok;

    /// Sends this [`Rpc`] to a remote peer.
    fn send(
        &self,
        to: To,
        args: Args,
    ) -> impl Future<Output = Result<Self::Ok, outbound::Error>> + marker::Send;
}

/// Handle [`Rpc`].
pub trait Handle<Rpc, Args> {
    /// Handles this [`Rpc`].
    async fn handle(self, args: Args) -> Result<()>;
}

/// [`Rpc`] callee (destination).
pub trait Callee<H>: marker::Send {
    fn establish_stream(
        &self,
        client: &Client<H>,
        rpc_id: Id,
    ) -> impl Future<Output = Result<BiDirectionalStream, ConnectionHandlerError>> + marker::Send;
}

impl<H: Handshake> Callee<H> for (&PeerId, &Multiaddr) {
    fn establish_stream(
        &self,
        client: &Client<H>,
        rpc_id: Id,
    ) -> impl Future<Output = Result<BiDirectionalStream, ConnectionHandlerError>> + marker::Send
    {
        client.establish_stream(self.0, self.1, rpc_id)
    }
}

/// [`Callee`] representing an arbitrary peer.
pub struct AnyPeer;

impl<H: Handshake> Callee<H> for AnyPeer {
    fn establish_stream(
        &self,
        client: &Client<H>,
        rpc_id: Id,
    ) -> impl Future<Output = Result<BiDirectionalStream, ConnectionHandlerError>> + marker::Send
    {
        client.establish_stream_any(rpc_id)
    }
}

/// Base [`Rpc`] implementation.
pub struct RpcImpl<const ID: Id, Kind, Req, Resp>(PhantomData<(Kind, Req, Resp)>);

impl<const ID: Id, Kind, Req, Resp> RpcImpl<ID, Kind, Req, Resp> {
    /// Handles this [`Rpc`].
    pub async fn handle<S: Handle<Self, Args>, Args>(stream: S, args: Args) -> Result<()> {
        stream.handle(args).await
    }

    /// Sends this [`Rpc`] to a remote peer.
    pub async fn send<C: Send<Self, To, Args>, To, Args>(
        client: &C,
        to: To,
        args: Args,
    ) -> Result<C::Ok, outbound::Error> {
        client.send(to, args).await
    }

    /// Same as [`RpcImpl::send`], but takes an owned version of the client.
    pub async fn send_owned<C: Send<Self, To, Args>, To, Args>(
        client: C,
        to: To,
        args: Args,
    ) -> Result<C::Ok, outbound::Error> {
        Self::send(&client, to, args).await
    }
}

impl<const ID: Id, K, Req, Resp> Rpc for RpcImpl<ID, K, Req, Resp>
where
    K: Kind,
{
    const ID: Id = ID;
    const NAME: &'static str = ConstName::<ID>::STRING;
    type Kind = K;
}

/// Unary (request-response) RPC.
pub type Unary<const ID: Id, Req, Resp> = RpcImpl<ID, kind::Unary, Req, Resp>;

impl<const ID: Id, Req, Resp, To, H> Send<Unary<ID, Req, Resp>, To, Req> for Client<H>
where
    Req: Message,
    Resp: Message,
    To: Callee<H>,
    H: marker::Send + Sync + 'static,
{
    type Ok = Resp;

    fn send(
        &self,
        to: To,
        req: Req,
    ) -> impl Future<Output = Result<Self::Ok, outbound::Error>> + marker::Send {
        async move {
            let (mut rx, mut tx) = to.establish_stream(self, ID).await?.upgrade::<Resp, Req>();

            tx.send(req).await?;
            Ok(rx.recv_message().await?)
        }
    }
}

impl<const ID: Id, F, Fut, Req, Resp> Handle<Unary<ID, Req, Resp>, F> for BiDirectionalStream
where
    Req: Message,
    Resp: Message,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Resp>,
{
    async fn handle(self, f: F) -> Result<()> {
        let (mut rx, mut tx) = self.upgrade::<Req, Resp>();
        let req = rx.recv_message().await?;
        let resp = f(req).await;
        tx.send(resp).await?;
        Ok(())
    }
}

/// RPC with bi-directional streaming.
pub type Streaming<const ID: Id, Req, Resp> = RpcImpl<ID, kind::Streaming, Req, Resp>;

impl<const ID: Id, F, Fut, Ok, Req, Resp, To, H> Send<Streaming<ID, Req, Resp>, To, F> for Client<H>
where
    Req: Message,
    Resp: Message,
    F: FnOnce(SendStream<Req>, RecvStream<Resp>) -> Fut + marker::Send,
    Fut: Future<Output = Result<Ok, Error>> + marker::Send,
    To: Callee<H>,
    H: marker::Send + Sync + 'static,
{
    type Ok = Ok;

    fn send(
        &self,
        to: To,
        f: F,
    ) -> impl Future<Output = Result<Self::Ok, outbound::Error>> + marker::Send {
        async move {
            let (rx, tx) = to.establish_stream(self, ID).await?.upgrade::<Resp, Req>();

            f(tx, rx).await.map_err(Into::into)
        }
    }
}

impl<const ID: Id, F, Fut, Req, Resp> Handle<Streaming<ID, Req, Resp>, F> for BiDirectionalStream
where
    Req: Message,
    Resp: Message,
    F: FnOnce(RecvStream<Req>, SendStream<Resp>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    async fn handle(self, f: F) -> Result<()> {
        let (rx, tx) = self.upgrade();
        f(rx, tx).await
    }
}

/// "Fire and forget" RPC, which sends a request and doesn't wait for any
/// response.
pub type Oneshot<const ID: Id, Msg> = RpcImpl<ID, kind::Oneshot, Msg, ()>;

impl<const ID: Id, To, Msg, H> Send<Oneshot<ID, Msg>, To, Msg> for Client<H>
where
    Msg: Message,
    To: Callee<H>,
    H: marker::Send + Sync + 'static,
{
    type Ok = ();

    fn send(
        &self,
        to: To,
        msg: Msg,
    ) -> impl Future<Output = Result<Self::Ok, outbound::Error>> + marker::Send {
        async move {
            let (_, mut tx) = to.establish_stream(self, ID).await?.upgrade::<(), Msg>();

            tx.send(msg).await?;
            Ok(())
        }
    }
}

impl<const ID: Id, F, Fut, Msg> Handle<Oneshot<ID, Msg>, F> for BiDirectionalStream
where
    Msg: Message,
    F: FnOnce(Msg) -> Fut,
    Fut: Future<Output = ()>,
{
    async fn handle(self, f: F) -> Result<()> {
        let (mut rx, _) = self.upgrade::<Msg, ()>();
        let req = rx.recv_message().await?;
        f(req).await;
        Ok(())
    }
}

/// Error of handing an [`Rpc`].
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum Error {
    #[error("IO: {0:?}")]
    IO(io::ErrorKind),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("RPC timed out")]
    Timeout,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::IO(e.kind())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const fn metric_labels<RPC: Rpc>(future_name: &'static str) -> [metrics::Label; 3] {
    [
        wc::future_metrics::future_name(future_name),
        metrics::Label::from_static_parts("rpc_kind", RPC::Kind::NAME),
        metrics::Label::from_static_parts("rpc_name", RPC::NAME),
    ]
}

impl<S, RPC, Args> Handle<RPC, Args> for Metered<S>
where
    RPC: Rpc,
    S: Handle<RPC, Args>,
{
    async fn handle(self, args: Args) -> Result<()> {
        self.inner
            .handle(args)
            .with_labeled_metrics(const { &metric_labels::<RPC>("inbound_rpc") })
            .await
    }
}

impl<C, RPC, To, Args> Send<RPC, To, Args> for Metered<C>
where
    RPC: Rpc,
    C: Send<RPC, To, Args>,
    To: marker::Send,
    Args: marker::Send,
{
    type Ok = C::Ok;

    async fn send(&self, to: To, args: Args) -> Result<Self::Ok, outbound::Error> {
        self.inner
            .send(to, args)
            .with_labeled_metrics(const { &metric_labels::<RPC>("outbound_rpc") })
            .await
    }
}

impl<S, RPC, Args> Handle<RPC, Args> for WithTimeouts<S>
where
    RPC: Rpc,
    S: Handle<RPC, Args>,
    Timeouts: GetTimeout<RPC::Kind>,
{
    async fn handle(self, args: Args) -> Result<()> {
        if let Some(timeout) = self.timeouts.get_timeout() {
            self.inner
                .handle(args)
                .with_timeout(timeout)
                .await
                .map_err(|_| Error::Timeout)?
        } else {
            self.inner.handle(args).await
        }
    }
}

impl<C, RPC, To, Args> Send<RPC, To, Args> for WithTimeouts<C>
where
    RPC: Rpc,
    C: Send<RPC, To, Args>,
    Args: marker::Send,
    To: marker::Send,
    Timeouts: GetTimeout<RPC::Kind>,
{
    type Ok = C::Ok;

    async fn send(&self, to: To, args: Args) -> Result<Self::Ok, outbound::Error> {
        if let Some(timeout) = self.timeouts.get_timeout() {
            self.inner
                .send(to, args)
                .with_timeout(timeout)
                .await
                .map_err(|_| Error::Timeout)?
        } else {
            self.inner.send(to, args).await
        }
    }
}

/// Inbound/outbound RPC timeouts.
#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub unary: Option<Duration>,
    pub streaming: Option<Duration>,
    pub oneshot: Option<Duration>,
}

/// Generic timeout getter for [`Timeouts`] struct.
pub trait GetTimeout<RpcKind> {
    /// Returns the configured timeout for the specified RPC [`Kind`].
    fn get_timeout(&self) -> Option<Duration>;
}

impl GetTimeout<kind::Unary> for Timeouts {
    fn get_timeout(&self) -> Option<Duration> {
        self.unary
    }
}

impl GetTimeout<kind::Streaming> for Timeouts {
    fn get_timeout(&self) -> Option<Duration> {
        self.streaming
    }
}

impl GetTimeout<kind::Oneshot> for Timeouts {
    fn get_timeout(&self) -> Option<Duration> {
        self.oneshot
    }
}
