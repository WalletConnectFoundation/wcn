use {
    crate::{
        kind,
        transport::{self, BiDirectionalStream, NoHandshake, RecvStream, SendStream},
        Id as RpcId,
        Message,
        Rpc,
    },
    derive_more::From,
    futures::{Future, SinkExt as _},
    libp2p::{identity, Multiaddr},
    std::{borrow::Cow, collections::HashSet, io, time::Duration},
};

pub mod middleware;

/// [`Client`] config.
#[derive(Clone)]
pub struct Config<H = NoHandshake> {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Known remote peer [`Multiaddr`]s.
    pub known_peers: HashSet<Multiaddr>,

    /// [`Handshake`] implementation to use for connection establishment.
    pub handshake: H,

    /// Connection timeout.
    pub connection_timeout: Duration,
}

/// RPC client.
pub trait Client<A: Sync = Multiaddr>: Send + Sync {
    /// Sends an outbound RPC.
    fn send_rpc<Fut: Future<Output = Result<Ok>> + Send, Ok>(
        &self,
        addr: &A,
        rpc_id: RpcId,
        f: impl FnOnce(BiDirectionalStream) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send;

    /// Sends an unary RPC.
    fn send_unary<RPC: Rpc<Kind = kind::Unary>>(
        &self,
        addr: &A,
        request: RPC::Request,
    ) -> impl Future<Output = Result<RPC::Response>> + Send {
        self.send_rpc(addr, RPC::ID, |stream| async {
            let (mut rx, mut tx) = stream.upgrade::<RPC::Response, RPC::Request>();
            tx.send(request).await?;
            Ok(rx.recv_message().await?)
        })
    }

    /// Sends a streaming RPC.
    fn send_streaming<RPC: Rpc<Kind = kind::Streaming>, Fut, Ok>(
        &self,
        addr: &A,
        f: impl FnOnce(SendStream<RPC::Request>, RecvStream<RPC::Response>) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send
    where
        Fut: Future<Output = Result<Ok>> + Send,
    {
        self.send_rpc(addr, RPC::ID, |stream| async {
            let (rx, tx) = stream.upgrade::<RPC::Response, RPC::Request>();
            f(tx, rx).await.map_err(Into::into)
        })
    }

    /// Sends a oneshot RPC.
    fn send_oneshot<RPC: Rpc<Kind = kind::Oneshot>>(
        &self,
        addr: &A,
        msg: RPC::Request,
    ) -> impl Future<Output = Result<()>> {
        self.send_rpc(addr, RPC::ID, |stream| async {
            let (_, mut tx) = stream.upgrade::<RPC::Response, RPC::Request>();
            tx.send(msg).await?;
            Ok(())
        })
    }
}

/// Marker trait that should accompany [`Client`] impls in order to blanket impl
/// the middleware extension traits.
pub trait Marker {}

#[derive(Clone, Debug, thiserror::Error, From, Eq, PartialEq)]
pub enum Error {
    /// Transport error.
    #[error(transparent)]
    Transport(transport::Error),

    /// RPC error.
    #[error("RPC: {_0:?}")]
    Rpc(RpcError),
}

impl Error {
    /// Creates a new client-side [`RpcError`] with the provided error code.
    pub fn rpc(code: &'static str) -> Self {
        RpcError::new(code).into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcError {
    pub code: Cow<'static, str>,
    pub description: Option<Cow<'static, str>>,
    source: RpcErrorSource,
}

impl RpcError {
    /// Creates a new client-side RPC error with the provided error code.
    pub fn new(code: &'static str) -> Self {
        Self {
            code: code.into(),
            description: None,
            source: RpcErrorSource::Client,
        }
    }

    pub fn source(&self) -> RpcErrorSource {
        self.source
    }
}

/// The source of an [`RpcError`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

impl<const ID: RpcId, Req, Resp> super::Unary<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub async fn send<A: Sync>(client: &impl Client<A>, addr: &A, req: Req) -> Result<Resp> {
        client.send_unary::<Self>(addr, req).await
    }
}

impl<const ID: RpcId, Req, Resp> super::Streaming<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub async fn send<A: Sync, F, Fut, Ok>(client: &impl Client<A>, addr: &A, f: F) -> Result<Ok>
    where
        F: FnOnce(SendStream<Req>, RecvStream<Resp>) -> Fut + Send,
        Fut: Future<Output = Result<Ok>> + Send,
    {
        client.send_streaming::<Self, _, _>(addr, f).await
    }
}

impl<const ID: RpcId, Msg> super::Oneshot<ID, Msg>
where
    Msg: Message,
{
    pub async fn send<A: Sync>(client: &impl Client<A>, addr: &A, msg: Msg) -> Result<()> {
        client.send_oneshot::<Self>(addr, msg).await
    }
}

/// Arbitrary remote peer.
pub struct AnyPeer;
