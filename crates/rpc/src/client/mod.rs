use {
    crate::{
        kind,
        transport::{self, BiDirectionalStream, NoHandshake, RecvStream, SendStream},
        Error as RpcError,
        ForceSendFuture,
        Id as RpcId,
        Message,
        Rpc,
    },
    derive_more::{derive::Display, From},
    futures::{Future, SinkExt as _},
    libp2p::{identity, Multiaddr},
    std::{collections::HashSet, io, time::Duration},
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
    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok: Send>(
        &'a self,
        addr: &'a A,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a;

    /// Sends an unary RPC.
    fn send_unary<'a, RPC: Rpc<Kind = kind::Unary>>(
        &'a self,
        addr: &'a A,
        request: &'a RPC::Request,
    ) -> impl Future<Output = Result<RPC::Response>> + Send + 'a {
        async move {
            self.send_rpc(addr, RPC::ID, &move |stream| async move {
                let (mut rx, mut tx) = stream.upgrade::<RpcResult<RPC>, RPC::Request>();
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
        f: &'a (impl Fn(SendStream<RPC::Request>, RecvStream<RpcResult<RPC>>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a
    where
        Fut: Future<Output = Result<Ok>> + Send,
    {
        async move {
            self.send_rpc(addr, RPC::ID, &move |stream| async move {
                let (rx, tx) = stream.upgrade::<RpcResult<RPC>, RPC::Request>();
                f(tx, rx).await.map_err(Into::into)
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
                let (_, mut tx) = stream.upgrade::<RPC::Response, RPC::Request>();
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

#[derive(Clone, Debug, thiserror::Error, From, Eq, PartialEq)]
pub enum Error {
    /// Transport error.
    #[error(transparent)]
    Transport(transport::Error),

    /// RPC error.
    #[error("{source} RPC: {error:?}")]
    Rpc {
        #[source]
        error: RpcError,
        source: RpcErrorSource,
    },
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

impl<const ID: RpcId, Req, Resp> super::Streaming<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub fn send<'a, A: Sync, F, Fut, Ok: Send>(
        client: &'a impl Client<A>,
        addr: &'a A,
        f: &'a F,
    ) -> impl Future<Output = Result<Ok>> + Send + 'a
    where
        F: Fn(SendStream<Req>, RecvStream<RpcResult<Self>>) -> Fut + Send + Sync + 'a,
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
