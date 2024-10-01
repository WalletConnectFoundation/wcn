use {
    crate::{
        transport::{self, BiDirectionalStream, Handshake, NoHandshake, RecvStream, SendStream},
        Id as RpcId,
        Message,
        Result as RpcResult,
    },
    futures::{Future, SinkExt as _},
    libp2p::{identity::Keypair, Multiaddr, PeerId},
    std::io,
};

pub mod middleware;

/// Server config.
#[derive(Clone)]
pub struct Config {
    /// Server name. For metric purposes.
    pub name: &'static str,

    /// [`Multiaddr`] of the server.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Maximum allowed amount of concurrent connections.
    pub max_concurrent_connections: u32,

    /// Maximum allowed amount of concurrent RPCs.
    pub max_concurrent_rpcs: u32,
}

/// Info about an inbound connection.
#[derive(Debug, Clone)]
pub struct ConnectionInfo<H = ()> {
    /// [`PeerId`] of the remote peer.
    pub peer_id: PeerId,

    /// [`Multiaddr`] of the remote peer.
    pub remote_address: Multiaddr,

    /// Handshake data.
    pub handshake_data: H,
}

/// RPC server.
pub trait Server<H: Handshake = NoHandshake>: Clone + Send + Sync + 'static {
    /// Handles an inbound RPC.
    fn handle_rpc(
        &self,
        id: RpcId,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<H::Ok>,
    ) -> impl Future<Output = ()> + Send;
}

/// Marker trait that should accompany [`Server`] impls in order to blanket impl
/// the middleware extension traits.
pub trait Marker {}

/// RPC [`Server`] error.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum Error {
    /// Transport error.
    #[error(transparent)]
    Transport(#[from] transport::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Transport(err.into())
    }
}

/// RPC [`Server`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<const ID: RpcId, Req, Resp> super::Unary<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(Req) -> Fut,
        Fut: Future<Output = RpcResult<Resp>>,
    {
        let (mut rx, mut tx) = stream.upgrade::<Req, RpcResult<Resp>>();
        let req = rx.recv_message().await?;
        let resp = f(req).await;
        tx.send(resp).await?;
        Ok(())
    }
}

impl<const ID: RpcId, Req, Resp> super::Streaming<ID, Req, Resp>
where
    Req: Message,
    Resp: Message,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(RecvStream<Req>, SendStream<RpcResult<Resp>>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let (rx, tx) = stream.upgrade();
        f(rx, tx).await
    }
}

impl<const ID: RpcId, Msg> super::Oneshot<ID, Msg>
where
    Msg: Message,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(Msg) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (mut rx, _) = stream.upgrade::<Msg, ()>();
        let req = rx.recv_message().await?;
        f(req).await;
        Ok(())
    }
}
