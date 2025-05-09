use {
    crate::{
        transport::{
            self,
            BiDirectionalStream,
            Codec,
            Handshake,
            HandshakeData,
            NoHandshake,
            RecvStream,
            SendStream,
        },
        Id as RpcId,
        Message,
        Result as RpcResult,
        ServerName,
    },
    futures::{Future, SinkExt as _},
    libp2p::{Multiaddr, PeerId},
    std::io,
};

pub mod middleware;

/// Server config.
#[derive(Clone, Debug)]
pub struct Config<H = NoHandshake> {
    /// Name of the server.
    pub name: ServerName,

    /// [`Handshake`] implementation of the server.
    pub handshake: H,
}

/// Info about an inbound connection.
#[derive(Debug, Clone)]
pub struct ConnectionInfo<H, S> {
    /// [`PeerId`] of the remote peer.
    pub peer_id: PeerId,

    /// [`Multiaddr`] of the remote peer.
    pub remote_address: Multiaddr,

    /// Handshake data.
    pub handshake_data: H,

    /// Local data storage for stateful connections.
    pub storage: S,
}

pub type ClientConnectionInfo<S> =
    ConnectionInfo<HandshakeData<<S as Server>::Handshake>, <S as Server>::ConnectionData>;

/// RPC server.
pub trait Server: Clone + Send + Sync + 'static {
    /// [`Handshake`] implementation of this RPC [`Server`].
    type Handshake: Handshake;

    /// Local data storage for stateful connections.
    type ConnectionData: Default + Clone + Send + Sync;

    /// Serialization codec.
    type Codec: Codec;

    /// Returns [`Config`] of this [`Server`].
    fn config(&self) -> &Config<Self::Handshake>;

    /// Handles an inbound RPC.
    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a;
}

/// Into [`Server`] converter.
pub trait IntoServer {
    type Server: Server;

    /// Converts `self` into [`Server`].
    fn into_rpc_server(self) -> Self::Server;
}

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

impl<const ID: RpcId, Req, Resp, C> super::Unary<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message,
    C: Codec,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(Req) -> Fut,
        Fut: Future<Output = RpcResult<Resp>>,
    {
        let (mut rx, mut tx) = stream.upgrade::<Req, RpcResult<Resp>, C>();
        let req = rx.recv_message().await?;
        let resp = f(req).await;
        tx.send(&resp).await?;
        Ok(())
    }
}

impl<const ID: RpcId, Req, Resp, C> super::Streaming<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message,
    C: Codec,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(RecvStream<Req, C>, SendStream<RpcResult<Resp>, C>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let (rx, tx) = stream.upgrade();
        f(rx, tx).await
    }
}

impl<const ID: RpcId, Msg, C> super::Oneshot<ID, Msg, C>
where
    Msg: Message,
    C: Codec,
{
    pub async fn handle<F, Fut>(stream: BiDirectionalStream, f: F) -> Result<()>
    where
        F: FnOnce(Msg) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (mut rx, _) = stream.upgrade::<Msg, (), C>();
        let req = rx.recv_message().await?;
        f(req).await;
        Ok(())
    }
}
