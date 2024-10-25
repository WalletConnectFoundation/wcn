use {
    crate::{
        transport::{
            self,
            BiDirectionalStream,
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
pub struct ConnectionInfo<H = ()> {
    /// [`PeerId`] of the remote peer.
    pub peer_id: PeerId,

    /// [`Multiaddr`] of the remote peer.
    pub remote_address: Multiaddr,

    /// Handshake data.
    pub handshake_data: H,
}

/// RPC server.
pub trait Server: Clone + Send + Sync + 'static {
    /// [`Handshake`] implementation of this RPC [`Server`].
    type Handshake: Handshake;

    /// Returns [`Config`] of this [`Server`].
    fn config(&self) -> &Config<Self::Handshake>;

    /// Handles an inbound RPC.
    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream,
        conn_info: &'a ConnectionInfo<HandshakeData<Self::Handshake>>,
    ) -> impl Future<Output = ()> + Send + '_;
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
        tx.send(&resp).await?;
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
