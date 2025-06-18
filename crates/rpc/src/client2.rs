use {
    crate::{
        self as rpc,
        quic,
        transport::{self, BiDirectionalStream, NoHandshake, RecvStream, SendStream},
        ConnectionHandler,
        Message,
        RpcV2,
        ServerName,
    },
    arc_swap::ArcSwap,
    futures::{future::Shared, Sink, SinkExt, Stream, TryStreamExt as _},
    libp2p::{identity, PeerId},
    std::{
        future::Future,
        io,
        marker::PhantomData,
        net::{SocketAddr, SocketAddrV4},
        pin::Pin,
        sync::Arc,
        time::Duration,
    },
    tokio::io::AsyncWriteExt,
};

pub trait Api: super::Api {
    type OutboundConnectionHandler: OutboundConnectionHandler;
    type OutboundRpcHandler;
}

pub trait OutboundConnectionHandler: Clone + Send + Sync + 'static {
    type Api;

    fn handle(
        &self,
        conn: &mut OutboundConnection<Self::Api>,
    ) -> impl Future<Output = InboundConnectionHandlerResult> + Send;
}

pub struct Config {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Connection timeout.
    pub connection_timeout: Duration,

    /// [`transport::Priority`] of the client.
    pub priority: transport::Priority,
}

pub struct Client<API: Api> {
    config: Arc<Config>,
    quic: quinn::Endpoint,
    connection_handler: API::OutboundConnectionHandler,
    rpc_handler: API::OutboundRpcHandler,
}

impl<API: Api> Client<API> {
    pub fn new(
        connection_handler: API::OutboundConnectionHandler,
        rpc_handler: API::OutboundRpcHandler,
        cfg: Config,
    ) -> Result<Self, Error> {
        let transport_config = quic::new_quinn_transport_config(64u32 * 1024);
        let socket_addr = SocketAddr::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0);
        let endpoint = quic::new_quinn_endpoint(
            socket_addr,
            &cfg.keypair,
            transport_config,
            None,
            cfg.priority,
        )
        .map_err(|err| Error::Transport(err.to_string()))?;

        Ok(Client {
            config: Arc::new(cfg),
            quic: endpoint,
            connection_handler,
            rpc_handler,
        })
    }

    pub fn connect(&self, addr: SocketAddrV4, peer_id: PeerId) -> OutboundConnection<API> {
        todo!()
    }
}

pub struct Outbound<RPC: RpcV2> {
    send: transport::SendStream<RPC::Request, RPC::Codec>,
    recv: transport::RecvStream<RPC::Response, RPC::Codec>,
}

pub struct OutboundConnection<API: Api> {
    inner: Arc<OutboundConnectionInner<API>>,
}

struct OutboundConnectionInner<API:Api> {
    quic: Arc<ArcSwap<Option<quinn::Connection>>>,

    mutex: Arc<tokio::sync::Mutex<()>>,
    connect_fut: tokio::task::JoinHandle<()>,
    
    rpc_handler: API::OutboundRpcHandler,
}

impl<API: Api> OutboundConnection<API> {
    fn new(addr: SocketAddrV4, peer_id: PeerId) -> Self {
        let quic = Arc::new(ArcSwap::new(Arc::new(None)));
        let mutex = Arc::new(tokio::sync::Mutex::new(()));

        let quic_clone = quic.clone();
        let mutex_guard = mutex.lock_owned();
        let connect_fut = tokio::spawn(async move {
            let _mutex_guard = mutex_guard;
            
        });
    }
    
    pub fn send<RPC: RpcV2>(&self) -> Outbound<RPC> {
        ArcSwap::
    }
}

impl<const ID: u8, API, Request, Response, Codec, H>
    RpcSender<rpc::StreamingV2<ID, API, Request, Response, Codec>, H> for OutboundConnection<API>
where
    API: rpc::Api,
    Request: Message,
    Response: Message,
    Codec: transport::Codec,
    H: RpcHandler<Request, Response>,
{
    type Output = H::Output;

    fn send(&self, handler: H) -> impl Future<Output = RpcSenderResult<Self::Output>> + Send + '_ {
        async move {
            let (mut tx, rx) = self
                .quic
                .open_bi()
                .await
                .map_err(|err| RpcSenderError::Transport(format!("open bi: {err:?}")))?;

            tx.write_u8(ID)
                .await
                .map_err(|err| RpcSenderError::Transport(format!("write rpc id: {err:?}")))?;

            let (mut rx, mut tx) =
                BiDirectionalStream::new(tx, rx).upgrade::<Response, Request, Codec>();

            // we use &mut here instead of passing owned values to force the implementor not
            // to move the values outside of the `RpcSender`, otherwise some
            // middleware implementations may work incorrectly.
            handler
                .handle(&mut tx, &mut rx)
                .await
                .map_err(|err| RpcSenderError::Transport(err.to_string()))
        }
    }
}

impl<const ID: u8, API, Request, Response, Codec>
    rpc::StreamingV2<ID, API, Request, Response, Codec>
where
    Request: Message,
    Response: Message,
    Codec: transport::Codec,
{
    pub async fn send<'a, S, F, Fut, T>(sender: &'a S, f: F) -> RpcSenderResult<S::Output>
    where
        S: RpcSender<Self, F>,
        F: FnOnce(&'a mut SendStream<Request, Codec>, &'a mut RecvStream<Response, Codec>) -> Fut
            + 'a,
        Fut: Future<Output = Result<T, transport::Error>>,
    {
        RpcSender::<Self, F>::send(sender, f).await
    }
}

impl<const ID: u8, API, Request, Response, Codec>
    RpcSender<rpc::UnaryV2<ID, API, Request, Response, Codec>, Request> for OutboundConnection<API>
where
    API: rpc::Api,
    Request: Message,
    Response: Message,
    Codec: transport::Codec,
{
    type Output = Response;

    fn send<'a>(
        &'a self,
        request: Request,
    ) -> impl Future<Output = RpcSenderResult<Response>> + Send + 'a {
        RpcSender::<rpc::StreamingV2<ID, API, Request, Response, Codec>, _>::send(
            self,
            |tx, rx| async {
                tx.send(request).await?;
                rx.recv_message().await
            },
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OutboundConnectionError {
    #[error("Transport: {_0}")]
    Transport(String),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Transport: {_0}")]
    Transport(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RpcSenderError {
    #[error("Transport: {0}")]
    Transport(String),
}

pub type RpcSenderResult<T = ()> = Result<T, RpcSenderError>;
