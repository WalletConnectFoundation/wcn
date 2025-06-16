use {
    crate::{
        self as rpc,
        quic,
        transport::{self, BiDirectionalStream, NoHandshake, RecvStream, SendStream},
        Message,
        ServerName,
    },
    futures::{Sink, SinkExt, Stream, TryStreamExt as _},
    libp2p::{identity, PeerId},
    std::{
        future::Future,
        io,
        marker::PhantomData,
        net::{SocketAddr, SocketAddrV4},
        sync::Arc,
        time::Duration,
    },
    tokio::io::AsyncWriteExt,
};

pub trait RpcSender<RPC, Args>: Send + Sync + 'static {
    type Output;

    fn send<'a>(
        &'a self,
        args: Args,
    ) -> impl Future<Output = RpcSenderResult<Self::Output>> + Send + 'a
    where
        Args: 'a;
}

pub struct Config {
    /// [`identity::Keypair`] of the client.
    pub keypair: identity::Keypair,

    /// Connection timeout.
    pub connection_timeout: Duration,

    /// [`transport::Priority`] of the client.
    pub priority: transport::Priority,
}

pub struct Client {
    config: Arc<Config>,
    quic: quinn::Endpoint,
}

impl Client {
    pub fn new(cfg: Config) -> Result<Self, Error> {
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
        })
    }

    pub fn connect<API: rpc::Api>(
        &self,
        addr: SocketAddrV4,
        peer_id: PeerId,
    ) -> OutboundConnection<API> {
        OutboundConnection {
            quic: quic::client::ConnectionHandler::new(
                peer_id,
                addr.into(),
                API::NAME,
                self.quic.clone(),
                NoHandshake,
                self.config.connection_timeout,
            ),
            _marker: PhantomData,
        }
    }
}

pub struct OutboundConnection<API> {
    quic: quic::client::ConnectionHandler,
    _marker: PhantomData<API>,
}

impl<const ID: u8, API, Request, Response, Codec, F, Fut, T>
    RpcSender<rpc::StreamingV2<ID, API, Request, Response, Codec>, F> for OutboundConnection<API>
where
    API: rpc::Api,
    Request: Message,
    Response: Message,
    Codec: transport::Codec,
    F: FnOnce(&mut SendStream<Request, Codec>, &mut RecvStream<Response, Codec>) -> Fut + Send,
    Fut: Future<Output = Result<T, transport::Error>> + Send,
{
    type Output = T;

    fn send<'a>(&'a self, f: F) -> impl Future<Output = RpcSenderResult<T>> + Send + 'a
    where
        F: 'a,
    {
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

            f(&mut tx, &mut rx)
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

pub struct OutboundRpc {
    stream: BiDirectionalStream,
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
