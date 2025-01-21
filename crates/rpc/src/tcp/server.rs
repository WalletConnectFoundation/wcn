use {
    super::{socketaddr_to_multiaddr, Error},
    crate::{
        server::{self, InboundConnectionError, InboundConnectionResult},
        tcp::connection_error_kind,
        ConnectionHeader,
    },
    futures::{
        future::BoxFuture,
        io::{ReadHalf, WriteHalf},
        AsyncReadExt as _,
        FutureExt,
    },
    libp2p::{identity::Keypair, Multiaddr, PeerId},
    std::{
        future::{self, Future},
        net::SocketAddr,
        sync::Arc,
        time::Duration,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::MissedTickBehavior,
    },
    tokio_rustls::{server::TlsStream, TlsAcceptor},
    tokio_stream::{wrappers::IntervalStream, StreamExt},
    tokio_util::compat::{
        Compat,
        FuturesAsyncReadCompatExt,
        FuturesAsyncWriteCompatExt,
        TokioAsyncReadCompatExt as _,
    },
    wc::future::FutureExt as _,
};

/// [`Acceptor`] config.
pub struct AcceptorConfig {
    /// Port to bind the server to.
    pub port: u16,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,
}

/// TCP connection acceptor.
#[derive(Debug)]
pub struct Acceptor {
    addr: Multiaddr,
    tls_config: Arc<rustls::ServerConfig>,
    inner: TcpListener,
}

impl Acceptor {
    /// Creates a new [`Acceptor`] using the provided [`Config`].
    pub async fn new(cfg: AcceptorConfig) -> Result<Self, Error> {
        let tls_config = libp2p_tls::make_server_config(&cfg.keypair)
            .map_err(|err| Error::Tls(err.to_string()))?;

        let socket = super::new_socket()?;
        let addr = SocketAddr::new([0, 0, 0, 0].into(), cfg.port);
        socket.bind(&addr.into())?;
        socket.set_reuse_address(true)?;
        socket.listen(1)?;

        let listener = TcpListener::from_std(std::net::TcpListener::from(socket))?;

        Ok(Self {
            addr: socketaddr_to_multiaddr(addr),
            tls_config: Arc::new(tls_config),
            inner: listener,
        })
    }
}

impl crate::Acceptor for Acceptor {
    fn address(&self) -> &Multiaddr {
        &self.addr
    }

    fn accept(
        &self,
    ) -> impl Future<
        Output = Option<
            impl Future<Output = InboundConnectionResult<impl server::InboundConnection>>
                + Send
                + 'static,
        >,
    > {
        let tls_config = self.tls_config.clone();

        async {
            loop {
                let (stream, addr) = match self.inner.accept().await {
                    Ok(ok) => ok,
                    Err(err) => {
                        tracing::warn!(?err, "Failed to accept TCP connection");
                        continue;
                    }
                };

                return Some(async move {
                    let mut stream = TlsAcceptor::from(tls_config).accept(stream).await?;

                    let header = ConnectionHeader::read(&mut stream).await?;

                    let peer_id = extract_peer_id(&stream)?;

                    let cfg = yamux::Config::default();
                    let mut conn =
                        yamux::Connection::new(stream.compat(), cfg, yamux::Mode::Server);

                    let keep_alive_stream =
                        future::poll_fn(|cx| conn.poll_new_outbound(cx)).await?;

                    Ok(InboundConnection {
                        header,
                        peer_id,
                        multiaddr: socketaddr_to_multiaddr(addr),
                        inner: conn,
                        keep_alive_fut: InboundConnection::keep_alive(keep_alive_stream).boxed(),
                    })
                });
            }
        }
    }
}

fn extract_peer_id<T>(conn: &TlsStream<T>) -> InboundConnectionResult<PeerId> {
    let certificate = conn
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.iter().next())
        .ok_or(InboundConnectionError::ExtractPeerId(
            "Missing TLS Certificate".to_string(),
        ))?;

    let peer_id = libp2p_tls::certificate::parse(certificate)
        .map_err(|err| {
            InboundConnectionError::ExtractPeerId(format!(
                "Failed to parse TLS certificate: {err:?}"
            ))
        })?
        .peer_id();

    Ok(peer_id)
}

pub struct InboundConnection {
    header: ConnectionHeader,
    peer_id: PeerId,
    multiaddr: Multiaddr,
    inner: yamux::Connection<Compat<TlsStream<TcpStream>>>,
    keep_alive_fut: BoxFuture<'static, InboundConnectionResult<()>>,
}

impl InboundConnection {
    async fn keep_alive(stream: yamux::Stream) -> InboundConnectionResult<()> {
        let mut stream = stream.compat_write();

        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut ticker = IntervalStream::new(interval);
        while ticker.next().await.is_some() {
            async {
                let code = rand::random();

                stream.write_u64(code).await?;
                if stream.read_u64().await? != code {
                    return Err(InboundConnectionError::other("invalid_keep_alive_response"));
                }

                Ok(())
            }
            .with_timeout(Duration::from_secs(2))
            .await
            .map_err(|_| InboundConnectionError::other("keep_alive_timeout"))??;
        }

        Ok(())
    }
}

impl crate::InboundConnection for InboundConnection {
    type Read = Compat<ReadHalf<yamux::Stream>>;
    type Write = Compat<WriteHalf<yamux::Stream>>;

    fn header(&self) -> &ConnectionHeader {
        &self.header
    }

    fn peer_info(&self) -> InboundConnectionResult<(PeerId, Multiaddr)> {
        Ok((self.peer_id, self.multiaddr.clone()))
    }

    fn accept_stream(
        &mut self,
    ) -> impl Future<
        Output = server::Result<(Self::Read, Self::Write), server::InboundConnectionError>,
    > + Send {
        async {
            tokio::select! {
                res = self.keep_alive_fut.as_mut() => match res {
                    Ok(()) => Err(InboundConnectionError::other("keep_alive_stream_finished")),
                    Err(err) => Err(err),
                },
                opt = future::poll_fn(|cx| self.inner.poll_next_inbound(cx)) => match opt {
                    Some(Ok(stream)) => {
                        let (rx, tx) = stream.split();
                        Ok((rx.compat(), tx.compat_write()))
                    },
                    Some(Err(err)) => Err(err.into()),
                    None => Err(yamux::ConnectionError::Closed.into()),
                }
            }
        }
    }
}

impl From<yamux::ConnectionError> for InboundConnectionError {
    fn from(err: yamux::ConnectionError) -> Self {
        use yamux::ConnectionError as Err;

        let kind = match &err {
            Err::Io(err) => return Self::IO(err.kind()),
            other => connection_error_kind(other),
        };

        InboundConnectionError::Other {
            kind,
            details: err.to_string(),
        }
    }
}
