use {
    super::Error,
    crate::{server, ConnectionHeader, InboundConnectionError, InboundConnectionResult},
    futures::{FutureExt, TryFutureExt as _},
    libp2p::{identity::Keypair, Multiaddr, PeerId},
    quinn::crypto::rustls::QuicServerConfig,
    std::{future::Future, net::SocketAddr, sync::Arc},
};

/// [`Acceptor`] config.
pub struct AcceptorConfig {
    /// [`Multiaddr`] to bind the server to.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Maximum allowed amount of concurrent streams.
    pub max_concurrent_streams: u32,
}

/// QUIC connection acceptor.
#[derive(Clone, Debug)]
pub struct Acceptor {
    address: Multiaddr,
    endpoint: quinn::Endpoint,
}

impl Acceptor {
    /// Creates a new [`Socket`] using the provided [`Config`].
    pub fn new(cfg: AcceptorConfig) -> Result<Self, Error> {
        let transport_config = super::new_quinn_transport_config(cfg.max_concurrent_streams);

        let server_tls_config = libp2p_tls::make_server_config(&cfg.keypair)
            .map_err(|err| Error::Tls(err.to_string()))?;
        let server_tls_config = QuicServerConfig::try_from(server_tls_config)
            .map_err(|err| Error::Tls(err.to_string()))?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_tls_config));
        server_config.transport = transport_config.clone();
        server_config.migration(false);

        let socket_addr = match super::multiaddr_to_socketaddr(&cfg.addr)? {
            SocketAddr::V4(v4) => SocketAddr::new([0, 0, 0, 0].into(), v4.port()),
            SocketAddr::V6(v6) => SocketAddr::new([0, 0, 0, 0, 0, 0, 0, 0].into(), v6.port()),
        };

        let endpoint = super::new_quinn_endpoint(
            socket_addr,
            &cfg.keypair,
            transport_config,
            Some(server_config),
        )?;

        Ok(Self {
            address: cfg.addr,
            endpoint,
        })
    }
}

impl server::Acceptor for Acceptor {
    fn address(&self) -> &Multiaddr {
        &self.address
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
        self.endpoint.accept().map(|opt| {
            opt.map(|incoming| async {
                let conn = incoming.accept()?.await?;
                let mut uni = conn.accept_uni().await?;
                let header = ConnectionHeader::read(&mut uni).await?;

                Ok(Connection {
                    header,
                    inner: conn,
                })
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct Connection {
    header: ConnectionHeader,
    inner: quinn::Connection,
}

impl server::InboundConnection for Connection {
    type Read = quinn::RecvStream;
    type Write = quinn::SendStream;

    fn header(&self) -> &ConnectionHeader {
        &self.header
    }

    fn peer_info(&self) -> InboundConnectionResult<(PeerId, Multiaddr)> {
        super::connection_peer_id(&self.inner)
            .map(|peer_id| {
                let addr = super::socketaddr_to_multiaddr(self.inner.remote_address());
                (peer_id, addr)
            })
            .map_err(|err| InboundConnectionError::ExtractPeerId(err.to_string()))
    }

    fn accept_stream(
        &mut self,
    ) -> impl Future<
        Output = server::Result<(Self::Read, Self::Write), server::InboundConnectionError>,
    > + Send {
        self.inner
            .accept_bi()
            .map_ok(|(tx, rx)| (rx, tx))
            .map_err(Into::into)
    }
}

impl From<quinn::ConnectionError> for server::InboundConnectionError {
    fn from(err: quinn::ConnectionError) -> Self {
        server::InboundConnectionError::Other {
            kind: super::connection_error_kind(&err),
            details: err.to_string(),
        }
    }
}
