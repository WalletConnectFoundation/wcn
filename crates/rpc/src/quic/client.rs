use {
    crate::{client, ConnectionHeader, OutboundConnectionError, OutboundConnectionResult},
    derive_more::From,
    futures::TryFutureExt as _,
    libp2p::{identity::Keypair, Multiaddr},
    std::{future::Future, net::SocketAddr},
};

/// Transport responsible for establishing outbound QUIC connections.
#[derive(Clone, Debug)]
pub struct Connector {
    endpoint: quinn::Endpoint,
}

impl Connector {
    /// Creates a new [`Connector`].
    pub fn new(keypair: Keypair) -> Result<Self, super::Error> {
        let transport_config = super::new_quinn_transport_config(64u32 * 1024);
        let socket_addr = SocketAddr::new(std::net::Ipv4Addr::new(0, 0, 0, 0).into(), 0);
        let endpoint = super::new_quinn_endpoint(socket_addr, &keypair, transport_config, None)?;
        Ok(Self { endpoint })
    }
}

impl client::Connector for Connector {
    type Connection = quinn::Connection;

    fn connect(
        &self,
        multiaddr: &Multiaddr,
        header: ConnectionHeader,
    ) -> impl Future<Output = OutboundConnectionResult<Self::Connection>> {
        async move {
            let addr = super::try_multiaddr_to_socketaddr(multiaddr)
                .ok_or(OutboundConnectionError::InvalidMultiaddr)?;

            // `libp2p_tls` uses this "l" placeholder as server_name.
            let conn = self.endpoint.connect(addr, "l")?.await?;

            header.write(&mut conn.open_uni().await?).await?;

            Ok(conn)
        }
    }
}

impl client::OutboundConnection for quinn::Connection {
    type Read = quinn::RecvStream;
    type Write = quinn::SendStream;

    fn id(&self) -> usize {
        self.stable_id()
    }

    fn establish_stream(
        &self,
    ) -> impl Future<Output = OutboundConnectionResult<(Self::Read, Self::Write)>> + Send {
        self.open_bi()
            .map_ok(|(tx, rx)| (rx, tx))
            .map_err(Into::into)
    }
}

impl From<quinn::ConnectError> for OutboundConnectionError {
    fn from(err: quinn::ConnectError) -> Self {
        use quinn::ConnectError as Err;

        let kind = match err {
            Err::EndpointStopping => "endpoint_stopping",
            Err::CidsExhausted => "cids_exhausted",
            Err::InvalidServerName(_) => "invalid_server_name",
            Err::InvalidRemoteAddress(_) => "invalid_remote_address",
            Err::NoDefaultClientConfig => "no_default_client_config",
            Err::UnsupportedVersion => "quic_unsupported_version",
        };

        OutboundConnectionError::Other {
            kind,
            details: err.to_string(),
        }
    }
}

impl From<quinn::ConnectionError> for OutboundConnectionError {
    fn from(err: quinn::ConnectionError) -> Self {
        OutboundConnectionError::Other {
            kind: super::connection_error_kind(&err),
            details: err.to_string(),
        }
    }
}
