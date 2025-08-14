#[cfg(target_os = "linux")]
use nix::sys::socket::{setsockopt, sockopt};
use {
    crate::transport::{self, Priority},
    libp2p_identity::{Keypair, PeerId},
    quinn::{crypto::rustls::QuicClientConfig, rustls::pki_types::CertificateDer, VarInt},
    std::{
        io,
        net::{SocketAddr, UdpSocket},
        sync::Arc,
        time::Duration,
    },
};

pub(crate) fn new_quinn_transport_config(
    max_concurrent_streams: u32,
) -> Arc<quinn::TransportConfig> {
    const STREAM_WINDOW: u32 = 4 * 1024 * 1024; // 4 MiB

    // Our tests are too slow and connections get dropped because of missing keep
    // alive messages. Setting idle timeout higher for debug builds.
    let max_idle_timeout_ms = if cfg!(debug_assertions) { 5000 } else { 200 };

    let mut transport = quinn::TransportConfig::default();
    // Disable uni-directional streams and datagrams.
    transport
        .max_concurrent_uni_streams(1u32.into())
        .max_concurrent_bidi_streams(max_concurrent_streams.into())
        .datagram_receive_buffer_size(None)
        .keep_alive_interval(Some(Duration::from_millis(100)))
        .max_idle_timeout(Some(VarInt::from_u32(max_idle_timeout_ms).into()))
        .allow_spin(false)
        .receive_window((2 * STREAM_WINDOW).into())
        .stream_receive_window(STREAM_WINDOW.into())
        .send_window(2 * STREAM_WINDOW as u64);

    Arc::new(transport)
}

pub(crate) fn new_quinn_endpoint(
    socket_addr: SocketAddr,
    keypair: &Keypair,
    transport_config: Arc<quinn::TransportConfig>,
    server_config: Option<quinn::ServerConfig>,
    priority: transport::Priority,
) -> Result<quinn::Endpoint, Error> {
    let client_tls_config =
        libp2p_tls::make_client_config(keypair, None).map_err(|err| Error::Tls(err.to_string()))?;
    let client_tls_config =
        QuicClientConfig::try_from(client_tls_config).map_err(|err| Error::Tls(err.to_string()))?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(client_tls_config));
    client_config.transport_config(transport_config);

    let socket = new_udp_socket(socket_addr, priority).map_err(Error::Socket)?;

    let mut endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        server_config,
        socket,
        Arc::new(quinn::TokioRuntime),
    )?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn new_udp_socket(addr: SocketAddr, priority: Priority) -> io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    tracing::info!(udp_send_buffer_size = socket.send_buffer_size()?);
    tracing::info!(udp_recv_buffer_size = socket.recv_buffer_size()?);

    let (so_priority, tos) = match priority {
        Priority::High => (6, IpTosDscp::Ef),
        Priority::Low => (0, IpTosDscp::Le),
    };

    // make MacOS enjoyers happy
    #[cfg(target_os = "linux")]
    if let Err(err) = setsockopt(&socket, sockopt::Priority, &so_priority) {
        tracing::warn!(?err, "Failed to set `SO_PRIORITY`");
    }

    #[cfg(not(target_os = "linux"))]
    let _ = so_priority;

    if let Err(err) = socket.set_tos_v4(tos as u32) {
        tracing::warn!(?err, "Failed to set `IP_TOS`");
    }

    socket.bind(&addr.into())?;
    Ok(socket.into())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to generate a TLS certificate: {0}")]
    Tls(String),

    #[error("failed to create, configure or bind a UDP socket")]
    Socket(#[from] io::Error),

    #[error("invalid connection rate limit")]
    InvalidConnectionRate,
}

pub(crate) fn connection_peer_id(conn: &quinn::Connection) -> Result<PeerId, ExtractPeerIdError> {
    use ExtractPeerIdError as Error;

    let identity = conn.peer_identity().ok_or(Error::MissingPeerIdentity)?;
    let certificate = identity
        .downcast::<Vec<CertificateDer<'_>>>()
        .map_err(|_| Error::DowncastPeerIdentity)?
        .into_iter()
        .next()
        .ok_or(Error::MissingTlsCertificate)?;

    let peer_id = libp2p_tls::certificate::parse(&certificate)
        .map_err(Error::ParseTlsCertificate)?
        .peer_id();

    Ok(peer_id)
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractPeerIdError {
    #[error("Missing peer identity")]
    MissingPeerIdentity,

    #[error("Failed to downcast peer identity")]
    DowncastPeerIdentity,

    #[error("Missing TLS certificate")]
    MissingTlsCertificate,

    #[error("Failed to parse TLS certificate: {0:?}")]
    ParseTlsCertificate(libp2p_tls::certificate::ParseError),
}

// Source: https://github.com/mozilla/neqo/blob/bb45c7436f583a7ed2e408beffbf809b70142848/neqo-common/src/tos.rs#L69
/// Diffserv codepoints, mapped to the upper six bits of the TOS field.
/// <https://www.iana.org/assignments/dscp-registry/dscp-registry.xhtml>
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(u8)]
enum IpTosDscp {
    /// Expedited Forwarding, RFC3246
    Ef = 0b1011_1000,

    /// Lower-Effort, RFC8622
    Le = 0b0000_0100,
}
