use {
    libp2p::Multiaddr,
    std::{io, net::SocketAddr, time::Duration},
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Connector;

#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::{Acceptor, AcceptorConfig};
use socket2::Socket;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to generate a TLS certificate: {0}")]
    Tls(String),

    #[error("failed to create, configure or bind a UDP socket")]
    Socket(#[from] io::Error),
}

fn socketaddr_to_multiaddr(addr: impl Into<SocketAddr>) -> Multiaddr {
    use libp2p::multiaddr::Protocol;

    let addr = addr.into();

    let mut result = Multiaddr::from(addr.ip());
    result.push(Protocol::Tcp(addr.port()));
    result.push(Protocol::Tls);
    result
}

/// Tries to turn a TCP multiaddress into a TCP [`SocketAddr`]. Returns None if
/// the format of the multiaddr is wrong.
fn try_multiaddr_to_socketaddr(addr: &Multiaddr) -> Option<SocketAddr> {
    use libp2p::multiaddr::Protocol;

    let mut iter = addr.iter();
    let proto1 = iter.next()?;
    let proto2 = iter.next()?;

    Some(match (proto1, proto2) {
        (Protocol::Ip4(ip), Protocol::Tcp(port)) => SocketAddr::new(ip.into(), port),
        (Protocol::Ip6(ip), Protocol::Tcp(port)) => SocketAddr::new(ip.into(), port),

        // TODO: Remove. Temporary
        (Protocol::Ip4(ip), Protocol::Udp(port)) => SocketAddr::new(ip.into(), port),
        (Protocol::Ip6(ip), Protocol::Udp(port)) => SocketAddr::new(ip.into(), port),
        _ => return None,
    })
}

fn connection_error_kind(err: &yamux::ConnectionError) -> &'static str {
    use yamux::ConnectionError as Err;
    match err {
        Err::Io(_) => "io",
        Err::Decode(_) => "yamux_decode",
        Err::NoMoreStreamIds => "no_more_stream_ids",
        Err::Closed => "connection_closed",
        Err::TooManyStreams => "too_many_streams",
        _ => "yamux_other",
    }
}

fn new_socket() -> io::Result<Socket> {
    use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};

    let socket = Socket::new(
        Domain::IPV4,
        Type::STREAM.nonblocking(),
        Some(Protocol::TCP),
    )?;
    socket.set_nonblocking(true)?;
    socket.set_tcp_user_timeout(Some(Duration::from_secs(2)))?;
    socket.set_tcp_keepalive(
        // 1s is the smallest allowed `Duration`
        &TcpKeepalive::new()
            .with_time(Duration::from_secs(1))
            .with_interval(Duration::from_secs(1))
            .with_retries(3),
    )?;

    Ok(socket)
}
