use {network::multiaddr, std::net::SocketAddr};

pub fn network_addr(addr: SocketAddr) -> multiaddr::Multiaddr {
    let mut result = multiaddr::Multiaddr::from(addr.ip());
    result.push(multiaddr::Protocol::Udp(addr.port()));
    result.push(multiaddr::Protocol::QuicV1);
    result
}
