use libp2p::{identity::Keypair, Multiaddr};

/// QUIC RPC server config.
pub struct Config {
    /// Name of the server. For metrics purposes only.
    pub name: &'static str,

    /// [`Multiaddr`] to bind the server to.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Maximum allowed amount of concurrent connections.
    pub max_concurrent_connections: u32,

    /// Maximum allowed amount of concurrent streams.
    pub max_concurrent_streams: u32,
}

// /// Runs the provided [`rpc::Server`] using QUIC protocol.
// pub fn run(rpc_server: impl rpc::Server, cfg: Config) -> Result<impl
// Future<Output = ()>, Error> {     multiplex((rpc_server,), cfg)
// }
