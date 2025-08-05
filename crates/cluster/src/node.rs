//! Node within a WCN cluster.

use {
    derive_more::derive::AsRef,
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
    std::net::{Ipv4Addr, SocketAddrV4},
};

/// Node within a WCN cluster.
///
/// The IP address is currently being encrypted using a format-preserving
/// encryption algorithm.
// TODO: encrypt
#[derive(AsRef, Debug, Clone, Copy)]
pub struct Node {
    /// [`PeerId`] of the [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// node operator are allowed to have the same [`PeerId`].
    #[as_ref]
    pub peer_id: PeerId,

    /// [`Ipv4Addr`] of the [`Node`].
    pub ipv4_addr: Ipv4Addr,

    /// Primary RPC server port.
    pub primary_port: u16,

    /// Secondary RPC server port.
    pub secondary_port: u16,
}

impl Node {
    /// Builds [`SocketAddrV4`] using [`Node::ipv4_addr`] and
    /// [`Node::primary_port`].
    pub fn primary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ipv4_addr, self.primary_port)
    }

    /// Builds [`SocketAddrV4`] using [`Node::ipv4_addr`] and
    /// [`Node::secondary_port`].
    pub fn secondary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ipv4_addr, self.secondary_port)
    }
}

// NOTE: The on-chain serialization is non self-describing!
// This `struct` can not be changed, a `struct` with a new schema version should
// be created instead.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct V0 {
    pub peer_id: PeerId,
    pub ipv4_addr: Ipv4Addr,
    pub primary_port: u16,
    pub secondary_port: u16,
}

impl From<Node> for V0 {
    fn from(node: Node) -> Self {
        Self {
            peer_id: node.peer_id,
            ipv4_addr: node.ipv4_addr,
            primary_port: node.primary_port,
            secondary_port: node.secondary_port,
        }
    }
}

impl From<V0> for Node {
    fn from(node: V0) -> Self {
        Self {
            peer_id: node.peer_id,
            ipv4_addr: node.ipv4_addr,
            primary_port: node.primary_port,
            secondary_port: node.secondary_port,
        }
    }
}
