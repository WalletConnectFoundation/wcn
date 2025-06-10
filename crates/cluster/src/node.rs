//! Node within a WCN cluster.

use {
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
    std::net::SocketAddrV4,
};

/// Node within a WCN cluster.
///
/// The IP address is currently being encrypted using a format-preserving
/// encryption algorithm.
// TODO: encrypt
#[derive(Debug, Clone, Copy)]
pub struct Node {
    /// [`PeerId`] of the [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// node operator are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of the [`Node`].
    pub addr: SocketAddrV4,
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct V0 {
    /// [`PeerId`] of this [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// [`NodeOperator`] are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of this [`Node`].
    pub addr: SocketAddrV4,
}

impl From<Node> for V0 {
    fn from(node: Node) -> Self {
        Self {
            peer_id: node.peer_id,
            addr: node.addr,
        }
    }
}

impl From<V0> for Node {
    fn from(node: V0) -> Self {
        Self {
            peer_id: node.peer_id,
            addr: node.addr,
        }
    }
}
