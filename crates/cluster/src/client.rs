//! Client of a WCN cluster.

use {
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
};

/// Client of a WCN cluster.
#[derive(Debug, Clone)]
pub struct Client {
    /// [`PeerId`] of the [`Client`]. Used for authentication.
    pub peer_id: PeerId,
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct V0 {
    pub peer_id: PeerId,
}

impl From<Client> for V0 {
    fn from(client: Client) -> Self {
        Self {
            peer_id: client.peer_id,
        }
    }
}

impl From<V0> for Client {
    fn from(client: V0) -> Self {
        Self {
            peer_id: client.peer_id,
        }
    }
}
