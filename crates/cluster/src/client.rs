//! Client of a WCN cluster.

use {
    libp2p_identity::PeerId,
    serde::{Deserialize, Serialize},
    smallvec::SmallVec,
};

/// Client of a WCN cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Client {
    /// [`PeerId`] of the [`Client`]. Used for authentication.
    pub peer_id: PeerId,

    /// WCN DB namespaces this [`Client`] is authorized to use.
    pub authorized_namespaces: SmallVec<[u8; 8]>,
}

// NOTE: The on-chain serialization is non self-describing!
// This `struct` can not be changed, a `struct` with a new schema version should
// be created instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct V0 {
    pub peer_id: PeerId,
    pub authorized_namespaces: SmallVec<[u8; 8]>,
}

impl From<Client> for V0 {
    fn from(client: Client) -> Self {
        Self {
            peer_id: client.peer_id,
            authorized_namespaces: client.authorized_namespaces,
        }
    }
}

impl From<V0> for Client {
    fn from(client: V0) -> Self {
        Self {
            peer_id: client.peer_id,
            authorized_namespaces: client.authorized_namespaces,
        }
    }
}
