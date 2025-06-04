//! Client of a WCN cluster.

use {
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
};

/// On-chain data of a [`Client`].
#[derive(Debug, Clone)]
pub struct Data {
    /// [`PeerId`] of the [`Client`]. Used for authentication.
    pub peer_id: PeerId,
}

/// Client of a WCN cluster.
#[derive(Debug)]
pub struct Client {
    data: VersionedData,
}

impl Client {
    /// Returns [`PeerId`] of this [`Client`].
    pub fn peer_id(&self) -> &PeerId {
        match &self.data {
            VersionedData::V0(data) => &data.peer_id,
        }
    }
}

/// Borrowed [`Client`].
#[derive(Debug)]
pub struct ClientRef<'a> {
    data: VersionedDataRef<'a>,
}

impl<'a> ClientRef<'a> {
    /// Converts this [`ClientRef`] into an owned [`Client`].
    pub fn to_owned(self) -> Client {
        Client {
            data: match self.data {
                VersionedDataRef::V0(data) => VersionedData::V0(*data),
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum VersionedData {
    V0(DataV0),
}

#[derive(Debug)]
enum VersionedDataRef<'a> {
    V0(&'a DataV0),
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct DataV0 {
    pub peer_id: PeerId,
}

impl From<Data> for DataV0 {
    fn from(data: Data) -> Self {
        Self {
            peer_id: data.peer_id,
        }
    }
}
