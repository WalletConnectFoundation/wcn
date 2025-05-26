//! Node within a WCN cluster.

use {
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
    std::net::SocketAddrV4,
};

/// On-chain data of a [`Node`].
///
/// The IP address is currently being encrypted using a format-preserving
/// encryption algorithm.
#[derive(Debug)]
pub struct Data {
    /// [`PeerId`] of the [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// node operator are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of the [`Node`].
    pub addr: SocketAddrV4,
}

/// Node within a WCN cluster.
#[derive(Debug)]
pub struct Node {
    data: VersionedData,
}

impl Node {
    /// Returns [`PeerId`] of this [`Node`].
    pub fn peer_id(&self) -> &PeerId {
        match &self.data {
            VersionedData::V0(data) => &data.peer_id,
        }
    }

    /// Returns [`SocketAddrV4`] of this [`Node`].
    pub fn addr(&self) -> SocketAddrV4 {
        match self.data {
            VersionedData::V0(data) => data.addr,
        }
    }

    pub(super) fn encrypt(&mut self) {}

    pub(super) fn decrypt(&mut self) {
        // FF1::new(key, radix);

        // let fpe_ff = FF1::<Aes256>::new(&[0; 32], 256).unwrap();
    }
}

/// Borrowed [`Node`].
#[derive(Debug)]
pub struct NodeRef<'a> {
    data: VersionedDataRef<'a>,
}

impl<'a> NodeRef<'a> {
    /// Converts this [`NodeRef`] into an owned [`Node`].
    pub fn to_owned(self) -> Node {
        Node {
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
    /// [`PeerId`] of this [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// [`NodeOperator`] are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of this [`Node`].
    pub addr: SocketAddrV4,
}

impl From<Data> for DataV0 {
    fn from(data: Data) -> Self {
        Self {
            peer_id: data.peer_id,
            addr: data.addr,
        }
    }
}
