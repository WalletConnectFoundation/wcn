use {
    crate::contract,
    libp2p::identity::PeerId,
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, net::SocketAddrV4},
};

/// Globally unique identifier of an [`Operator`];
pub type OperatorId = contract::PublicKey;

/// Locally unique identifier of an [`Operator`] within a WCN cluster.
///
/// Refers to a position within the [`Operators`] slot map.
pub type OperatorIdx = u8;

/// Name of an [`Operator`].
///
/// Used for informational purposes only.
/// Expected to be unique within the cluster, but not enforced to.
#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorName(String);

impl OperatorName {
    /// Maximum allowed length of an [`OperatorName`] (in bytes).
    pub const MAX_LENGTH: usize = 32;

    /// Tries to create a new [`OperatorName`] out of the provided [`ToString`].
    ///
    /// Returns `None` if the string length exceeds [`Self::MAX_LENGTH`].
    pub fn new(s: impl ToString) -> Option<Self> {
        let s = s.to_string();
        (s.len() <= Self::MAX_LENGTH).then_some(Self(s))
    }

    /// Returns a reference to the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Entity operating a set of WCN nodes within a regional WCN cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct Operator {
    /// ID of this [`Operator`].
    pub id: OperatorId,

    /// Name of this [`Operator`].
    pub name: OperatorName,

    /// List of [`Node`]s being operated by this [`Operator`].
    pub nodes: Vec<Node>,

    /// List of clients of this [`Operator`].
    ///
    /// Those clients are allowed to use the WCN network on behalf of this
    /// [`Operator`].
    pub clients: Vec<PeerId>,
}

/// Node within a WCN network.
#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    /// [`PeerId`] of this [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// [`NodeOperator`] are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of this [`Node`].
    pub addr: SocketAddrV4,
}

impl Node {
    pub fn decrypt(&mut self) {
        // FF1::new(key, radix);

        // let fpe_ff = FF1::<Aes256>::new(&[0; 32], 256).unwrap();
    }
}

/// Slot map of [`Operator`]s.
pub(super) struct Operators {
    id_to_idx: HashMap<OperatorId, OperatorIdx>,

    // TODO: assert length
    slots: Vec<Option<Operator>>,
}

impl Operators {
    /// Returns whether this map contains the [`Operator`] with the provided
    /// [`OperatorId`].
    pub(super) fn contains(&self, id: &OperatorId) -> bool {
        self.get(id).is_some()
    }

    /// Gets an [`Operator`] by [`OperatorId`].
    pub(super) fn get(&self, id: &OperatorId) -> Option<&Operator> {
        self.get_by_idx(*self.id_to_idx.get(id)?)
    }

    /// Gets an [`Operator`] by [`OperatorIdx`].
    pub(super) fn get_by_idx(&self, idx: OperatorIdx) -> Option<&Operator> {
        self.slots.get(idx as usize)?.as_ref()
    }

    /// Returns the list of [`Operator`] slots.
    pub(super) fn slots(&self) -> &[Option<Operator>] {
        &self.slots
    }
}
