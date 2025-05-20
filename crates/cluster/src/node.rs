use {
    crate::contract,
    libp2p::identity::PeerId,
    std::{collections::HashMap, mem, net::SocketAddrV4},
};

/// Globally unique identifier of an [`Operator`];
pub type OperatorId = contract::PublicKey;

/// Locally unique identifier of an [`Operator`] within a regional WCN cluster
/// keyspace.
///
/// Refers to a position within the [`Operators`] slot map.
pub type OperatorIdx = u8;

/// Name of an [`Operator`].
///
/// Used for informational purposes only.
/// Expected to be unique within the cluster, but not enforced to.
#[derive(Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
pub struct Node {
    /// [`PeerId`] of this [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// [`NodeOperator`] are allowed to have the same [`PeerId`].
    pub peer_id: PeerId,

    /// [`SocketAddrV4`] of this [`Node`].
    pub addr: SocketAddrV4,
}

/// Collection of [`Operator`]s within a regional WCN cluster.
///
/// Allows to retrieve stored [`Operator`]s using both [`OperatorIdx`]es
/// and [`OperatorId`]s.
///
/// Guarantees stable ordering on removal -- doesn't shift any elements
/// therefore preserving [`OperatorIdx`] validity.
#[derive(Debug)]
pub struct Operators {
    id_to_slot_idx: HashMap<OperatorId, OperatorIdx>,
    slots: Vec<Option<Operator>>,
}

impl Default for Operators {
    fn default() -> Self {
        Self {
            id_to_slot_idx: HashMap::new(),
            slots: Vec::new(),
        }
    }
}

impl Operators {
    /// Maximum allowed number of slots in this collection.
    pub const MAX_SLOTS: usize = u8::MAX as usize + 1;

    // Creates a new empty collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Tries to create a new collection out of the provided list of existing
    /// slots.
    ///
    /// Returns `None` if the number of slots exceeds [`Self::MAX_SLOTS`].
    pub fn from_slots(slots: Vec<Option<Operator>>) -> Option<Self> {
        if slots.len() > Self::MAX_SLOTS {
            return None;
        }

        let mut key_to_slot_idx = HashMap::new();
        for (idx, value) in slots.iter().enumerate() {
            if let Some(v) = value {
                key_to_slot_idx.insert(v.id.clone(), idx as u8);
            }
        }

        Some(Self {
            id_to_slot_idx: key_to_slot_idx,
            slots,
        })
    }

    /// Returns the list of [`Operator] slots.
    pub fn slots(&self) -> &[Option<Operator>] {
        &self.slots
    }

    /// Gets [`Operator`] by [`OperatorId`].
    pub fn get(&self, id: &OperatorId) -> Option<&Operator> {
        self.id_to_slot_idx
            .get(id)
            .and_then(|&idx| self.slots[idx as usize].as_ref())
    }

    /// Gets [`Operator`] by [`OperatorIdx`].
    pub fn get_by_idx(&self, idx: OperatorIdx) -> Option<&Operator> {
        let idx = idx as usize;
        if idx >= self.slots.len() {
            return None;
        }

        self.slots[idx].as_ref()
    }

    /// Indicates whether this collection contains the [`Operator`] with the
    /// specified [`OperatorId`].
    pub fn contains(&self, id: &OperatorId) -> bool {
        self.id_to_slot_idx.contains_key(id)
    }

    /// Returns an [`Iterator`] over the [`Operator`] slots in this collection.
    pub fn iter(&self) -> impl Iterator<Item = (OperatorIdx, &Operator)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, n)| n.as_ref().map(|n| (idx as u8, n)))
    }

    fn allocate_slot(&mut self) -> Option<u8> {
        if let Some(idx) = self.slots.iter().position(|n| n.is_none()) {
            return Some(idx as u8);
        };

        if self.slots.len() == Self::MAX_SLOTS {
            return None;
        }

        self.slots.push(None);
        Some((self.slots.len() - 1) as u8)
    }

    /// Inserts an [`Operator`] into the collection.
    pub fn insert(
        &mut self,
        operator: Operator,
    ) -> Result<Option<Operator>, TooManyOperatorsError> {
        let idx = if let Some(idx) = self.id_to_slot_idx.get(&operator.id) {
            *idx
        } else {
            let idx = self.allocate_slot().ok_or(TooManyOperatorsError)?;
            let _ = self.id_to_slot_idx.insert(operator.id.clone(), idx);
            idx
        };

        let mut operator = Some(operator);
        mem::swap(&mut operator, &mut self.slots[idx as usize]);
        Ok(operator)
    }

    /// Removes the [`Operator`] with the specified [`OperatorId`] from this
    /// collection.
    pub fn remove(&mut self, id: &OperatorId) -> Option<Operator> {
        if let Some(idx) = self.id_to_slot_idx.remove(id) {
            return self.slots[idx as usize].take();
        }

        None
    }
}

/// Error of trying to insert too many elements into [`Operators`] collection.
#[derive(Clone, Debug, thiserror::Error)]
#[error("Maximum number of node operator reached: {}", Operators::MAX_SLOTS)]
pub struct TooManyOperatorsError;
