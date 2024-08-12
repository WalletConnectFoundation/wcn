//! Data structures related to nodes in an IRN cluster.

use {
    super::{keyspace, Nodes},
    serde::{de::DeserializeOwned, Serialize},
    std::{collections::HashMap, error::Error as StdError, fmt, hash::Hash, mem, sync::Arc},
};

/// Index of a [`Node`] in a [`SlotMap`].
pub type Idx = u8;

/// Node in a [`Cluster`](super::Cluster).
pub trait Node: Clone + fmt::Debug + Eq + Send + Sync + 'static {
    /// Unique identifiear of a [`Node`].
    type Id: Clone
        + fmt::Debug
        + Hash
        + Eq
        + Ord
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// Returns the [`Node::Id`] of this [`Node`].
    fn id(&self) -> &Self::Id;

    /// Checks whether this [`Node`] can be added to a
    /// [`Cluster`](super::Cluster) with the specified list of existing
    /// [`Nodes`].
    fn can_add(&self, nodes: &Nodes<Self>) -> Result<(), impl StdError>;

    /// Checks whether this [`Node`] can be updated to the provided value in a
    /// [`Cluster`](super::Cluster) with the specified list of existing
    /// [`Nodes`].
    fn can_update(&self, nodes: &Nodes<Self>, new_state: &Self) -> Result<(), impl StdError>;
}

/// State of a [`Node`] in a [`Cluster`](super::Cluster).
#[derive(Clone, Debug)]
pub enum State<N: Node> {
    /// [`Node`] is pulling data from other [`Node`]s
    /// [`Cluster`](super::Cluster) using the specified
    /// [`keyspace::MigrationPlan`].
    Pulling(Arc<keyspace::MigrationPlan<N>>),

    /// [`Node`] is operating normally.
    Normal,

    /// [`Node`] is in a process of a restart initiated by
    /// [`Cluster::shutdown_node`].
    Restarting,

    /// [`Node`] is in a process of decommissioning initiated by
    /// [`Cluster::decommission_node`].
    Decommissioning,
}

impl<N: Node> PartialEq for State<N> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pulling(a), Self::Pulling(b)) => Arc::ptr_eq(a, b),
            (Self::Normal, Self::Normal) => true,
            (Self::Restarting, Self::Restarting) => true,
            (Self::Decommissioning, Self::Decommissioning) => true,
            _ => false,
        }
    }
}

/// [`Node::Id`].
pub type Id<N> = <N as Node>::Id;

/// Collection which allows to retrieve stored [`Node`]s using both [`Idx`]es
/// and [`Id`]s.
///
/// The collection has stable ordering which is guaranteed to be preserved after
/// de/serialization.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotMap<N: Node> {
    id_to_slot_idx: HashMap<N::Id, u8>,
    slots: Vec<Slot<N>>,
}

impl<N: Node> Default for SlotMap<N> {
    fn default() -> Self {
        Self {
            id_to_slot_idx: HashMap::new(),
            slots: Vec::new(),
        }
    }
}

pub(super) type Slot<N> = Option<N>;

impl<N: Node> SlotMap<N> {
    pub(super) const MAX_SLOTS: usize = u8::MAX as usize + 1;

    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn from_slots(slots: Vec<Slot<N>>) -> Result<Self, SlotMapError> {
        if slots.len() > Self::MAX_SLOTS {
            return Err(SlotMapError::Full);
        }

        let mut key_to_slot_idx = HashMap::new();
        for (idx, value) in slots.iter().enumerate() {
            if let Some(v) = value {
                key_to_slot_idx.insert(v.id().clone(), idx as u8);
            }
        }

        Ok(Self {
            id_to_slot_idx: key_to_slot_idx,
            slots,
        })
    }

    pub(super) fn slots(&self) -> &[Slot<N>] {
        &self.slots
    }

    /// Gets [`Node`] by [`Node::Id`].
    pub fn get(&self, id: &N::Id) -> Option<&N> {
        self.id_to_slot_idx
            .get(id)
            .and_then(|&idx| self.slots[idx as usize].as_ref())
    }

    /// Gets [`Node`] by [`Idx`].
    pub fn get_by_idx(&self, idx: Idx) -> Option<&N> {
        let idx = idx as usize;
        if idx >= self.slots.len() {
            return None;
        }

        self.slots[idx].as_ref()
    }

    /// Indicates whether this collection contains the [`Node`] with the
    /// specified [`Node::Id`].
    pub fn contains(&self, id: &N::Id) -> bool {
        self.id_to_slot_idx.contains_key(id)
    }

    /// Returns an [`Iterator`] over the [`Node`] slots in this collection.
    pub fn iter(&self) -> impl Iterator<Item = (Idx, &N)> {
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

    /// Inserts a [`Node`] into the collection.
    pub fn insert(&mut self, node: N) -> Result<Option<N>, SlotMapError> {
        let idx = if let Some(idx) = self.id_to_slot_idx.get(node.id()) {
            *idx
        } else {
            let idx = self.allocate_slot().ok_or(SlotMapError::Full)?;
            let _ = self.id_to_slot_idx.insert(node.id().clone(), idx);
            idx
        };

        let mut node = Some(node);
        mem::swap(&mut node, &mut self.slots[idx as usize]);
        Ok(node)
    }

    /// Removes the [`Node`] with the specified [`Node::Id`] from this
    /// collection.
    pub fn remove(&mut self, id: &N::Id) -> Option<N> {
        if let Some(idx) = self.id_to_slot_idx.remove(id) {
            return self.slots[idx as usize].take();
        }

        None
    }
}

/// [`SlotMap`] error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SlotMapError {
    /// [`SlotMap`] is full.
    #[error("SlotMap is full")]
    Full,
}

impl From<SlotMapError> for super::Error {
    fn from(err: SlotMapError) -> Self {
        match err {
            SlotMapError::Full => Self::TooManyNodes,
        }
    }
}
