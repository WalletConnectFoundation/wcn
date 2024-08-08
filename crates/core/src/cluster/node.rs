use {
    super::{keyspace, Nodes},
    serde::{de::DeserializeOwned, Serialize},
    std::{collections::HashMap, error::Error as StdError, fmt, hash::Hash, mem, sync::Arc},
};

pub type Idx = u8;

pub trait Node: Clone + fmt::Debug + Eq + Send + Sync + 'static {
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

    fn id(&self) -> &Self::Id;

    fn can_add(&self, nodes: &Nodes<Self>) -> Result<(), impl StdError>;
    fn can_update(&self, nodes: &Nodes<Self>, new_state: &Self) -> Result<(), impl StdError>;
}

#[derive(Clone, Debug)]
pub enum State<N: Node> {
    Pulling(Arc<keyspace::MigrationPlan<N>>),
    Normal,
    Restarting,
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

pub type Id<N> = <N as Node>::Id;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct SlotMap<N: Node> {
    id_to_slot_idx: HashMap<N::Id, u8>,
    slots: Vec<Slot<N>>,
}

pub(super) type Slot<N> = Option<N>;

impl<N: Node> SlotMap<N> {
    pub(super) const MAX_SLOTS: usize = u8::MAX as usize + 1;

    pub(super) fn new() -> Self {
        Self {
            id_to_slot_idx: HashMap::new(),
            slots: Vec::new(),
        }
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

    pub fn get(&self, id: &N::Id) -> Option<&N> {
        self.id_to_slot_idx
            .get(id)
            .and_then(|&idx| self.slots[idx as usize].as_ref())
    }

    // pub fn get_slot(&self, id: &N::Id) -> Option<(Idx, &N)> {
    //     self.id_to_slot_idx
    //         .get(id)
    //         .and_then(|&idx| self.slots[idx as usize].as_ref().map(|n| (idx, n)))
    // }

    pub fn get_by_idx(&self, idx: u8) -> Option<&N> {
        let idx = idx as usize;
        if idx >= self.slots.len() {
            return None;
        }

        self.slots[idx].as_ref()
    }

    // pub fn get_mut(&mut self, id: &N::Id) -> Option<&mut N> {
    //     self.id_to_slot_idx
    //         .get(id)
    //         .and_then(|&idx| self.slots[idx as usize].as_mut())
    // }

    pub fn contains(&self, id: &N::Id) -> bool {
        self.id_to_slot_idx.contains_key(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u8, &N)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, n)| n.as_ref().map(|n| (idx as u8, n)))
    }

    // pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (u16, &mut N)> {
    //     self.slots
    //         .iter_mut()
    //         .enumerate()
    //         .filter_map(|(idx, slot)| slot.map(|n| (idx, n)))
    // }

    // fn swap_slot(&mut self, slot: &mut Slot<N>) {
    //     let slot_idx = slot.idx as usize;

    //     if slot_idx >= self.slots.0.len() {
    //         self.slots.0.resize(slot_idx + 1, None);
    //     }

    //     mem::swap(&mut slot.node, &mut self.slots.0[slot_idx])
    // }

    // pub fn swap_slots(&mut self, slots: &mut [Slot<N>]) {
    //     for slot in &mut slots {
    //         self.swap_slot(slot);
    //     }
    // }

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

    pub fn remove(&mut self, id: &N::Id) -> Option<N> {
        if let Some(idx) = self.id_to_slot_idx.remove(id) {
            return self.slots[idx as usize].take();
        }

        None
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SlotMapError {
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
