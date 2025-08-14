//! Slot map of [`NodeOperator`]s.

use {
    crate::{self as cluster, node_operator, Node, NodeOperator},
    indexmap::IndexMap,
    libp2p_identity::PeerId,
    std::sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

/// Collection of [`NodeOperator`]s within a WCN Cluster.
///
/// Acts like a slot map with stable ordering and without shifts.
///
/// Once [`NodeOperator`] gets added into this collection it gets assigned a
/// stable [`node_operator::Idx`]. This index never changes unless the
/// [`NodeOperator`] gets removed, then the index may be reused by another
/// [`NodeOperator`].
#[derive(Debug, Clone)]
pub struct NodeOperators<N = Node> {
    id_to_idx: IndexMap<node_operator::Id, node_operator::Idx>,

    slots: Vec<Option<NodeOperator<N>>>,

    // for load balancing
    counter: Arc<AtomicUsize>,
}

impl<N> NodeOperators<N> {
    pub(super) fn from_slots(
        slots: impl IntoIterator<Item = Option<NodeOperator<N>>>,
    ) -> Result<Self, CreationError> {
        let mut slots: Vec<_> = slots.into_iter().collect();

        if slots.len() < cluster::MIN_OPERATORS as usize {
            return Err(CreationError::TooFewOperators(slots.len()));
        }

        if slots.len() > cluster::MAX_OPERATORS {
            return Err(CreationError::TooManyOperatorSlots(slots.len()));
        }

        let mut id_to_idx = IndexMap::with_capacity(slots.len());

        for (idx, slot) in slots.iter_mut().enumerate() {
            if let Some(operator) = slot {
                if id_to_idx.insert(*operator.as_ref(), idx as u8).is_some() {
                    return Err(CreationError::OperatorDuplicate(*operator.as_ref()));
                };
            }
        }

        Ok(Self {
            id_to_idx,
            slots,
            // overflows and starts from `0`
            counter: Arc::new(usize::MAX.into()),
        })
    }

    /// Indicates whether any of the [`NodeOperators`] contains a node with the
    /// provided ID.
    pub fn contains_node(&self, peer_id: &PeerId) -> bool
    where
        N: AsRef<PeerId>,
    {
        // TODO: Consider optimizing by building a lookup table.
        self.slots.iter().any(|opt| {
            opt.as_ref()
                .map(|op| op.nodes().iter().any(|node| node.as_ref() == peer_id))
                .unwrap_or_default()
        })
    }

    /// Indicates whether any of the [`NodeOperators`] contains a client with
    /// the provided ID.
    pub fn contains_client(&self, peer_id: &PeerId) -> bool
    where
        N: AsRef<PeerId>,
    {
        // TODO: Consider optimizing by building a lookup table.
        self.slots.iter().any(|opt| {
            opt.as_ref()
                .map(|op| op.clients.iter().any(|client| &client.peer_id == peer_id))
                .unwrap_or_default()
        })
    }

    /// Indicates whether the client with the specified [`PeerId`] is authorized
    /// to use the specified namespace of the [`node_operator`].
    pub fn is_authorized_client(
        &self,
        peer_id: &PeerId,
        operator_id: &node_operator::Id,
        namespace_idx: u8,
    ) -> bool {
        // TODO: Consider optimizing by building a lookup table.
        self.get(operator_id)
            .map(|operator| {
                operator.clients.iter().any(|client| {
                    &client.peer_id == peer_id
                        && client.authorized_namespaces.contains(&namespace_idx)
                })
            })
            .unwrap_or_default()
    }

    /// Returns a [`NodeOperator`] responsible for the next request.
    ///
    /// [`NodeOperator`]s are being iterated in round-robin fashion for
    /// load-balancing purposes.
    pub fn next(&self) -> &NodeOperator<N> {
        // we've checked this in the constructor
        debug_assert!(!self.id_to_idx.is_empty());

        let n = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        let idx = self.id_to_idx[n % self.id_to_idx.len()];
        // if `id_to_idx` map contains the index the slot should always exist
        self.slots[idx as usize].as_ref().unwrap()
    }

    pub(super) fn into_slots(self) -> Vec<Option<NodeOperator<N>>> {
        self.slots
    }

    pub fn slots(&self) -> &[Option<NodeOperator<N>>] {
        &self.slots
    }

    #[cfg(feature = "testing")]
    pub(super) fn free_idx(&self) -> Option<node_operator::Idx> {
        let opt = self
            .slots
            .iter()
            .enumerate()
            .find_map(|(idx, slot)| slot.is_none().then_some(idx as u8));

        Some(match opt {
            Some(idx) => idx,
            None if self.slots.len() != cluster::MAX_OPERATORS => self.slots.len() as u8,
            _ => return None,
        })
    }

    pub(super) fn set(&mut self, idx: node_operator::Idx, mut slot: Option<NodeOperator<N>>) {
        if let Some(id) = self.get_by_idx(idx).map(|op| *op.as_ref()) {
            self.id_to_idx.shift_remove(&id);
        }

        if self.slots.len() >= idx as usize {
            self.expand(idx);
        }

        if let Some(operator) = &mut slot {
            self.id_to_idx.insert(*operator.as_ref(), idx);
        }

        self.slots[idx as usize] = slot;
    }

    fn expand(&mut self, idx: node_operator::Idx) {
        let desired_len = (idx as usize) + 1;
        let slots_to_add = desired_len.checked_sub(self.slots.len());

        for _ in 0..slots_to_add.unwrap_or_default() {
            self.slots.push(None);
        }
    }

    /// Returns whether this map contains the [`NodeOperator`] with the provided
    /// [`Id`].
    pub(super) fn contains(&self, id: &node_operator::Id) -> bool {
        self.get_idx(id).is_some()
    }

    /// Returns whether this map contains the [`NodeOperator`] with the provided
    /// [`Idx`].
    pub(super) fn contains_idx(&self, idx: node_operator::Idx) -> bool {
        self.get_by_idx(idx).is_some()
    }

    /// Gets an [`NodeOperator`] by [`Id`].
    pub fn get(&self, id: &node_operator::Id) -> Option<&NodeOperator<N>> {
        self.get_by_idx(self.get_idx(id)?)
    }

    /// Gets an [`NodeOperator`] by [`Idx`].
    pub fn get_by_idx(&self, idx: node_operator::Idx) -> Option<&NodeOperator<N>> {
        self.slots.get(idx as usize)?.as_ref()
    }

    /// Gets an [`Idx`] by [`Id`].
    pub(super) fn get_idx(&self, id: &node_operator::Id) -> Option<node_operator::Idx> {
        self.id_to_idx.get(id).copied()
    }

    pub(super) fn occupied_indexes(&self) -> impl Iterator<Item = node_operator::Idx> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.is_some().then_some(idx as u8))
    }

    pub(super) fn require_idx(
        &self,
        id: &node_operator::Id,
    ) -> Result<node_operator::Idx, node_operator::NotFoundError> {
        self.get_idx(id).ok_or(node_operator::NotFoundError(*id))
    }

    pub(super) fn require_not_exists(
        &self,
        id: &node_operator::Id,
    ) -> Result<&Self, node_operator::AlreadyExistsError> {
        if self.contains(id) {
            return Err(node_operator::AlreadyExistsError(*id));
        }

        Ok(self)
    }

    pub(super) fn require_not_full(&self) -> Result<&Self, NoAvailableSlotsError> {
        if self.slots.len() < cluster::MAX_OPERATORS || self.slots.iter().any(Option::is_none) {
            return Ok(self);
        }

        Err(NoAvailableSlotsError)
    }

    pub(super) fn require_free_slot(
        &self,
        idx: node_operator::Idx,
    ) -> Result<&Self, SlotOccupiedError> {
        if self.contains_idx(idx) {
            return Err(SlotOccupiedError(idx));
        }

        Ok(self)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreationError {
    #[error("Too many operator slots: {_0} > {}", cluster::MAX_OPERATORS)]
    TooManyOperatorSlots(usize),

    #[error("Too few operators: {_0} < {}", cluster::MIN_OPERATORS)]
    TooFewOperators(usize),

    #[error("Duplicate NodeOperator(id: {_0})")]
    OperatorDuplicate(node_operator::Id),
}

#[derive(Debug, thiserror::Error)]
#[error("Slot {_0} in NodeOperators slot map is already occupied")]
pub struct SlotOccupiedError(pub node_operator::Idx);

#[derive(Debug, thiserror::Error)]
#[error("No available slots in NodeOperators slot")]
pub struct NoAvailableSlotsError;
