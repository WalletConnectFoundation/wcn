//! Slot map of [`NodeOperator`]s.

use {
    crate::{self as cluster, node_operator},
    indexmap::IndexMap,
    std::sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

/// Slot map of [`NodeOperator`]s.
#[derive(Debug, Clone)]
pub struct NodeOperators<N> {
    id_to_idx: IndexMap<node_operator::Id, node_operator::Idx>,

    slots: Vec<Option<N>>,

    // for load balancing
    counter: Arc<AtomicUsize>,
}

impl<N: AsRef<node_operator::Id>> NodeOperators<N> {
    pub(super) fn new(slots: impl IntoIterator<Item = Option<N>>) -> Result<Self, CreationError> {
        let slots: Vec<_> = slots.into_iter().collect();

        if slots.len() < cluster::MIN_OPERATORS as usize {
            return Err(CreationError::TooFewOperators(slots.len()));
        }

        if slots.len() > cluster::MAX_OPERATORS {
            return Err(CreationError::TooManyOperatorSlots(slots.len()));
        }

        let mut id_to_idx = IndexMap::with_capacity(slots.len());

        for (idx, slot) in slots.iter().enumerate() {
            if let Some(operator) = &slot {
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

    /// Returns a [`NodeOperator`] responsible for the next request.
    ///
    /// [`NodeOperator`]s are being iterated in round-robin fashion for
    /// load-balancing purposes.
    pub fn next(&self) -> &N {
        // we've checked this in the constructor
        debug_assert!(!self.id_to_idx.is_empty());

        let n = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        let idx = self.id_to_idx[n % self.id_to_idx.len()];
        // if `id_to_idx` map contains the index the slot should always exist
        self.slots[idx as usize].as_ref().unwrap()
    }

    pub(super) fn try_map<T, E>(
        self,
        f: impl Fn(N) -> Result<T, E>,
    ) -> Result<NodeOperators<T>, E> {
        Ok(NodeOperators {
            id_to_idx: self.id_to_idx,
            slots: self
                .slots
                .into_iter()
                .map(|opt| opt.map(&f).transpose())
                .collect::<Result<_, _>>()?,
            counter: self.counter,
        })
    }

    pub(super) fn into_slots(self) -> Vec<Option<N>> {
        self.slots
    }

    pub(super) fn set(&mut self, idx: node_operator::Idx, slot: Option<N>) {
        if let Some(id) = self.get_by_idx(idx).map(|op| *op.as_ref()) {
            self.id_to_idx.shift_remove(&id);
        }

        if self.slots.len() >= idx as usize {
            self.expand(idx);
        }

        if let Some(operator) = &slot {
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
    pub fn get(&self, id: &node_operator::Id) -> Option<&N> {
        self.get_by_idx(self.get_idx(id)?)
    }

    /// Gets an [`NodeOperator`] by [`Idx`].
    pub fn get_by_idx(&self, idx: node_operator::Idx) -> Option<&N> {
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
