//! Slot map of [`NodeOperator`]s.

use {
    crate::{self as cluster, node_operator, NodeOperator},
    itertools::Itertools as _,
    std::collections::HashMap,
};

/// Slot map of [`NodeOperator`]s.
#[derive(Debug, Clone)]
pub struct NodeOperators<D = node_operator::Data> {
    id_to_idx: HashMap<node_operator::Id, node_operator::Idx>,

    slots: Vec<Option<NodeOperator<D>>>,
}

/// [`NodeOperators`] with [`node_operator::SerializedData`].
pub type Serialized = NodeOperators<node_operator::SerializedData>;

impl<Data> NodeOperators<Data> {
    pub(super) fn new(
        slots: impl IntoIterator<Item = Option<NodeOperator<Data>>>,
    ) -> Result<Self, CreationError> {
        let slots: Vec<_> = slots.into_iter().collect();

        if slots.len() > cluster::MAX_OPERATORS {
            return Err(CreationError::TooManyOperatorSlots(slots.len()));
        }

        let mut id_to_idx = HashMap::with_capacity(slots.len());

        for (idx, slot) in slots.iter().enumerate() {
            if let Some(operator) = &slot {
                if id_to_idx.insert(operator.id, idx as u8).is_some() {
                    return Err(CreationError::OperatorDuplicate(operator.id));
                };
            }
        }

        Ok(Self { id_to_idx, slots })
    }

    pub(super) fn into_slots(self) -> Vec<Option<NodeOperator<Data>>> {
        self.slots
    }

    pub(super) fn set(&mut self, idx: node_operator::Idx, slot: Option<NodeOperator<Data>>) {
        if let Some(id) = self.get_by_idx(idx).map(|op| op.id) {
            self.id_to_idx.remove(&id);
        }

        if self.slots.len() >= idx as usize {
            self.expand(idx);
        }

        if let Some(operator) = &slot {
            self.id_to_idx.insert(operator.id, idx);
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
    pub fn get(&self, id: &node_operator::Id) -> Option<&NodeOperator<Data>> {
        self.get_by_idx(self.get_idx(id)?)
    }

    /// Gets an [`NodeOperator`] by [`Idx`].
    pub(super) fn get_by_idx(&self, idx: node_operator::Idx) -> Option<&NodeOperator<Data>> {
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

impl Serialized {
    pub(super) fn deserialize(
        self,
    ) -> Result<NodeOperators, node_operator::DataDeserializationError> {
        Ok(NodeOperators {
            id_to_idx: self.id_to_idx,
            slots: self
                .slots
                .into_iter()
                .map(|slot| slot.map(NodeOperator::deserialize).transpose())
                .try_collect()?,
        })
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
