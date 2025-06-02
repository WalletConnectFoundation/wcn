//! Entity operating a set of nodes within a WCN cluster.

use {
    crate::{
        client,
        node,
        smart_contract,
        LogicalError,
        Node,
        Version as ClusterVersion,
        View as ClusterView,
    },
    arc_swap::ArcSwap,
    derive_more::From,
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, ops::Sub, sync::Arc},
};

/// Globally unique identifier of a [`NodeOperator`];
pub type Id = smart_contract::PublicKey;

/// Locally unique identifier of a [`NodeOperator`] within a WCN cluster.
///
/// Refers to a position within the [`NodeOperators`] slot map.
pub type Idx = u8;

/// Name of a [`NodeOperator`].
///
/// Used for informational purposes only.
/// Expected to be unique within the cluster, but not enforced to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Name(String);

impl Name {
    /// Maximum allowed length of a [`Name`] (in bytes).
    pub const MAX_LENGTH: usize = 32;

    /// Tries to create a new [`Name`] out of the provided [`ToString`].
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

/// On-chain data of a [`NodeOperator`].
#[derive(Clone, Debug)]
pub struct Data {
    /// Name of the [`NodeOperator`].
    pub name: Name,

    /// List of [`node`]s of the [`NodeOperator`].
    pub nodes: Vec<node::Data>,

    /// List of [`client`]s authorized to use the WCN cluster on behalf of the
    /// [`NodeOperator`].
    pub clients: Vec<client::Data>,
}

/// [`NodeOperator`] [`Data`] serialized for on-chain storage.
#[derive(Debug)]
pub struct SerializedData(Vec<u8>);

/// [`NodeOperator`] [`Data`] that can be shared and modified across threads.
pub struct SharedData(Arc<ArcSwap<VersionedData>>);

/// Entity operating a set of [`Node`]s within a WCN cluster.
#[derive(Clone, Debug)]
pub struct NodeOperator<D = Data> {
    id: Id,
    data: D,
}

/// [`NodeOperator`] with [`SerializedData`].
pub type SerializedNodeOperator = NodeOperator<SerializedData>;

/// [`NodeOperator`] with [`VersionedData`].
pub type VersionedNodeOperator = NodeOperator<VersionedData>;

/// [`NodeOperator`] with [`SharedData`].
pub type SharedNodeOperator = NodeOperator<SharedData>;

impl<D> NodeOperator<D> {
    /// Creates a [`NewNodeOperator`].
    pub fn new(id: Id, data: Data) -> NodeOperator {
        NodeOperator { id, data }
    }

    /// Returns [`Id`] of this [`NodeOperator`].
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// Returns data of this [`NodeOperator`].
    pub fn data(&self) -> &D {
        &self.data
    }
}

/// Event of a new [`NodeOperator`] being added to a WCN cluster.
pub struct Added {
    /// [`NodeOperator`] being added.
    pub operator: VersionedNodeOperator,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Added {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        todo!()
    }
}

/// Event of a [`NodeOperator`] being updated.
pub struct Updated {
    /// Updated [`NodeOperator`].
    pub operator: VersionedNodeOperator,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Updated {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        let mut applied = false;

        if let Some(migration) = &mut view.migration {
            if let Some(data) = migration
                .keyspace_mut()
                .operator_data_mut(self.operator.id())
            {
                *data = self.operator.data.clone();
                applied = true;
            }
        }

        if let Some(data) = view.keyspace.operator_data_mut(self.operator.id()) {
            *data = self.operator.data;
            applied = true;
        }

        if !applied {
            return Err(LogicalError::UnknownOperator(self.operator.id));
        }

        Ok(())
    }
}

/// Event of a [`NodeOperator`] being removed from a WCN cluster.
pub struct Removed {
    /// [`Id`] of the [`NodeOperator`] being removed.
    pub id: Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Removed {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        todo!()
    }
}

impl NodeOperator {
    pub(super) fn serialize(self) -> Result<SerializedNodeOperator, DataSerializationError> {
        Ok(VersionedNodeOperator {
            id: self.id,
            data: self.data.serialize()?,
        })
    }
}

impl Data {
    pub(super) fn serialize(self) -> Result<SerializedData, DataSerializationError> {
        use DataSerializationError as Error;

        // TODO: encrypt

        let data = DataV0 {
            name: self.name,
            nodes: self.nodes.into_iter().map(Into::into).collect(),
            clients: self.clients.into_iter().map(Into::into).collect(),
        };

        let size = postcard::experimental::serialized_size(&data).map_err(Error::from_postcard)?;

        // reserve first byte for versioning
        let mut buf = Vec::with_capacity(size + 1);
        buf[0] = 0; // current schema version
        postcard::to_slice(&data, &mut buf[1..]).map_err(Error::from_postcard)?;
        Ok(SerializedData(buf))
    }
}

/// Backwards-compatible [`NodeOperator`] [`Data`] supporting multiple versions.
#[derive(Debug, Clone, From)]
pub struct VersionedData(VersionedDataInner);

impl VersionedData {
    fn v0(data: DataV0) -> Self {
        Self(VersionedDataInner::V0(data))
    }
}

#[derive(Debug, Clone)]
enum VersionedDataInner {
    V0(DataV0),
}

impl VersionedNodeOperator {
    fn deserialize(id: Id, data_bytes: &[u8]) -> Result<Self, DataDeserializationError> {
        use DataDeserializationError as Error;

        if data_bytes.is_empty() {
            return Err(DataDeserializationError::EmptyBuffer);
        }

        let schema_version = data_bytes[0];
        let bytes = &data_bytes[1..];

        let data = match schema_version {
            0 => postcard::from_bytes(bytes).map(VersionedData::v0),
            ver => return Err(Error::UnknownSchemaVersion(ver)),
        }
        .map_err(Error::from_postcard)?;

        Ok(Self { id, data })
    }
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DataV0 {
    name: Name,
    nodes: Vec<node::DataV0>,
    clients: Vec<client::DataV0>,
}

#[derive(Debug, thiserror::Error)]
pub enum DataSerializationError {
    #[error("Codec: {0}")]
    Codec(String),
}

impl DataSerializationError {
    fn from_postcard(err: postcard::Error) -> Self {
        Self::Codec(format!("{err:?}"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DataDeserializationError {
    #[error("Empty data buffer")]
    EmptyBuffer,

    #[error("Codec: {0}")]
    Codec(String),

    #[error("Unknown schema version: {0}")]
    UnknownSchemaVersion(u8),
}

impl DataDeserializationError {
    fn from_postcard(err: postcard::Error) -> Self {
        Self::Codec(format!("{err:?}"))
    }
}

impl SerializedData {
    /// Validates that [`SerializedData`] size doesn't exceed the provided
    /// `limit`.
    pub(super) fn validate_size(&self, limit: u16) -> Result<(), DataTooLargeError> {
        let value = self.0.len();
        let limit = limit as usize;

        if value > limit {
            return Err(DataTooLargeError { value, limit });
        }

        Ok(())
    }
}

/// [`SerializedData`] size is too large.
#[derive(Debug, thiserror::Error)]
#[error("Node operator data size is too large (value: {value}, limit: {limit})")]
pub struct DataTooLargeError {
    value: usize,
    limit: usize,
}

/// Slot map of [`NodeOperator`]s.
#[derive(Debug, Clone)]
pub(super) struct NodeOperators {
    id_to_idx: HashMap<Id, Idx>,

    // TODO: assert length
    slots: Vec<Option<VersionedNodeOperator>>,
}

impl NodeOperators {
    pub(super) fn set(&mut self, idx: Idx, slot: Option<VersionedNodeOperator>) {
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

    fn expand(&mut self, idx: Idx) {
        let desired_len = (idx as usize) + 1;
        let slots_to_add = desired_len.checked_sub(self.slots.len());

        for _ in 0..slots_to_add.unwrap_or_default() {
            self.slots.push(None);
        }
    }

    /// Returns whether this map contains the [`NodeOperator`] with the provided
    /// [`Id`].
    pub(super) fn contains(&self, id: &Id) -> bool {
        self.get(id).is_some()
    }

    /// Gets an [`NodeOperator`] by [`Id`].
    pub(super) fn get(&self, id: &Id) -> Option<&VersionedNodeOperator> {
        self.get_by_idx(self.get_idx(id)?)
    }

    /// Gets a mutable reference to a [`NodeOperator`] [`Data`].
    pub(super) fn get_data_mut(&mut self, id: &Id) -> Option<&mut VersionedData> {
        self.get_by_idx_mut(self.get_idx(id)?)
            .map(|operator| &mut operator.data)
    }

    /// Gets an [`NodeOperator`] by [`Idx`].
    pub(super) fn get_by_idx(&self, idx: Idx) -> Option<&VersionedNodeOperator> {
        self.slots.get(idx as usize)?.as_ref()
    }

    /// Mutable version of [`NodeOperators::get_by_idx`].
    fn get_by_idx_mut(&mut self, idx: Idx) -> Option<&mut VersionedNodeOperator> {
        self.slots.get_mut(idx as usize)?.as_mut()
    }

    /// Gets an [`Idx`] by [`Id`].
    pub(super) fn get_idx(&self, id: &Id) -> Option<Idx> {
        self.id_to_idx.get(id).copied()
    }

    /// Returns the list of [`NodeOperator`] slots.
    pub(super) fn slots(&self) -> &[Option<VersionedNodeOperator>] {
        &self.slots
    }

    pub(super) fn freeSlots(&self) -> impl Iterator<Item = Idx> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.is_none().then_some(idx.try_into().unwrap()))
    }
}
