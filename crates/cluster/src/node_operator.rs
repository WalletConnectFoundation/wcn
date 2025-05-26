//! Entity operating a set of nodes within a WCN cluster.

use {
    crate::{client, node, smart_contract, Node},
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
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
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug)]
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

impl SerializedData {
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}

/// New [`NodeOperator`] that is not a member of WCN cluster yet.
pub struct NewNodeOperator {
    /// [`Id`] of this [`NewNodeOperator`].
    pub id: Id,

    /// [`Data`] of this [`NewNodeOperator`].
    pub data: Data,
}

/// Entity operating a set of [`Node`]s within a WCN cluster.
#[derive(Debug)]
pub struct NodeOperator {
    id: Id,
    data: VersionedData,
}

/// On-chain state of an [`NodeOperator`] has been updated.
pub struct Updated {
    /// Updated [`NodeOperator`].
    pub operator: NodeOperator,
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

#[derive(Debug)]
enum VersionedData {
    V0(DataV0),
}

impl NodeOperator {
    fn deserialize(id: Id, data_bytes: &[u8]) -> Result<Self, DataDeserializationError> {
        use DataDeserializationError as Error;

        if data_bytes.is_empty() {
            return Err(DataDeserializationError::EmptyBuffer);
        }

        let schema_version = data_bytes[0];
        let bytes = &data_bytes[1..];

        let data = match schema_version {
            0 => postcard::from_bytes(bytes).map(VersionedData::V0),
            ver => return Err(Error::UnknownSchemaVersion(ver)),
        }
        .map_err(Error::from_postcard)?;

        Ok(Self { id, data })
    }
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Debug, Deserialize, Serialize)]
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

/// Slot map of [`NodeOperator`]s.
pub(super) struct NodeOperators {
    id_to_idx: HashMap<Id, Idx>,

    // TODO: assert length
    slots: Vec<Option<NodeOperator>>,
}

impl NodeOperators {
    /// Returns whether this map contains the [`NodeOperator`] with the provided
    /// [`Id`].
    pub(super) fn contains(&self, id: &Id) -> bool {
        self.get(id).is_some()
    }

    /// Gets an [`NodeOperator`] by [`Id`].
    pub(super) fn get(&self, id: &Id) -> Option<&NodeOperator> {
        self.get_by_idx(self.get_idx(id)?)
    }

    /// Gets an [`NodeOperator`] by [`Idx`].
    pub(super) fn get_by_idx(&self, idx: Idx) -> Option<&NodeOperator> {
        self.slots.get(idx as usize)?.as_ref()
    }

    /// Gets an [`Idx`] by [`Id`].
    pub(super) fn get_idx(&self, id: &Id) -> Option<Idx> {
        self.id_to_idx.get(id).copied()
    }

    /// Returns the list of [`NodeOperator`] slots.
    pub(super) fn slots(&self) -> &[Option<NodeOperator>] {
        &self.slots
    }

    pub(super) fn freeSlots(&self) -> impl Iterator<Item = Idx> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.is_none().then_some(idx.try_into().unwrap()))
    }
}
