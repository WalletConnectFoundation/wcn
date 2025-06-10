//! Entity operating a set of nodes within a WCN cluster.

use {
    crate::{
        self as cluster,
        client,
        node,
        smart_contract,
        Client,
        Node,
        Version as ClusterVersion,
    },
    derive_more::derive::Into,
    serde::{Deserialize, Serialize},
};

/// Globally unique identifier of a [`NodeOperator`];
pub type Id = smart_contract::AccountAddress;

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

    /// List of [`Node`]s of the [`NodeOperator`].
    pub nodes: Vec<Node>,

    /// List of [`Client`]s authorized to use the WCN cluster on behalf of the
    /// [`NodeOperator`].
    pub clients: Vec<Client>,
}

/// [`NodeOperator`] [`Data`] serialized for on-chain storage.
#[derive(Debug, Into)]
pub struct SerializedData(pub(crate) Vec<u8>);

/// [`NodeOperator`] with [`SerializedData`].
pub type Serialized = NodeOperator<SerializedData>;

/// Entity operating a set of [`Node`]s within a WCN cluster.
#[derive(Clone, Debug)]
pub struct NodeOperator<D = Data> {
    /// ID of this [`NodeOperator`].
    pub id: Id,

    /// On-chain data of this [`NodeOperator`].
    pub data: D,
}

/// Event of a new [`NodeOperator`] being added to a WCN cluster.
#[derive(Debug)]
pub struct Added<D = Data> {
    /// [`Idx`] in the [`NodeOperators`] slot map the [`NodeOperator`] is being
    /// placed to.
    pub idx: Idx,

    /// [`NodeOperator`] being added.
    pub operator: NodeOperator<D>,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Added<SerializedData> {
    pub(super) fn deserialize(self) -> Result<Added, DataDeserializationError> {
        Ok(Added {
            idx: self.idx,
            operator: self.operator.deserialize()?,
            cluster_version: self.cluster_version,
        })
    }
}

/// Event of a [`NodeOperator`] being updated.
#[derive(Debug)]
pub struct Updated<D = Data> {
    /// Updated [`NodeOperator`].
    pub operator: NodeOperator<D>,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Updated<SerializedData> {
    pub(super) fn deserialize(self) -> Result<Updated, DataDeserializationError> {
        Ok(Updated {
            operator: self.operator.deserialize()?,
            cluster_version: self.cluster_version,
        })
    }
}

/// Event of a [`NodeOperator`] being removed from a WCN cluster.
#[derive(Debug)]
pub struct Removed {
    /// [`Id`] of the [`NodeOperator`] being removed.
    pub id: Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl NodeOperator {
    pub(super) fn serialize(self) -> Result<NodeOperator<SerializedData>, DataSerializationError> {
        Ok(NodeOperator {
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
        let mut buf = vec![0; size + 1];
        buf[0] = 0; // current schema version
        postcard::to_slice(&data, &mut buf[1..]).map_err(Error::from_postcard)?;
        Ok(SerializedData(buf))
    }
}

impl NodeOperator<SerializedData> {
    pub(super) fn deserialize(self) -> Result<NodeOperator, DataDeserializationError> {
        use DataDeserializationError as Error;

        let data_bytes = self.data.0;

        if data_bytes.is_empty() {
            return Err(DataDeserializationError::EmptyBuffer);
        }

        let schema_version = data_bytes[0];
        let bytes = &data_bytes[1..];

        let data = match schema_version {
            0 => postcard::from_bytes::<DataV0>(bytes).map(Into::into),
            ver => return Err(Error::UnknownSchemaVersion(ver)),
        }
        .map_err(Error::from_postcard)?;

        Ok(NodeOperator { id: self.id, data })
    }
}

// NOTE: The on-chain serialization is non self-describing! Every change to
// the schema should be handled by creating a new version.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DataV0 {
    name: Name,
    nodes: Vec<node::V0>,
    clients: Vec<client::V0>,
}

impl From<Data> for DataV0 {
    fn from(data: Data) -> Self {
        Self {
            name: data.name,
            nodes: data.nodes.into_iter().map(Into::into).collect(),
            clients: data.clients.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<DataV0> for Data {
    fn from(data: DataV0) -> Self {
        Self {
            name: data.name,
            nodes: data.nodes.into_iter().map(Into::into).collect(),
            clients: data.clients.into_iter().map(Into::into).collect(),
        }
    }
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
    /// Validates that [`SerializedData`] size doesn't exceed
    /// [`cluster::Settings::max_node_operator_data_bytes`].
    pub(super) fn validate(&self, settings: &cluster::Settings) -> Result<(), DataTooLargeError> {
        let value = self.0.len();
        let limit = settings.max_node_operator_data_bytes as usize;

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

#[derive(Debug, thiserror::Error)]
#[error("Node operator (id: {0}) is not a member of the cluster")]
pub struct NotFoundError(pub Id);

#[derive(Debug, thiserror::Error)]
#[error("Node operator (id: {0}) is already a member of the cluster")]
pub struct AlreadyExistsError(pub Id);
