use {
    crate::{StorageError, StorageResult},
    relay_rpc::{
        domain::{ClientId, Topic},
        rpc::watch::WatchType,
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        fmt::{Debug, Display, Formatter},
        hash::Hash,
    },
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataTag {
    Generic,
    WatchTable,
    ClientProjectRouting,
    AddressedMailbox,
    UnaddressedMailbox,
    MailboxGossip,
    Mailbox,
    RoutingTable,
    RoutingActivity,
    Unknown,
}

impl DataTag {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Generic => "generic_data",
            Self::WatchTable => "watch_table",
            Self::ClientProjectRouting => "client_project_routing",
            Self::AddressedMailbox => "addressed_mailbox",
            Self::UnaddressedMailbox => "unaddressed_mailbox",
            Self::MailboxGossip => "mailbox_gossip",
            Self::Mailbox => "mailbox",
            Self::RoutingTable => "routing_table",
            Self::RoutingActivity => "routing_activity",
            Self::Unknown => "unknown",
        }
    }
}

/// IRN storage uses shared column families to store data. So in order to avoid
/// key collisions we're prefixing them with a unique value. A `u8` value should
/// be enough to cover all actual all key types.
#[allow(dead_code)]
#[repr(u8)]
enum KeyPrefix {
    Generic = 0,
    WatchTable = 1,
    ClientProjectRouting = 2,
    AddressedMailbox = 3,
    UnaddressedMailbox = 4,
    RoutingTable = 5,
    RoutingActivity = 6,
}

/// Convert a key to bytes that can be used in sharded storage.
///
/// Cluster keys that include partitioning information are serialized using
/// custom logic. They start with a partition key, followed by the actual key,
/// without any extra formatting (or representation optimization).
///
/// The minimal yet unaltered representation must be provided, i.e. fields'
/// binary representations are concatenated, without any kind of metadata. This
/// helps avoiding any unnecessary space overhead. Additionally, this allows the
/// key range queries based on partition key to work correctly.
pub trait ToBytes {
    fn to_bytes(&self) -> StorageResult<Vec<u8>>;
}

/// Convert bytes produced by `to_bytes` back to a key structure.
pub trait FromBytes: Sized {
    fn from_bytes(bytes: &[u8]) -> StorageResult<Self>;
}

/// Marker trait, used to mark keys that are positioned on the hash ring.
pub trait PositionedKey {
    /// Returns the position of the key on the hash ring.
    /// Legacy keys are always at position 0.
    fn position(&self) -> u64;
}

/// Marker trait, used to mark keys without sharding information.
pub trait LegacyKey: Serialize + DeserializeOwned {}

/// Legacy keys do not possess any sharding information, so they are always at
/// position 0.
impl<T: LegacyKey> PositionedKey for T {
    fn position(&self) -> u64 {
        0
    }
}

/// Marker trait, allowing legacy keys and keys possessing sharding information
/// to be used in the same contexts.
///
/// Previously, all keys passed to storage interfaces were plain strings, so any
/// cluster key should implement the `Display` trait.
pub trait ClusterKey:
    'static + Display + Send + Sync + Clone + Debug + Hash + PartialEq + Eq + Unpin
{
    /// Data tag recorded for analytics purposes. Should be unique for each
    /// cluster key type.
    fn data_tag(&self) -> DataTag;
}

/// Storage key for the watch routing data.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct WatchTableKey {
    pub client_id: ClientId,
    pub watch_type: WatchType,
}

impl Display for WatchTableKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let watch_type = match self.watch_type {
            WatchType::Subscriber => "subscriber",
            WatchType::Publisher => "publisher",
        };

        write!(f, "watch/{}/{}", self.client_id, watch_type)
    }
}

impl LegacyKey for WatchTableKey {}

impl ClusterKey for WatchTableKey {
    fn data_tag(&self) -> DataTag {
        DataTag::WatchTable
    }
}

impl ToBytes for WatchTableKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        let client_id = self
            .client_id
            .decode()
            .map_err(|_| StorageError::Serialize)?;
        let client_id = client_id.as_ref();

        let watch_type: u8 = match self.watch_type {
            WatchType::Publisher => 0,
            WatchType::Subscriber => 1,
        };

        let mut data = Vec::with_capacity(client_id.len() + 2);
        data.push(KeyPrefix::WatchTable as u8);
        data.extend_from_slice(client_id);
        data.push(watch_type);

        Ok(data)
    }
}

/// Storage key for the client project routing.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct ClientProjectRoutingKey(pub ClientId);

impl Display for ClientProjectRoutingKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "client/{}/project", self.0)
    }
}

impl LegacyKey for ClientProjectRoutingKey {}

impl ClusterKey for ClientProjectRoutingKey {
    fn data_tag(&self) -> DataTag {
        DataTag::ClientProjectRouting
    }
}

impl ToBytes for ClientProjectRoutingKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(prefixed_binary_key(
            KeyPrefix::ClientProjectRouting,
            self.0.decode().map_err(|_| StorageError::Serialize)?,
        ))
    }
}

/// Storage key for the mailbox.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct MailboxKey(pub Topic, pub Option<ClientId>);

impl Display for MailboxKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let recipient_id = self.1.as_ref().map(ClientId::to_string).unwrap_or_default();
        write!(f, "mailbox/{}/{}", self.0, recipient_id)
    }
}

impl LegacyKey for MailboxKey {}

impl ClusterKey for MailboxKey {
    fn data_tag(&self) -> DataTag {
        if self.1.is_some() {
            DataTag::AddressedMailbox
        } else {
            DataTag::UnaddressedMailbox
        }
    }
}

impl ToBytes for MailboxKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        let topic = self.0.decode().map_err(|_| StorageError::Serialize)?;
        let topic = topic.as_ref();

        let client_id = self
            .1
            .as_ref()
            .map(ClientId::decode)
            .transpose()
            .map_err(|_| StorageError::Serialize)?;
        let client_id = client_id.as_ref().map(AsRef::as_ref);

        let len = 1 + topic.len() + client_id.map(|data| data.len()).unwrap_or(0);

        let prefix = if self.1.is_some() {
            KeyPrefix::AddressedMailbox
        } else {
            KeyPrefix::UnaddressedMailbox
        };

        let mut data = Vec::with_capacity(len);
        data.push(prefix as u8);
        data.extend_from_slice(topic);

        if let Some(client_id) = client_id {
            data.extend_from_slice(client_id);
        }

        Ok(data)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MailboxGossipKey;

impl LegacyKey for MailboxGossipKey {}

impl ClusterKey for MailboxGossipKey {
    fn data_tag(&self) -> DataTag {
        DataTag::MailboxGossip
    }
}

impl Display for MailboxGossipKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.data_tag().as_str())
    }
}

impl ToBytes for MailboxGossipKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(self.data_tag().as_str().as_bytes().into())
    }
}

impl FromBytes for MailboxGossipKey {
    fn from_bytes(bytes: &[u8]) -> StorageResult<Self> {
        let key = Self;

        if bytes != key.data_tag().as_str().as_bytes() {
            Err(StorageError::Deserialize)
        } else {
            Ok(key)
        }
    }
}

/// Cache key for the routing table.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct RoutingTableKey(pub Topic);

impl Display for RoutingTableKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "routing/table/{}", self.0)
    }
}

impl LegacyKey for RoutingTableKey {}

impl ClusterKey for RoutingTableKey {
    fn data_tag(&self) -> DataTag {
        DataTag::RoutingTable
    }
}

impl ToBytes for RoutingTableKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(prefixed_binary_key(
            KeyPrefix::RoutingTable,
            self.0.decode().map_err(|_| StorageError::Serialize)?,
        ))
    }
}

/// Cache key for the routing table activity counter.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct RoutingActivityKey(pub Topic);

impl Display for RoutingActivityKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "routing/activity/{}", self.0)
    }
}

impl LegacyKey for RoutingActivityKey {}

impl ClusterKey for RoutingActivityKey {
    fn data_tag(&self) -> DataTag {
        DataTag::RoutingActivity
    }
}

impl ToBytes for RoutingActivityKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(prefixed_binary_key(
            KeyPrefix::RoutingActivity,
            self.0.decode().map_err(|_| StorageError::Serialize)?,
        ))
    }
}

/// Generic key for custom data consisting of arbitrary bytes.
///
/// Note: In order to avoid key collisions, all `GenericKey` used within a
/// storage namespace must have the same length, or have distinct prefixes of
/// the same length.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub struct GenericKey(Vec<u8>);

impl GenericKey {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self(data.into())
    }
}

impl Display for GenericKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let key = data_encoding::HEXLOWER.encode(&self.0);
        write!(f, "generic/{}", key)
    }
}

impl LegacyKey for GenericKey {}

impl ClusterKey for GenericKey {
    fn data_tag(&self) -> DataTag {
        DataTag::Generic
    }
}

impl ToBytes for GenericKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(prefixed_binary_key(KeyPrefix::Generic, &self.0))
    }
}

impl FromBytes for GenericKey {
    fn from_bytes(bytes: &[u8]) -> StorageResult<Self> {
        Ok(Self(bytes.into()))
    }
}

fn prefixed_binary_key(prefix: KeyPrefix, data: impl AsRef<[u8]>) -> Vec<u8> {
    let data = data.as_ref();
    let mut result = Vec::with_capacity(1 + data.len());
    result.push(prefix as u8);
    result.extend_from_slice(data);
    result
}
