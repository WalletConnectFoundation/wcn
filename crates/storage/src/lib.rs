use {
    async_trait::async_trait,
    futures_util::{stream::BoxStream, Stream, StreamExt},
    keys::ToBytes,
    relay_rpc::{
        domain::{ClientId, MessageId, Topic},
        rpc::{self, Params, Request},
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        collections::HashSet,
        fmt::{self, Debug},
        hash::Hash,
        net::SocketAddr,
        ops::Deref,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    wc::metrics::otel::KeyValue,
    wither::bson,
};
pub use {
    error::{StorageError, StorageResult},
    keys::ClusterKey,
    util::*,
};

pub mod error;
pub mod keys;
pub mod util;

/// Options for streaming client mailbox data. These have their defaults per
/// storage type.
#[derive(Debug, Default, Clone)]
pub struct StreamingOptions {
    /// The number of keys per database query.
    pub keys_per_query: Option<usize>,
    /// The number of items per batch for batched database queries.
    pub items_per_batch: Option<usize>,
}

/// Holder the type of data will be serialized to be stored.
pub type Data = Vec<u8>;

/// Collection of common traits for the serializable data.
pub trait Serializable:
    'static + Serialize + DeserializeOwned + Hash + PartialEq + Eq + Send + Sync + Clone + Debug + Unpin
{
}

impl<T> Serializable for T where
    T: 'static
        + Serialize
        + DeserializeOwned
        + Hash
        + PartialEq
        + Eq
        + Send
        + Sync
        + Clone
        + Debug
        + Unpin
{
}

pub trait ToMapField: 'static + Send + Sync + Debug + Clone + PartialEq + Eq + Unpin {
    type FieldType: 'static + ToBytes + Send + Sync;

    fn to_field(&self) -> StorageResult<Self::FieldType>;
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AddressedMailboxMapField(pub MessageId);

impl ToBytes for AddressedMailboxMapField {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        Ok(self.0.as_ref().to_be_bytes().into())
    }
}

#[async_trait]
pub trait SetStorage<K, V>: 'static + Send + Sync + Debug
where
    K: ClusterKey,
    V: Serializable,
{
    /// Retrieve data related with the given key.
    async fn smembers(&self, key: &K) -> StorageResult<HashSet<V>>;

    /// Add the value into the set for the given key.
    async fn sadd(&self, key: &K, value: &[&V], ttl: Option<Duration>) -> StorageResult<()>;

    /// Remove the value from the set of the given key.
    async fn srem(&self, key: &K, value: &V) -> StorageResult<()>;

    /// Returns the set cardinality (number of elements) of the set stored at
    /// the given key.
    async fn scard(&self, key: &K) -> StorageResult<usize>;

    /// Returns the remaining time to live of a set member stored at the given
    /// key.
    async fn ttl(&self, key: &K, value: Option<&V>) -> StorageResult<Option<Duration>>;

    /// Set a time to live for a set member stored at the given key. After the
    /// timeout has expired, the member will automatically be deleted. Passing
    /// `None` will remove the current timeout and persist the key.
    async fn set_ttl(&self, key: &K, value: Option<&V>, ttl: Option<Duration>)
        -> StorageResult<()>;
}

#[async_trait]
pub trait StreamableSetStorage<K, V>: SetStorage<K, V>
where
    K: ClusterKey,
    V: Serializable,
{
    /// Creates a [`Stream`] that returns items of the set stored at the given
    /// key.
    ///
    /// This function is designed to stream from multiple keys simultaneously.
    async fn smembers_stream<'a>(
        &'a self,
        keys: BoxStream<'a, K>,
        opts: StreamingOptions,
    ) -> StorageResult<StorageStream<V>>;
}

#[async_trait]
pub trait MapStorage<K, F, V>: 'static + Send + Sync + Debug
where
    K: ClusterKey,
    F: 'static + Send + Sync,
    V: Serializable,
{
    /// Retrieve all values from a map.
    async fn hvals(&self, key: &K) -> StorageResult<Vec<V>>;

    /// Set field value of a map.
    async fn hset(&self, key: &K, field: &F, value: &V, ttl: Option<Duration>)
        -> StorageResult<()>;

    /// Get field value of a map.
    async fn hget(&self, key: &K, field: &F) -> StorageResult<Option<V>>;

    /// Delete field of a map.
    async fn hdel(&self, key: &K, field: &F) -> StorageResult<()>;

    /// Returns the number of fields in a map.
    async fn hcard(&self, key: &K) -> StorageResult<usize>;

    /// Returns remaining TTL of a specific field in a map.
    async fn ttl(&self, key: &K, field: &F) -> StorageResult<Option<Duration>>;

    /// Set TTL of a specific field in a map.
    async fn set_ttl(&self, key: &K, field: &F, ttl: Option<Duration>) -> StorageResult<()>;
}

#[async_trait]
pub trait StreamableMapStorage<K, F, V>: MapStorage<K, F, V>
where
    K: ClusterKey,
    F: 'static + Send + Sync,
    V: Serializable,
{
    /// Creates a [`Stream`] that returns values of the map stored at the given
    /// key.
    ///
    /// This function is designed to stream from multiple keys simultaneously.
    async fn hvals_stream<'a>(
        &'a self,
        keys: BoxStream<'a, K>,
        opts: StreamingOptions,
    ) -> StorageResult<StorageStream<V>>;
}

/// Thin wrapper around the underlying data stream to make the stream [`Sized`],
/// which is a requirement for most stream methods.
pub struct StorageStream<'a, T: Serializable>(
    Pin<Box<dyn Stream<Item = StorageResult<T>> + Send + 'a>>,
);

impl<'a, T> StorageStream<'a, T>
where
    T: Serializable,
{
    pub fn new<U>(stream: U) -> Self
    where
        U: Stream<Item = StorageResult<T>> + Send + 'a,
    {
        Self(Box::pin(stream))
    }
}

impl<'a, T> Stream for StorageStream<'a, T>
where
    T: Serializable,
{
    type Item = StorageResult<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

#[async_trait]
pub trait KeyValueStorage<K, V>: 'static + Send + Sync + Debug
where
    K: ClusterKey,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    /// Retrieve the data associated with the given key.
    async fn get(&self, key: &K) -> StorageResult<Option<V>>;

    /// Set the value for the given key.
    async fn set(&self, key: &K, value: &V, ttl: Option<Duration>) -> StorageResult<()>;

    /// Delete the value associated with the given key.
    async fn del(&self, key: &K) -> StorageResult<()>;

    /// Increment the integer value of a key by the given amount. If the key
    /// does not exist, it is set to 0 before performing the operation. Returns
    /// the value of the key after the increment is applied. An error is
    /// returned if the key contains a value of the wrong type or contains a
    /// string that can not be represented as integer.
    async fn incr(&self, key: &K, delta: i64) -> StorageResult<i64>;

    /// Returns the remaining time to live of a key that has a timeout.
    async fn ttl(&self, key: &K) -> StorageResult<Option<Duration>>;

    /// Set a timeout on key. After the timeout has expired, the key will
    /// automatically be deleted. Passing `None` will remove the current timeout
    /// and persist the key.
    async fn set_ttl(&self, key: &K, ttl: Option<Duration>) -> StorageResult<()>;
}

#[async_trait]
impl<K, V, P> KeyValueStorage<K, V> for P
where
    K: ClusterKey,
    V: Serialize + DeserializeOwned + Send + Sync,
    P: Deref + fmt::Debug + Send + Sync + 'static,
    P::Target: KeyValueStorage<K, V>,
{
    async fn get(&self, key: &K) -> StorageResult<Option<V>> {
        self.deref().get(key).await
    }

    async fn set(&self, key: &K, value: &V, ttl: Option<Duration>) -> StorageResult<()> {
        self.deref().set(key, value, ttl).await
    }

    async fn del(&self, key: &K) -> StorageResult<()> {
        self.deref().del(key).await
    }

    async fn incr(&self, key: &K, delta: i64) -> StorageResult<i64> {
        self.deref().incr(key, delta).await
    }

    async fn ttl(&self, key: &K) -> StorageResult<Option<Duration>> {
        self.deref().ttl(key).await
    }

    async fn set_ttl(&self, key: &K, ttl: Option<Duration>) -> StorageResult<()> {
        self.deref().set_ttl(key, ttl).await
    }
}

pub type BoxGossipStream<K, V> =
    Pin<Box<dyn Stream<Item = StorageResult<GossipMessage<K, V>>> + Send>>;

/// Represents a gossip messages received via a [`GossipSubscriber`].
#[derive(Debug, Clone)]
pub struct GossipMessage<K, V> {
    /// The gossip channel this message was received from.
    pub channel: K,
    /// The gossip message payload.
    pub payload: V,
}

impl<K, V> GossipMessage<K, V> {
    pub fn new(channel: K, payload: V) -> Self {
        Self { channel, payload }
    }
}

#[async_trait]
pub trait GossipPublisher<K, V>: 'static + Send + Sync
where
    K: ToBytes,
    V: Serialize,
{
    /// Publishes a message to a gossip channel represented by the storage key.
    async fn publish(&self, channel: &K, payload: &V) -> StorageResult<()>;
}

#[async_trait]
pub trait GossipSubscriber<K, V>: Send + Sync {
    type Stream: Stream<Item = StorageResult<GossipMessage<K, V>>> + Send;

    /// Creates a subscription stream for the specified channels.
    async fn subscribe(&self, channels: Vec<K>) -> StorageResult<Self::Stream>;
}

pub struct BoxGossipSubscriber<T> {
    inner: T,
}

impl<T> BoxGossipSubscriber<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<K, V, T> GossipSubscriber<K, V> for BoxGossipSubscriber<T>
where
    T: GossipSubscriber<K, V> + Send + Sync,
    T::Stream: 'static,
    K: Send + 'static,
{
    type Stream = BoxGossipStream<K, V>;

    async fn subscribe(&self, channels: Vec<K>) -> StorageResult<Self::Stream> {
        Ok(self.inner.subscribe(channels).await?.boxed())
    }
}

pub mod routing_table {
    use super::*;

    /// Represents an entry of the routing table.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
    pub struct RoutingTableEntry {
        /// The client id of this entry mapping
        pub client_id: ClientId,
        /// The broker addr of this entry mapping
        pub relay_addr: SocketAddr,
    }

    #[derive(Clone)]
    pub struct RoutingTableEntryMapField(ClientId);

    impl ToBytes for RoutingTableEntryMapField {
        fn to_bytes(&self) -> StorageResult<Vec<u8>> {
            Ok(self
                .0
                .decode()
                .map_err(|_| StorageError::Deserialize)?
                .as_ref()
                .into())
        }
    }

    // This implementation assumes that we can only have one routing table entry per
    // client.
    impl ToMapField for RoutingTableEntry {
        type FieldType = RoutingTableEntryMapField;

        fn to_field(&self) -> StorageResult<Self::FieldType> {
            Ok(RoutingTableEntryMapField(self.client_id.clone()))
        }
    }
}

pub mod mailbox {
    use {
        super::*,
        relay_rpc::domain::{DecodedClientId, DecodedTopic},
        std::net::SocketAddrV4,
    };

    #[async_trait]
    pub trait AddressedMailboxStorage: 'static + std::fmt::Debug + Send + Sync {
        async fn store(&self, message: &MailboxMessage) -> Result<(), MailboxStorageError>;

        async fn remove(&self, message: &MailboxMessage) -> Result<(), MailboxStorageError>;

        async fn remove_by_id(
            &self,
            recipient_id: &ClientId,
            topic: &Topic,
            id: MessageId,
        ) -> Result<(), MailboxStorageError>;
    }

    #[async_trait]
    pub trait StreamableAddressedMailboxStorage: AddressedMailboxStorage {
        async fn retrieve_stream<'a>(
            &'a self,
            recipient_id: &ClientId,
            topics: BoxStream<'a, Topic>,
            opts: StreamingOptions,
        ) -> Result<StorageStream<MailboxMessage>, MailboxStorageError>;
    }

    #[derive(Debug, thiserror::Error)]
    pub enum MailboxStorageError {
        #[error("Invalid message type")]
        InvalidMessage,

        #[error("Message is too big")]
        MessageSize,

        #[error("Missing or invalid sender ID")]
        InvalidSender,

        #[error("Missing or invalid recipient ID")]
        InvalidRecipient,

        #[error("Invalid message TTL")]
        InvalidTtl,

        #[error("Invalid topic")]
        InvalidTopic,

        #[error("Serialization error: {0}")]
        Serialization(#[from] bson::ser::Error),

        #[error("Generic storage error: {0}")]
        Storage(#[from] StorageError),

        #[error("Database error: {0}")]
        Database(#[from] wither::WitherError),

        #[error("{0}")]
        Other(String),
    }

    /// Represents entry of mailbox cache.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
    pub struct MailboxMessage {
        /// ID of the sender.
        pub sender_id: Option<ClientId>,

        /// ID of the recipient. This is optional because the recipient is not
        /// known yet for messages broadcasted before any recipient has
        /// connected. It's a bit of an edge case. All messages with no
        /// recipient are returned when calling [`Mailbox::retrieve`].
        pub recipient_id: Option<ClientId>,

        /// The actual message sent from the sender.
        pub content: Request,

        /// The timestamp of the original message.
        ///
        /// Note: The `default` attribute is for backwards compatibility with
        /// older messages that didn't have the timestamp.
        #[serde(default)]
        pub timestamp: i64,
    }

    /// Represents the mailbox polling gossip payload.
    #[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
    pub struct MailboxGossipPayload<T> {
        /// The client for whom the message delivery failed.
        pub client_id: DecodedClientId,
        /// The topic of the failed to deliver message.
        pub topic: DecodedTopic,
        /// The publishing relay's address.
        pub relay_addr: SocketAddrV4,
        /// The reason this gossip was sent.
        pub reason: T,
    }

    /// The TTL for unaddressed mailbox messages.
    pub const UNADDRESSED_MAILBOX_TTL: Duration = Duration::from_secs(5 * 60); // 5 minutes

    impl MailboxMessage {
        pub fn is_addressed(&self) -> bool {
            self.recipient_id.is_some()
        }

        pub fn mailbox_kind(&self) -> MailboxKind {
            if self.is_addressed() {
                MailboxKind::Addressed
            } else {
                MailboxKind::Unaddressed
            }
        }

        pub fn ttl(&self) -> Duration {
            const DEFAULT_EXPIRATION_SECS: u64 = 15 * 60;
            if !self.is_addressed() {
                return UNADDRESSED_MAILBOX_TTL;
            }
            Duration::from_secs(match &self.content.params {
                Params::Publish(params) => params.ttl_secs as u64,
                _ => DEFAULT_EXPIRATION_SECS,
            })
        }

        pub fn topic(&self) -> Option<&Topic> {
            match &self.content.params {
                rpc::Params::Publish(params) => Some(&params.topic),
                _ => None,
            }
        }

        pub fn storage_key(&self) -> Result<keys::MailboxKey, MailboxStorageError> {
            let topic = self.topic().ok_or(MailboxStorageError::InvalidTopic)?;

            topic
                .decode()
                .map_err(|_| MailboxStorageError::InvalidTopic)?;

            self.recipient_id
                .as_ref()
                .map(|client_id| client_id.decode())
                .transpose()
                .map_err(|_| MailboxStorageError::InvalidRecipient)?;

            Ok(keys::MailboxKey(topic.clone(), self.recipient_id.clone()))
        }
    }

    #[derive(Clone)]
    pub struct UnaddressedMailboxMapField {
        sender_id: ClientId,
        message_id: MessageId,
    }

    impl ToBytes for UnaddressedMailboxMapField {
        fn to_bytes(&self) -> StorageResult<Vec<u8>> {
            let sender_id = self
                .sender_id
                .decode()
                .map_err(|_| StorageError::Deserialize)?;
            let sender_id = sender_id.as_ref();
            let message_id = self.message_id.value().to_be_bytes();

            let mut data = Vec::with_capacity(sender_id.len() + message_id.len());
            data.extend_from_slice(sender_id);
            data.extend_from_slice(&message_id);
            Ok(data)
        }
    }

    // This implementation is only relevant for the unaddresed mailbox, where the
    // messages are stored as a `set` type. Addressed mailbox already uses `map`
    // storage, but the fields are derived differently there.
    impl ToMapField for MailboxMessage {
        type FieldType = UnaddressedMailboxMapField;

        fn to_field(&self) -> StorageResult<Self::FieldType> {
            let sender_id = self.sender_id.as_ref().cloned().ok_or_else(|| {
                StorageError::Other(MailboxStorageError::InvalidSender.to_string())
            })?;
            let message_id = self.content.id;

            Ok(UnaddressedMailboxMapField {
                sender_id,
                message_id,
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MailboxKind {
        Unaddressed,
        Addressed,
    }

    impl From<MailboxKind> for KeyValue {
        fn from(val: MailboxKind) -> Self {
            // Note: These are left as legacy names to avoid metrics breaking changes.
            let value = match val {
                MailboxKind::Unaddressed => "ephemeral",
                MailboxKind::Addressed => "persistent",
            };

            KeyValue::new("mailbox_kind", value)
        }
    }

    // Push notification representation
    #[derive(Serialize)]
    pub struct NotificationDetails {
        pub title: String,
        pub body: String,
        pub icon: Option<String>,
        pub url: Option<String>,
    }
}

pub mod watch_table {
    use {
        super::*,
        chrono::{DateTime, Utc},
        relay_rpc::rpc::watch::WatchStatus,
    };

    bitflags::bitflags! {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct WatchStatusFlags: u8 {
            const Accepted  = 0b00000001;
            const Queued    = 0b00000010;
            const Delivered = 0b00000100;
        }
    }

    impl From<WatchStatus> for WatchStatusFlags {
        fn from(value: WatchStatus) -> Self {
            match value {
                WatchStatus::Accepted => Self::Accepted,
                WatchStatus::Queued => Self::Queued,
                WatchStatus::Delivered => Self::Delivered,
            }
        }
    }

    impl From<&[WatchStatus]> for WatchStatusFlags {
        fn from(value: &[WatchStatus]) -> Self {
            let mut flags = Self::empty();

            for status in value {
                flags |= Self::from(*status);
            }

            flags
        }
    }

    /// Represents an entry of the watch table.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
    pub struct WatchTableEntry {
        /// Service name of the watcher.
        pub service_url: String,

        /// The webhook URL to notify.
        pub webhook_url: String,

        /// Store expiration timestamp because of current TTL limitations of the
        /// underlying storage.
        #[serde(with = "chrono::serde::ts_seconds")]
        pub expiration: DateTime<Utc>,

        /// List of tags for this watcher.
        ///
        /// Note: These must be ordered for binary search.
        pub tags: Vec<u32>,

        /// List of statuses for this watcher.
        #[serde(with = "crate::util::flags_as_bits")]
        pub statuses: WatchStatusFlags,
    }

    impl WatchTableEntry {
        /// Returns whether the entry is the same as the other one based on the
        /// unique combination of service URL and webhook URL.
        #[inline]
        pub fn is_same_entry(&self, other: &Self) -> bool {
            self.is_same(&other.service_url, &other.webhook_url)
        }

        /// Returns whether this is the entry for the given unique combination
        /// of service URL and webhook URL.
        #[inline]
        pub fn is_same(&self, service_url: &str, webhook_url: &str) -> bool {
            self.service_url == service_url && self.webhook_url == webhook_url
        }

        /// Returns whether the entry is expired.
        #[inline]
        pub fn is_expired(&self) -> bool {
            self.expiration < Utc::now()
        }

        /// Performs a binary search for the given tag.
        #[inline]
        pub fn includes_tag(&self, tag: u32) -> bool {
            self.tags.binary_search(&tag).is_ok()
        }

        /// Returns whether the watch entry includes the given status.
        #[inline]
        pub fn includes_status(&self, status: WatchStatus) -> bool {
            self.statuses.contains(status.into())
        }
    }

    #[derive(Clone)]
    pub struct WatchTableEntryMapField {
        service_url: String,
        webhook_url: String,
    }

    impl WatchTableEntryMapField {
        pub fn new(service_url: impl Into<String>, webhook_url: impl Into<String>) -> Self {
            Self {
                service_url: service_url.into(),
                webhook_url: webhook_url.into(),
            }
        }
    }

    impl ToBytes for WatchTableEntryMapField {
        fn to_bytes(&self) -> StorageResult<Vec<u8>> {
            let service_url = self.service_url.as_bytes();
            let webhook_url = self.webhook_url.as_bytes();
            let mut data = Vec::with_capacity(service_url.len() + webhook_url.len());
            data.extend_from_slice(service_url);
            data.extend_from_slice(webhook_url);
            Ok(data)
        }
    }

    // This implementation assumes that the entries should be unique per
    // `service_url + webhook_url` combination. See
    // [`WatchTableEntry::is_same_entry`].
    impl ToMapField for WatchTableEntry {
        type FieldType = WatchTableEntryMapField;

        fn to_field(&self) -> StorageResult<Self::FieldType> {
            Ok(WatchTableEntryMapField {
                service_url: self.service_url.clone(),
                webhook_url: self.webhook_url.clone(),
            })
        }
    }
}
