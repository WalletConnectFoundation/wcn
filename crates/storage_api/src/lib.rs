#![allow(clippy::manual_async_fn)]

pub use {
    auth,
    irn_rpc::{identity, Multiaddr, PeerId},
};
use {
    irn_rpc::{self as rpc, transport},
    serde::{Deserialize, Serialize},
    std::{io, time::Duration},
    time::OffsetDateTime as DateTime,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

const RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("storage_api");

/// RPC error codes produced by this module.
mod error_code {
    /// Client is not authorized to perform the operation.
    pub const UNAUTHORIZED: &str = "unauthorized";

    /// Keyspace versions of the client and the server don't match.
    pub const KEYSPACE_VERSION_MISMATCH: &str = "keyspace_version_mismatch";

    /// Provided key was invalid.
    pub const INVALID_KEY: &str = "invalid_key";
}

/// Key in a KV storage.
#[derive(Clone, Debug)]
pub struct Key(Vec<u8>);

impl Key {
    /// Length of a [`Key`] namespace (prefix).
    pub const NAMESPACE_LEN: usize = auth::PUBLIC_KEY_LEN;

    const KIND_SHARED: u8 = 0;
    const KIND_PRIVATE: u8 = 1;

    /// Creates a new shared [`Key`] using the global namespace.
    pub fn shared(bytes: Vec<u8>) -> Self {
        Self::new(bytes, None)
    }

    /// Creates a new private [`Key`] using the provided `namespace`.
    pub fn private(namespace: &auth::PublicKey, bytes: Vec<u8>) -> Self {
        Self::new(bytes, Some(namespace))
    }

    /// Returns namespace of this [`Key`].
    pub fn namespace(&self) -> Option<&[u8; Self::NAMESPACE_LEN]> {
        match *self.0.first()? {
            Self::KIND_SHARED => Some(self.0[1..=Self::NAMESPACE_LEN + 1].try_into().unwrap()),
            _ => None,
        }
    }

    /// Returns the full byte representation of this [`Key`] (including
    /// namespace).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn from_raw_bytes(bytes: Vec<u8>) -> Option<Self> {
        match *bytes.first()? {
            Self::KIND_SHARED => Some(Self(bytes)),
            Self::KIND_PRIVATE if bytes.len() > Self::NAMESPACE_LEN + 1 => Some(Self(bytes)),
            _ => None,
        }
    }

    fn new(bytes: Vec<u8>, namespace: Option<&auth::PublicKey>) -> Self {
        let prefix_len = if namespace.is_some() {
            Self::NAMESPACE_LEN
        } else {
            0
        };

        let mut data = Vec::with_capacity(1 + prefix_len + bytes.len());

        if let Some(namespace) = namespace {
            data.push(Self::KIND_PRIVATE);
            data.extend_from_slice(namespace.as_ref());
        } else {
            data.push(Self::KIND_SHARED);
        };

        data.extend_from_slice(&bytes);
        Self(data)
    }
}

/// Value in a KV storage.
pub type Value = Vec<u8>;

/// Subkey of a [`MapEntry`].
pub type Field = Vec<u8>;

/// Basic KV storage entry.
#[derive(Clone, Debug)]
pub struct Entry {
    /// [`Key`] of this [`Entry`].
    pub key: Key,

    /// [`Value`] of this [`Entry`].
    pub value: Value,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

impl Entry {
    /// Creates a new [`Entry`].
    pub fn new(
        key: impl Into<Key>,
        value: impl Into<Value>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// Map entry in which each [`Value`] is associated with both [`Key`] and subkey
/// ([`Field`]).
#[derive(Clone, Debug)]
pub struct MapEntry {
    /// [`Key`] of this [`Entry`].
    pub key: Key,

    /// [`Field`] of this [`Entry`].
    pub field: Field,

    /// [`Value`] of this [`Entry`].
    pub value: Value,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

impl MapEntry {
    /// Creates a new [`MapEntry`].
    pub fn new(
        key: impl Into<Key>,
        field: impl Into<Field>,
        value: impl Into<Value>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// [`Entry`]/[`MapEntry`] without the associated [`Key`]/[`Field`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Value of this [`Record`].
    pub value: Value,

    /// Expiration time of the associated [`Entry`]/[`MapEntry`].
    pub expiration: EntryExpiration,

    /// Version of the associated [`Entry`]/[`MapEntry`].
    pub version: EntryVersion,
}

/// [`MapEntry`] without the associated [`Key`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MapRecord {
    /// Field of this [`MapRecord`].
    pub field: Field,

    /// Value of this [`MapRecord`].
    pub value: Value,

    /// Expiration time of the associated [`MapEntry`].
    pub expiration: EntryExpiration,

    /// Version of the associated [`MapEntry`].
    pub version: EntryVersion,
}

/// [`Entry`]/[`MapEntry`] expiration time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EntryExpiration {
    unix_timestamp_secs: u64,
}

impl From<UnixTimestampSecs> for EntryExpiration {
    fn from(timestamp: UnixTimestampSecs) -> Self {
        Self {
            unix_timestamp_secs: timestamp.0,
        }
    }
}

impl From<Duration> for EntryExpiration {
    fn from(dur: Duration) -> Self {
        Self {
            unix_timestamp_secs: (DateTime::now_utc() + dur).unix_timestamp() as u64,
        }
    }
}

impl From<DateTime> for EntryExpiration {
    fn from(dt: DateTime) -> Self {
        Self {
            unix_timestamp_secs: dt.unix_timestamp() as u64,
        }
    }
}

impl EntryExpiration {
    fn timestamp(&self) -> UnixTimestampSecs {
        UnixTimestampSecs(self.unix_timestamp_secs)
    }
}

/// [`Entry`]/[`MapEntry`] version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EntryVersion {
    unix_timestamp_micros: u64,
}

impl From<UnixTimestampMicros> for EntryVersion {
    fn from(timestamp: UnixTimestampMicros) -> Self {
        Self {
            unix_timestamp_micros: timestamp.0,
        }
    }
}

impl EntryVersion {
    #[allow(clippy::new_without_default)]
    pub fn new() -> EntryVersion {
        Self {
            unix_timestamp_micros: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        }
    }

    fn timestamp(&self) -> UnixTimestampMicros {
        UnixTimestampMicros(self.unix_timestamp_micros)
    }
}

/// Page of [`MapRecord`]s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapPage {
    /// [`MapRecords`] of this [`Page`].
    pub records: Vec<MapRecord>,

    /// Indicator of whether there's a next [`Page`] or not.
    pub has_next: bool,
}

impl MapPage {
    /// Returns cursor pointing to the next [`Page`].
    pub fn next_page_cursor(&self) -> Option<&Field> {
        self.records.last().map(|entry| &entry.field)
    }
}

#[cfg(feature = "client")]
impl Record {
    fn new(value: Value, expiration: UnixTimestampSecs, version: UnixTimestampMicros) -> Self {
        Self {
            value,
            expiration: expiration.into(),
            version: version.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampSecs(u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampMicros(u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtendedKey {
    inner: Vec<u8>,
    keyspace_version: Option<u64>,
}

type Get = rpc::Unary<{ rpc::id(b"get") }, GetRequest, Option<GetResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetRequest {
    key: ExtendedKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetResponse {
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type Set = rpc::Unary<{ rpc::id(b"set") }, SetRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetRequest {
    key: ExtendedKey,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type Del = rpc::Unary<{ rpc::id(b"del") }, DelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DelRequest {
    key: ExtendedKey,
    version: UnixTimestampMicros,
}

type GetExp = rpc::Unary<{ rpc::id(b"get_exp") }, GetExpRequest, Option<GetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpRequest {
    key: ExtendedKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpResponse {
    expiration: UnixTimestampSecs,
}

type SetExp = rpc::Unary<{ rpc::id(b"set_exp") }, SetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetExpRequest {
    key: ExtendedKey,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HGet = rpc::Unary<{ rpc::id(b"hget") }, HGetRequest, Option<HGetResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetRequest {
    key: ExtendedKey,
    field: Field,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetResponse {
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HSet = rpc::Unary<{ rpc::id(b"hset") }, HSetRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetRequest {
    key: ExtendedKey,
    field: Field,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HDel = rpc::Unary<{ rpc::id(b"hdel") }, HDelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HDelRequest {
    key: ExtendedKey,
    field: Field,
    version: UnixTimestampMicros,
}

type HGetExp = rpc::Unary<{ rpc::id(b"hget_exp") }, HGetExpRequest, Option<HGetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpRequest {
    key: ExtendedKey,
    field: Field,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpResponse {
    expiration: UnixTimestampSecs,
}

type HSetExp = rpc::Unary<{ rpc::id(b"hset_exp") }, HSetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetExpRequest {
    key: ExtendedKey,
    field: Field,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HCard = rpc::Unary<{ rpc::id(b"hcard") }, HCardRequest, HCardResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardRequest {
    key: ExtendedKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardResponse {
    cardinality: u64,
}

type HScan = rpc::Unary<{ rpc::id(b"hscan") }, HScanRequest, HScanResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanRequest {
    key: ExtendedKey,
    count: u32,
    cursor: Option<Field>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponse {
    records: Vec<HScanResponseRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponseRecord {
    field: Field,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

#[derive(Debug, Serialize, Deserialize)]
struct HandshakeRequest {
    access_token: auth::Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum HandshakeErrorResponse {
    InvalidToken(String),
}

type HandshakeResponse = Result<(), HandshakeErrorResponse>;

#[derive(Clone, Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error(transparent)]
    Transport(#[from] transport::Error),

    #[error("Invalid token: {_0}")]
    InvalidToken(String),
}

impl From<HandshakeErrorResponse> for HandshakeError {
    fn from(err: HandshakeErrorResponse) -> Self {
        match err {
            HandshakeErrorResponse::InvalidToken(err) => Self::InvalidToken(err),
        }
    }
}

impl From<io::Error> for HandshakeError {
    fn from(err: io::Error) -> Self {
        Self::Transport(err.into())
    }
}
