use {
    crate::{EntryExpiration, EntryVersion, Field, Value},
    serde::{Deserialize, Serialize},
    std::io,
    wcn_rpc::{self as rpc, transport},
};

#[cfg(feature = "rpc_client")]
pub mod client;
#[cfg(feature = "rpc_client")]
pub use client::Client;
#[cfg(feature = "rpc_server")]
pub mod server;
#[cfg(feature = "rpc_server")]
pub use server::Server;

/// Kind of a Storage API server.
pub enum ServerKind {
    /// Replication Coordinator server.
    Coordinator,

    /// Replica server.
    Replica,

    /// Database server.
    Database,
}

impl ServerKind {
    fn server_name(&self) -> rpc::ServerName {
        match self {
            Self::Coordinator => "StorageApiCoordinator",
            Self::Replica => "StorageApiReplica",
            Self::Database => "StorageApiDatabase",
        }
    }
}

/// RPC error codes produced by this module.
mod error_code {
    /// Client is not authorized to perform the operation.
    pub const UNAUTHORIZED: &str = "unauthorized";

    /// Keyspace versions of the client and the server don't match.
    pub const KEYSPACE_VERSION_MISMATCH: &str = "keyspace_version_mismatch";

    /// Provided key was invalid.
    pub const INVALID_KEY: &str = "invalid_key";
}

type Get = rpc::Unary<{ rpc::id(b"Get") }, GetRequest, Option<GetResponse>>;

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

type Set = rpc::Unary<{ rpc::id(b"Set") }, SetRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetRequest {
    key: ExtendedKey,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type Del = rpc::Unary<{ rpc::id(b"Del") }, DelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DelRequest {
    key: ExtendedKey,
    version: UnixTimestampMicros,
}

type GetExp = rpc::Unary<{ rpc::id(b"GetExp") }, GetExpRequest, Option<GetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpRequest {
    key: ExtendedKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpResponse {
    expiration: UnixTimestampSecs,
}

type SetExp = rpc::Unary<{ rpc::id(b"SetExp") }, SetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetExpRequest {
    key: ExtendedKey,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HGet = rpc::Unary<{ rpc::id(b"HGet") }, HGetRequest, Option<HGetResponse>>;

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

type HSet = rpc::Unary<{ rpc::id(b"HSet") }, HSetRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetRequest {
    key: ExtendedKey,
    field: Field,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HDel = rpc::Unary<{ rpc::id(b"HDel") }, HDelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HDelRequest {
    key: ExtendedKey,
    field: Field,
    version: UnixTimestampMicros,
}

type HGetExp = rpc::Unary<{ rpc::id(b"HGetExp") }, HGetExpRequest, Option<HGetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpRequest {
    key: ExtendedKey,
    field: Field,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpResponse {
    expiration: UnixTimestampSecs,
}

type HSetExp = rpc::Unary<{ rpc::id(b"HSetExp") }, HSetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetExpRequest {
    key: ExtendedKey,
    field: Field,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HCard = rpc::Unary<{ rpc::id(b"HCard") }, HCardRequest, HCardResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardRequest {
    key: ExtendedKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardResponse {
    cardinality: u64,
}

type HScan = rpc::Unary<{ rpc::id(b"HScan") }, HScanRequest, HScanResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanRequest {
    key: ExtendedKey,
    count: u32,
    cursor: Option<Field>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponse {
    records: Vec<HScanResponseRecord>,
    has_more: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponseRecord {
    field: Field,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampSecs(u64);

impl From<EntryExpiration> for UnixTimestampSecs {
    fn from(exp: EntryExpiration) -> Self {
        Self(exp.unix_timestamp_secs)
    }
}

impl From<UnixTimestampSecs> for EntryExpiration {
    fn from(timestamp: UnixTimestampSecs) -> Self {
        Self {
            unix_timestamp_secs: timestamp.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampMicros(u64);

impl From<EntryVersion> for UnixTimestampMicros {
    fn from(version: EntryVersion) -> Self {
        Self(version.unix_timestamp_micros)
    }
}

impl From<UnixTimestampMicros> for EntryVersion {
    fn from(timestamp: UnixTimestampMicros) -> Self {
        Self {
            unix_timestamp_micros: timestamp.0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtendedKey {
    inner: Vec<u8>,
    keyspace_version: Option<u64>,
}
