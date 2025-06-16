use {
    crate::{EntryExpiration, EntryVersion, Namespace},
    serde::{Deserialize, Serialize},
    wcn_rpc::{self as rpc},
};

#[cfg(feature = "rpc_client")]
pub mod client;
#[cfg(feature = "rpc_client")]
pub use client::Client;
#[cfg(feature = "rpc_server")]
pub mod server;
#[cfg(feature = "rpc_server")]
pub use server::Server;

/// Storage API RPC server hosted by a WCN Replication Coordinator.
pub const COORDINATOR_RPC_SERVER_NAME: rpc::ServerName =
    rpc::ServerName::new("StorageApiCoordinator");

/// Storage API RPC server hosted by a WCN Replica.
pub const REPLICA_RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("StorageApiReplica");

/// Storage API RPC server hosted by a WCN Database.
pub const DATABASE_RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("StorageApiDatabase");

/// RPC error codes produced by this module.
mod error_code {
    /// Client is not authorized to perform the operation.
    pub const UNAUTHORIZED: &str = "unauthorized";

    /// Keyspace versions of the client and the server don't match.
    pub const KEYSPACE_VERSION_MISMATCH: &str = "keyspace_version_mismatch";

    /// Provided key was invalid.
    pub const INVALID_KEY: &str = "invalid_key";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Context {
    namespace_node_operator_id: [u8; 20],
    namespace_id: u8,
    keyspace_version: Option<u64>,
}

impl 

#[derive(Debug, Serialize, Deserialize)]
struct Key(Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
struct Value(Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
struct Field(Vec<u8>);

type Get = rpc::Unary<{ rpc::id(b"Get") }, GetRequest, Option<GetResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetRequest {
    context: Context,
    key: Key,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetResponse {
    context: Context,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type Set = rpc::Unary<{ rpc::id(b"Set") }, SetRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetRequest {
    context: Context,
    key: Key,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type Del = rpc::Unary<{ rpc::id(b"Del") }, DelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DelRequest {
    context: Context,
    key: Key,
    version: UnixTimestampMicros,
}

type GetExp = rpc::Unary<{ rpc::id(b"GetExp") }, GetExpRequest, Option<GetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpRequest {
    context: Context,
    key: Key,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpResponse {
    expiration: UnixTimestampSecs,
}

type SetExp = rpc::Unary<{ rpc::id(b"SetExp") }, SetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetExpRequest {
    context: Context,
    key: Key,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HGet = rpc::Unary<{ rpc::id(b"HGet") }, HGetRequest, Option<HGetResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetRequest {
    context: Context,
    key: Key,
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
    context: Context,
    key: Key,
    field: Field,
    value: Value,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HDel = rpc::Unary<{ rpc::id(b"HDel") }, HDelRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HDelRequest {
    context: Context,
    key: Key,
    field: Field,
    version: UnixTimestampMicros,
}

type HGetExp = rpc::Unary<{ rpc::id(b"HGetExp") }, HGetExpRequest, Option<HGetExpResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpRequest {
    context: Context,
    key: Key,
    field: Field,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpResponse {
    expiration: UnixTimestampSecs,
}

type HSetExp = rpc::Unary<{ rpc::id(b"HSetExp") }, HSetExpRequest, ()>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetExpRequest {
    context: Context,
    key: Key,
    field: Field,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

type HCard = rpc::Unary<{ rpc::id(b"HCard") }, HCardRequest, HCardResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardRequest {
    context: Context,
    key: Key,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardResponse {
    cardinality: u64,
}

type HScan = rpc::Unary<{ rpc::id(b"HScan") }, HScanRequest, HScanResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanRequest {
    context: Context,
    key: Key,
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

impl From<Context> for Namespace {
    fn from(ctx: Context) -> Self {
        Self {
            node_operator_id: ctx.namespace_node_operator_id,
            id: ctx.namespace_id,
        }
    }
}

impl From<Key> for crate::Key {
    fn from(key: Key) -> Self {
        Self(key.0)
    }
}

impl From<crate::Key> for Key {
    fn from(key: crate::Key) -> Self {
        Self(key.0)
    }
}

impl From<Value> for crate::Value {
    fn from(value: Value) -> Self {
        Self(value.0)
    }
}

impl From<crate::Value> for Value {
    fn from(value: crate::Value) -> Self {
        Self(value.0)
    }
}

impl From<Field> for crate::Field {
    fn from(field: Field) -> Self {
        Self(field.0)
    }
}

impl From<crate::Field> for Field {
    fn from(field: crate::Field) -> Self {
        Self(field.0)
    }
}
