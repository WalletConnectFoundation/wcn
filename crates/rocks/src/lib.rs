pub use db::{
    context::{DataContext, UnixTimestampSecs},
    RocksBackend,
    RocksDatabaseBuilder,
    RocksdbDatabaseConfig,
};
use serde::{Deserialize, Serialize};

pub mod db;
pub mod util;

// TODO: This needs a better design to properly incorporate the native
// [`rocksdb::Error`] error kinds. For now though we only use the native errors
// for metrics purposes, so it's fine to have them serialized like this.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("Serialization failed")]
    Serialize,

    #[error("Deserialization failed")]
    Deserialize,

    #[error("Entry not found")]
    EntryNotFound,

    #[error("Backend error: {message}")]
    Backend { kind: String, message: String },

    #[error("Invalid column family")]
    InvalidColumnFamily,

    #[error("Worker channel closed")]
    WorkerChannelClosed,

    #[error("Worker queue overrun")]
    WorkerQueueOverrun,

    #[error("{0}")]
    Other(String),
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Self::Backend {
            kind: format!("{:?}", err.kind()),
            message: err.into_string(),
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

// TODO: This needs to be consolidated with the above error.
// Note: Any changes to this enum are considered breaking, since it's being
// serialized and sent over the wire.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageError {
    /// No value found for the given key.
    #[error("Entry not found")]
    EntryNotFound,

    /// Data encryption error from using private namespaces of the WCN.
    #[error("Data encryption failed: {0}")]
    Encryption(String),

    /// Unauthorized to use a private namespace.
    #[error("Missing namespace authorization")]
    Unauthorized,

    /// Unable to serialize data to store.
    #[error("Failed to serialize data")]
    Serialize,

    /// Unable to deserialize data from store.
    #[error("Failed deserialize data")]
    Deserialize,

    /// Requested operation is not supported by the storage backend.
    #[error("Operation not supported")]
    UnsupportedOperation,

    /// Requested timeout.
    #[error("Request timeout")]
    Timeout,

    /// Wrapper for a generic gossip error.
    #[error("Gossip error: {0}")]
    Gossip(String),

    /// Error produced by underlying database engine.
    #[error("Database error: {0}")]
    DbEngine(String),

    /// An unexpected error occurred.
    #[error("Other error: {0}")]
    Other(String),

    /// Temporary, separate error type to represent WCN storage backend specific
    /// errors (rocksdb). This is required to collect metrics per error kind in
    /// data shadowing. Once the shadowing is no longer needed, this error will
    /// likely be removed or redesigned.
    #[error("WCN storage backend error: {message}")]
    WcnBackend { kind: String, message: String },
}
