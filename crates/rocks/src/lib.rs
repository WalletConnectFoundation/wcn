extern crate core;

use serde::{Deserialize, Serialize};

pub mod db;
pub mod util;

pub use {
    db::{
        context::{DataContext, UnixTimestampSecs},
        RocksBackend,
        RocksDatabaseBuilder,
    },
    relay_storage::{StorageError, StorageResult},
};

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
