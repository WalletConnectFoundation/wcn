//! Common storage error and result types.

use serde::{Deserialize, Serialize};

/// The Result type returned by Storage functions
pub type StorageResult<T> = Result<T, StorageError>;

/// The error produced from most Storage functions
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageError {
    /// No value found for the given key.
    #[error("Entry not found")]
    EntryNotFound,

    /// Data encryption error from using private namespaces of the IRN.
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

    /// Temporary, separate error type to represent IRN storage backend specific
    /// errors (rocksdb). This is required to collect metrics per error kind in
    /// data shadowing. Once the shadowing is no longer needed, this error will
    /// likely be removed or redesigned.
    #[error("IRN storage backend error: {message}")]
    IrnBackend { kind: String, message: String },
}

pub trait StorageResultExt<T> {
    /// Converts [`StorageResult<T>`] into [`StorageResult<Option<T>>`], while
    /// returning `Ok(None)` on [`StorageError::EntryNotFound`].
    ///
    /// Useful to ignore [`StorageError::EntryNotFound`] when you don't care
    /// about it.
    fn optional(self) -> StorageResult<Option<T>>;
}

impl<T> StorageResultExt<T> for Result<T, StorageError> {
    fn optional(self) -> Result<Option<T>, StorageError> {
        self.map(Some).or_else(|err| {
            if matches!(err, StorageError::EntryNotFound) {
                Ok(None)
            } else {
                Err(err)
            }
        })
    }
}
