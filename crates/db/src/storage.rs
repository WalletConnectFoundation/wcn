pub use wcn_rocks::StorageError as Error;
use {
    crate::config::Config,
    storage_api::Namespace,
    wcn_rocks::{
        db::{
            cf::DbColumn,
            schema::{self, GenericKey},
        },
        RocksBackend,
        RocksDatabaseBuilder,
        StorageError,
        StorageResult,
    },
    xxhash_rust::xxh3::Xxh3Builder,
};

#[derive(Clone, Debug)]
pub struct Storage {
    db: RocksBackend,
    pub string: DbColumn<schema::StringColumn>,
    pub map: DbColumn<schema::MapColumn>,
}

impl Storage {
    /// Create a new storage instance.
    ///
    /// The `path` is a path to the database directory.
    pub fn new(config: &Config) -> StorageResult<Self> {
        let db = RocksDatabaseBuilder::new(config.rocksdb_dir.clone())
            .with_config(config.rocksdb.clone())
            .with_column_family(schema::StringColumn)
            .with_column_family(schema::InternalStringColumn)
            .with_column_family(schema::MapColumn)
            .with_column_family(schema::InternalMapColumn)
            .build()
            .map_err(map_err)?;

        Ok(Self {
            string: db.column().unwrap(),
            map: db.column().unwrap(),
            db,
        })
    }

    pub fn db(&self) -> &RocksBackend {
        &self.db
    }
}

fn map_err(err: wcn_rocks::Error) -> StorageError {
    use wcn_rocks::Error;

    match err {
        Error::Serialize => StorageError::Serialize,
        Error::Deserialize => StorageError::Deserialize,
        Error::EntryNotFound => StorageError::EntryNotFound,
        Error::InvalidColumnFamily | Error::WorkerChannelClosed | Error::WorkerQueueOverrun => {
            StorageError::DbEngine(err.to_string())
        }
        Error::Backend { kind, message } => StorageError::WcnBackend { kind, message },
        Error::Other(message) => StorageError::DbEngine(message),
    }
}

/// Prepares a [`GenericKey`] for use with [`wcn_rocks`].
pub fn key(namespace: &Namespace, key: &[u8]) -> GenericKey {
    static HASHER: Xxh3Builder = Xxh3Builder::new();

    // Determine position in the keyspace before modifying the key, for consistency
    // with the replication driver logic.
    let pos = std::hash::BuildHasher::hash_one(&HASHER, key);

    // Inject the namespace prefix.
    let key = KeyWrapper::private(namespace, key).into_bytes();

    GenericKey::new(pos, key)
}

/// Key wrapper that adds a prefix to support different key formats (e.g.
/// private vs shared).
///
/// Currently the only supported format is a private key (prefixed with a
/// [`Namespace`]). In future it may be extended to support other key formats.
#[derive(Clone, Debug)]
struct KeyWrapper(Vec<u8>);

impl KeyWrapper {
    /// Key that is prefixed with a [`Namespace`].
    const KIND_PRIVATE: u8 = 0;

    /// Creates a new private [`Key`] using the provided namespace.
    pub fn private(namespace: &Namespace, bytes: impl AsRef<[u8]>) -> Self {
        let ns_bytes = namespace.as_bytes();
        let key_bytes = bytes.as_ref();

        let mut combined = Vec::with_capacity(1 + ns_bytes.len() + key_bytes.len());

        // Write key kind prefix.
        combined.push(Self::KIND_PRIVATE);

        // Write namespace data.
        combined.extend_from_slice(ns_bytes);

        // Write the rest of the key.
        combined.extend_from_slice(key_bytes);

        Self(combined)
    }

    /// Converts this [`Key`] into bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}
