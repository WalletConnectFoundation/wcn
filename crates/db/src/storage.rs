pub use wcn_rocks::StorageError as Error;
use {
    crate::config::Config,
    wcn_rocks::{
        db::{cf::DbColumn, schema},
        RocksBackend,
        RocksDatabaseBuilder,
        StorageError,
        StorageResult,
    },
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
