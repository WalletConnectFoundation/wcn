pub use relay_rocks::StorageError as Error;
use {
    crate::Config,
    futures::{future, stream::BoxStream, Stream, StreamExt, TryFutureExt as _},
    raft::Infallible,
    relay_rocks::{
        db::{cf::DbColumn, migration::ExportItem, schema},
        RocksBackend, RocksDatabaseBuilder, StorageError, StorageResult,
    },
    std::{
        fmt::{self, Debug},
        future::Future,
        ops::RangeInclusive,
    },
    wcn::migration::{self, AnyError},
};

/// [`Storage`] backend.
#[derive(Clone, Debug)]
pub struct Storage {
    /// The underlying database.
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

    // Required for tests only.
    pub fn db(&self) -> &RocksBackend {
        &self.db
    }
}

impl<St, E> migration::StorageImport<St> for Storage
where
    St: Stream<Item = Result<ExportItem, E>> + Send + 'static,
    E: fmt::Debug,
{
    fn import(&self, data: St) -> impl Future<Output = Result<(), impl AnyError>> {
        self.db.import_all(data).map_err(map_err)
    }
}

impl migration::StorageExport for Storage {
    type Stream = BoxStream<'static, ExportItem>;

    fn export(
        &self,
        keyrange: RangeInclusive<u64>,
    ) -> impl Future<Output = Result<Self::Stream, impl AnyError>> {
        let stream = self
            .db
            .export((self.string.clone(), self.map.clone()), keyrange)
            .boxed();

        future::ok::<_, Infallible>(stream)
    }
}

fn map_err(err: relay_rocks::Error) -> StorageError {
    use relay_rocks::Error;

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
