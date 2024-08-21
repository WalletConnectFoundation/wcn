pub use relay_rocks::StorageError as Error;
use {
    crate::Config,
    derive_more::AsRef,
    futures::{future, stream::BoxStream, Stream, StreamExt, TryFutureExt as _},
    irn::{
        migration::{self, AnyError},
        replication::{
            self,
            Cardinality,
            NoRepair,
            Page,
            Reconcile,
            StorageOperation as Operation,
        },
    },
    raft::Infallible,
    relay_rocks::{
        db::{
            cf::DbColumn,
            context::UnixTimestampMicros,
            migration::ExportItem,
            schema::{self, GenericKey},
            types::{common::iterators::ScanOptions, map::Pair, MapStorage, StringStorage},
        },
        util::timestamp_micros,
        RocksBackend,
        RocksDatabaseBuilder,
        StorageError,
        StorageResult,
        UnixTimestampSecs,
    },
    serde::{Deserialize, Serialize},
    std::{
        fmt::{self, Debug},
        future::Future,
        ops::RangeInclusive,
    },
    wc::metrics::{future_metrics, FutureExt},
};

pub type Key = Vec<u8>;
pub type Field = Vec<u8>;
pub type Value = Vec<u8>;
pub type Cursor = Vec<u8>;

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Get {
    #[as_ref]
    pub key: Key,
}

impl Operation for Get {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Option<Value>;
    type RepairOperation = Set;

    fn repair_operation(&self, new_value: &Self::Output) -> Option<Self::RepairOperation> {
        let new_value = new_value.as_ref()?;

        Some(Set {
            key: self.key.clone(),
            value: new_value.clone(),
            // TODO: We should probably not be using `None` here. Even without querying the
            // expiration time, we can set to some default value (and once the original quorum peers
            // expire, this value will not affect the responses, and eventually will be garbage
            // collected).
            expiration: None,
            version: timestamp_micros(),
        })
    }
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Set {
    #[as_ref]
    pub key: Key,
    pub value: Value,
    pub expiration: Option<UnixTimestampSecs>,

    #[serde(default)]
    pub version: UnixTimestampMicros,
}

impl Operation for Set {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Del {
    #[as_ref]
    pub key: Key,
    pub version: UnixTimestampMicros,
}

impl Operation for Del {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct GetExp {
    #[as_ref]
    pub key: Key,
}

impl Operation for GetExp {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Option<UnixTimestampSecs>;
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct SetExp {
    #[as_ref]
    pub key: Key,
    pub expiration: Option<UnixTimestampSecs>,

    #[serde(default)]
    pub version: UnixTimestampMicros,
}

impl Operation for SetExp {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGet {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGet {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Option<Value>;
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HSet {
    #[as_ref]
    pub key: Key,
    pub field: Field,
    pub value: Value,
    pub expiration: Option<UnixTimestampSecs>,

    #[serde(default)]
    pub version: UnixTimestampMicros,
}

impl Operation for HSet {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HDel {
    #[as_ref]
    pub key: Key,
    pub field: Field,
    pub version: UnixTimestampMicros,
}

impl Operation for HDel {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HCard {
    #[as_ref]
    pub key: Key,
}

impl Operation for HCard {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Cardinality;
    type RepairOperation = NoRepair;

    fn reconcile_results(
        results: &[Self::Output],
        required_replicas: usize,
    ) -> Option<Self::Output> {
        Cardinality::reconcile(results.to_vec(), required_replicas)
    }
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGetExp {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGetExp {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Option<UnixTimestampSecs>;
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HSetExp {
    #[as_ref]
    pub key: Key,
    pub field: Field,
    pub expiration: Option<UnixTimestampSecs>,

    #[serde(default)]
    pub version: UnixTimestampMicros,
}

impl Operation for HSetExp {
    const IS_WRITE: bool = true;
    type Key = Key;
    type Output = ();
    type RepairOperation = NoRepair;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HFields {
    #[as_ref]
    pub key: Key,
}

impl Operation for HFields {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Vec<Field>;
    type RepairOperation = NoRepair;

    fn reconcile_results(
        results: &[Self::Output],
        required_replicas: usize,
    ) -> Option<Self::Output> {
        Vec::reconcile(results.to_vec(), required_replicas)
    }
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HVals {
    #[as_ref]
    pub key: Key,
}

impl Operation for HVals {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Vec<Value>;
    type RepairOperation = NoRepair;

    fn reconcile_results(
        results: &[Self::Output],
        required_replicas: usize,
    ) -> Option<Self::Output> {
        Vec::reconcile(results.to_vec(), required_replicas)
    }
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HScan {
    #[as_ref]
    pub key: Key,
    pub count: u32,
    pub cursor: Option<Cursor>,
}

impl Operation for HScan {
    const IS_WRITE: bool = false;
    type Key = Key;
    type Output = Page<(Cursor, Value)>;
    type RepairOperation = NoRepair;

    fn reconcile_results(
        results: &[Self::Output],
        required_replicas: usize,
    ) -> Option<Self::Output> {
        Page::reconcile(results.to_vec(), required_replicas)
    }
}

/// [`Storage`] backend.
#[derive(Clone, Debug)]
pub struct Storage {
    /// The underlying database.
    db: RocksBackend,

    string: DbColumn<schema::StringColumn>,
    map: DbColumn<schema::MapColumn>,
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

impl replication::Storage<Get> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: Get,
    ) -> impl Future<Output = Result<Option<Value>, Self::Error>> + Send {
        async move {
            self.string
                .get(&GenericKey::new(key_hash, op.key))
                .with_metrics(future_metrics!("storage_operation", "op_name" => "get"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<Set> for Storage {
    type Error = StorageError;

    fn exec(&self, key_hash: u64, op: Set) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.string
                .set(&key, &op.value, op.expiration, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "set"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<Del> for Storage {
    type Error = StorageError;

    fn exec(&self, key_hash: u64, op: Del) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.string
                .del(&key, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "del"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<GetExp> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: GetExp,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.string
                .exp(&key)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "get_exp"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<SetExp> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: SetExp,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.string
                .setexp(&key, op.expiration, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "set_exp"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HGet> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HGet,
    ) -> impl Future<Output = Result<Option<Value>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hget(&key, &op.field)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hget"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HSet> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HSet,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            let pair = Pair::new(op.field, op.value);
            self.map
                .hset(&key, &pair, op.expiration, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hset"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HDel> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HDel,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hdel(&key, &op.field, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hdel"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HCard> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HCard,
    ) -> impl Future<Output = Result<Cardinality, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hcard(&key)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hcard"))
                .await
                .map(|card| Cardinality(card as u64))
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HGetExp> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HGetExp,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hexp(&key, &op.field)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hget_exp"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HSetExp> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HSetExp,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hsetexp(&key, &op.field, op.expiration, op.version)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hset_exp"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HFields> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HFields,
    ) -> impl Future<Output = Result<Vec<Field>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hfields(&key)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hfields"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HVals> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HVals,
    ) -> impl Future<Output = Result<Vec<Value>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            self.map
                .hvals(&key)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hvals"))
                .await
                .map_err(map_err)
        }
    }
}

impl replication::Storage<HScan> for Storage {
    type Error = StorageError;

    fn exec(
        &self,
        key_hash: u64,
        op: HScan,
    ) -> impl Future<Output = Result<Page<(Cursor, Value)>, Self::Error>> + Send {
        async move {
            let key = GenericKey::new(key_hash, op.key);
            let opts = ScanOptions::new(op.count as usize).with_cursor(op.cursor);
            self.map
                .hscan(&key, opts)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hscan"))
                .await
                .map(|res| Page {
                    items: res.items,
                    has_more: res.has_more,
                })
                .map_err(map_err)
        }
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
        Error::Backend { kind, message } => StorageError::IrnBackend { kind, message },
        Error::Other(message) => StorageError::DbEngine(message),
    }
}
