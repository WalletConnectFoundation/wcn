pub use relay_rocks::StorageError as Error;
use {
    crate::Config,
    async_trait::async_trait,
    derive_more::{AsRef, From, TryInto},
    futures::{stream, stream::BoxStream, Stream, StreamExt, TryStreamExt},
    irn::{
        cluster::keyspace::{hashring::Positioned, KeyPosition, KeyRange},
        migration::{
            self,
            booting::PullDataError,
            CommitHintedOperations,
            Export,
            Import,
            StoreHinted,
        },
        replication::{
            Cardinality,
            Page,
            Read,
            ReconciledRead,
            ReplicatableOperation as Operation,
            ReplicatedRequest as Replicated,
            Write,
        },
    },
    relay_rocks::{
        db::{
            cf::DbColumn,
            context::UnixTimestampMicros,
            migration::{
                hinted_ops::{HintedOp, MapHintedOp, StringHintedOp},
                ExportItem,
            },
            schema::{self, GenericKey},
            types::{
                common::{iterators::ScanOptions, CommonStorage},
                map::Pair,
                MapStorage,
                StringStorage,
            },
        },
        util::timestamp_micros,
        RocksBackend,
        RocksDatabaseBuilder,
        StorageError,
        StorageResult,
        UnixTimestampSecs,
    },
    serde::{Deserialize, Serialize},
    std::{fmt, fmt::Debug},
    wc::future_metrics::FutureExt,
};

pub type Key = Vec<u8>;
pub type Field = Vec<u8>;
pub type Value = Vec<u8>;
pub type Cursor = Vec<u8>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MigrationHeader {
    pub key_range: KeyRange<KeyPosition>,
    pub cluster_view_version: u128,
}

pub enum MigrationRequest {
    PullDataRequest(MigrationHeader),
    PushDataRequest(MigrationHeader),
}

pub type PullDataResult = Result<ExportItem, PullDataError>;
pub type PushDataResponse = Result<(), PullDataError>;

#[derive(Clone, Debug, From, TryInto, Serialize, Deserialize)]
pub enum ReplicationRequest {
    Get(Replicated<Get>),
    Set(Replicated<Set>),
    Del(Replicated<Del>),
    GetExp(Replicated<GetExp>),
    SetExp(Replicated<SetExp>),

    HGet(Replicated<HGet>),
    HSet(Replicated<HSet>),
    HDel(Replicated<HDel>),
    HCard(Replicated<HCard>),
    HGetExp(Replicated<HGetExp>),
    HSetExp(Replicated<HSetExp>),
    HFields(Replicated<HFields>),
    HVals(Replicated<HVals>),
    HScan(Replicated<HScan>),
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Get {
    #[as_ref]
    pub key: Key,
}

impl Operation for Get {
    type Type = Read;
    type Key = Key;
    type Output = Option<Value>;
    type RepairOperation = Set;

    fn repair_operation(&self, new_value: Self::Output) -> Option<Self::RepairOperation> {
        new_value.as_ref()?;

        Some(Set {
            key: self.key.clone(),
            value: new_value.unwrap(),
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
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Del {
    #[as_ref]
    pub key: Key,
}

impl Operation for Del {
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct GetExp {
    #[as_ref]
    pub key: Key,
}

impl Operation for GetExp {
    type Type = Read;
    type Key = Key;
    type Output = Option<UnixTimestampSecs>;
    type RepairOperation = ();
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
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGet {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGet {
    type Type = Read;
    type Key = Key;
    type Output = Option<Value>;
    type RepairOperation = ();
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
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HDel {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HDel {
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HCard {
    #[as_ref]
    pub key: Key,
}

impl Operation for HCard {
    type Type = ReconciledRead;
    type Key = Key;
    type Output = Cardinality;
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGetExp {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGetExp {
    type Type = Read;
    type Key = Key;
    type Output = Option<UnixTimestampSecs>;
    type RepairOperation = ();
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
    type Type = Write;
    type Key = Key;
    type Output = ();
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HFields {
    #[as_ref]
    pub key: Key,
}

impl Operation for HFields {
    type Type = ReconciledRead;
    type Key = Key;
    type Output = Vec<Field>;
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HVals {
    #[as_ref]
    pub key: Key,
}

impl Operation for HVals {
    type Type = ReconciledRead;
    type Key = Key;
    type Output = Vec<Value>;
    type RepairOperation = ();
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HScan {
    #[as_ref]
    pub key: Key,
    pub count: u32,
    pub cursor: Option<Cursor>,
}

impl Operation for HScan {
    type Type = ReconciledRead;
    type Key = Key;
    type Output = Page<(Cursor, Value)>;
    type RepairOperation = ();
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
            .reader_batch_threads(config.rocksdb_num_batch_threads)
            .reader_callback_threads(config.rocksdb_num_callback_threads)
            .with_column_family(schema::StringColumn)
            .with_column_family(schema::InternalStringColumn)
            .with_column_family(schema::MapColumn)
            .with_column_family(schema::InternalMapColumn)
            .with_column_family(schema::InternalHintedOpsColumn)
            .build()
            .map_err(map_err)?;

        Ok(Self {
            string: db.column().unwrap(),
            map: db.column().unwrap(),
            db,
        })
    }

    // TODO: Consider turning tests that require direct access to the unrelying
    // storage into unit tests.

    // Required for tests only.
    pub fn string(&self) -> &DbColumn<schema::StringColumn> {
        &self.string
    }

    // Required for tests only.
    pub fn map(&self) -> &DbColumn<schema::MapColumn> {
        &self.map
    }

    // Required for tests only.
    pub fn db(&self) -> &RocksBackend {
        &self.db
    }
}

const fn metric_labels(op_name: &'static str) -> [metrics::Label; 2] {
    [
        wc::future_metrics::future_name("storage_operation"),
        metrics::Label::from_static_parts("op_name", op_name),
    ]
}

#[async_trait]
impl irn::Storage<Positioned<()>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, _: Positioned<()>) -> Result<Self::Ok, Self::Error> {
        Err(StorageError::UnsupportedOperation)
    }
}

#[async_trait]
impl irn::Storage<Positioned<Get>> for Storage {
    type Ok = Option<Value>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<Get>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.string
            .get(&key)
            .with_labeled_metrics(const { &metric_labels("get") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<Set>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<Set>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.string
            .set(&key, &op.inner.value, op.inner.expiration, op.inner.version)
            .with_labeled_metrics(const { &metric_labels("set") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<Set>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<Set>>) -> Result<Self::Ok, Self::Error> {
        let op = s.operation.inner;
        let op = StringHintedOp::Set {
            key: GenericKey::new(s.operation.position, op.key),
            value: op.value,
            expiration: op.expiration,
            version: op.version,
        };

        self.string
            .add_hinted_op(HintedOp::String(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_set") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<Del>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<Del>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.string
            .del(&key)
            .with_labeled_metrics(const { &metric_labels("del") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<Del>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<Del>>) -> Result<Self::Ok, Self::Error> {
        let op = StringHintedOp::Del {
            key: GenericKey::new(s.operation.position, s.operation.inner.key),
        };

        self.string
            .add_hinted_op(HintedOp::String(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_del") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<GetExp>> for Storage {
    type Ok = Option<UnixTimestampSecs>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<GetExp>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.string
            .exp(&key)
            .with_labeled_metrics(const { &metric_labels("get_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<SetExp>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<SetExp>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.string
            .setexp(&key, op.inner.expiration, op.inner.version)
            .with_labeled_metrics(const { &metric_labels("set_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<SetExp>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<SetExp>>) -> Result<Self::Ok, Self::Error> {
        let op = s.operation.inner;
        let op = StringHintedOp::SetExp {
            key: GenericKey::new(s.operation.position, op.key),
            expiration: op.expiration,
            version: op.version,
        };

        self.string
            .add_hinted_op(HintedOp::String(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_set_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HGet>> for Storage {
    type Ok = Option<Value>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HGet>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hget(&key, &op.inner.field)
            .with_labeled_metrics(const { &metric_labels("hget") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HSet>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HSet>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        let pair = Pair::new(op.inner.field, op.inner.value);
        self.map
            .hset(&key, &pair, op.inner.expiration, op.inner.version)
            .with_labeled_metrics(const { &metric_labels("hset") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<HSet>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<HSet>>) -> Result<Self::Ok, Self::Error> {
        let op = s.operation.inner;
        let op = MapHintedOp::Set {
            key: GenericKey::new(s.operation.position, op.key),
            field: op.field,
            value: op.value,
            expiration: op.expiration,
            version: op.version,
        };

        self.map
            .add_hinted_op(HintedOp::Map(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_hset") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HDel>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HDel>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hdel(&key, &op.inner.field)
            .with_labeled_metrics(const { &metric_labels("hdel") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<HDel>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<HDel>>) -> Result<Self::Ok, Self::Error> {
        let op = s.operation.inner;
        let op = MapHintedOp::Del {
            key: GenericKey::new(s.operation.position, op.key),
            field: op.field,
        };

        self.map
            .add_hinted_op(HintedOp::Map(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_hdel") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HCard>> for Storage {
    type Ok = Cardinality;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HCard>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hcard(&key)
            .with_labeled_metrics(const { &metric_labels("hcard") })
            .await
            .map(|card| Cardinality(card as u64))
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HGetExp>> for Storage {
    type Ok = Option<UnixTimestampSecs>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HGetExp>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hexp(&key, &op.inner.field)
            .with_labeled_metrics(const { &metric_labels("hget_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HSetExp>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HSetExp>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hsetexp(&key, &op.inner.field, op.inner.expiration, op.inner.version)
            .with_labeled_metrics(const { &metric_labels("hset_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<StoreHinted<Positioned<HSetExp>>> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, s: StoreHinted<Positioned<HSetExp>>) -> Result<Self::Ok, Self::Error> {
        let op = s.operation.inner;
        let op = MapHintedOp::SetExp {
            key: GenericKey::new(s.operation.position, op.key),
            field: op.field,
            expiration: op.expiration,
            version: op.version,
        };

        self.map
            .add_hinted_op(HintedOp::Map(op), s.operation.position)
            .with_labeled_metrics(const { &metric_labels("store_hinted_hset_exp") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HFields>> for Storage {
    type Ok = Vec<Field>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HFields>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hfields(&key)
            .with_labeled_metrics(const { &metric_labels("hfields") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HVals>> for Storage {
    type Ok = Vec<Value>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HVals>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        self.map
            .hvals(&key)
            .with_labeled_metrics(const { &metric_labels("hvals") })
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Positioned<HScan>> for Storage {
    type Ok = Page<(Cursor, Value)>;
    type Error = StorageError;

    async fn exec(&self, op: Positioned<HScan>) -> Result<Self::Ok, Self::Error> {
        let key = GenericKey::new(op.position, op.inner.key);
        let opts = ScanOptions::new(op.inner.count as usize).with_cursor(op.inner.cursor);
        self.map
            .hscan(&key, opts)
            .with_labeled_metrics(const { &metric_labels("hscan") })
            .await
            .map(|res| Page {
                items: res.items,
                has_more: res.has_more,
            })
            .map_err(map_err)
    }
}

#[async_trait]
impl<Data, E> irn::Storage<Import<Data>> for Storage
where
    Data: Stream<Item = Result<ExportItem, E>> + Send + 'static,
    E: fmt::Debug,
{
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, op: Import<Data>) -> Result<Self::Ok, Self::Error> {
        self.db.import_all(op.data).await.map_err(map_err)
    }
}

#[async_trait]
impl irn::Storage<Export> for Storage {
    type Ok = BoxStream<'static, ExportItem>;
    type Error = StorageError;

    async fn exec(&self, op: migration::Export) -> Result<Self::Ok, Self::Error> {
        let ranges = op.key_range.into_std_ranges();

        let stream = self
            .db
            .export((self.string.clone(), self.map.clone()), ranges.into_iter());

        Ok(stream.boxed())
    }
}

#[async_trait]
impl irn::Storage<CommitHintedOperations> for Storage {
    type Ok = ();
    type Error = StorageError;

    async fn exec(&self, ops: CommitHintedOperations) -> Result<Self::Ok, Self::Error> {
        stream::iter(ops.key_range.into_std_ranges())
            .map(Ok)
            .try_for_each_concurrent(2, |r| {
                let db = self.db.clone();

                async move {
                    tokio::task::spawn_blocking(move || {
                        db.commit_hinted_ops(r.start, r.end).map_err(map_err)
                    })
                    .with_labeled_metrics(const { &metric_labels("commit_hinted_ops") })
                    .await
                    .map_err(|e| StorageError::Other(format!("Join spawn_blocking: {e:?}")))?
                }
            })
            .await
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
