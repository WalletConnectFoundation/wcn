use {
    crate::storage::{self, Storage},
    std::sync::Arc,
    storage_api::{
        operation::{self, Output},
        MapEntry,
        MapPage,
        Operation,
        Record,
        RecordExpiration,
        RecordVersion,
        StorageApi,
    },
    tap::Pipe as _,
    wc::metrics::{future_metrics, FutureExt as _},
    wcn_rocks::db::{
        cf::DbColumn,
        schema,
        types::{common::iterators::ScanOptions, MapStorage as _, Pair, StringStorage as _},
    },
};

struct Inner {
    storage: Storage,
}

#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

impl Server {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }

    pub fn string_storage(&self) -> &DbColumn<schema::StringColumn> {
        &self.inner.storage.string
    }

    pub fn map_storage(&self) -> &DbColumn<schema::MapColumn> {
        &self.inner.storage.map
    }
}

impl StorageApi for Server {
    async fn execute(&self, op: Operation<'_>) -> storage_api::Result<Output> {
        use storage_api::{Error, ErrorKind};

        let res = match op.into_owned() {
            operation::Owned::Get(op) => self
                .string_storage()
                .get(&storage::key(&op.namespace, &op.key))
                .with_metrics(future_metrics!("storage_operation", "op_name" => "get"))
                .await
                .map(|res| Output::Record(res.map(map_record))),

            operation::Owned::Set(op) => self
                .string_storage()
                .set(
                    &storage::key(&op.namespace, &op.key),
                    &op.record.value,
                    op.record.expiration.to_unix_timestamp_secs(),
                    op.record.version.to_unix_timestamp_micros(),
                )
                .with_metrics(future_metrics!("storage_operation", "op_name" => "set"))
                .await
                .map(|_| Output::None(())),

            operation::Owned::Del(op) => self
                .string_storage()
                .del(
                    &storage::key(&op.namespace, &op.key),
                    op.version.to_unix_timestamp_micros(),
                )
                .with_metrics(future_metrics!("storage_operation", "op_name" => "del"))
                .await
                .map(|_| Output::None(())),

            operation::Owned::GetExp(op) => self
                .string_storage()
                .get_exp(&storage::key(&op.namespace, &op.key))
                .with_metrics(future_metrics!("storage_operation", "op_name" => "get_exp"))
                .await
                .pipe(map_expiration)
                .map(Output::Expiration),

            operation::Owned::SetExp(op) => self
                .string_storage()
                .set_exp(
                    &storage::key(&op.namespace, &op.key),
                    op.expiration.to_unix_timestamp_secs(),
                    op.version.to_unix_timestamp_micros(),
                )
                .with_metrics(future_metrics!("storage_operation", "op_name" => "set_exp"))
                .await
                .map(|_| Output::None(())),

            operation::Owned::HGet(op) => self
                .map_storage()
                .hget(&storage::key(&op.namespace, &op.key), &op.field)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hget"))
                .await
                .map(|res| Output::Record(res.map(map_record))),

            operation::Owned::HSet(op) => {
                let entry = op.entry;
                let record = entry.record;
                let pair = Pair::new(entry.field, record.value);
                let expiration = record.expiration.to_unix_timestamp_secs();
                let version = record.version.to_unix_timestamp_micros();

                self.map_storage()
                    .hset(
                        &storage::key(&op.namespace, &op.key),
                        &pair,
                        expiration,
                        version,
                    )
                    .with_metrics(future_metrics!("storage_operation", "op_name" => "hset"))
                    .await
                    .map(|_| Output::None(()))
            }

            operation::Owned::HDel(op) => self
                .map_storage()
                .hdel(
                    &storage::key(&op.namespace, &op.key),
                    &op.field,
                    op.version.to_unix_timestamp_micros(),
                )
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hdel"))
                .await
                .map(|_| Output::None(())),

            operation::Owned::HGetExp(op) => self
                .map_storage()
                .hget_exp(&storage::key(&op.namespace, &op.key), &op.field)
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hget_exp"))
                .await
                .pipe(map_expiration)
                .map(Output::Expiration),

            operation::Owned::HSetExp(op) => self
                .map_storage()
                .hset_exp(
                    &storage::key(&op.namespace, &op.key),
                    &op.field,
                    op.expiration.to_unix_timestamp_secs(),
                    op.version.to_unix_timestamp_micros(),
                )
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hset_exp"))
                .await
                .map(|_| Output::None(())),

            operation::Owned::HCard(op) => self
                .map_storage()
                .hcard(&storage::key(&op.namespace, &op.key))
                .with_metrics(future_metrics!("storage_operation", "op_name" => "hcard"))
                .await
                .map(|card| Output::Cardinality(card as u64)),

            operation::Owned::HScan(op) => {
                let opts = ScanOptions::new(op.count as usize).with_cursor(op.cursor);

                self.map_storage()
                    .hscan(&storage::key(&op.namespace, &op.key), opts)
                    .with_metrics(future_metrics!("storage_operation", "op_name" => "hscan"))
                    .await
                    .map(|res| {
                        let entries = res
                            .items
                            .into_iter()
                            .map(|rec| MapEntry {
                                field: rec.field,
                                record: Record {
                                    value: rec.value,
                                    expiration: RecordExpiration::from_unix_timestamp_secs(
                                        rec.expiration,
                                    ),
                                    version: RecordVersion::from_unix_timestamp_micros(rec.version),
                                },
                            })
                            .collect();

                        Output::MapPage(MapPage {
                            entries,
                            has_next: res.has_next,
                        })
                    })
            }
        };

        res.map_err(|err| Error::new(ErrorKind::Internal).with_message(err))
    }
}

#[inline]
fn map_record(rec: wcn_rocks::Record) -> storage_api::Record {
    storage_api::Record {
        value: rec.value,
        expiration: RecordExpiration::from_unix_timestamp_secs(rec.expiration),
        version: RecordVersion::from_unix_timestamp_micros(rec.version),
    }
}

#[inline]
fn map_expiration(
    res: Result<wcn_rocks::UnixTimestampSecs, wcn_rocks::Error>,
) -> Result<Option<RecordExpiration>, wcn_rocks::Error> {
    match res {
        Ok(timestamp) => Ok(Some(RecordExpiration::from_unix_timestamp_secs(timestamp))),
        Err(wcn_rocks::Error::EntryNotFound) => Ok(None),
        Err(err) => Err(err),
    }
}
