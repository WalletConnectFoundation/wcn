use {
    super::{
        cf::DbColumn,
        context::MergeOp,
        schema,
        types::common::iterators::DbIterator,
        DataContext,
    },
    crate::{
        db::{batch, cf::Column, schema::ColumnFamilyName, types::common::CommonStorage},
        util::serde::{deserialize, serialize},
        Error,
        RocksBackend,
    },
    futures::{Stream, StreamExt},
    serde::{Deserialize, Serialize},
    std::{
        fmt::{self, Debug},
        ops::RangeInclusive,
        pin::pin,
    },
};

/// Maximum number of key-value pairs to be imported in one batch.
///
/// When importing, data is coming one key-value pair at a time, it is
/// recommended to accumulate them in batches, to reduce the number of disk
/// operations.
const IMPORTER_BATCH_SIZE: u64 = 1024;

/// Container/frame used to box key-value data.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportFrame {
    pub cf: ColumnFamilyName,
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExportItem {
    Frame(Result<ExportFrame, Error>),
    Done(u64),
}

impl RocksBackend {
    /// Imports key-value pairs from incoming stream of key range values.
    ///
    /// Records are imported as they come, data stream is processed
    /// incrementally, without waiting for the whole data to come.
    ///
    /// Writes are batched to improve performance.
    pub async fn import_all<E: fmt::Debug>(
        &self,
        data: impl Stream<Item = Result<ExportItem, E>>,
    ) -> Result<(), Error> {
        let backend = self.clone();
        let mut total_items_processed = 0;
        let mut items_processed = 0;
        let mut batch = batch::WriteBatch::new(backend.clone());

        let mut data = pin!(data);
        while let Some(res) = data.next().await {
            let item = res.map_err(|e| Error::Other(format!("Data stream errored: {e:?}")))?;

            let ExportFrame { cf, key, value } = match item {
                ExportItem::Frame(frame) => {
                    frame.map_err(|e| Error::Other(format!("Exporter errored: {e:?}")))?
                }
                ExportItem::Done(n) if n != total_items_processed => {
                    return Err(Error::Other(format!(
                        "`Done({n})` frame indicates more items than were actually processed \
                         ({total_items_processed})"
                    )));
                }
                ExportItem::Done(_) => {
                    // Write the last batch to database.
                    return backend.write_batch(batch);
                }
            };

            let ctx = deserialize::<DataContext<Vec<u8>>>(&value)?;
            let value = serialize(&MergeOp::from(ctx))?;

            batch.merge(cf, &key, &value);

            // Write batch to database if it is full. Start a new batch.
            total_items_processed += 1;
            items_processed += 1;
            if items_processed >= IMPORTER_BATCH_SIZE {
                backend.write_batch(batch)?;
                batch = batch::WriteBatch::new(backend.clone());
                items_processed = 0;
            }
        }

        Err(Error::Other(
            "Corrupted data stream: `Done` frame missing".to_string(),
        ))
    }

    /// Exports key-value pairs as a [`Stream`].
    pub fn export(
        &self,
        (string, map): (DbColumn<schema::StringColumn>, DbColumn<schema::MapColumn>),
        range: RangeInclusive<u64>,
    ) -> impl Stream<Item = ExportItem> {
        // Because rocks iterators capture DB lifetime we would also need to include it
        // in "Exporter" stream, and then it's going to leak through the whole
        // codebase. The way to fix it is to own `DBColumn`s, but that requires
        // having a self-referential struct. And the only way to implement it
        // without using `unsafe` is via an anonymous stream (`async_stream`'s
        // using local thread channel under the hood).
        async_stream::try_stream! {
            let mut items_processed = 0;

            // rocks iterators are exclusive -- [start, end)
            let start = Some(*range.start());
            let end = (*range.end()).checked_add(1);

            let string_iter = DbIterator::new(
                schema::StringColumn::NAME,
                string.scan_by_position(start, end)?,
            );

            let map_iter = DbIterator::new(
                schema::MapColumn::NAME,
                map.scan_by_position(start, end)?,
            );

            for item in string_iter {
                yield ExportItem::Frame(item);
                items_processed += 1;
            }

            for item in map_iter {
                yield ExportItem::Frame(item);
                items_processed += 1;
            }

            yield ExportItem::Done(items_processed);
        }
        .map(|res| match res {
            Ok(item) => item,
            Err(e) => ExportItem::Frame(Err(e)),
        })
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::{
            db::{
                schema::{
                    test_types::{TestKey, TestValue},
                    MapColumn,
                    StringColumn,
                },
                types::{MapStorage, Pair, Record, StringStorage},
            },
            util::{db_path::DBPath, timestamp_micros, timestamp_secs},
            RocksDatabaseBuilder,
        },
        futures::StreamExt,
        std::convert::Infallible,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn export_import() {
        const NUM_ENTRIES: usize = 100_000;

        let path = DBPath::new("export_import_src");
        let src_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .with_column_family(MapColumn)
            .build()
            .unwrap();

        let path = DBPath::new("export_import_dest");
        let dest_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .with_column_family(MapColumn)
            .build()
            .unwrap();

        let mut string_entries = Vec::with_capacity(NUM_ENTRIES);
        let mut map_entries = Vec::with_capacity(NUM_ENTRIES);

        let expiration = timestamp_secs() + 600;
        let timestamp = timestamp_micros();

        let string = src_db.column::<StringColumn>().unwrap();
        let map = src_db.column::<MapColumn>().unwrap();
        for _ in 0..NUM_ENTRIES {
            let key = TestKey::new(rand::random::<u64>()).into();
            let subkey = TestKey::new(rand::random::<u64>()).into();
            let val = TestValue::new(rand::random::<u64>().to_string()).into();

            string.set(&key, &val, expiration, timestamp).await.unwrap();
            string_entries.push((key.clone(), val.clone()));

            let pair = Pair::new(subkey, val);
            map.hset(&key, &pair, expiration, timestamp).await.unwrap();
            map_entries.push((key, pair));
        }

        let mut n_exported = 0;

        const KEYRANGE_SIZE: u64 = ((u64::MAX as u128 + 1) / (u8::MAX as u128 + 1)) as u64;
        dbg!(KEYRANGE_SIZE);

        let mut idx = 0u64;

        loop {
            let start = idx.checked_mul(KEYRANGE_SIZE).unwrap();
            let range = RangeInclusive::new(start, start.checked_add(KEYRANGE_SIZE - 1).unwrap());

            let data = src_db.export((string.clone(), map.clone()), range.clone());

            dest_db
                .import_all(
                    data.inspect(|item| {
                        if let ExportItem::Frame(Ok(_)) = item {
                            n_exported += 1
                        }
                    })
                    .map(Ok::<_, Infallible>),
                )
                .await
                .unwrap();

            if *range.end() == u64::MAX {
                break;
            }

            idx += 1;
        }

        assert_eq!(n_exported, NUM_ENTRIES * 2);

        let string = dest_db.column::<StringColumn>().unwrap();
        for (key, val) in string_entries {
            let got = string.get(&key).await.unwrap();
            assert_eq!(
                got,
                Some(Record {
                    value: val,
                    expiration,
                    version: timestamp,
                })
            );
        }

        let map = dest_db.column::<MapColumn>().unwrap();
        for (key, pair) in map_entries {
            let got = map.hget(&key, &pair.field).await.unwrap();
            assert_eq!(
                got,
                Some(Record {
                    value: pair.value,
                    expiration,
                    version: timestamp,
                })
            );
        }
    }
}
