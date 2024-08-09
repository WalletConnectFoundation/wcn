use {
    crate::{
        db::{
            batch::WriteBatch,
            cf,
            migration::hinted_ops::HintedOp,
            schema::InternalHintedOpsColumn,
        },
        util::serde::serialize,
        Error,
        UnixTimestampSecs,
    },
    async_trait::async_trait,
    byteorder::{BigEndian, WriteBytesExt},
    std::{fmt::Debug, future::Future},
};

pub mod iterators;

/// Defines key position within a keyspace.
pub type KeyPosition = u64;

/// Trait exposing common storage methods.
#[async_trait]
pub trait CommonStorage<C: cf::Column>: 'static + Debug + Send + Sync {
    /// Creates a new write batch.
    ///
    /// Whenever multiple writes are to be performed, it is recommended to batch
    /// them together either for performance or atomicity reasons.
    fn create_batch(&self) -> WriteBatch;

    /// Processes a batch of writes.
    fn write_batch(&self, batch: WriteBatch) -> Result<(), Error>;

    /// Returns `RocksDB` iterator for a provided hash ring position range.
    fn scan_by_position(
        &self,
        left: Option<KeyPosition>,
        right: Option<KeyPosition>,
    ) -> Result<rocksdb::DBIterator, Error>;

    /// Returns the expiration of the key in unix timestamp in seconds format.
    fn expiration(
        &self,
        key: &C::KeyType,
        subkey: Option<&C::SubKeyType>,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Error>> + Send + Sync;

    /// Saves hinted operation for later application.
    ///
    /// Hinted operation is tagged with a number, which is useful for selecting
    /// a range of operations to commit. For instance, this allows to commit
    /// only those hinted ops that belong to a certain key range.
    ///
    /// All hinted operations keys are suffixed with an auto-incrementing
    /// sequence number to ensure the order of operations.
    fn add_hinted_op(
        &self,
        op: HintedOp,
        tag: u64,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Commit hinted operations with tags within a certain range.
    ///
    /// Operations are applied in batches of `HINTED_OPS_BATCH_SIZE` size.
    fn commit_hinted_ops(&self, left: Option<u64>, right: Option<u64>) -> Result<(), Error>;
}

#[async_trait]
impl<C: cf::Column> CommonStorage<C> for cf::DbColumn<C> {
    fn create_batch(&self) -> WriteBatch {
        WriteBatch::new(self.backend.clone())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<(), Error> {
        self.backend.write_batch(batch)
    }

    fn scan_by_position(
        &self,
        left: Option<KeyPosition>,
        right: Option<KeyPosition>,
    ) -> Result<rocksdb::DBIterator, Error> {
        let left = left.map(|pos| pos.to_be_bytes());
        let right = right.map(|pos| pos.to_be_bytes());

        let iter = self.backend.range_iterator::<C, _>(left, right);
        Ok(iter)
    }

    fn expiration(
        &self,
        key: &C::KeyType,
        subkey: Option<&C::SubKeyType>,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Error>> + Send + Sync {
        async move {
            let key = if let Some(subkey) = subkey {
                C::ext_key(key, subkey)?
            } else {
                C::storage_key(key)?
            };

            let data = self
                .backend
                .get::<C::ValueType>(C::NAME, key)
                .await?
                .ok_or(Error::EntryNotFound)?;

            // Treat expired entries as non-existing.
            if data.expired() || data.payload().is_none() {
                return Err(Error::EntryNotFound);
            }

            Ok(data.expiration_timestamp())
        }
    }

    fn add_hinted_op(
        &self,
        op: HintedOp,
        tag: u64,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let mut key = Vec::with_capacity(8 + 8);
            key.write_u64::<BigEndian>(tag)
                .map_err(|_| Error::Serialize)?;
            key.write_u64::<BigEndian>(self.backend.seq_num_gen.next())
                .map_err(|_| Error::Serialize)?;
            let value = serialize(&op)?;
            self.backend
                .put(<InternalHintedOpsColumn as cf::Column>::NAME, key, value)
                .await
        }
    }

    fn commit_hinted_ops(&self, left: Option<u64>, right: Option<u64>) -> Result<(), Error> {
        self.backend.commit_hinted_ops(left, right)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            db::{
                migration::hinted_ops::{MapHintedOp, StringHintedOp, HINTED_OPS_BATCH_SIZE},
                schema::{
                    test_types::{TestKey, TestValue},
                    MapColumn,
                    StringColumn,
                },
                types::{MapStorage, StringStorage},
            },
            util::db_path::DBPath,
            RocksDatabaseBuilder,
        },
    };

    fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
        crate::util::timestamp_secs() + added
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn hinted_ops() {
        let path = DBPath::new("hinted_ops");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .with_column_family(MapColumn)
            .with_column_family(InternalHintedOpsColumn)
            .build()
            .unwrap();

        let db = rocks_db.column::<InternalHintedOpsColumn>().unwrap();

        // String operations.
        {
            let col = &rocks_db.column::<StringColumn>().unwrap();
            let test_keys: Vec<_> = vec![TestKey::new(42), TestKey::new(43), TestKey::new(44)];
            let test_vals = [
                TestValue::new("value1"),
                TestValue::new("value2"),
                TestValue::new("value3"),
            ];

            let expiration = timestamp(10);
            let hinted_ops = vec![
                // set(key=42, value=value1)
                HintedOp::String(StringHintedOp::Set {
                    key: test_keys[0].clone().into(),
                    value: test_vals[0].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=43, value=value2)
                HintedOp::String(StringHintedOp::Set {
                    key: test_keys[1].clone().into(),
                    value: test_vals[1].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=44, value=value3)
                HintedOp::String(StringHintedOp::Set {
                    key: test_keys[2].clone().into(),
                    value: test_vals[2].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // setexp(key=43, expiration=5)
                HintedOp::String(StringHintedOp::SetExp {
                    key: test_keys[1].clone().into(),
                    expiration: Some(expiration),
                    version: 2,
                }),
                // del(key=42)
                HintedOp::String(StringHintedOp::Del {
                    key: test_keys[0].clone().into(),
                    version: 2,
                }),
            ];

            for op in &hinted_ops {
                col.add_hinted_op(op.clone(), 0).await.unwrap();
            }

            // Make sure no values are visible before commit.
            for key in &test_keys {
                assert_eq!(col.get(&key.clone().into()).await.unwrap(), None);
            }

            db.backend.commit_hinted_ops(None, None).unwrap();

            // Make sure all values are visible after commit.
            for (key, value) in test_keys.iter().zip(test_vals.iter()) {
                // Deleted key, must not be present.
                if key == &test_keys[0] {
                    assert_eq!(col.get(&key.clone().into()).await.unwrap(), None);
                    continue;
                }
                assert_eq!(
                    col.get(&key.clone().into()).await.unwrap(),
                    Some(value.clone().into())
                );
            }

            assert_eq!(
                col.exp(&test_keys[1].clone().into()).await.unwrap(),
                Some(expiration)
            );
        }

        // Ensure that hint list is empty.
        let iter = db
            .backend
            .range_iterator::<InternalHintedOpsColumn, Vec<u8>>(None, None)
            .collect::<Vec<_>>();
        assert!(iter.is_empty());

        // Map operations.
        {
            let col = &rocks_db.column::<MapColumn>().unwrap();

            let keys = vec![TestKey::new(42), TestKey::new(43), TestKey::new(44)];
            let fields = vec![TestKey::new(142), TestKey::new(143), TestKey::new(144)];
            let vals = vec![
                TestValue::new("value1"),
                TestValue::new("value2"),
                TestValue::new("value3"),
            ];
            let expiration = timestamp(10);
            let ops = vec![
                // set(key=42, field=142, value=value1)
                HintedOp::Map(MapHintedOp::Set {
                    key: keys[0].clone().into(),
                    field: fields[0].clone().into(),
                    value: vals[0].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=42, field=143, value=value2), another field to the same key
                HintedOp::Map(MapHintedOp::Set {
                    key: keys[0].clone().into(),
                    field: fields[1].clone().into(),
                    value: vals[1].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=42, field=144, value=value2), another field to the same key
                HintedOp::Map(MapHintedOp::Set {
                    key: keys[0].clone().into(),
                    field: fields[2].clone().into(),
                    value: vals[1].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=43, field=143, value=value2)
                HintedOp::Map(MapHintedOp::Set {
                    key: keys[1].clone().into(),
                    field: fields[1].clone().into(),
                    value: vals[1].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // set(key=44, field=144, value=value3)
                HintedOp::Map(MapHintedOp::Set {
                    key: keys[2].clone().into(),
                    field: fields[2].clone().into(),
                    value: vals[2].clone().into(),
                    expiration: None,
                    version: 1,
                }),
                // setexp(key=42, field=143, expiration=5)
                HintedOp::Map(MapHintedOp::SetExp {
                    key: keys[0].clone().into(),
                    field: fields[1].clone().into(),
                    expiration: Some(expiration),
                    version: 2,
                }),
                // del(key=42, field=142)
                HintedOp::Map(MapHintedOp::Del {
                    key: keys[0].clone().into(),
                    field: fields[0].clone().into(),
                    version: 2,
                }),
            ];

            for op in &ops {
                col.add_hinted_op(op.clone(), 0).await.unwrap();
            }

            // Make sure no values are visible before commit.
            for (key, field) in keys.iter().zip(fields.iter()) {
                assert_eq!(
                    col.hget(&key.clone().into(), &field.clone().into())
                        .await
                        .unwrap(),
                    None
                );
            }

            // Make sure all values are visible after commit.
            db.backend.commit_hinted_ops(None, None).unwrap();

            for (key, field, value) in itertools::multizip((&keys, &fields, &vals)) {
                // Deleted pair, must not be present.
                if key == &keys[0] && field == &fields[0] {
                    assert_eq!(
                        col.hget(&key.clone().into(), &field.clone().into())
                            .await
                            .unwrap(),
                        None
                    );
                    continue;
                }
                assert_eq!(
                    col.hget(&key.clone().into(), &field.clone().into())
                        .await
                        .unwrap(),
                    Some(value.clone().into())
                );
            }

            // Assert expiration set on field=143, but not on field=144.
            assert_eq!(
                col.hexp(&keys[0].clone().into(), &fields[1].clone().into())
                    .await
                    .unwrap(),
                Some(expiration)
            );
            assert_eq!(
                col.hexp(&keys[0].clone().into(), &fields[2].clone().into())
                    .await
                    .unwrap(),
                None
            );
        }

        // Ensure that hint list is empty.
        let iter = db
            .backend
            .range_iterator::<InternalHintedOpsColumn, Vec<u8>>(None, None)
            .collect::<Vec<_>>();
        assert!(iter.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn hinted_ops_batching() {
        let path = DBPath::new("hinted_ops_batching");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .with_column_family(InternalHintedOpsColumn)
            .build()
            .unwrap();

        let db = rocks_db.column::<InternalHintedOpsColumn>().unwrap();

        // Set enough map elements, so that items are processed in several batches.
        for i in 0..(HINTED_OPS_BATCH_SIZE * 2 + 1) {
            let op = HintedOp::Map(MapHintedOp::Set {
                key: TestKey::new(42).into(),
                field: TestKey::new(i as u64).into(),
                value: TestValue::new("value").into(),
                expiration: None,
                version: 1,
            });
            db.add_hinted_op(op, i as u64).await.unwrap();
        }

        // Ensure that no data is visible before commit.
        let col = &rocks_db.column::<MapColumn>().unwrap();
        assert_eq!(col.hcard(&TestKey::new(42).into()).await.unwrap(), 0);

        // Commit hinted ops.
        db.backend.commit_hinted_ops(None, None).unwrap();

        // Ensure that all data is visible after commit.
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap(),
            HINTED_OPS_BATCH_SIZE * 2 + 1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn hinted_ops_key_ranges() {
        let path = DBPath::new("hinted_ops_key_ranges");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .with_column_family(InternalHintedOpsColumn)
            .build()
            .unwrap();

        let db = rocks_db.column::<InternalHintedOpsColumn>().unwrap();

        // When setting map elements, tag each hinted op with id.
        const OPS_SIZE: u64 = 1024;
        for i in 0..OPS_SIZE {
            let op = HintedOp::Map(MapHintedOp::Set {
                key: TestKey::new(42).into(),
                field: TestKey::new(i).into(),
                value: TestValue::new("value").into(),
                expiration: None,
                version: 1,
            });
            db.add_hinted_op(op, i).await.unwrap();
        }

        // Ensure that no data is visible before commit.
        let col = &rocks_db.column::<MapColumn>().unwrap();
        assert_eq!(col.hcard(&TestKey::new(42).into()).await.unwrap(), 0);

        // Commit the portion of the hinted ops.
        db.backend
            .commit_hinted_ops(Some(0), Some(OPS_SIZE / 2))
            .unwrap();

        // Ensure that part of the data is visible after commit.
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap() as u64,
            OPS_SIZE / 2
        );

        // Processing the same range shouldn't result in any new operations committed.
        db.backend
            .commit_hinted_ops(Some(0), Some(OPS_SIZE / 2))
            .unwrap();
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap() as u64,
            OPS_SIZE / 2
        );

        // Commit the portion of the hinted ops (having the right bound only).
        db.backend
            .commit_hinted_ops(None, Some(OPS_SIZE / 4 * 3))
            .unwrap();
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap() as u64,
            OPS_SIZE / 4 * 3
        );

        // Commit the portion of the hinted ops (having the left bound only).
        db.backend
            .commit_hinted_ops(Some(OPS_SIZE - 25), None)
            .unwrap();
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap() as u64,
            OPS_SIZE / 4 * 3 + 25
        );

        // Commit the portion of the hinted ops (in the middle of data set).
        db.backend
            .commit_hinted_ops(Some(OPS_SIZE / 4 * 3 + 10), Some(OPS_SIZE - 50))
            .unwrap();
        assert_eq!(
            col.hcard(&TestKey::new(42).into()).await.unwrap() as u64,
            OPS_SIZE - 35
        );
    }
}
