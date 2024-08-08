//! Map storage for `RocksDB`.

use {
    crate::{
        db::{
            batch,
            cf::{Column, DbColumn},
            context::{self, UnixTimestampMicros},
            schema::MapColumn,
            types::common::{iterators, CommonStorage},
        },
        util::serde::serialize,
        Error,
        UnixTimestampSecs,
    },
    serde::{Deserialize, Serialize},
    std::{fmt::Debug, future::Future, hash::Hash},
};

/// Defines `(field, value)` pair.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Default)]
pub struct Pair<F, V> {
    pub field: F,
    pub value: V,
}

impl<F, V> Pair<F, V> {
    pub fn new(field: F, value: V) -> Self {
        Self { field, value }
    }
}

/// Main interface for map data type.
pub trait MapStorage<C: Column>: CommonStorage<C> {
    /// Sets the specified `(field, value)` pair for the hash stored at `key`.
    ///
    /// Time complexity: `O(1)`.
    fn hset(
        &self,
        key: &C::KeyType,
        pair: &Pair<C::SubKeyType, C::ValueType>,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Sets the specified list of `(field, value)` pairs for the hash stored at
    /// `key`.
    ///
    /// If you need to set a single `(field, value)` pair, use [`hset`].
    /// Additionally, [`hset`] allows to set a TTL on per member basis.
    ///
    /// Time complexity: `O(n)`.
    fn hmset(
        &self,
        key: &C::KeyType,
        pairs: &[&Pair<C::SubKeyType, C::ValueType>],
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Returns the value associated with `field` in the hash stored at `key`.
    ///
    /// Time complexity: `O(1)`.
    fn hget(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<Option<C::ValueType>, Error>> + Send + Sync;

    /// Returns if `field` is an existing field in the hash stored at `key`.
    ///
    /// Time complexity: `O(1)`.
    fn hexists(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<bool, Error>> + Send + Sync;

    /// Removes the specified `field` from the hash stored at `key`.
    ///
    /// Time complexity: `O(1)`.
    fn hdel(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Returns all values in the hash stored at `key`.
    ///
    /// Time complexity: `O(n)`, where `n` is the size of hash.
    fn hvals(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Vec<C::ValueType>, Error>> + Send + Sync;

    /// Returns all field names in the hash stored at `key`.
    ///
    /// Time complexity: `O(n)`, where `n` is the size of hash.
    fn hfields(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Vec<C::SubKeyType>, Error>> + Send + Sync;

    /// Returns the set cardinality (number of elements) of the hash stored at
    /// key.
    ///
    /// Time complexity: `O(n)`.
    fn hcard(&self, key: &C::KeyType) -> impl Future<Output = Result<usize, Error>> + Send + Sync;

    /// Iterates over the items in the hash stored at `key`, and returns a batch
    /// of items of specified size.
    ///
    /// Also returns a cursor that can be used to retrieve the next batch of
    /// items.
    fn hscan(
        &self,
        key: &C::KeyType,
        opts: iterators::ScanOptions<iterators::GenericCursor>,
    ) -> impl Future<Output = Result<iterators::ScanResult<C::ValueType>, Error>> + Send + Sync;

    /// Returns the remaining time to live of a map value stored at the given
    /// key.
    fn hexp(
        &self,
        key: &C::KeyType,
        subkey: &C::SubKeyType,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Error>> + Send + Sync;

    /// Set a time to live for a map value stored at the given key. After the
    /// timeout has expired, the value will automatically be deleted. Passing
    /// `None` will remove the current timeout and persist the key.
    fn hsetexp(
        &self,
        key: &C::KeyType,
        subkey: &C::SubKeyType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;
}

impl<C: Column> MapStorage<C> for DbColumn<C> {
    fn hset(
        &self,
        key: &C::KeyType,
        pair: &Pair<C::SubKeyType, C::ValueType>,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let key = C::ext_key(key, &pair.field)?;
            let value = serialize(
                &context::MergeOp::<&C::ValueType>::new(update_timestamp)
                    .with_payload(&pair.value, expiration),
            )?;

            self.backend.merge(C::NAME, key, value).await
        }
    }

    fn hmset(
        &self,
        key: &C::KeyType,
        pairs: &[&Pair<C::SubKeyType, C::ValueType>],
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            // Batch updates as an atomic write operation.
            let mut batch = batch::WriteBatch::new(self.backend.clone());

            for &pair in pairs {
                let key = C::ext_key(key, &pair.field)?;
                let value = serialize(
                    &context::MergeOp::<&C::ValueType>::new(update_timestamp)
                        .with_payload(&pair.value, expiration),
                )?;

                batch.merge(C::NAME, key, value);
            }

            self.backend.write_batch(batch)
        }
    }

    fn hget(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<Option<C::ValueType>, Error>> + Send + Sync {
        async move {
            let ext_key = C::ext_key(key, field)?;
            let entry = self.backend.get::<C::ValueType>(C::NAME, ext_key).await?;
            entry.map_or_else(|| Ok(None), |data| Ok(data.into_payload()))
        }
    }

    fn hexists(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<bool, Error>> + Send + Sync {
        async move { self.hget(key, field).await.map(|v| v.is_some()) }
    }

    fn hdel(
        &self,
        key: &C::KeyType,
        field: &C::SubKeyType,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let ext_key = C::ext_key(key, field)?;
            self.backend.delete(C::NAME, ext_key).await
        }
    }

    fn hvals(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Vec<C::ValueType>, Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;

            self.backend
                .exec_blocking(|b| {
                    let iter = b.prefix_iterator::<C, _>(key);
                    iterators::KeyValueIterator::<C>::new(iter)
                        .values()
                        .collect()
                })
                .await
        }
    }

    fn hfields(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Vec<C::SubKeyType>, Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;

            self.backend
                .exec_blocking(|b| {
                    let iter = b.prefix_iterator::<C, _>(key);
                    iterators::KeyValueIterator::<C>::new(iter)
                        .subkeys()
                        .collect()
                })
                .await
        }
    }

    fn hcard(&self, key: &C::KeyType) -> impl Future<Output = Result<usize, Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;

            self.backend
                .exec_blocking(|b| Ok(b.prefix_iterator::<C, _>(key).count()))
                .await
        }
    }

    fn hscan(
        &self,
        key: &C::KeyType,
        opts: iterators::ScanOptions<iterators::GenericCursor>,
    ) -> impl Future<Output = Result<iterators::ScanResult<C::ValueType>, Error>> + Send + Sync
    {
        async move {
            let key = key.clone();

            self.backend
                .exec_blocking(move |b| {
                    iterators::scan::<C, iterators::CursorAndValue, _>(&b, &key, opts)
                })
                .await
        }
    }

    fn hexp(
        &self,
        key: &C::KeyType,
        subkey: &C::SubKeyType,
    ) -> impl Future<Output = Result<Option<UnixTimestampSecs>, Error>> + Send + Sync {
        self.expiration(key, Some(subkey))
    }

    fn hsetexp(
        &self,
        key: &C::KeyType,
        subkey: &C::SubKeyType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let expiration = expiration
                .map(context::TimestampUpdate::Set)
                .unwrap_or(context::TimestampUpdate::Unset);

            let key = C::ext_key(key, subkey)?;
            let value = serialize(
                &context::MergeOp::<C::ValueType>::new(update_timestamp)
                    .with_expiration(expiration),
            )?;

            self.backend.merge(C::NAME, key, value).await
        }
    }
}

impl DbColumn<MapColumn> {
    pub(crate) fn hset_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<MapColumn as Column>::KeyType,
        pair: &Pair<<MapColumn as Column>::SubKeyType, <MapColumn as Column>::ValueType>,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let key = MapColumn::ext_key(key, &pair.field)?;
        let value = serialize(
            &context::MergeOp::<&<MapColumn as Column>::ValueType>::new(update_timestamp)
                .with_payload(&pair.value, expiration),
        )?;

        batch.merge(MapColumn::NAME, key, value);

        Ok(())
    }

    pub(crate) fn hdel_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<MapColumn as Column>::KeyType,
        field: &<MapColumn as Column>::SubKeyType,
    ) -> Result<(), Error> {
        let key = MapColumn::ext_key(key, field)?;
        batch.delete(<MapColumn as Column>::NAME, key);
        Ok(())
    }

    pub(crate) fn hsetexp_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<MapColumn as Column>::KeyType,
        subkey: &<MapColumn as Column>::SubKeyType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let expiration = expiration
            .map(context::TimestampUpdate::Set)
            .unwrap_or(context::TimestampUpdate::Unset);

        let key = MapColumn::ext_key(key, subkey)?;
        let value = serialize(
            &context::MergeOp::<<MapColumn as Column>::ValueType>::new(update_timestamp)
                .with_expiration(expiration),
        )?;

        batch.merge(<MapColumn as Column>::NAME, key, value);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            db::{
                schema::test_types::{TestKey, TestMapValue, TestValue},
                types::common::iterators::ScanResult,
            },
            util::{db_path::DBPath, timestamp_micros, timestamp_secs},
            RocksDatabaseBuilder,
        },
        core::time::Duration,
        std::collections::{BTreeSet, HashSet},
    };

    fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
        crate::util::timestamp_secs() + added
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_ops() {
        let path = DBPath::new("map_basic_ops");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db = &rocks_db.column::<MapColumn>().unwrap();

        let key = TestKey::new(42).into();
        let val1 = TestMapValue::generate();
        let val2 = TestMapValue::generate();
        let val3 = TestMapValue::generate();

        // Make sure that messages don't exist.
        assert_eq!(db.hget(&key, &val1.field().into()).await.unwrap(), None);
        assert_eq!(db.hget(&key, &val2.field().into()).await.unwrap(), None);
        assert!(db.hvals(&key).await.unwrap().is_empty());

        // Add messages to the store.
        db.hset(&key, &val1.clone().into(), None, timestamp_micros())
            .await
            .unwrap();

        // Make sure messages saved correctly.
        assert_eq!(
            db.hget(&key, &val1.field().into()).await.unwrap(),
            Some(val1.value().into())
        );
        assert_eq!(db.hget(&key, &val2.field().into()).await.unwrap(), None);

        // Only now second message is stored.
        db.hset(&key, &val2.clone().into(), None, timestamp_micros())
            .await
            .unwrap();
        assert_eq!(
            db.hget(&key, &val2.field().into()).await.unwrap(),
            Some(val2.value().into())
        );

        // Check collection.
        assert_eq!(
            BTreeSet::from_iter(db.hvals(&key).await.unwrap()),
            BTreeSet::from_iter(vec![val1.value().into_vec(), val2.value().into_vec()])
        );

        // Remove non-existent (should have no effect).
        db.hdel(&key, &val3.field().into()).await.unwrap();
        assert_eq!(
            db.hget(&key, &val1.field().into()).await.unwrap(),
            Some(val1.value().into())
        );
        assert_eq!(
            db.hget(&key, &val2.field().into()).await.unwrap(),
            Some(val2.value().into())
        );

        // Remove messages.
        db.hdel(&key, &val1.field().into()).await.unwrap();
        assert_eq!(db.hget(&key, &val1.field().into()).await.unwrap(), None);
        assert_eq!(
            db.hget(&key, &val2.field().into()).await.unwrap(),
            Some(val2.value().into())
        );
        db.hdel(&key, &val2.field().into()).await.unwrap();
        assert_eq!(db.hget(&key, &val1.field().into()).await.unwrap(), None);
        assert_eq!(db.hget(&key, &val2.field().into()).await.unwrap(), None);
        assert!(db.hvals(&key).await.unwrap().is_empty());

        // TTL support.
        let key = &TestKey::new(43).into();
        {
            // Try to get TTL on non-existent key.
            let res = db.hexp(key, &val1.field().into()).await;
            assert!(matches!(res, Err(Error::EntryNotFound)));

            // No TTL.
            db.hset(key, &val1.clone().into(), None, timestamp_micros())
                .await
                .unwrap();
            let res = db.hexp(key, &val1.field().into()).await;
            assert!(matches!(res, Ok(None)));

            // Set TTL to 5 sec.
            let expiration = timestamp(5);
            db.hsetexp(
                key,
                &val1.field().into(),
                Some(expiration),
                timestamp_micros(),
            )
            .await
            .unwrap();
            let ttl = db.hexp(key, &val1.field().into()).await.unwrap();
            assert_eq!(ttl, Some(expiration));

            // Per-member TTLs.
            let expiration5s = timestamp(5);
            db.hset(
                key,
                &val2.clone().into(),
                Some(expiration5s),
                timestamp_micros(),
            )
            .await
            .unwrap();
            let expiration10s = timestamp(10);
            db.hset(
                key,
                &val3.clone().into(),
                Some(expiration10s),
                timestamp_micros(),
            )
            .await
            .unwrap();
            let ttl = db.hexp(key, &val2.field().into()).await.unwrap();
            assert_eq!(ttl, Some(expiration5s));
            let ttl = db.hexp(key, &val3.field().into()).await.unwrap();
            assert_eq!(ttl, Some(expiration10s));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn modification_timetsamp() {
        let path = DBPath::new("string_modification_timetsamp");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db = &rocks_db.column::<MapColumn>().unwrap();

        // Payload.
        {
            let key = TestKey::new(42).into();
            let val1 = TestMapValue::generate();
            let val2 = TestMapValue::new(val1.field(), TestMapValue::generate().value());
            let timestamp1 = timestamp_micros();
            let timestamp2 = timestamp_micros() + 1;

            // Make sure that no data exists.
            assert_eq!(db.hget(&key, &val1.field().into()).await.unwrap(), None);

            // Add data.
            db.hset(&key, &val1.clone().into(), None, timestamp1)
                .await
                .unwrap();
            assert_eq!(
                db.hget(&key, &val1.field().into()).await.unwrap(),
                Some(val1.value().into())
            );

            // Update the data with higher timestamp value. It's expected to succeed.
            db.hset(&key, &val2.clone().into(), None, timestamp2)
                .await
                .unwrap();
            assert_eq!(
                db.hget(&key, &val1.field().into()).await.unwrap(),
                Some(val2.value().into())
            );

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.hset(&key, &val1.clone().into(), None, timestamp1)
                .await
                .unwrap();
            assert_eq!(
                db.hget(&key, &val1.field().into()).await.unwrap(),
                Some(val2.value().into())
            );
        }

        // Expiry.
        {
            let key = TestKey::new(42).into();
            let val1 = TestMapValue::generate();
            let expiry1 = timestamp_secs() + 30;
            let expiry2 = expiry1 + 30;
            let timestamp1 = timestamp_micros();
            let timestamp2 = timestamp_micros() + 1;

            // Make sure that no data exists.
            assert_eq!(db.hget(&key, &val1.field().into()).await.unwrap(), None);

            // Add data.
            db.hset(&key, &val1.clone().into(), Some(expiry1), timestamp1)
                .await
                .unwrap();
            assert_eq!(
                db.hget(&key, &val1.field().into()).await.unwrap(),
                Some(val1.value().into())
            );
            assert_eq!(
                db.hexp(&key, &val1.field().into()).await.unwrap(),
                Some(expiry1)
            );

            // Update the data with higher timestamp value. It's expected to succeed.
            db.hsetexp(&key, &val1.field().into(), Some(expiry2), timestamp2)
                .await
                .unwrap();
            assert_eq!(
                db.hexp(&key, &val1.field().into()).await.unwrap(),
                Some(expiry2)
            );

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.hsetexp(&key, &val1.field().into(), Some(expiry1), timestamp1)
                .await
                .unwrap();
            assert_eq!(
                db.hexp(&key, &val1.field().into()).await.unwrap(),
                Some(expiry2)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_duplicates() {
        let path = DBPath::new("map_handle_duplicates");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();

        let db = &rocks_db.column::<MapColumn>().unwrap();

        let val1 = &TestMapValue::new(TestValue::new("data1"), TestValue::new("data1"));
        let val2 = &TestMapValue::new(TestValue::new("data2"), TestValue::new("data2"));
        let val3 = &TestMapValue::new(TestValue::new("data3"), TestValue::new("data3"));

        {
            let key = &TestKey::new(42).into();
            assert_eq!(db.hvals(key).await.unwrap(), Vec::<Vec<u8>>::new());
            let values = &[
                &val2.clone().into(),
                &val2.clone().into(),
                &val2.clone().into(),
                &val3.clone().into(),
                &val1.clone().into(),
                &val3.clone().into(),
            ];
            // Duplicates are filtered out on addition.
            db.hmset(key, values, None, timestamp_micros())
                .await
                .unwrap();
            db.hmset(key, values, None, timestamp_micros())
                .await
                .unwrap();
            let expected: Vec<Vec<u8>> = vec![
                val1.value().into(),
                val2.value().into(),
                val3.value().into(),
            ];
            assert_eq!(db.hvals(key).await.unwrap(), expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_keys_removed() {
        let path = DBPath::new("map_expired_keys_removed");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db_full = rocks_db.column::<MapColumn>().unwrap();
        let db = &db_full.clone();

        let key1 = TestKey::new(1).into();
        let key2 = TestKey::new(2).into();
        let msg1 = TestMapValue::generate();
        let msg2 = TestMapValue::generate();
        let value_pair1 = msg1.clone().into();
        let value_pair2 = msg2.clone().into();

        db.hset(&key2, &value_pair1, None, timestamp_micros())
            .await
            .unwrap();
        db.hset(&key2, &value_pair2, None, timestamp_micros())
            .await
            .unwrap();

        {
            // Add some records with TTL, to be removed in compaction.
            let expiration = timestamp(1);
            db.hset(&key1, &value_pair1, Some(expiration), timestamp_micros())
                .await
                .unwrap();
            db.hset(&key1, &value_pair2, Some(expiration), timestamp_micros())
                .await
                .unwrap();
            assert_eq!(
                BTreeSet::from_iter(db.hvals(&key1).await.unwrap()),
                BTreeSet::from_iter(vec![value_pair1.value.clone(), value_pair2.value.clone()])
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            assert!(db.hvals(&key1).await.unwrap().is_empty());
        }

        // Make sure that second map is not affected by garbage collection.
        assert_eq!(
            BTreeSet::from_iter(db.hvals(&key2).await.unwrap()),
            BTreeSet::from_iter(vec![value_pair1.value.clone(), value_pair2.value.clone()])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded() {
        let path = DBPath::new("map_multithreaded");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db_full = rocks_db.column::<MapColumn>().unwrap();
        let db = &db_full.clone();

        let db1 = db_full.clone().into_map_storage();
        let db2 = db_full.clone().into_map_storage();
        let db3 = db_full.clone().into_map_storage();

        const N: usize = 25_000;

        let h1 = tokio::spawn(async move {
            for i in (0..N).step_by(2) {
                let key = &TestKey::new(42).into();
                let val = &TestMapValue::new(
                    TestValue::new(i.to_string()),
                    TestValue::new(i.to_string()),
                )
                .into();
                db1.hset(key, val, None, timestamp_micros()).await.unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            for i in (1..N).step_by(2) {
                let key = &TestKey::new(42).into();
                let val = &TestMapValue::new(
                    TestValue::new(i.to_string()),
                    TestValue::new(i.to_string()),
                )
                .into();
                db2.hset(key, val, None, timestamp_micros()).await.unwrap();
            }
        });
        let h3 = tokio::spawn(async move {
            // Writing to another key should not interfere.
            for i in N..2 * N {
                let key = &TestKey::new(43).into();
                let val = &TestMapValue::new(
                    TestValue::new(i.to_string()),
                    TestValue::new(i.to_string()),
                )
                .into();
                db3.hset(key, val, None, timestamp_micros()).await.unwrap();
            }
        });
        futures_util::future::join_all(vec![h1, h2, h3]).await;

        let expected: Vec<Vec<u8>> = (0..N)
            .map(|i| TestValue::new(i.to_string()).into())
            .collect();
        let key = &TestKey::new(42).into();
        assert_eq!(db.hvals(key).await.unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cardinality() {
        let path = DBPath::new("map_cardinality");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db_full = rocks_db.column::<MapColumn>().unwrap();
        let db = &db_full.clone();

        let key = &TestKey::new(1).into();

        // Empty set should have cardinality of 0.
        assert_eq!(db.hcard(key).await.unwrap(), 0);

        // Adding an element, should increase the cardinality.
        let msg = TestMapValue::generate();
        db.hset(key, &msg.clone().into(), None, timestamp_micros())
            .await
            .unwrap();
        assert_eq!(db.hcard(key).await.unwrap(), 1);

        // Adding the same element again should not change the cardinality.
        for _ in 0..10 {
            db.hset(key, &msg.clone().into(), None, timestamp_micros())
                .await
                .unwrap();
            assert_eq!(db.hcard(key).await.unwrap(), 1);
        }

        // Adding a different element should increase the cardinality.
        for i in 0..5_usize {
            let msg = TestMapValue::generate();
            db.hset(key, &msg.into(), Some(timestamp(1)), timestamp_micros())
                .await
                .unwrap();
            assert_eq!(db.hcard(key).await.unwrap(), i + 2);
        }

        // Trigger merge.
        db_full.compact();

        // Items added with TTL should be removed in compaction.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Cleanup.
        db_full.compact();

        assert_eq!(db.hcard(key).await.unwrap(), 1);

        // When set element is removed, cardinality is updated correctly.
        db.hdel(key, &msg.field().into()).await.unwrap();
        assert_eq!(db.hcard(key).await.unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan() {
        let path = DBPath::new("map_hscan");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(MapColumn)
            .build()
            .unwrap();
        let db_full = rocks_db.column::<MapColumn>().unwrap();
        let db = &db_full.clone();

        let key1 = TestKey::new(1).into();
        let key2 = TestKey::new(2).into();

        let target_cardinality = 10;
        let data1 = (0..target_cardinality)
            .map(|_| TestMapValue::generate())
            .collect::<Vec<_>>();
        let data2 = (0..target_cardinality)
            .map(|_| TestMapValue::generate())
            .collect::<Vec<_>>();

        assert_eq!(db.hcard(&key1).await.unwrap(), 0);
        assert_eq!(db.hcard(&key2).await.unwrap(), 0);
        for msg in &data1 {
            db.hset(&key1, &msg.clone().into(), None, timestamp_micros())
                .await
                .unwrap();
        }
        for msg in &data2 {
            db.hset(&key2, &msg.clone().into(), None, timestamp_micros())
                .await
                .unwrap();
        }
        assert_eq!(db.hcard(&key1).await.unwrap(), target_cardinality);
        assert_eq!(db.hcard(&key2).await.unwrap(), target_cardinality);

        let items_per_page = 5;

        // Validate the first map data.
        let ScanResult {
            items: page1,
            has_more,
        } = db
            .hscan(&key1, iterators::ScanOptions::new(items_per_page))
            .await
            .unwrap();
        assert!(has_more);
        let cursor = page1.last().map(|data| data.0.clone());

        let ScanResult {
            items: page2,
            has_more,
        } = db
            .hscan(
                &key1,
                iterators::ScanOptions::new(items_per_page).with_cursor(cursor.unwrap()),
            )
            .await
            .unwrap();
        assert!(!has_more);

        let mut data1_received = HashSet::new();
        data1_received.extend(page1.into_iter().map(|data| data.1));
        data1_received.extend(page2.into_iter().map(|data| data.1));
        assert_eq!(
            data1_received,
            HashSet::from_iter(data1.into_iter().map(|data| data.value().into()))
        );

        // Validate the second map data.
        let ScanResult {
            items: page1,
            has_more,
        } = db
            .hscan(&key2, iterators::ScanOptions::new(items_per_page))
            .await
            .unwrap();
        assert!(has_more);
        let cursor = page1.last().map(|data| data.0.clone());

        let ScanResult {
            items: page2,
            has_more,
        } = db
            .hscan(
                &key2,
                iterators::ScanOptions::new(items_per_page).with_cursor(cursor.unwrap()),
            )
            .await
            .unwrap();
        assert!(!has_more);

        let mut data2_received = HashSet::new();
        data2_received.extend(page1.into_iter().map(|data| data.1));
        data2_received.extend(page2.into_iter().map(|data| data.1));
        assert_eq!(
            data2_received,
            HashSet::from_iter(data2.into_iter().map(|data| data.value().into()))
        );
    }
}
