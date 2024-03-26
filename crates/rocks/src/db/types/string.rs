//! Key-value storage for `RocksDB`.

use {
    crate::{
        db::{
            batch,
            cf::{Column, DbColumn},
            context::{self, UnixTimestampMicros},
            schema::StringColumn,
            types::common::CommonStorage,
        },
        util::serde::serialize,
        Error,
        UnixTimestampSecs,
    },
    async_trait::async_trait,
};

#[async_trait]
pub trait StringStorage<C>: CommonStorage<C>
where
    C: Column,
{
    /// Gets value for a provided `key`.
    ///
    /// Time complexity: `O(1)`.
    async fn get(&self, key: &C::KeyType) -> Result<Option<C::ValueType>, Error>;

    /// Sets value for a provided key.
    ///
    /// Time complexity: `O(1)`.
    async fn set(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error>;

    /// Delete the value associated with the given key.
    ///
    /// Time complexity: `O(1)`.
    async fn del(&self, key: &C::KeyType) -> Result<(), Error>;

    /// Returns the remaining time to live of a key that has a timeout.
    async fn exp(&self, key: &C::KeyType) -> Result<Option<UnixTimestampSecs>, Error>;

    /// Set a timeout on key. After the timeout has expired, the key will
    /// automatically be deleted. Passing `None` will remove the current timeout
    /// and persist the key.
    async fn setexp(
        &self,
        key: &C::KeyType,
        expiry: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<C: Column> StringStorage<C> for DbColumn<C> {
    async fn get(&self, key: &C::KeyType) -> Result<Option<C::ValueType>, Error> {
        let key = C::storage_key(key)?;
        let entry = self.backend.get::<C::ValueType>(C::NAME, key).await?;
        entry.map_or_else(|| Ok(None), |data| Ok(data.into_payload()))
    }

    async fn set(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let key = C::storage_key(key)?;
        let value = serialize(
            &context::MergeOp::<&C::ValueType>::new(update_timestamp)
                .with_payload(value, expiration),
        )?;

        self.backend.merge(C::NAME, key, value).await
    }

    async fn del(&self, key: &C::KeyType) -> Result<(), Error> {
        let key = C::storage_key(key)?;
        self.backend.delete(C::NAME, key).await
    }

    async fn exp(&self, key: &C::KeyType) -> Result<Option<UnixTimestampSecs>, Error> {
        self.expiration(key, None).await
    }

    async fn setexp(
        &self,
        key: &C::KeyType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let expiration = expiration
            .map(context::TimestampUpdate::Set)
            .unwrap_or(context::TimestampUpdate::Unset);

        let key = C::storage_key(key)?;
        let value = serialize(
            &context::MergeOp::<C::ValueType>::new(update_timestamp).with_expiration(expiration),
        )?;

        self.backend.merge(C::NAME, key, value).await
    }
}

/// Methods for adding batched operations to a given write batch.
impl DbColumn<StringColumn> {
    pub(crate) fn set_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<StringColumn as Column>::KeyType,
        value: &<StringColumn as Column>::ValueType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let key = StringColumn::storage_key(key)?;
        let value = serialize(
            &context::MergeOp::<&<StringColumn as Column>::ValueType>::new(update_timestamp)
                .with_payload(value, expiration),
        )?;

        batch.merge(StringColumn::NAME, key, value);

        Ok(())
    }

    pub(crate) fn del_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<StringColumn as Column>::KeyType,
    ) -> Result<(), Error> {
        let key = StringColumn::storage_key(key)?;
        batch.delete(<StringColumn as Column>::NAME, key);
        Ok(())
    }

    pub(crate) fn setexp_batched(
        &self,
        batch: &mut batch::WriteBatch,
        key: &<StringColumn as Column>::KeyType,
        expiration: Option<UnixTimestampSecs>,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let expiration = expiration
            .map(context::TimestampUpdate::Set)
            .unwrap_or(context::TimestampUpdate::Unset);

        let key = StringColumn::storage_key(key)?;
        let value = serialize(
            &context::MergeOp::<<StringColumn as Column>::ValueType>::new(update_timestamp)
                .with_expiration(expiration),
        )?;

        batch.merge(StringColumn::NAME, key, value);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            db::schema::{
                test_types::{TestKey, TestValue},
                InternalStringColumn,
            },
            util::{db_path::DBPath, timestamp_micros, timestamp_secs},
            RocksDatabaseBuilder,
        },
        std::{sync::Arc, thread, time::Duration},
        test_log::test,
    };

    fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
        crate::util::timestamp_secs() + added
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_ops() {
        let path = DBPath::new("string_basic_ops");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db: &dyn StringStorage<_> = &rocks_db.column::<StringColumn>().unwrap();

        let key = &TestKey::new(42).into();
        let val = &TestValue::new("value1").into();
        {
            // Make sure that no data exists.
            assert_eq!(db.get(key).await.unwrap(), None);

            // Add data.
            db.set(key, val, None, timestamp_micros()).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val.clone()));

            // Update data.
            let val_upd = TestValue::new("updated value").into();
            db.set(key, &val_upd, None, timestamp_micros())
                .await
                .unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val_upd));

            // Remove data.
            db.del(key).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), None);
        }

        // TTL support.
        let key = &TestKey::new(43).into();
        let val = &TestValue::new("value2").into();
        {
            // Try to get TTL on non-existent key.
            let res = db.exp(key).await;
            assert!(matches!(res, Err(Error::EntryNotFound)));

            // No TTL.
            db.set(key, val, None, timestamp_micros()).await.unwrap();
            let res = db.exp(key).await;
            assert!(matches!(res, Ok(None)));

            // Set TTL to 5 sec.
            let expiration = timestamp(5);
            db.setexp(key, Some(expiration), timestamp_micros())
                .await
                .unwrap();
            let ttl = db.exp(key).await.unwrap();
            assert_eq!(ttl, Some(expiration));
        }
    }

    // TODO: Remove this test once we've completed the data pruning and removed the
    // pruning code from compaction filters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn fix_invalid_timestamps() {
        let path = DBPath::new("string_basic_ops");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db_full = rocks_db.column::<StringColumn>().unwrap();
        let db: &dyn StringStorage<_> = &rocks_db.column::<StringColumn>().unwrap();

        let key1 = TestKey::new(1).into();
        let key2 = TestKey::new(2).into();
        let key3 = TestKey::new(3).into();
        let key4 = TestKey::new(4).into();
        let key5 = TestKey::new(5).into();
        let key6 = TestKey::new(6).into();
        let val = TestValue::new("value1").into();

        assert_eq!(db.get(&key1).await.unwrap(), None);
        assert_eq!(db.get(&key2).await.unwrap(), None);
        assert_eq!(db.get(&key3).await.unwrap(), None);
        assert_eq!(db.get(&key4).await.unwrap(), None);
        assert_eq!(db.get(&key5).await.unwrap(), None);
        assert_eq!(db.get(&key6).await.unwrap(), None);

        db.set(&key1, &val, None, timestamp_micros()).await.unwrap();
        db.set(&key2, &val, Some(timestamp_secs() + 1), timestamp_micros())
            .await
            .unwrap();
        db.set(&key3, &val, Some(timestamp_secs() + 5), timestamp_micros())
            .await
            .unwrap();
        db.set(&key4, &val, Some(u64::MAX), timestamp_micros())
            .await
            .unwrap();
        db.set(&key5, &val, None, u64::MAX).await.unwrap();
        db.set(&key6, &val, None, timestamp_micros()).await.unwrap();
        db.set(&key6, &val, None, u64::MAX).await.unwrap();

        // Trigger merge.
        db_full.compact();

        thread::sleep(Duration::from_secs(2));

        // Cleanup.
        db_full.compact();

        // Compaction is expected to remove the following:
        //  - key2: expired;
        //  - key4: invalid TTL;
        //  - key5: invalid creation timestamp;
        //  - key6: invalid update timestamp.
        assert_eq!(db.get(&key1).await.unwrap(), Some(val.clone()));
        assert_eq!(db.get(&key2).await.unwrap(), None);
        assert_eq!(db.get(&key3).await.unwrap(), Some(val.clone()));
        assert_eq!(db.get(&key4).await.unwrap(), None);
        assert_eq!(db.get(&key5).await.unwrap(), None);
        assert_eq!(db.get(&key6).await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn modification_timetsamp() {
        let path = DBPath::new("string_modification_timetsamp");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db: &dyn StringStorage<_> = &rocks_db.column::<StringColumn>().unwrap();

        // Payload.
        {
            let key = &TestKey::new(42).into();
            let val1 = &TestValue::new("value1").into();
            let val2 = &TestValue::new("value2").into();
            let timestamp1 = timestamp_micros();
            let timestamp2 = timestamp1 + 1;

            // Make sure that no data exists.
            assert_eq!(db.get(key).await.unwrap(), None);

            // Add data.
            db.set(key, val1, None, timestamp1).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val1.clone()));

            // Update the data with higher timestamp value. It's expected to succeed.
            db.set(key, val2, None, timestamp2).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val2.clone()));

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.set(key, val1, None, timestamp1).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val2.clone()));
        }

        // Expiry.
        {
            let key = &TestKey::new(43).into();
            let val = &TestValue::new("value3").into();
            let expiry1 = timestamp_secs() + 30;
            let expiry2 = expiry1 + 30;
            let timestamp1 = timestamp_micros();
            let timestamp2 = timestamp1 + 1;

            // Make sure that no data exists.
            assert_eq!(db.get(key).await.unwrap(), None);

            // Add data.
            db.set(key, val, Some(expiry1), timestamp1).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), Some(val.clone()));
            assert_eq!(db.exp(key).await.unwrap(), Some(expiry1));

            // Update the data with higher timestamp value. It's expected to succeed.
            db.setexp(key, Some(expiry2), timestamp2).await.unwrap();
            assert_eq!(db.exp(key).await.unwrap(), Some(expiry2));

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.setexp(key, Some(expiry1), timestamp1).await.unwrap();
            assert_eq!(db.exp(key).await.unwrap(), Some(expiry2));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded() {
        let path = DBPath::new("string_multithreaded");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();
        let db = rocks_db.column::<StringColumn>().unwrap();

        let db1: Arc<dyn StringStorage<_>> = db.clone().into_string_storage();
        let db2: Arc<dyn StringStorage<_>> = db.clone().into_string_storage();

        const N: usize = 25_000;

        let h1 = tokio::spawn(async move {
            for i in (0..N).step_by(2) {
                let key = &TestKey::new(i as u64).into();
                let val = &TestValue::new(format!("value_{i}")).into();
                db1.set(key, val, None, timestamp_micros()).await.unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            for i in (1..N).step_by(2) {
                let key = &TestKey::new(i as u64).into();
                let val = TestValue::new(format!("value_{i}")).into();
                db2.set(key, &val, None, timestamp_micros()).await.unwrap();
            }
        });
        futures::future::join_all(vec![h1, h2]).await;

        for i in 0..N {
            let key = &TestKey::new(i as u64).into();
            let val = TestValue::new(format!("value_{i}")).into();
            assert_eq!(db.get(key).await.unwrap(), Some(val));
        }
    }

    #[test(tokio::test(flavor = "multi_thread"))]
    async fn expired_keys_removed() {
        let path = DBPath::new("string_expired_keys_removed");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        // db_full is unbounded object exposing all methods, and db is coerced to
        // dynamic trait object -- which exposes only trait object's methods.
        let db_full = rocks_db.column::<StringColumn>().unwrap();
        let db: &dyn StringStorage<_> = &db_full.clone();

        let key1 = &TestKey::new(1).into();
        let key2 = &TestKey::new(2).into();
        let val = TestValue::new("value").into();
        db.set(key1, &val, Some(timestamp(1)), timestamp_micros())
            .await
            .unwrap();
        db.set(key2, &val, None, timestamp_micros()).await.unwrap();

        // Trigger merge.
        db_full.compact();

        thread::sleep(Duration::from_secs(2));

        // Cleanup.
        db_full.compact();

        // Read data using the backend directly to avoid expiration-based filtering of
        // the result.
        assert!(db_full
            .backend
            .get::<Vec<u8>>(StringColumn::NAME, StringColumn::storage_key(key1).unwrap())
            .await
            .unwrap()
            .is_none());
        assert!(db_full
            .backend
            .get::<Vec<u8>>(StringColumn::NAME, StringColumn::storage_key(key2).unwrap())
            .await
            .unwrap()
            .is_some());
    }

    #[test(tokio::test(flavor = "multi_thread"))]
    async fn expired_keys_not_returned() {
        let path = DBPath::new("string_expired_keys_removed");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db_full = rocks_db.column::<StringColumn>().unwrap();
        let db: &dyn StringStorage<_> = &db_full.clone();

        let key1 = &TestKey::new(1).into();
        let key2 = &TestKey::new(2).into();
        let val = TestValue::new("value").into();
        db.set(key1, &val, Some(timestamp(1)), timestamp_micros())
            .await
            .unwrap();
        db.set(key2, &val, None, timestamp_micros()).await.unwrap();

        thread::sleep(Duration::from_secs(2));

        assert!(db.get(key1).await.unwrap().is_none());
        assert!(db.get(key2).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_cfs() {
        let path = DBPath::new("string_multiple_cfs");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .with_column_family(InternalStringColumn)
            .build()
            .unwrap();

        // The similarly structured columns, saved into different column families.
        let cf1: &dyn StringStorage<_> = &rocks_db.column::<StringColumn>().unwrap();
        let cf2: &dyn StringStorage<_> = &rocks_db.column::<InternalStringColumn>().unwrap();

        let key = &TestKey::new(42).into();
        let val1: Vec<u8> = TestValue::new("value1").into();
        let val2: Vec<u8> = TestValue::new("value2").into();
        {
            // Make sure that no data exists in any column family.
            assert_eq!(cf1.get(key).await.unwrap(), None);
            assert_eq!(cf2.get(key).await.unwrap(), None);

            // Add different values under the same key in different column families.
            cf1.set(key, &val1, None, timestamp_micros()).await.unwrap();
            assert_eq!(cf1.get(key).await.unwrap(), Some(val1.clone()));
            assert_eq!(cf2.get(key).await.unwrap(), None); // db1 update only!
            cf2.set(key, &val2, None, timestamp_micros()).await.unwrap();
            assert_eq!(cf1.get(key).await.unwrap(), Some(val1.clone())); // still val1
            assert_eq!(cf2.get(key).await.unwrap(), Some(val2.clone()));

            // Update data.
            let updated_val = TestValue::new("updated value").into();
            cf1.set(key, &updated_val, None, timestamp_micros())
                .await
                .unwrap();
            cf2.set(key, &updated_val, None, timestamp_micros())
                .await
                .unwrap();
            assert_eq!(cf1.get(key).await.unwrap(), Some(updated_val.clone()));
            assert_eq!(cf2.get(key).await.unwrap(), Some(updated_val.clone()));

            // Remove data.
            cf1.del(key).await.unwrap();
            assert_eq!(cf1.get(key).await.unwrap(), None);
            assert_eq!(cf2.get(key).await.unwrap(), Some(updated_val.clone())); // still exists
            cf2.del(key).await.unwrap();
            assert_eq!(cf2.get(key).await.unwrap(), None);
        }
    }
}
