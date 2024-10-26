//! Key-value storage for `RocksDB`.

use {
    crate::{
        db::{
            cf::{Column, DbColumn},
            context::{self, UnixTimestampMicros},
            types::common::CommonStorage,
        },
        util::serde::serialize,
        Error,
        UnixTimestampSecs,
    },
    std::future::Future,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record<V = Vec<u8>> {
    pub value: V,
    pub expiration: UnixTimestampSecs,
    pub version: UnixTimestampMicros,
}

pub trait StringStorage<C>: CommonStorage<C>
where
    C: Column,
{
    /// Gets value for a provided `key`.
    ///
    /// Time complexity: `O(1)`.
    fn get(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Option<Record<C::ValueType>>, Error>> + Send + Sync;

    /// Sets value and expiration for a provided key.
    ///
    /// Time complexity: `O(1)`.
    fn set(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        expiration: UnixTimestampSecs,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Sets value for a provided key (only if the value already exists).
    ///
    /// Time complexity: `O(1)`.
    fn set_val(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Delete the value associated with the given key.
    ///
    /// Time complexity: `O(1)`.
    fn del(
        &self,
        key: &C::KeyType,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Returns the remaining time to live of a key that has a timeout.
    fn exp(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<UnixTimestampSecs, Error>> + Send + Sync;

    /// Set a timeout on key. After the timeout has expired, the key will
    /// automatically be deleted. Passing `None` will remove the current timeout
    /// and persist the key.
    fn setexp(
        &self,
        key: &C::KeyType,
        expiry: UnixTimestampSecs,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;
}

impl<C: Column> StringStorage<C> for DbColumn<C> {
    fn get(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<Option<Record<C::ValueType>>, Error>> + Send + Sync {
        async {
            let key = C::storage_key(key)?;
            let Some(data) = self.backend.get::<C::ValueType>(C::NAME, key).await? else {
                return Ok(None);
            };

            let expiration = data.expiration_timestamp();

            let version = data
                .modification_timestamp()
                .unwrap_or_else(|| data.creation_timestamp());

            Ok(data.into_payload().map(|value| Record {
                value,
                expiration,
                version,
            }))
        }
    }

    fn set(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        expiration: UnixTimestampSecs,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;
            let value = serialize(&context::MergeOp::set(value, expiration, update_timestamp))?;

            self.backend.merge(C::NAME, key, value).await
        }
    }

    fn set_val(
        &self,
        key: &C::KeyType,
        value: &C::ValueType,
        update_timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;
            let value = serialize(&context::MergeOp::set_val(value, update_timestamp))?;

            self.backend.merge(C::NAME, key, value).await
        }
    }

    fn del(
        &self,
        key: &C::KeyType,
        timestamp: UnixTimestampMicros,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync {
        async move {
            let key = C::storage_key(key)?;
            let value = serialize(&context::MergeOp::<&C::ValueType>::del(timestamp))?;
            self.backend.merge(C::NAME, key, value).await
        }
    }

    fn exp(
        &self,
        key: &C::KeyType,
    ) -> impl Future<Output = Result<UnixTimestampSecs, Error>> + Send + Sync {
        self.expiration(key, None)
    }

    async fn setexp(
        &self,
        key: &C::KeyType,
        expiration: UnixTimestampSecs,
        update_timestamp: UnixTimestampMicros,
    ) -> Result<(), Error> {
        let key = C::storage_key(key)?;
        let value = serialize(&context::MergeOp::<C::ValueType>::set_exp(
            expiration,
            update_timestamp,
        ))?;

        self.backend.merge(C::NAME, key, value).await
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
                StringColumn,
            },
            util::{db_path::DBPath, timestamp_micros, timestamp_secs},
            RocksDatabaseBuilder,
        },
        std::{thread, time::Duration},
        test_log::test,
    };

    fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
        crate::util::timestamp_secs() + added
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_ops() {
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_basic_ops");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db = &rocks_db.column::<StringColumn>().unwrap();

        let key = &TestKey::new(42).into();
        let val = &TestValue::new("value1").into();
        {
            // Make sure that no data exists.
            assert_eq!(db.get(key).await.unwrap(), None);

            // Add data.
            let timestamp = timestamp_micros();
            db.set(key, val, expiration, timestamp).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val.clone(),
                    expiration,
                    version: timestamp,
                })
            );

            // Update data.
            let val_upd = TestValue::new("updated value").into();
            let ver_upd = timestamp_micros();
            db.set(key, &val_upd, expiration, ver_upd).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val_upd,
                    expiration,
                    version: ver_upd
                })
            );

            // Remove data.
            db.del(key, timestamp_micros()).await.unwrap();
            assert_eq!(db.get(key).await.unwrap(), None);
        }

        // TTL support.
        let key = &TestKey::new(43).into();
        let val = &TestValue::new("value2").into();
        {
            // Try to get TTL on non-existent key.
            let res = db.exp(key).await;
            assert!(matches!(res, Err(Error::EntryNotFound)));

            db.set(key, val, expiration, timestamp_micros())
                .await
                .unwrap();
            let res = db.exp(key).await;
            assert_eq!(res, Ok(expiration));

            // Set TTL to 5 sec.
            let expiration = timestamp(5);
            db.setexp(key, expiration, timestamp_micros())
                .await
                .unwrap();
            let ttl = db.exp(key).await.unwrap();
            assert_eq!(ttl, expiration);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn modification_timetsamp() {
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_modification_timetsamp");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db = &rocks_db.column::<StringColumn>().unwrap();

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
            db.set(key, val1, expiration, timestamp1).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val1.clone(),
                    expiration,
                    version: timestamp1,
                })
            );

            // Update the data with higher timestamp value. It's expected to succeed.
            db.set(key, val2, expiration, timestamp2).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val2.clone(),
                    expiration,
                    version: timestamp2
                })
            );

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.set(key, val1, expiration, timestamp1).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val2.clone(),
                    expiration,
                    version: timestamp2,
                })
            );

            // Delete the data with lower timestamp value. It's expected to be ignored.
            db.del(key, timestamp1).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val2.clone(),
                    expiration,
                    version: timestamp2,
                })
            );
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
            db.set(key, val, expiry1, timestamp1).await.unwrap();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val.clone(),
                    expiration: expiry1,
                    version: timestamp1,
                })
            );
            assert_eq!(db.exp(key).await.unwrap(), expiry1);

            // Update the data with higher timestamp value. It's expected to succeed.
            db.setexp(key, expiry2, timestamp2).await.unwrap();
            assert_eq!(db.exp(key).await.unwrap(), expiry2);

            // Update the data with lower timestamp value. It's expected to be ignored.
            db.setexp(key, expiry1, timestamp1).await.unwrap();
            assert_eq!(db.exp(key).await.unwrap(), expiry2);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded() {
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_multithreaded");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();
        let db = rocks_db.column::<StringColumn>().unwrap();

        let db1 = db.clone().into_string_storage();
        let db2 = db.clone().into_string_storage();

        const N: usize = 25_000;

        let timestamp = timestamp_micros();

        let h1 = tokio::spawn(async move {
            for i in (0..N).step_by(2) {
                let key = &TestKey::new(i as u64).into();
                let val = &TestValue::new(format!("value_{i}")).into();
                db1.set(key, val, expiration, timestamp).await.unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            for i in (1..N).step_by(2) {
                let key = &TestKey::new(i as u64).into();
                let val = TestValue::new(format!("value_{i}")).into();
                db2.set(key, &val, expiration, timestamp).await.unwrap();
            }
        });
        futures::future::join_all(vec![h1, h2]).await;

        for i in 0..N {
            let key = &TestKey::new(i as u64).into();
            let val = TestValue::new(format!("value_{i}")).into();
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Record {
                    value: val,
                    expiration,
                    version: timestamp,
                })
            );
        }
    }

    #[test(tokio::test(flavor = "multi_thread"))]
    async fn expired_keys_removed() {
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_expired_keys_removed");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        // db_full is unbounded object exposing all methods, and db is coerced to
        // dynamic trait object -- which exposes only trait object's methods.
        let db_full = rocks_db.column::<StringColumn>().unwrap();
        let db = &db_full.clone();

        let key1 = &TestKey::new(1).into();
        let key2 = &TestKey::new(2).into();
        let val = TestValue::new("value").into();
        db.set(key1, &val, timestamp(1), timestamp_micros())
            .await
            .unwrap();
        db.set(key2, &val, expiration, timestamp_micros())
            .await
            .unwrap();

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
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_expired_keys_removed");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();

        let db_full = rocks_db.column::<StringColumn>().unwrap();
        let db = &db_full.clone();

        let key1 = &TestKey::new(1).into();
        let key2 = &TestKey::new(2).into();
        let val = TestValue::new("value").into();
        db.set(key1, &val, timestamp(1), timestamp_micros())
            .await
            .unwrap();
        db.set(key2, &val, expiration, timestamp_micros())
            .await
            .unwrap();

        thread::sleep(Duration::from_secs(2));

        assert!(db.get(key1).await.unwrap().is_none());
        assert!(db.get(key2).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_cfs() {
        let expiration = timestamp_secs() + 600;

        let path = DBPath::new("string_multiple_cfs");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .with_column_family(InternalStringColumn)
            .build()
            .unwrap();

        // The similarly structured columns, saved into different column families.
        let cf1 = &rocks_db.column::<StringColumn>().unwrap();
        let cf2 = &rocks_db.column::<InternalStringColumn>().unwrap();

        let key = &TestKey::new(42).into();
        let val1: Vec<u8> = TestValue::new("value1").into();
        let val2: Vec<u8> = TestValue::new("value2").into();
        {
            // Make sure that no data exists in any column family.
            assert_eq!(cf1.get(key).await.unwrap(), None);
            assert_eq!(cf2.get(key).await.unwrap(), None);

            // Add different values under the same key in different column families.
            let timestamp = timestamp_micros();
            cf1.set(key, &val1, expiration, timestamp_micros())
                .await
                .unwrap();
            assert_eq!(
                cf1.get(key).await.unwrap(),
                Some(Record {
                    value: val1.clone(),
                    expiration,
                    version: timestamp,
                })
            );
            assert_eq!(cf2.get(key).await.unwrap(), None); // db1 update only!
            let timestamp2 = timestamp_micros();
            cf2.set(key, &val2, expiration, timestamp2).await.unwrap();
            assert_eq!(
                cf1.get(key).await.unwrap(),
                Some(Record {
                    value: val1.clone(),
                    expiration,
                    version: timestamp,
                })
            ); // still val1
            assert_eq!(
                cf2.get(key).await.unwrap(),
                Some(Record {
                    value: val2.clone(),
                    expiration,
                    version: timestamp2,
                })
            );

            // Update data.
            let updated_val = TestValue::new("updated value").into();
            let updated_ver = timestamp_micros();
            cf1.set(key, &updated_val, expiration, updated_ver)
                .await
                .unwrap();
            cf2.set(key, &updated_val, expiration, updated_ver)
                .await
                .unwrap();
            assert_eq!(
                cf1.get(key).await.unwrap(),
                Some(Record {
                    value: updated_val.clone(),
                    expiration,
                    version: updated_ver,
                })
            );
            assert_eq!(
                cf2.get(key).await.unwrap(),
                Some(Record {
                    value: updated_val.clone(),
                    expiration,
                    version: updated_ver,
                })
            );

            // Remove data.
            cf1.del(key, timestamp_micros()).await.unwrap();
            assert_eq!(cf1.get(key).await.unwrap(), None);
            assert_eq!(
                cf2.get(key).await.unwrap(),
                Some(Record {
                    value: updated_val.clone(),
                    expiration,
                    version: updated_ver,
                })
            ); // still exists
            cf2.del(key, timestamp_micros()).await.unwrap();
            assert_eq!(cf2.get(key).await.unwrap(), None);
        }
    }
}
