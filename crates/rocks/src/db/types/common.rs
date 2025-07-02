use {
    crate::{
        db::{batch::WriteBatch, cf},
        Error,
        UnixTimestampSecs,
    },
    async_trait::async_trait,
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
    ) -> Result<rocksdb::DBIterator<'_>, Error>;

    /// Returns the expiration of the key in unix timestamp in seconds format.
    fn expiration(
        &self,
        key: &C::KeyType,
        subkey: Option<&C::SubKeyType>,
    ) -> impl Future<Output = Result<UnixTimestampSecs, Error>> + Send + Sync;
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
    ) -> Result<rocksdb::DBIterator<'_>, Error> {
        let left = left.map(|pos| pos.to_be_bytes());
        let right = right.map(|pos| pos.to_be_bytes());

        let iter = self.backend.range_iterator::<C, _>(left, right);
        Ok(iter)
    }

    fn expiration(
        &self,
        key: &C::KeyType,
        subkey: Option<&C::SubKeyType>,
    ) -> impl Future<Output = Result<UnixTimestampSecs, Error>> + Send + Sync {
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
}
