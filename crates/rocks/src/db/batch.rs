//! Utilities for atomic batch committing into database.

use crate::{db::cf::ColumnFamilyName, RocksBackend};

pub struct WriteBatch {
    pub(crate) write_batch: rocksdb::WriteBatchWithTransaction<true>,
    backend: RocksBackend,
}

impl WriteBatch {
    pub fn new(backend: RocksBackend) -> Self {
        Self {
            backend,
            write_batch: rocksdb::WriteBatchWithTransaction::default(),
        }
    }

    pub(crate) fn _delete<K>(&mut self, cf: ColumnFamilyName, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.write_batch.delete_cf(&self.backend.cf_handle(cf), key)
    }

    pub(crate) fn merge<K, V>(&mut self, cf: ColumnFamilyName, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_batch
            .merge_cf(&self.backend.cf_handle(cf), key, value)
    }
}
