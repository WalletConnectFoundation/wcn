use {
    crate::{
        db::{
            batch,
            cf::{Column, DbColumn},
            context::UnixTimestampMicros,
            schema::{self, columns::InternalHintedOpsColumn},
            types::map::Pair,
        },
        util::serde::deserialize,
        Error,
        RocksBackend,
        UnixTimestampSecs,
    },
    serde::{Deserialize, Serialize},
};

/// The number of hinted operations to process in a single batch.
pub const HINTED_OPS_BATCH_SIZE: usize = 1024;

#[derive(Debug, Serialize, Deserialize, Hash, Clone, PartialEq, Eq)]
pub enum StringHintedOp<C: Column> {
    Set {
        key: C::KeyType,
        value: C::ValueType,
        expiration: Option<UnixTimestampSecs>,
        version: UnixTimestampMicros,
    },
    Del {
        key: C::KeyType,
        version: UnixTimestampMicros,
    },
    SetExp {
        key: C::KeyType,
        expiration: Option<UnixTimestampSecs>,
        version: UnixTimestampMicros,
    },
}

#[derive(Debug, Serialize, Deserialize, Hash, Clone, PartialEq, Eq)]
pub enum MapHintedOp<C: Column> {
    Set {
        key: C::KeyType,
        field: C::SubKeyType,
        value: C::ValueType,
        expiration: Option<UnixTimestampSecs>,
        version: UnixTimestampMicros,
    },
    Del {
        key: C::KeyType,
        field: C::SubKeyType,
        version: UnixTimestampMicros,
    },
    SetExp {
        key: C::KeyType,
        field: C::SubKeyType,
        expiration: Option<UnixTimestampSecs>,
        version: UnixTimestampMicros,
    },
}

#[derive(Debug, Serialize, Deserialize, Hash, Clone, PartialEq, Eq)]
pub enum HintedOp {
    String(StringHintedOp<schema::StringColumn>),
    Map(MapHintedOp<schema::MapColumn>),
}

#[derive(Debug, thiserror::Error)]
#[error("Column family not available")]
struct ColumnFamilyNotAvailable;

impl RocksBackend {
    /// Iterates over all hinted operations and applies them to the main
    /// columns.
    pub fn commit_hinted_ops(&self, left: Option<u64>, right: Option<u64>) -> Result<(), Error> {
        let cf_err_fn = || Error::Other(ColumnFamilyNotAvailable.to_string());
        let generic_string = self
            .column::<schema::StringColumn>()
            .ok_or_else(cf_err_fn)?;
        let generic_map = self.column::<schema::MapColumn>().ok_or_else(cf_err_fn)?;

        let backend = self.clone();
        let mut items_processed = 0;
        let mut batch = batch::WriteBatch::new(backend.clone());

        let iter = self.range_iterator::<InternalHintedOpsColumn, Vec<u8>>(
            left.map(|k| k.to_be_bytes().to_vec()),
            right.map(|k| k.to_be_bytes().to_vec()),
        );
        for result in iter {
            let (seq_no, op) = result?;
            let op = deserialize::<HintedOp>(&op)?;

            match op {
                HintedOp::String(op) => {
                    handle_string_op(&generic_string, &mut batch, op)?;
                }
                HintedOp::Map(op) => handle_map_op(&generic_map, &mut batch, op)?,
            }

            // Operations are only applied once.
            batch.delete(InternalHintedOpsColumn::NAME, seq_no);

            // When the batch is full, write it to the database.
            items_processed += 2;
            if items_processed >= HINTED_OPS_BATCH_SIZE {
                backend.write_batch(batch)?;
                batch = batch::WriteBatch::new(backend.clone());
                items_processed = 0;
            }
        }

        backend.write_batch(batch)
    }
}

fn handle_map_op(
    col: &DbColumn<schema::MapColumn>,
    batch: &mut batch::WriteBatch,
    op: MapHintedOp<schema::MapColumn>,
) -> Result<(), Error> {
    match op {
        MapHintedOp::Set {
            key,
            field,
            value,
            expiration,
            version,
        } => {
            let pair = Pair::new(field, value);
            col.hset_batched(batch, &key, &pair, expiration, version)
        }
        MapHintedOp::Del {
            key,
            field,
            version,
        } => col.hdel_batched(batch, &key, &field, version),
        MapHintedOp::SetExp {
            key,
            field,
            expiration,
            version,
        } => col.hsetexp_batched(batch, &key, &field, expiration, version),
    }
}

fn handle_string_op(
    col: &DbColumn<schema::StringColumn>,
    batch: &mut batch::WriteBatch,
    op: StringHintedOp<schema::StringColumn>,
) -> Result<(), Error> {
    match op {
        StringHintedOp::Set {
            key,
            value,
            expiration,
            version,
        } => col.set_batched(batch, &key, &value, expiration, version),
        StringHintedOp::Del { key, version } => col.del_batched(batch, &key, version),
        StringHintedOp::SetExp {
            key,
            expiration,
            version,
        } => col.setexp_batched(batch, &key, expiration, version),
    }
}
