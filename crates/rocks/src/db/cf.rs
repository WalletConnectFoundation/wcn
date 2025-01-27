//! Column family related traits and structs.
use {
    super::types::{MapStorage, StringStorage},
    crate::{
        db::{compaction, schema::GenericKey},
        util::serde::{deserialize, serialize, FromBytes, ToBytes},
        Error,
        RocksBackend,
    },
    rocksdb::ColumnFamily,
    serde::{de::DeserializeOwned, Serialize},
    std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc},
};

pub use crate::db::schema::ColumnFamilyName;

/// Contains a full description of a column residing within some column family.
///
/// This is main abstraction around the `RocksDB` column families. A single
/// column family can support multiple columns of the same data type.
///
/// Once column is backend-injected (and turns into [`DbColumn`]), clients will
/// be able to interface with the backend, and thus the underlying database.
pub trait Column: 'static + Debug + Send + Sync + Sized {
    /// Column family where this column is stored.
    const NAME: ColumnFamilyName;

    /// Flag indicating whether column family uses prefix seek.
    /// If set to `true`, then a filter will be extracted to allow seeking
    /// within a prefix.
    const PREFIX_SEEK_ENABLED: bool;

    /// Defines compaction filter factory to be used when compacting `SSTables`.
    /// If no compaction factory is required for a given column family, use
    /// [`compaction::NoopCompactionFilterFactory`].
    type CompactionFilterFactory: compaction::CompactionFilterFactory<Self>;

    /// Primary key identifying an object within a column family.
    type KeyType: Send + Sync + Clone + Hash + ToBytes + FromBytes;

    /// Some data types store multi-part keys, to uniquely identify an object.
    ///
    /// For example, set data type is stored as `(key, sub-key) -> ()` key-value
    /// pair, where `sub-key` is an element identifier within a set identified
    /// by `key`.
    ///
    /// Maps are stored as `(key, sub-key) -> value`, where `sub-key` is a field
    /// name within a map identified by `key`.
    type SubKeyType: Serialize + DeserializeOwned + PartialEq + Send + Sync;

    /// Defines how column family values are stored.
    type ValueType: Serialize + DeserializeOwned + Send + Sync;

    /// Defines how values are merged.
    type MergeOperator: compaction::MergeOperator;

    /// Coverts key to bytes that are stored in database.
    ///
    /// Since multiple columns can reside in a single column family, all keys
    /// should be prefixed with column identifier.
    fn storage_key(key: &Self::KeyType) -> Result<Vec<u8>, Error> {
        key.to_bytes().map_err(|_| Error::Serialize)
    }

    /// Creates an extended key with value appended at the end of the extended
    /// key prefix.
    ///
    /// The key is a concatenation of `key` + `field`.
    ///
    /// Set elements are stored as a part of database keys, so ext-key needs to
    /// be extended to contain the element itself (set data structure is
    /// treated as a hashmap, with elements stored in keys and values being
    /// empty -- although we wrap the empty values into `DataContext` to store
    /// extra information s.t. per-element expiration times).
    ///
    /// So, for a set identified by `key` and containing elements `field1`,
    /// `field2`, the extended keys and corresponding values will be:
    /// - `(ext-key1 = (key, field1)) -> ()`
    /// - `(ext-key2 = (key, field2)) -> ()`
    ///
    /// Notice that both items have the same key prefix of `key`.
    /// This allows to get all of the container's elements by querying the
    /// database for keys starting with that prefix (prefix seek).
    ///
    /// Map elements ext-keys are extended exactly the same way as set elements.
    /// The only difference is that while set values are `()` (empty), map
    /// values are not. So, for a map identified by `key` and
    /// having a value pair `field1 -> value1`, the extended key and
    /// corresponding value will be:
    /// - `(ext-key1 = (key, field1)) -> value1`
    fn ext_key(key: &Self::KeyType, field: &Self::SubKeyType) -> Result<Vec<u8>, Error> {
        Self::ext_key_from_slice(key, &serialize(field)?)
    }

    fn ext_key_from_slice(key: &Self::KeyType, value: &[u8]) -> Result<Vec<u8>, Error> {
        let prefix = Self::storage_key(key)?;
        let mut out_key = Vec::with_capacity(prefix.len() + value.len());
        out_key.extend_from_slice(&prefix);
        out_key.extend_from_slice(value);
        Ok(out_key)
    }

    /// Parses the bytes stored in database into a key.
    ///
    /// If column family uses extended keys (key is a concatenation of
    /// container identifier and sub-key) then this function parses only
    /// part of incoming key (container identifier) and returns bytes processed.
    fn parse_key(storage_key: &[u8]) -> Result<(Self::KeyType, usize), Error> {
        let left = 0;
        let right = GenericKey::calculate_size(storage_key).map_err(|_| Error::Deserialize)?;
        let parsed_key =
            Self::KeyType::from_bytes(&storage_key[left..right]).map_err(|_| Error::Deserialize)?;
        Ok((parsed_key, right))
    }

    /// Some column families may store keys and values within storage keys. This
    /// function allows to extract elements from such key.
    fn parse_ext_key(ext_key: &[u8]) -> Result<(Self::KeyType, Self::SubKeyType), Error> {
        let (key, bytes_processed) = Self::parse_key(ext_key)?;
        let value = deserialize::<Self::SubKeyType>(&ext_key[bytes_processed..])?;
        Ok((key, value))
    }

    /// Returns the slice corresponding to the `Self::SubKeyType` data from the
    /// full storage key.
    fn split_ext_key(storage_key: &[u8]) -> Result<&[u8], Error> {
        GenericKey::calculate_size(storage_key)
            .map(|prefix_len| &storage_key[prefix_len..])
            .map_err(|_| Error::Deserialize)
    }
}

/// Column with [`RocksBackend`] injected, allowing implementations to interact
/// with the underlying database.
///
/// This struct is returned by `RocksDatabase`, and serves as main API for
/// database clients.
#[derive(Debug, Clone)]
pub struct DbColumn<C>
where
    C: Column,
{
    pub(crate) backend: RocksBackend,
    _phantom: PhantomData<C>,
}

impl<C> DbColumn<C>
where
    C: Column,
{
    pub fn new(backend: RocksBackend) -> Self {
        Self {
            backend,
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn handle(&self) -> &ColumnFamily {
        self.backend.cf_handle(C::NAME)
    }

    #[inline]
    pub fn compact(&self) {
        self.backend.compact(C::NAME);
    }
}

impl<C> DbColumn<C>
where
    C: Column,
    DbColumn<C>: StringStorage<C>,
{
    #[inline]
    pub fn into_string_storage(self) -> Arc<impl StringStorage<C>> {
        Arc::new(self)
    }
}

impl<C> DbColumn<C>
where
    C: Column,
    DbColumn<C>: MapStorage<C>,
{
    #[inline]
    pub fn into_map_storage(self) -> Arc<impl MapStorage<C>> {
        Arc::new(self)
    }
}
