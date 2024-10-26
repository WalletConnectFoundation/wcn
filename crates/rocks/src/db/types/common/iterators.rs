use {
    crate::{
        db::{cf, migration::ExportFrame, schema::ColumnFamilyName, types::map},
        util::serde::deserialize,
        DataContext,
        Error,
        RocksBackend,
    },
    itertools::Itertools,
    serde::{Deserialize, Serialize},
    std::marker::PhantomData,
};

/// Wrapper around raw `RocksDB` iterator, typed by column family.
pub struct DbIterator<'a> {
    cf: ColumnFamilyName,
    iter: rocksdb::DBIterator<'a>,
}

impl<'a> DbIterator<'a> {
    /// Creates a new iterator for the given column family.
    pub fn new(cf: ColumnFamilyName, iter: rocksdb::DBIterator<'a>) -> Self {
        Self { cf, iter }
    }
}

impl<'a> IntoIterator for DbIterator<'a> {
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + 'a>;
    type Item = Result<ExportFrame, Error>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter.map(move |res| {
            res.map(|(key, value)| ExportFrame {
                cf: self.cf,
                key,
                value,
            })
            .map_err(Into::into)
        }))
    }
}

pub type GenericCursor = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanResult<T = Vec<u8>> {
    pub items: Vec<map::Record<GenericCursor, T>>,
    pub has_more: bool,
}

impl<T> ScanResult<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
            has_more: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanOptions<C> {
    pub count: usize,
    pub cursor: Option<C>,
}

impl<C> ScanOptions<C> {
    pub fn new(count: usize) -> Self {
        Self {
            count,
            cursor: None,
        }
    }

    pub fn with_cursor(mut self, cursor: impl Into<Option<C>>) -> Self {
        self.cursor = cursor.into();
        self
    }
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

/// Iterator to iterate over all the key/value pairs for a given key.
///
/// This iterator accepts `DBIterator` and traverses it transforming the values
/// (extracting, deserializing) as necessary.
pub struct KeyValueIterator<'a, C: cf::Column> {
    iter: rocksdb::DBIterator<'a>,
    _column: PhantomData<C>,
}

impl<'a, C> KeyValueIterator<'a, C>
where
    C: cf::Column,
{
    pub(crate) fn new(iter: rocksdb::DBIterator<'a>) -> Self {
        Self {
            iter,
            _column: PhantomData,
        }
    }

    pub fn entries(
        self,
    ) -> impl Iterator<Item = Result<(C::KeyType, C::SubKeyType, C::ValueType), Error>> + 'a {
        self.filter_map_ok(|data| {
            parse_kv::<C>(&data)
                .map(|(k, f, v)| v.map(|v| (k, f, v)))
                .transpose()
        })
        .flatten()
    }

    pub fn keys(self) -> impl Iterator<Item = Result<(C::KeyType, C::SubKeyType), Error>> + 'a {
        self.map_ok(|data| parse_key::<C>(&data)).flatten()
    }

    pub fn subkeys(self) -> impl Iterator<Item = Result<C::SubKeyType, Error>> + 'a {
        self.map_ok(|data| parse_subkey::<C>(&data)).flatten()
    }

    pub fn values(self) -> impl Iterator<Item = Result<C::ValueType, Error>> + 'a {
        self.filter_map_ok(|data| parse_value::<C>(&data).transpose())
            .flatten()
    }
}

impl<'a, C> Iterator for KeyValueIterator<'a, C>
where
    C: cf::Column,
{
    type Item = Result<KVBytes, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.iter.next()?.map_err(Into::into);

        Some(result)
    }
}

#[inline]
#[allow(clippy::type_complexity)]
fn parse_kv<C: cf::Column>(
    data: &KVBytes,
) -> Result<(C::KeyType, C::SubKeyType, Option<C::ValueType>), Error> {
    let (key, subkey) = parse_key::<C>(data)?;
    Ok((key, subkey, parse_value::<C>(data)?))
}

#[inline]
fn parse_key<C: cf::Column>((key, _): &KVBytes) -> Result<(C::KeyType, C::SubKeyType), Error> {
    C::parse_ext_key(key).map_err(Into::into)
}

#[inline]
fn parse_subkey<C: cf::Column>(data: &KVBytes) -> Result<C::SubKeyType, Error> {
    Ok(parse_key::<C>(data)?.1)
}

#[inline]
fn parse_value<C: cf::Column>((_, value): &KVBytes) -> Result<Option<C::ValueType>, Error> {
    deserialize::<DataContext<C::ValueType>>(value).map(DataContext::into_payload)
}

pub struct MapRecords;

impl<C> IterTransform<C> for MapRecords
where
    C: cf::Column,
{
    type Value = map::Record<GenericCursor, C::ValueType>;

    fn map(data: &KVBytes) -> Result<Option<Self::Value>, Error> {
        let subkey_slice = C::split_ext_key(&data.0)?;

        let data = deserialize::<DataContext<C::ValueType>>(&data.1)?;

        let exp = data.expiration_timestamp();

        let version = data
            .modification_timestamp()
            .unwrap_or_else(|| data.creation_timestamp());

        Ok(data.into_payload().map(|value| map::Record {
            field: subkey_slice.to_vec(),
            value,
            expiration: exp,
            version,
        }))
    }
}

pub struct CursorAndSubKey;

impl<C> IterTransform<C> for CursorAndSubKey
where
    C: cf::Column,
{
    type Value = (GenericCursor, C::SubKeyType);

    fn map(data: &KVBytes) -> Result<Option<Self::Value>, Error> {
        let subkey_slice: &[u8] = C::split_ext_key(&data.0)?;
        let subkey = parse_subkey::<C>(data)?;
        let data = parse_value::<C>(data)?.map(|_| (subkey_slice.into(), subkey));
        Ok(data)
    }
}

pub trait IterTransform<C: cf::Column> {
    type Value;

    fn map(data: &KVBytes) -> Result<Option<Self::Value>, Error>;
}

// TODO: This code should be merged with `KeyValueIterator` or replaced by it.
pub fn scan<C, T, V>(
    backend: &RocksBackend,
    key: &C::KeyType,
    opts: ScanOptions<GenericCursor>,
) -> Result<ScanResult<V>, Error>
where
    C: cf::Column,
    T: IterTransform<C, Value = map::Record<GenericCursor, V>>,
{
    let iter = if let Some(cursor) = &opts.cursor {
        // If we have a cursor, use a different version of the iterator, which skips to
        // the first key.
        let key = C::ext_key_from_slice(key, cursor)?;
        backend.prefix_iterator_with_cursor::<C, _>(key)
    } else {
        // If we don't have a cursor, use the regular prefix iterator.
        let key = C::storage_key(key)?;
        backend.prefix_iterator::<C, _>(key)
    };

    let mut iter = KeyValueIterator::<C>::new(iter)
        .map_ok(|data| T::map(&data))
        .flatten()
        .filter_map(Result::transpose)
        .peekable();

    let mut result = ScanResult::with_capacity(opts.count);

    // The start cursor points to the last item returned by the previous scan. So we
    // should skip if it's the same item. But if it was removed, the iterator will
    // start from the next item.
    let is_same_cursor = matches!(
        (iter.peek(), &opts.cursor),
        (Some(Ok(rec)), Some(start_cursor)) if &rec.field == start_cursor
    );

    if is_same_cursor {
        // Skip the same item we've returned in the previous scan.
        iter.next();
    }

    while let Some(item) = iter.next() {
        result.items.push(item?);

        if result.items.len() >= opts.count {
            result.has_more = iter.next().is_some();
            break;
        }
    }

    Ok(result)
}
