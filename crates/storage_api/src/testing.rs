use {
    crate::{
        operation,
        DataFrame,
        DataItem,
        DataType,
        Error,
        ErrorKind,
        KeyspaceVersion,
        MapEntry,
        MapPage,
        Namespace,
        Operation,
        Record,
        RecordExpiration,
        Result,
        StorageApi,
    },
    derive_where::derive_where,
    futures::{stream, Stream, TryStreamExt},
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap},
        hash::{BuildHasher, Hash},
        iter,
        ops::RangeInclusive,
        sync::{Arc, Mutex},
    },
    xxhash_rust::xxh3::Xxh3Builder,
};

#[derive_where(Clone, Default)]
pub struct FakeRegistry<K> {
    inner: Arc<Mutex<RegistryInner<K>>>,
}

#[derive_where(Default)]
struct RegistryInner<K> {
    storages: HashMap<K, FakeStorage>,
}

impl<K> FakeRegistry<K>
where
    K: Eq + Hash,
{
    pub fn get(&self, key: K) -> FakeStorage {
        self.inner
            .lock()
            .unwrap()
            .storages
            .entry(key)
            .or_default()
            .clone()
    }

    pub fn for_each(&self, f: impl Fn(&K, &FakeStorage)) {
        self.inner
            .lock()
            .unwrap()
            .storages
            .iter()
            .for_each(|(k, v)| f(k, v))
    }
}

#[derive(Clone, Default)]
pub struct FakeStorage {
    inner: Arc<Mutex<Inner>>,
}

impl FakeStorage {
    pub fn break_(&self) {
        self.inner.lock().unwrap().broken = true;
    }

    pub fn expect_keyspace_version(&self, version: u64) {
        self.inner.lock().unwrap().expected_keyspace_version = Some(version);
    }
}

#[derive(Default)]
struct Inner {
    broken: bool,
    expected_keyspace_version: Option<u64>,

    kv: BTreeMap<Vec<u8>, Record>,
    map: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Record>>,
}

impl StorageApi for FakeStorage {
    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        let mut this = self.inner.lock().unwrap();

        if this.broken {
            return Err(Error::new(ErrorKind::Internal));
        }

        if let Some(version) = this.expected_keyspace_version {
            if operation.keyspace_version() != Some(version) {
                return Err(Error::keyspace_version_mismatch());
            }
        }

        #[allow(clippy::unit_arg)]
        Ok(match operation.into_owned() {
            operation::Owned::Get(op) => this.get(op).into(),
            operation::Owned::Set(op) => this.set(op).into(),
            operation::Owned::Del(op) => this.del(op).into(),
            operation::Owned::GetExp(op) => this.get_exp(op).into(),
            operation::Owned::SetExp(op) => this.set_exp(op).into(),
            operation::Owned::HGet(op) => this.hget(op).into(),
            operation::Owned::HSet(op) => this.hset(op).into(),
            operation::Owned::HDel(op) => this.hdel(op).into(),
            operation::Owned::HGetExp(op) => this.hget_exp(op).into(),
            operation::Owned::HSetExp(op) => this.hset_exp(op).into(),
            operation::Owned::HCard(op) => this.hcard(op).into(),
            operation::Owned::HScan(op) => this.hscan(op).into(),
        })
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        _keyspace_version: KeyspaceVersion,
    ) -> Result<impl Stream<Item = Result<DataItem>> + Send> {
        let this = self.inner.lock().unwrap();

        if this.broken {
            return Err(Error::new(ErrorKind::Internal));
        }

        let start = keyrange.start().to_be_bytes().to_vec();
        let end = keyrange.end().to_be_bytes().to_vec();

        let kv: Vec<_> = this
            .kv
            .range(start.clone()..=end.clone())
            .map(|(key, record)| {
                let key = KeyPayload {
                    key: key.clone(),
                    field: vec![],
                };

                Ok(DataItem::Frame(DataFrame {
                    data_type: DataType::Kv,
                    key: serde_json::to_vec(&key).unwrap(),
                    value: serde_json::to_vec(record).unwrap(),
                }))
            })
            .collect();

        let map: Vec<_> = this
            .map
            .range(start..=end)
            .flat_map(|(key, entries)| {
                entries.iter().map(|(field, record)| {
                    let key = KeyPayload {
                        key: key.clone(),
                        field: field.clone(),
                    };

                    Ok(DataItem::Frame(DataFrame {
                        data_type: DataType::Map,
                        key: serde_json::to_vec(&key).unwrap(),
                        value: serde_json::to_vec(record).unwrap(),
                    }))
                })
            })
            .collect();

        let count = kv.len() + map.len();

        let iter = kv
            .into_iter()
            .chain(map.into_iter())
            .chain(iter::once(Ok(DataItem::Done(count as u64))));

        Ok(stream::iter(iter))
    }

    async fn write_data(&self, stream: impl Stream<Item = Result<DataItem>> + Send) -> Result<()> {
        let data: Vec<_> = stream.try_collect().await?;

        let mut this = self.inner.lock().unwrap();

        if this.broken {
            return Err(Error::new(ErrorKind::Internal));
        }

        let mut processed = 0;

        for item in data {
            match item {
                DataItem::Frame(frame) => {
                    let key: KeyPayload = serde_json::from_slice(&frame.key).unwrap();
                    let record: Record = serde_json::from_slice(&frame.value).unwrap();

                    match frame.data_type {
                        DataType::Kv => this.kv.insert(key.key, record),
                        DataType::Map => this
                            .map
                            .entry(key.key)
                            .or_default()
                            .insert(key.field, record),
                    };

                    processed += 1;
                }
                DataItem::Done(count) => assert_eq!(count, processed),
            };
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct KeyPayload {
    key: Vec<u8>,
    field: Vec<u8>,
}

fn key(namespace: &Namespace, bytes: &[u8]) -> Vec<u8> {
    static HASHER: Xxh3Builder = Xxh3Builder::new();

    HASHER
        .hash_one(bytes)
        .to_be_bytes()
        .into_iter()
        .chain(namespace.as_bytes().iter().copied())
        .chain(bytes.iter().copied())
        .collect()
}

impl Inner {
    fn get(&self, op: operation::Get) -> Option<Record> {
        let key = key(&op.namespace, &op.key);
        self.kv.get(&key).cloned()
    }

    fn set(&mut self, op: operation::Set) {
        let key = key(&op.namespace, &op.key);
        let _ = self.kv.insert(key, op.record);
    }

    fn del(&mut self, op: operation::Del) {
        let key = key(&op.namespace, &op.key);
        let _ = self.kv.remove(&key);
    }

    fn get_exp(&self, op: operation::GetExp) -> Option<RecordExpiration> {
        let key = key(&op.namespace, &op.key);
        self.kv.get(&key).map(|rec| rec.expiration)
    }

    fn set_exp(&mut self, op: operation::SetExp) {
        let key = key(&op.namespace, &op.key);
        if let Some(rec) = self.kv.get_mut(&key) {
            rec.expiration = op.expiration;
        }
    }

    fn hget(&self, op: operation::HGet) -> Option<Record> {
        let key = key(&op.namespace, &op.key);
        self.map.get(&key)?.get(&op.field).cloned()
    }

    fn hset(&mut self, op: operation::HSet) {
        let _ = self
            .map
            .entry(key(&op.namespace, &op.key))
            .or_default()
            .insert(op.entry.field, op.entry.record);
    }

    fn hdel(&mut self, op: operation::HDel) {
        let key = key(&op.namespace, &op.key);

        let Some(sub) = self.map.get_mut(&key) else {
            return;
        };

        let _ = sub.remove(&op.field);
        if sub.is_empty() {
            self.map.remove(&key);
        }
    }

    fn hget_exp(&self, op: operation::HGetExp) -> Option<RecordExpiration> {
        let key = key(&op.namespace, &op.key);
        self.map.get(&key)?.get(&op.field).map(|rec| rec.expiration)
    }

    fn hset_exp(&mut self, op: operation::HSetExp) {
        let key = key(&op.namespace, &op.key);

        let Some(sub) = self.map.get_mut(&key) else {
            return;
        };

        if let Some(rec) = sub.get_mut(&op.field) {
            rec.expiration = op.expiration;
        }
    }

    fn hcard(&self, op: operation::HCard) -> u64 {
        self.map
            .get(&key(&op.namespace, &op.key))
            .map(|sub| sub.len() as u64)
            .unwrap_or_default()
    }

    fn hscan(&self, op: operation::HScan) -> MapPage {
        let Some(sub) = self.map.get(&op.key) else {
            return MapPage {
                entries: Vec::new(),
                has_next: false,
            };
        };

        let mut entries = sub.iter().map(|(field, record)| MapEntry {
            field: field.clone(),
            record: record.clone(),
        });

        MapPage {
            entries: (&mut entries).take(op.count as usize).collect(),
            has_next: entries.next().is_some(),
        }
    }
}
