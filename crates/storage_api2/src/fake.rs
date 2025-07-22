use {
    crate::{
        operation,
        Error,
        ErrorKind,
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
    std::{
        collections::{BTreeMap, HashMap},
        hash::{BuildHasher, Hash},
        sync::{Arc, Mutex},
    },
    xxhash_rust::xxh3::Xxh3Builder,
};

#[derive_where(Clone, Default)]
pub struct Registry<K> {
    inner: Arc<Mutex<RegistryInner<K>>>,
}

#[derive_where(Default)]
struct RegistryInner<K> {
    storages: HashMap<K, Storage>,
}

impl<K> Registry<K>
where
    K: Eq + Hash,
{
    pub fn get(&self, key: K) -> Storage {
        self.inner
            .lock()
            .unwrap()
            .storages
            .entry(key)
            .or_default()
            .clone()
    }
}

#[derive(Clone, Default)]
pub struct Storage {
    inner: Arc<Mutex<Inner>>,
}

impl Storage {
    pub fn break_(&self) {
        self.inner.lock().unwrap().broken = true;
    }
}

#[derive(Default)]
struct Inner {
    broken: bool,
    kv: BTreeMap<Vec<u8>, Record>,
    map: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Record>>,
}

impl StorageApi for Storage {
    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        let mut this = self.inner.lock().unwrap();

        if this.broken {
            return Err(Error::new(ErrorKind::Internal));
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
