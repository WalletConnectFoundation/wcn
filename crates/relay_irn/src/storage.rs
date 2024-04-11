use {async_trait::async_trait, std::error::Error as StdError};

#[async_trait]
pub trait Storage<Op>: Clone + Send + Sync + 'static {
    type Ok;
    type Error: StdError + Send;

    async fn exec(&self, op: Op) -> Result<Self::Ok, Self::Error>;
}

#[cfg(any(feature = "testing", test))]
pub use stub::Stub;
#[cfg(any(feature = "testing", test))]
pub mod stub {
    use {
        super::async_trait,
        crate::{
            cluster::keyspace::{hashring::Positioned, KeyPosition, KeyRange},
            migration::{CommitHintedOperations, Export, Import, StoreHinted},
            replication::{Read, ReplicatableOperation, Write},
            Storage,
        },
        derive_more::AsRef,
        serde::{Deserialize, Serialize},
        std::{
            collections::{BTreeMap, HashSet},
            sync::{
                atomic::{self, AtomicU64},
                Arc,
                RwLock,
                RwLockReadGuard,
                RwLockWriteGuard,
            },
            time::Duration,
        },
    };

    #[derive(AsRef, Clone, Debug)]
    pub struct Get(#[as_ref] pub u64);

    impl ReplicatableOperation for Get {
        type Type = Read;
        type Key = u64;
        type Output = Option<u64>;
        type RepairOperation = Set;

        fn repair_operation(&self, new_value: Self::Output) -> Option<Self::RepairOperation> {
            Some(Set(self.0, new_value.unwrap()))
        }
    }

    #[derive(AsRef, Clone, Debug)]
    pub struct Set(#[as_ref] pub u64, pub u64);

    impl ReplicatableOperation for Set {
        type Type = Write;
        type Key = u64;
        type Output = ();
        type RepairOperation = ();
    }

    #[derive(AsRef, Clone, Debug)]
    pub struct Del(#[as_ref] pub u64);

    impl ReplicatableOperation for Del {
        type Type = Write;
        type Key = u64;
        type Output = ();
        type RepairOperation = ();
    }

    pub type Entry = ((KeyPosition, u64), u64);

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Data {
        pub entries: BTreeMap<(KeyPosition, u64), u64>,
    }

    impl Data {
        pub fn generate() -> Data {
            Self {
                entries: (0..1000)
                    .map(|_| rand::random::<((KeyPosition, u64), u64)>())
                    .collect(),
            }
        }
    }

    #[derive(Clone, Debug)]
    enum HintedOperation {
        Set(Positioned<Set>),
        Del(Positioned<Del>),
    }

    impl HintedOperation {
        fn position(&self) -> KeyPosition {
            match self {
                Self::Set(p) => p.position,
                Self::Del(p) => p.position,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Stub {
        inner: Arc<RwLock<Inner>>,
        calls: Arc<AtomicU64>,
    }

    impl Default for Stub {
        fn default() -> Self {
            Self::new()
        }
    }

    #[derive(Debug)]
    struct Inner {
        entries: BTreeMap<(KeyPosition, u64), u64>,

        hinted_ops: Vec<HintedOperation>,
        hinted_ops_commit_in_progress: HashSet<KeyRange<KeyPosition>>,
    }

    impl Stub {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(RwLock::new(Inner {
                    entries: BTreeMap::new(),
                    hinted_ops: Vec::new(),
                    hinted_ops_commit_in_progress: HashSet::new(),
                })),
                calls: Arc::new(Default::default()),
            }
        }

        fn read(&self) -> RwLockReadGuard<Inner> {
            self.inner.read().unwrap()
        }

        fn write(&self) -> RwLockWriteGuard<Inner> {
            self.inner.write().unwrap()
        }

        fn store_hinted_op(&self, op: HintedOperation) {
            let mut this = self.write();

            if this
                .hinted_ops_commit_in_progress
                .iter()
                .any(|range| range.contains(&op.position()))
            {
                panic!("Trying to store hinted op during commit");
            }

            this.hinted_ops.push(op);
        }

        pub fn populate(&self, data: Data) {
            self.write().entries.extend(data.entries);
        }

        pub fn data_from_ranges(&self, ranges: &[KeyRange<KeyPosition>]) -> Data {
            let entries = self
                .read()
                .entries
                .clone()
                .into_iter()
                .filter(|((pos, _), _)| ranges.iter().any(|range| range.contains(pos)))
                .collect();

            Data { entries }
        }

        pub fn data(&self) -> Data {
            Data {
                entries: self.read().entries.clone(),
            }
        }
    }

    #[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
    #[error("{0}")]
    pub struct Error(String);

    #[async_trait]
    impl Storage<Positioned<Get>> for Stub {
        type Ok = Option<u64>;
        type Error = Error;

        async fn exec(&self, op: Positioned<Get>) -> Result<Self::Ok, Self::Error> {
            Ok(self.read().entries.get(&(op.position, op.inner.0)).copied())
        }
    }

    #[async_trait]
    impl Storage<Positioned<Set>> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, op: Positioned<Set>) -> Result<Self::Ok, Self::Error> {
            self.write()
                .entries
                .insert((op.position, op.inner.0), op.inner.1);
            self.calls.fetch_add(1, atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Positioned<Del>> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, op: Positioned<Del>) -> Result<Self::Ok, Self::Error> {
            self.write().entries.remove(&(op.position, op.inner.0));
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<Export> for Stub {
        type Ok = Data;
        type Error = Error;

        async fn exec(&self, op: Export) -> Result<Self::Ok, Self::Error> {
            Ok(self.data_from_ranges(&[op.key_range]))
        }
    }

    #[async_trait]
    impl Storage<Import<Data>> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, op: Import<Data>) -> Result<Self::Ok, Self::Error> {
            self.write().entries.extend(op.data.entries);

            Ok(())
        }
    }

    #[async_trait]
    impl Storage<StoreHinted<Positioned<Set>>> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, s: StoreHinted<Positioned<Set>>) -> Result<Self::Ok, Self::Error> {
            self.store_hinted_op(HintedOperation::Set(s.operation));
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<StoreHinted<Positioned<Del>>> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, s: StoreHinted<Positioned<Del>>) -> Result<Self::Ok, Self::Error> {
            self.store_hinted_op(HintedOperation::Del(s.operation));
            Ok(())
        }
    }

    #[async_trait]
    impl Storage<CommitHintedOperations> for Stub {
        type Ok = ();
        type Error = Error;

        async fn exec(&self, op: CommitHintedOperations) -> Result<Self::Ok, Self::Error> {
            let mut hinted_ops = Vec::new();
            self.write().hinted_ops.retain(|hinted| {
                if op.key_range.contains(&hinted.position()) {
                    hinted_ops.push(hinted.clone());
                    false
                } else {
                    true
                }
            });

            if !hinted_ops.is_empty() {
                self.write()
                    .hinted_ops_commit_in_progress
                    .insert(op.key_range);

                tokio::time::sleep(Duration::from_millis(200)).await;

                let mut this = self.write();

                for op in hinted_ops {
                    match op {
                        HintedOperation::Set(op) => {
                            this.entries.insert((op.position, op.inner.0), op.inner.1);
                        }
                        HintedOperation::Del(op) => {
                            this.entries.remove(&(op.position, op.inner.0));
                        }
                    };
                }

                this.hinted_ops_commit_in_progress.remove(&op.key_range);
            }

            Ok(())
        }
    }
}
