//! Raft storage abstraction.

pub use openraft::AnyError as Error;
use {
    super::{LogEntry as Entry, LogId, State, StoredMembership, TypeConfig, Vote},
    crate::{ApplyResult, OpenRaft},
    async_trait::async_trait,
    openraft::{ErrorSubject, ErrorVerb, OptionalSend, StorageIOError},
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fmt, io::Cursor, ops::RangeBounds, sync::Arc},
    tokio::sync::{watch, RwLock},
};

type StorageError<C> = openraft::StorageError<<C as TypeConfig>::NodeId>;
type Snapshot<C> = openraft::Snapshot<OpenRaft<C>>;
type SnapshotMeta<C> = openraft::SnapshotMeta<<C as TypeConfig>::NodeId, <C as TypeConfig>::Node>;
type LogState<C> = openraft::LogState<OpenRaft<C>>;

/// Persistent raft [`Storage`].
#[async_trait]
pub trait Storage<C: TypeConfig>: Send + Sync + 'static {
    /// Reads [`Vote`].
    async fn read_vote(&mut self) -> Result<Option<Vote<C>>, Error>;

    /// Writes [`Vote`].
    async fn write_vote(&mut self, vote: &Vote<C>) -> Result<(), Error>;

    /// Reads [`Log`].
    async fn read_log(&mut self) -> Result<Option<Log<C>>, Error>;

    /// Writes [`Log`].
    async fn write_log(&mut self, log: &Log<C>) -> Result<(), Error>;

    /// Reads [`State`].
    async fn read_state(&mut self) -> Result<Option<C::State>, Error>;

    /// Writes [`State`].
    async fn write_state(&mut self, state: &C::State) -> Result<(), Error>;
}

#[derive(Clone)]
pub(crate) struct Adapter<C: TypeConfig, S> {
    storage: S,

    vote: Option<Vote<C>>,
    log: Arc<RwLock<Log<C>>>,
    state: Arc<watch::Sender<Arc<C::State>>>,
}

impl<C: TypeConfig, S> Adapter<C, S>
where
    S: Storage<C>,
{
    pub async fn new(
        mut storage: S,
        state: Arc<watch::Sender<Arc<C::State>>>,
    ) -> Result<Self, Error> {
        if let Some(s) = storage.read_state().await? {
            state.send_replace(Arc::new(s));
        };

        let vote = storage.read_vote().await?;
        let log = storage.read_log().await?.unwrap_or_default();

        Ok(Self {
            storage,
            vote,
            log: Arc::new(RwLock::new(log)),
            state,
        })
    }

    async fn modify_and_write_log(
        &mut self,
        f: impl FnOnce(&mut Log<C>),
    ) -> Result<(), StorageError<C>> {
        f(&mut *self.log.write().await);
        self.storage
            .write_log(&*self.log.read().await)
            .await
            .map_err(|e| StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, e).into())
    }

    async fn modify_and_write_state(
        &mut self,
        f: impl FnOnce(&mut C::State),
    ) -> Result<(), StorageError<C>> {
        self.state.send_modify(|arc| f(Arc::make_mut(arc)));

        let state = (*self.state.borrow()).clone();
        self.storage.write_state(&*state).await.map_err(|e| {
            StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Write, e).into()
        })
    }
}

fn build_snapshot<C: TypeConfig, St: State<C>>(state: &St) -> Result<Snapshot<C>, StorageError<C>> {
    let last_log_id = state.last_applied_log_id();
    let meta = SnapshotMeta::<C> {
        last_log_id,
        last_membership: state.stored_membership().clone(),
        snapshot_id: last_log_id.map(|id| id.to_string()).unwrap_or_default(),
    };

    let data = postcard::to_allocvec(state).map_err(|e| {
        StorageIOError::new(
            ErrorSubject::Snapshot(Some(meta.signature())),
            ErrorVerb::Read,
            Error::new(&e),
        )
    })?;

    Ok(Snapshot::<C> {
        meta,
        snapshot: Box::new(Cursor::new(data)),
    })
}

#[async_trait]
impl<C: TypeConfig, S> openraft::RaftStorage<OpenRaft<C>> for Adapter<C, S>
where
    S: Storage<C>,
{
    type LogReader = LogReader<C>;
    type SnapshotBuilder = SnapshotBuilder<C::State>;

    async fn save_vote(&mut self, vote: &Vote<C>) -> Result<(), StorageError<C>> {
        self.vote = Some(*vote);
        self.storage
            .write_vote(vote)
            .await
            .map_err(|e| StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, e).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C>>, StorageError<C>> {
        Ok(self.vote)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<C>>
    where
        I: IntoIterator<Item = Entry<C>> + OptionalSend,
    {
        self.modify_and_write_log(|log| log.append(entries)).await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<C>,
    ) -> Result<(), StorageError<C>> {
        self.modify_and_write_log(|log| log.delete_since_inclusive(log_id))
            .await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<C>) -> Result<(), StorageError<C>> {
        self.modify_and_write_log(|log| log.purge_upto_inclusive(log_id))
            .await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C>>, StoredMembership<C>), StorageError<C>> {
        let state = self.state.borrow();
        Ok((
            state.last_applied_log_id(),
            state.stored_membership().clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<C>],
    ) -> Result<Vec<ApplyResult<C>>, StorageError<C>> {
        let mut responses = Vec::with_capacity(entries.len());

        self.modify_and_write_state(|state| {
            for entry in entries {
                responses.push(state.apply(entry));
            }
        })
        .await?;

        Ok(responses)
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<C>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<C>> {
        let data = snapshot.into_inner();

        let new_state = postcard::from_bytes(data.as_slice()).map_err(|e| {
            StorageIOError::new(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                Error::new(&e),
            )
        })?;

        self.modify_and_write_state(|state| *state = new_state)
            .await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C>> {
        build_snapshot(&**self.state.borrow()).map(Some)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        Ok(self.log.read().await.state())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            log: self.log.clone(),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            state: self.state.clone(),
        }
    }
}

#[async_trait]
impl<C: TypeConfig, S> openraft::RaftLogReader<OpenRaft<C>> for Adapter<C, S>
where
    S: Storage<C>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<C>> {
        Ok(self.log.read().await.entries(range.clone()))
    }
}

pub struct LogReader<C: TypeConfig> {
    log: Arc<RwLock<Log<C>>>,
}

#[async_trait]
impl<C: TypeConfig> openraft::RaftLogReader<OpenRaft<C>> for LogReader<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<C>> {
        Ok(self.log.read().await.entries(range))
    }
}

pub struct SnapshotBuilder<State> {
    state: Arc<watch::Sender<Arc<State>>>,
}

#[async_trait]
impl<C: TypeConfig, St: State<C>> openraft::RaftSnapshotBuilder<OpenRaft<C>>
    for SnapshotBuilder<St>
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C>> {
        build_snapshot(&**self.state.borrow())
    }
}

/// The [`Raft`] log.
///
/// [`Raft`]: super::Raft
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Log<C: TypeConfig> {
    /// ID of the last purged log entry.
    /// Entries can only be purged if they are applied to the state or the
    /// snapshot.
    pub last_purged_id: Option<LogId<C>>,

    // TODO: Consider using `Vec`. I tried and failed miserably.
    // There are some edge cases in the `openraft` internals screwing up the naive 'offset'
    // approach. And I'm done with it for now.
    // The `openraft` memtable example uses `BTreeMap`, so I just copy-pasted it and optimized a
    // bit.
    /// The list of entries.
    pub entries: BTreeMap<u64, Entry<C>>,
}

impl<C: TypeConfig> fmt::Debug for Log<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Log")
            .field("last_purged_id", &self.last_purged_id)
            .field("entries", &self.entries)
            .finish()
    }
}

impl<C: TypeConfig> PartialEq for Log<C> {
    fn eq(&self, other: &Self) -> bool {
        self.last_purged_id == other.last_purged_id && self.entries == other.entries
    }
}

impl<C: TypeConfig> Log<C> {
    fn state(&self) -> LogState<C> {
        LogState {
            last_purged_log_id: self.last_purged_id,
            last_log_id: self
                .entries
                .values()
                .next_back()
                .map(|e| e.log_id)
                .or(self.last_purged_id),
        }
    }

    fn entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send + Sync>(
        &self,
        range: RB,
    ) -> Vec<Entry<C>> {
        self.entries.range(range).map(|(_, e)| e.clone()).collect()
    }

    fn append<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = Entry<C>> + OptionalSend,
    {
        self.entries
            .extend(entries.into_iter().map(|e| (e.log_id.index, e)));
    }

    fn delete_since_inclusive(&mut self, log_id: LogId<C>) {
        let _ = self.entries.split_off(&log_id.index);
    }

    fn purge_upto_inclusive(&mut self, log_id: LogId<C>) {
        self.entries = self.entries.split_off(&(log_id.index + 1));
        self.last_purged_id = Some(log_id);
    }
}

#[cfg(test)]
pub mod test {
    use {
        super::{async_trait, watch, Arc, Error, Log, StorageError, Vote},
        crate::{
            test::{State, C},
            OpenRaft,
        },
    };

    #[derive(Clone, Default)]
    pub struct Storage {
        state: Option<State>,
        log: Option<Log<C>>,
        vote: Option<Vote<C>>,
    }

    #[async_trait]
    impl super::Storage<C> for Storage {
        async fn read_vote(&mut self) -> Result<Option<Vote<C>>, Error> {
            Ok(self.vote)
        }

        async fn write_vote(&mut self, vote: &Vote<C>) -> Result<(), Error> {
            self.vote = Some(*vote);
            Ok(())
        }

        async fn read_log(&mut self) -> Result<Option<Log<C>>, Error> {
            Ok(self.log.clone())
        }

        async fn write_log(&mut self, log: &Log<C>) -> Result<(), Error> {
            self.log = Some(log.clone());
            Ok(())
        }

        async fn read_state(&mut self) -> Result<Option<State>, Error> {
            Ok(self.state.clone())
        }

        async fn write_state(&mut self, state: &State) -> Result<(), Error> {
            self.state = Some(state.clone());
            Ok(())
        }
    }

    type S = crate::OpenRaftStorageHack<C, Storage>;

    #[async_trait]
    impl openraft::testing::StoreBuilder<OpenRaft<C>, S, S> for Storage {
        async fn build(&self) -> Result<((), S, S), StorageError<C>> {
            let (tx, _) = watch::channel(Arc::new(State::default()));

            let adapter = super::Adapter::new(self.clone(), Arc::new(tx))
                .await
                .expect("Adapter::new");

            let (storage, state_machine) = S::new(adapter);

            Ok(((), storage, state_machine))
        }
    }

    #[test]
    fn openraft_suite() -> Result<(), StorageError<C>> {
        openraft::testing::Suite::test_all(Storage::default())
    }
}
