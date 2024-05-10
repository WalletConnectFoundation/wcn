//! Raft storage adapter.

use {
    async_trait::async_trait,
    raft::{storage::Error, Log, Vote},
    serde::{de::DeserializeOwned, Serialize},
    std::{io, path::PathBuf},
};

const RAFT_VOTE_FILE_NAME: &str = "vote.json";
const RAFT_LOG_FILE_NAME: &str = "log.json";
const RAFT_STATE_FILE_NAME: &str = "state.json";

#[derive(Clone)]
pub(super) struct Adapter {
    raft_dir: PathBuf,
}

impl Adapter {
    pub(super) fn new(raft_dir: PathBuf) -> Self {
        Self { raft_dir }
    }

    async fn read<T: DeserializeOwned>(&self, file_name: &str) -> Result<Option<T>, Error> {
        let bytes = match tokio::fs::read(self.raft_dir.join(file_name)).await {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::new(&e)),
        };

        serde_json::from_slice(&bytes).map_err(|e| Error::new(&e))
    }

    async fn write<T: Serialize>(&self, file_name: &'static str, t: &T) -> Result<(), Error> {
        let bytes = serde_json::to_vec(t).map_err(|e| Error::new(&e))?;

        tokio::fs::write(self.raft_dir.join(file_name), bytes)
            .await
            .map_err(|e| Error::new(&e))
    }
}

#[async_trait]
impl<C: raft::TypeConfig> raft::Storage<C> for Adapter {
    async fn read_vote(&mut self) -> Result<Option<Vote<C>>, Error> {
        self.read(RAFT_VOTE_FILE_NAME).await
    }

    async fn write_vote(&mut self, vote: &Vote<C>) -> Result<(), Error> {
        self.write(RAFT_VOTE_FILE_NAME, vote).await
    }

    async fn read_log(&mut self) -> Result<Option<Log<C>>, Error> {
        self.read(RAFT_LOG_FILE_NAME).await
    }

    async fn write_log(&mut self, log: &Log<C>) -> Result<(), Error> {
        self.write(RAFT_LOG_FILE_NAME, log).await
    }

    async fn read_state(&mut self) -> Result<Option<C::State>, Error> {
        self.read(RAFT_STATE_FILE_NAME).await
    }

    async fn write_state(&mut self, state: &C::State) -> Result<(), Error> {
        self.write(RAFT_STATE_FILE_NAME, state).await
    }
}

#[cfg(test)]
mod test {
    use {
        crate::{
            consensus::{Change, Node, State, StoredMembership},
            TypeConfig,
        },
        irn::{cluster, PeerId},
        raft::{testing::log_id, Storage},
        std::{
            collections::{BTreeMap, BTreeSet},
            path::PathBuf,
        },
    };

    fn adapter(dir: PathBuf) -> impl raft::Storage<TypeConfig> {
        super::Adapter::new(dir)
    }

    #[tokio::test]
    async fn raft_storage() {
        let dir = tempfile::TempDir::new().unwrap();

        let mut storage = adapter(dir.as_ref().to_owned());

        assert_eq!(storage.read_vote().await, Ok(None));
        assert_eq!(storage.read_log().await, Ok(None));
        assert_eq!(storage.read_state().await, Ok(None));

        let peer_id = PeerId::random();

        let vote = raft::Vote::<TypeConfig>::new(1, peer_id);
        assert_eq!(storage.write_vote(&vote).await, Ok(()));
        assert_eq!(storage.read_vote().await, Ok(Some(vote)));

        let mut config = BTreeSet::new();
        config.insert(peer_id);

        let mut nodes = BTreeMap::new();
        nodes.insert(peer_id, Node::default());

        let membership = raft::Membership::new(vec![config], nodes);

        let log = raft::Log {
            last_purged_id: Some(log_id(1, peer_id, 1)),
            entries: {
                let mut map = BTreeMap::new();

                map.insert(1, raft::LogEntry::default());

                let normal = raft::LogEntry {
                    log_id: log_id(2, peer_id, 1),
                    payload: raft::LogEntryPayload::Normal(Change::NodeOperationMode(
                        PeerId::default(),
                        cluster::NodeOperationMode::Booting,
                    )),
                };
                map.insert(2, normal);

                let membership = raft::LogEntry {
                    log_id: log_id(2, peer_id, 2),
                    payload: raft::LogEntryPayload::Membership(membership.clone()),
                };
                map.insert(2, membership);
                map
            },
        };
        assert_eq!(storage.write_log(&log).await, Ok(()));
        assert_eq!(storage.read_vote().await, Ok(Some(vote)));

        let state = State {
            last_applied_log: Some(log_id(1, peer_id, 1)),
            membership: StoredMembership::new(Some(log_id(1, peer_id, 1)), membership),
            cluster_view: Default::default(),
        };
        assert_eq!(storage.write_state(&state).await, Ok(()));
        assert_eq!(storage.read_state().await, Ok(Some(state)));
    }
}
