use {
    crate::{network::rpc, Config, Multiaddr, Network, RemoteNode, TypeConfig},
    anyhow::Context,
    async_trait::async_trait,
    backoff::{future::retry, ExponentialBackoff},
    derive_more::{Deref, Display},
    futures::{stream, StreamExt},
    irn::{
        cluster::{self, ClusterView},
        PeerId,
        ShutdownReason,
    },
    raft::{Raft, RemoteError},
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, fmt::Debug, io, time::Duration},
    tap::{Pipe, TapFallible},
};

mod storage;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct State {
    last_applied_log: Option<LogId>,
    membership: StoredMembership,
    cluster_view: ClusterView,
}

impl State {
    pub fn into_cluster_view(self) -> ClusterView {
        self.cluster_view
    }
}

impl raft::State<TypeConfig> for State {
    type Ok = bool;
    type Error = cluster::UpdateNodeOpModeError;

    fn apply(&mut self, entry: &LogEntry) -> cluster::UpdateNodeOpModeResult {
        let res = match &entry.payload {
            raft::LogEntryPayload::Blank => Ok(true),
            raft::LogEntryPayload::Normal(Change::NodeOperationMode(id, mode)) => {
                self.cluster_view.update_node_op_mode(*id, *mode)
            }
            raft::LogEntryPayload::Membership(membership) => {
                let current_nodes = self.cluster_view.nodes();

                let nodes = membership.nodes().map(|(id, node)| {
                    let node = current_nodes
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| cluster::Node::new(*id, node.0.clone()));
                    (node.peer_id, node)
                });

                let nodes: HashMap<_, _> = nodes.collect();
                if current_nodes != &nodes {
                    self.cluster_view.set_peers(nodes);
                }

                self.membership = StoredMembership::new(Some(entry.log_id), membership.clone());

                Ok(true)
            }
        };
        self.last_applied_log = Some(entry.log_id);
        res
    }

    /// Returns the [`LogId`] of the last applied [`LogEntry`] to this
    /// [`State`] (if any).
    fn last_applied_log_id(&self) -> Option<LogId> {
        self.last_applied_log
    }

    /// Returns the [`StoredMembership`] of this [`State`].
    fn stored_membership(&self) -> &StoredMembership {
        &self.membership
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    NodeOperationMode(PeerId, cluster::NodeOperationMode),
}

// `openraft::Node` has the `Default` bound, however `libp2p::Mutliaddr` doesn't
// impl it. See https://github.com/datafuselabs/openraft/issues/890
#[derive(Clone, Debug, Display, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Node(pub Multiaddr);

impl Default for Node {
    fn default() -> Self {
        Self(Multiaddr::empty())
    }
}

#[derive(Clone, Deref)]
pub struct Consensus {
    id: PeerId,

    #[deref]
    raft: raft::RaftImpl<TypeConfig, Network>,

    network: Network,
}

#[derive(Debug, thiserror::Error)]
pub enum InitializationError {
    #[error("Failed to create Raft directory: {0:?}")]
    DirectoryCreation(io::Error),

    #[error("Failed to initialize Raft task: {0:?}")]
    Task(Error<raft::InitializeError<TypeConfig>>),

    #[error("Failed to join the consensus")]
    Join,
}

impl Consensus {
    /// Spawns a new [`Consensus`] task and returns it's handle.
    pub async fn new(cfg: &Config, network: Network) -> Result<Consensus, InitializationError> {
        tokio::fs::create_dir_all(&cfg.raft_dir)
            .await
            .map_err(InitializationError::DirectoryCreation)?;

        let raft_config = raft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            replication_lag_threshold: 50,
            snapshot_policy: raft::SnapshotPolicy::LogsSinceLast(50),
            // Due to some debug assertions and race conditions in raft code, the value of `0`
            // causes tests to be incredibly flaky. So for debug/testing builds we use a different
            // value.
            max_in_snapshot_log_to_keep: if cfg!(debug_assertions) { 100 } else { 0 },
            install_snapshot_timeout: 15000,
            send_snapshot_timeout: 15000,
            ..Default::default()
        };

        raft::new(
            cfg.id,
            raft_config,
            cfg.bootstrap_nodes
                .clone()
                .map(|nodes| nodes.into_iter().map(|(id, addr)| (id, Node(addr)))),
            State::default(),
            network.clone(),
            storage::Adapter::new(cfg.raft_dir.clone()),
        )
        .await
        .map(|raft| Self {
            id: cfg.id,
            raft,
            network,
        })
        .map_err(InitializationError::Task)
    }

    pub async fn init(&self, cfg: &Config) -> Result<(), InitializationError> {
        // If it's a bootstrap launch or a subsequent launch of a bootnode we don't need
        // to add a new member manually.
        if cfg.bootstrap_nodes.is_some() {
            return Ok(());
        }

        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(1),
            max_elapsed_time: Some(Duration::from_secs(10)),
            ..Default::default()
        };
        retry(backoff, move || async {
            let req = AddMemberRequest {
                node_id: self.id,
                node: Node(cfg.addr.clone()),
                learner_only: !cfg.is_raft_member,
            };

            for peer_id in cfg.known_peers.keys() {
                let peer = self.network.get_peer(*peer_id);

                let res = async {
                    peer.send_rpc::<rpc::raft::AddMember, _>(req.clone())
                        .await
                        .context("outbound::Error")??
                        .pipe(Ok::<_, anyhow::Error>)
                }
                .await;

                match res {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        tracing::warn!(?err, %peer_id, "failed to add a member via a remote peer")
                    }
                }
            }

            Err(InitializationError::Join.into())
        })
        .await
    }

    pub async fn shutdown(&self, reason: ShutdownReason) {
        match reason {
            ShutdownReason::Decommission => {
                let mut voter_ids = self.raft.state().membership.voter_ids();
                let is_voter = voter_ids.any(|id| id == self.id);

                let raft = self.raft.clone();
                let req = RemoveMemberRequest {
                    node_id: self.id,
                    is_learner: !is_voter,
                };

                let backoff = ExponentialBackoff {
                    initial_interval: Duration::from_secs(1),
                    ..Default::default()
                };
                let _ = retry(backoff, move || {
                    let raft = raft.clone();
                    let req = req.clone();

                    async move {
                        raft.remove_member(req)
                            .await
                            .tap_err(|err| tracing::error!(?err, "Raft::remove_member"))
                            .map_err(backoff::Error::transient)
                    }
                })
                .await;
            }
            ShutdownReason::Restart => {}
        }

        self.raft.shutdown().await
    }

    /// Returns the current [`ClusterView`].
    pub fn cluster_view(&self) -> ClusterView {
        self.raft.state().cluster_view
    }
}

pub type ClusterChangesStream = stream::Map<raft::UpdatesStream<State>, fn(State) -> ClusterView>;

#[async_trait]
impl irn::Consensus for Consensus {
    type Stream = ClusterChangesStream;
    type Error = Error<ClientWriteFail>;

    async fn update_node_op_mode(
        &self,
        id: PeerId,
        mode: cluster::NodeOperationMode,
    ) -> Result<cluster::UpdateNodeOpModeResult, Self::Error> {
        self.raft
            .propose_change(raft::ProposeChangeRequest {
                change: Change::NodeOperationMode(id, mode),
            })
            .await
            .map(|resp| resp.data)
    }

    fn changes(&self) -> ClusterChangesStream {
        self.raft.updates().map(State::into_cluster_view)
    }
}

#[async_trait]
impl raft::Network<TypeConfig> for Network {
    type Client = RemoteNode;

    async fn new_client(&self, target: PeerId, _node: &Node) -> Self::Client {
        self.get_peer(target)
    }
}

#[async_trait]
impl Raft<TypeConfig, raft::RpcApi> for RemoteNode {
    async fn add_member(&self, req: AddMemberRequest) -> AddMemberRpcResult {
        self.send_rpc::<rpc::raft::AddMember, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }

    async fn remove_member(&self, req: RemoveMemberRequest) -> RemoveMemberRpcResult {
        self.send_rpc::<rpc::raft::RemoveMember, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }

    async fn propose_change(&self, req: ProposeChangeRequest) -> ProposeChangeRpcResult {
        self.send_rpc::<rpc::raft::ProposeChange, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }

    async fn append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesRpcResult {
        self.send_rpc::<rpc::raft::AppendEntries, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }

    async fn install_snapshot(&self, req: InstallSnapshotRequest) -> InstallSnapshotRpcResult {
        self.send_rpc::<rpc::raft::InstallSnapshot, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }

    async fn vote(&self, req: VoteRequest) -> VoteRpcResult {
        self.send_rpc::<rpc::raft::Vote, _>(req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(self.id(), e).into())
    }
}

impl raft::TypeConfig for TypeConfig {
    type State = State;
    type Change = Change;
    type NodeId = PeerId;
    type Node = Node;
}

type LogId = raft::LogId<TypeConfig>;
type LogEntry = raft::LogEntry<TypeConfig>;
type StoredMembership = raft::StoredMembership<TypeConfig>;
type Error<E> = raft::Error<TypeConfig, E>;
type RpcError<E> = raft::RpcError<TypeConfig, E>;
type ClientWriteFail = raft::ClientWriteFail<TypeConfig>;

pub(super) type AddMemberRequest = raft::AddMemberRequest<TypeConfig>;
pub(super) type AddMemberResult = raft::AddMemberResult<TypeConfig>;
pub(super) type AddMemberRpcResult = raft::AddMemberRpcResult<TypeConfig>;

pub(super) type RemoveMemberRequest = raft::RemoveMemberRequest<TypeConfig>;
pub(super) type RemoveMemberResult = raft::RemoveMemberResult<TypeConfig>;
pub(super) type RemoveMemberRpcResult = raft::RemoveMemberRpcResult<TypeConfig>;

pub(super) type ProposeChangeRequest = raft::ProposeChangeRequest<TypeConfig>;
pub(super) type ProposeChangeRpcResult = raft::ProposeChangeRpcResult<TypeConfig>;
pub(super) type ProposeChangeResult = raft::ProposeChangeResult<TypeConfig>;

pub(super) type AppendEntriesRequest = raft::AppendEntriesRequest<TypeConfig>;
pub(super) type AppendEntriesRpcResult = raft::AppendEntriesRpcResult<TypeConfig>;
pub(super) type AppendEntriesResult = raft::AppendEntriesResult<TypeConfig>;

pub(super) type InstallSnapshotRequest = raft::InstallSnapshotRequest<TypeConfig>;
pub(super) type InstallSnapshotRpcResult = raft::InstallSnapshotRpcResult<TypeConfig>;
pub(super) type InstallSnapshotResult = raft::InstallSnapshotResult<TypeConfig>;

pub(super) type VoteRequest = raft::VoteRequest<TypeConfig>;
pub(super) type VoteRpcResult = raft::VoteRpcResult<TypeConfig>;
pub(super) type VoteResult = raft::VoteResult<TypeConfig>;
