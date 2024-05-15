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
    parking_lot::Mutex,
    raft::{Raft, RemoteError, State as _},
    serde::{Deserialize, Serialize},
    std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        io,
        sync::Arc,
        time::Duration,
    },
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

    authorization: Option<Authorization>,

    initial_membership: Arc<Mutex<Option<raft::Membership<PeerId, Node>>>>,

    bootstrap_nodes: Arc<Option<HashMap<PeerId, Multiaddr>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authorization {
    allowed_candidates: HashSet<libp2p::PeerId>,
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
            authorization: cfg.authorized_raft_candidates.as_ref().map(|candidates| {
                Authorization {
                    allowed_candidates: candidates.clone(),
                }
            }),
            initial_membership: Default::default(),
            bootstrap_nodes: Arc::new(cfg.bootstrap_nodes.clone()),
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
                    Ok(resp) => {
                        // After this request is succeeded the cluster already considers this node
                        // to be a member, however the node itself doesn't yet have its membership
                        // updated.
                        // To update the membership the leader needs to either append entries to
                        // this node or install a snapshot.
                        // We want to have an authorization check on these requests, however as the
                        // membership is not yet updated locally we don't know what to check.
                        // So we store this "initial" membership, which we receive in the response.

                        // Unwrap here is fine - membership should be `Some` in the response because
                        // this node issued a change to the membership.
                        *self.initial_membership.lock() = Some(resp.membership.unwrap());

                        return Ok(());
                    }
                    Err(err) => {
                        tracing::warn!(%err, %peer_id, "failed to add a member via a remote peer")
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

    pub async fn add_member(
        &self,
        peer_id: &libp2p::PeerId,
        req: AddMemberRequest,
    ) -> AddMemberResult {
        // Skip authorization if not configured.
        if let Some(auth) = &self.authorization {
            let state = self.raft.state();
            let membership = state.stored_membership();

            // if local node is a voter, do the whitelist-based authorization.
            if membership.voter_ids().any(|i| i == self.id) {
                // if this is a proxy request - a learner node trying to promote
                // a candidate via this voter node - check that
                // the requestor is a member of the cluster.
                if peer_id != &req.node_id.id && !membership.nodes().any(|(i, _)| &i.id == peer_id)
                {
                    return Err(unauthorized_error());
                }

                if !auth.allowed_candidates.contains(&req.node_id.id) {
                    return Err(unauthorized_error());
                }
            }
            // otherwise forward the request to a voter.
            else {
                let Some(voter_id) = membership.voter_ids().next() else {
                    return Err(unauthorized_error());
                };

                return self
                    .network
                    .get_peer(voter_id)
                    .add_member(req)
                    .await
                    .map_err(|err| {
                        Error::APIError(raft::ClientWriteFail::Local(
                            raft::LocalClientWriteFail::Other(format!(
                                "Failed to delegate authorization to a voter: {err}"
                            )),
                        ))
                    });
            }
        }

        self.raft.add_member(req).await
    }

    pub async fn remove_member(
        &self,
        peer_id: &libp2p::PeerId,
        req: RemoveMemberRequest,
    ) -> RemoveMemberResult {
        let state = self.raft.state();
        let membership = state.stored_membership();

        // Nodes are generally only allowed to remove themselves.
        // Voters are allowed to remove anyone.
        if peer_id != &req.node_id.id && !membership.voter_ids().any(|i| &i.id == peer_id) {
            return Err(unauthorized_error());
        }

        self.raft.remove_member(req).await
    }

    pub fn is_member(&self, peer_id: &libp2p::PeerId) -> bool {
        let state = self.raft.state();
        let membership = state.stored_membership();

        if membership.nodes().any(|(i, _)| &i.id == peer_id) {
            return true;
        }

        if let Some(nodes) = self.bootstrap_nodes.as_ref() {
            if nodes.keys().any(|i| &i.id == peer_id) {
                return true;
            }
        }

        if let Some(m) = self.initial_membership.lock().as_ref() {
            if m.nodes().any(|(i, _)| &i.id == peer_id) {
                return true;
            }
        }

        tracing::warn!(
            %peer_id,
            "Non-member node tried to access an internal Raft RPC"
        );

        false
    }
}

fn unauthorized_error() -> Error<raft::ClientWriteFail<TypeConfig>> {
    Error::APIError(raft::ClientWriteFail::Local(
        raft::LocalClientWriteFail::Unauthorized,
    ))
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
