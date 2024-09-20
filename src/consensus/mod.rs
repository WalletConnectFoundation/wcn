use {
    crate::{cluster, contract, network::rpc, Cluster, Config, Network, RemoteNode, TypeConfig},
    anyhow::Context,
    async_trait::async_trait,
    backoff::{future::retry, ExponentialBackoff},
    derive_more::{AsRef, Deref, Display},
    futures::FutureExt as _,
    irn::fsm::ShutdownReason,
    irn_rpc::{
        quic::{self, socketaddr_to_multiaddr},
        Client as _,
        Multiaddr,
    },
    libp2p::PeerId,
    parking_lot::Mutex,
    raft::{
        storage::{Snapshot, SnapshotMeta},
        Raft as _,
        RemoteError,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::{HashMap, HashSet},
        error::Error as StdError,
        fmt::Debug,
        future::Future,
        io::{self, Cursor},
        sync::Arc,
        time::Duration,
    },
    tap::{Pipe, TapFallible},
    tokio::sync::watch,
};

mod storage;

#[derive(Clone)]
pub struct State {
    last_applied_log: Option<LogId>,
    membership: StoredMembership,
    cluster: cluster::Viewable,
}

impl Default for State {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            membership: StoredMembership::default(),
            cluster: Cluster::new().into_viewable(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StateSnapshot<'a> {
    last_applied_log: Option<LogId>,
    membership: StoredMembership,
    cluster: cluster::Snapshot<'a>,
}

impl raft::State<TypeConfig> for State {
    type Ok = bool;
    type Error = cluster::Error;

    fn apply(&mut self, entry: &LogEntry) -> Result<bool, Self::Error> {
        let res = match &entry.payload {
            raft::LogEntryPayload::Blank => Ok(true),
            raft::LogEntryPayload::Normal(change) => match change {
                Change::AddNode(c) => self
                    .cluster
                    .modify(|cluster| cluster.add_node(c.node.clone()).map(|()| true))
                    .map_err(Into::into),

                Change::CompletePull(c) => self
                    .cluster
                    .modify(|cluster| cluster.complete_pull(&c.node_id, c.keyspace_version))
                    .map_err(Into::into),

                Change::ShutdownNode(c) => self
                    .cluster
                    .modify(|cluster| cluster.shutdown_node(&c.id))
                    .map_err(Into::into),

                Change::StartupNode(c) => self
                    .cluster
                    .modify(|cluster| cluster.startup_node(c.node.clone()).map(|()| true))
                    .map_err(Into::into),

                Change::DecommissionNode(c) => self
                    .cluster
                    .modify(|cluster| cluster.decommission_node(&c.id).map(|()| true))
                    .map_err(Into::into),
            },
            raft::LogEntryPayload::Membership(membership) => {
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

    fn snapshot(&self) -> Result<raft::storage::Snapshot<TypeConfig>, impl StdError + 'static> {
        let last_log_id = self.last_applied_log_id();
        let meta = SnapshotMeta::<TypeConfig> {
            last_log_id,
            last_membership: self.stored_membership().clone(),
            snapshot_id: last_log_id.map(|id| id.to_string()).unwrap_or_default(),
        };

        let cluster = self.cluster.view().cluster();

        postcard::to_allocvec(&StateSnapshot {
            last_applied_log: meta.last_log_id,
            membership: meta.last_membership.clone(),
            cluster: cluster.snapshot(),
        })
        .map(|data| Snapshot::<TypeConfig> {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    fn install_snapshot(
        &mut self,
        meta: &raft::storage::SnapshotMeta<TypeConfig>,
        snapshot: Box<io::Cursor<Vec<u8>>>,
    ) -> Result<(), impl StdError + Send + 'static> {
        let data = snapshot.into_inner();

        let snapshot: StateSnapshot<'static> = postcard::from_bytes(data.as_slice())
            .map_err(|e| cluster::Error::Bug(format!("failed to deserialize snapshot: {e}")))?;

        let new_cluster = Cluster::from_snapshot(snapshot.cluster)?;
        self.cluster.modify(|c| {
            *c = new_cluster;
            Ok(true)
        })?;

        self.last_applied_log = meta.last_log_id;
        self.membership = meta.last_membership.clone();

        Ok::<_, cluster::Error>(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    AddNode(AddNode),
    CompletePull(CompletePull),
    ShutdownNode(ShutdownNode),
    StartupNode(StartupNode),
    DecommissionNode(DecommissionNode),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapCluster {
    pub nodes: Vec<cluster::Node>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddNode {
    pub node: cluster::Node,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletePull {
    pub node_id: PeerId,
    pub keyspace_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownNode {
    pub id: PeerId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupNode {
    pub node: cluster::Node,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecommissionNode {
    pub id: PeerId,
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
pub struct Raft {
    id: PeerId,

    #[deref]
    inner: raft::RaftImpl<TypeConfig, Network>,

    network: Network,

    authorization: Option<Authorization>,

    initial_membership: Arc<Mutex<Option<raft::Membership<NodeId, Node>>>>,

    bootstrap_nodes: Arc<Option<Vec<PeerId>>>,
    is_voter: bool,

    stake_validator: Option<contract::StakeValidator>,
}

#[derive(Clone, Deref)]
pub struct Consensus {
    #[deref]
    inner: Raft,
    cluster_view: cluster::View,
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

    #[error("Failed to spawn Raft server")]
    SpawnServer(#[from] quic::Error),

    #[error("Cluster needs to be bootstrapped, but no bootstrap nodes are provided")]
    NoBootstrapNodes,

    #[error("Missing address for bootstrap node {_0}")]
    BootstrapNodeAddrMissing(PeerId),

    #[error(transparent)]
    Cluster(irn::cluster::Error),
}

impl Consensus {
    /// Spawns a new [`Consensus`] task and returns it's handle.
    pub async fn new(
        cfg: &Config,
        network: Network,
        stake_validator: Option<contract::StakeValidator>,
    ) -> Result<Consensus, InitializationError> {
        tokio::fs::create_dir_all(&cfg.raft_dir)
            .await
            .map_err(InitializationError::DirectoryCreation)?;

        let raft_config = raft::Config {
            heartbeat_interval: 1000,
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

        let (tx, mut state) = watch::channel(None);
        let storage_adapter = storage::Adapter::new(cfg.raft_dir.clone(), tx);

        let server_addr = socketaddr_to_multiaddr((cfg.server_addr, cfg.raft_server_port));

        let bootstrap_nodes: Option<HashMap<_, _>> = cfg
            .bootstrap_nodes
            .clone()
            .map(|nodes| {
                let map_fn = |id| {
                    if id == cfg.id {
                        return Ok((NodeId(id), Node(server_addr.clone())));
                    };

                    cfg.known_peers
                        .get(&id)
                        .map(|addr| (NodeId(id), Node(addr.clone())))
                        .ok_or_else(|| InitializationError::BootstrapNodeAddrMissing(id))
                };

                nodes.into_iter().map(map_fn).collect::<Result<_, _>>()
            })
            .transpose()?;

        let raft = raft::new(
            NodeId(cfg.id),
            raft_config,
            bootstrap_nodes.map(|nodes| nodes.into_iter()),
            network.clone(),
            storage_adapter,
        )
        .await
        .map_err(InitializationError::Task)?;

        let raft = Raft {
            id: cfg.id,
            inner: raft,
            network,
            authorization: cfg.authorized_raft_candidates.as_ref().map(|candidates| {
                Authorization {
                    allowed_candidates: candidates.clone(),
                }
            }),
            initial_membership: Default::default(),
            bootstrap_nodes: Arc::new(cfg.bootstrap_nodes.clone()),
            is_voter: cfg.is_raft_voter,
            stake_validator,
        };

        Network::spawn_raft_server(cfg, server_addr.clone(), raft.clone())?;

        raft.init(cfg, &server_addr).await?;

        let cluster_view = loop {
            state.changed().await.unwrap();

            if let Some(state) = state.borrow_and_update().clone() {
                break state.cluster.view();
            };
        };

        let consensus = Self {
            inner: raft,
            cluster_view,
        };

        Ok(consensus)
    }

    pub async fn shutdown(&self, reason: ShutdownReason) {
        match reason {
            ShutdownReason::Decommission => {
                let raft = self.inner.clone();
                let req = RemoveMemberRequest {
                    node_id: NodeId(self.id),
                    is_learner: self.bootstrap_nodes.is_none() && !self.is_voter,
                };

                let backoff = ExponentialBackoff {
                    initial_interval: Duration::from_secs(1),
                    ..Default::default()
                };
                let _ = retry(backoff, move || {
                    let raft = raft.clone();
                    let req = req.clone();

                    async move {
                        raft.inner
                            .remove_member(req)
                            .await
                            .tap_err(|err| tracing::error!(?err, "Raft::remove_member"))
                            .map_err(backoff::Error::transient)
                    }
                })
                .await;
            }
            ShutdownReason::Restart => {}
        }

        self.inner.shutdown().await
    }
}

impl Raft {
    async fn init(&self, cfg: &Config, server_addr: &Multiaddr) -> Result<(), InitializationError> {
        self.wait_init().await;

        let membership = self.metrics().membership_config;
        let nodes: &HashMap<_, _> = &membership.nodes().collect();

        tracing::info!(%server_addr, ?nodes, "init");

        let node = nodes.get(&NodeId(self.id));

        // If there are no other nodes in the membership, that means that this is a new
        // node, and it needs to be added to the cluster.
        //
        // If this node is within the membership already and it's address hasn't
        // been changed, we don't need to re-add it.
        if matches!(node, Some(node) if &node.0 == server_addr && nodes.len() > 1) {
            return Ok(());
        }

        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(1),
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        retry(backoff, move || async {
            let req = AddMemberRequest {
                node_id: NodeId(self.id),
                node: Node(server_addr.clone()),
                learner_only: !self.is_voter,
                payload: cfg.eth_address.as_ref().map(|addr| AddMemberPayload {
                    operator_eth_address: addr.clone(),
                }),
            };

            // It happens sometimes that a leader restarts quick enough, so a new leader doesn't get
            // elected. We just re-add the leader locally to update its address.
            if self.metrics().current_leader == Some(NodeId(self.id)) {
                if let Err(err) = self.inner.add_member(req.clone()).await {
                    tracing::warn!(?err, "leader failed to update itself");
                } else {
                    return Ok(());
                };
            }

            let known_members = nodes
                .iter()
                .filter_map(|(NodeId(id), Node(addr))| (id != &self.id).then_some((id, addr)));

            for (peer_id, multiaddr) in cfg.known_peers.iter().chain(known_members) {
                let peer = self.network.get_peer(peer_id, multiaddr);

                let res = async {
                    rpc::raft::AddMember::send(&peer.client, peer.multiaddr.as_ref(), req.clone())
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
                        tracing::warn!(?err, %peer_id, %multiaddr, "failed to add a member via a remote peer")
                    }
                }
            }

            Err(InitializationError::Join.into())
        })
        .await
    }

    pub async fn add_member(
        &self,
        peer_id: &libp2p::PeerId,
        req: AddMemberRequest,
    ) -> AddMemberResult {
        // Skip authorization if not configured.
        if let Some(auth) = &self.authorization {
            // Adding new voters is forbidden for clusters with enabled authorization.
            if !req.learner_only {
                return Err(unauthorized_error());
            }

            let membership = self.inner.metrics().membership_config;

            // if local node is a voter, do the whitelist-based authorization.
            if membership.voter_ids().any(|i| i.0 == self.id) {
                // if this is a proxy request - a learner node trying to promote
                // a candidate via this voter node - check that
                // the requestor is a member of the cluster.
                if peer_id != &req.node_id.0 && !membership.nodes().any(|(i, _)| &i.0 == peer_id) {
                    return Err(unauthorized_error());
                }

                if !auth.allowed_candidates.contains(&req.node_id.0) {
                    return Err(unauthorized_error());
                }

                if let Some(validator) = self.stake_validator.as_ref() {
                    let Some(operator_eth_address) =
                        req.payload.as_ref().map(|p| &p.operator_eth_address)
                    else {
                        tracing::warn!(id = %req.node_id, "Operator didn't provide ETH address");
                        return Err(unauthorized_error());
                    };
                    if let Err(err) = validator.validate_stake(operator_eth_address).await {
                        tracing::warn!(id = %req.node_id, ?err, "Stake validation failed");
                        return Err(unauthorized_error());
                    }
                }
            }
            // otherwise forward the request to a voter.
            else {
                let voter_ids: HashSet<_> = membership.voter_ids().collect();
                let Some((voter_id, voter_addr)) = membership
                    .nodes()
                    .find_map(|(id, n)| voter_ids.contains(id).then_some((id.0, n.0.clone())))
                else {
                    return Err(unauthorized_error());
                };

                return self
                    .network
                    .get_peer(&voter_id, &voter_addr)
                    .into_owned()
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

        self.inner.add_member(req).await
    }

    pub async fn remove_member(
        &self,
        peer_id: &libp2p::PeerId,
        req: RemoveMemberRequest,
    ) -> RemoveMemberResult {
        let membership = self.inner.metrics().membership_config;

        // Nodes are generally only allowed to remove themselves.
        // Voters are allowed to remove anyone.
        if peer_id != &req.node_id.0 && !membership.voter_ids().any(|i| &i.0 == peer_id) {
            return Err(unauthorized_error());
        }

        self.inner.remove_member(req).await
    }

    pub fn is_member(&self, peer_id: &libp2p::PeerId) -> bool {
        let membership = self.inner.metrics().membership_config;

        if membership.nodes().any(|(i, _)| &i.0 == peer_id) {
            return true;
        }

        if let Some(nodes) = self.bootstrap_nodes.as_ref() {
            if nodes.contains(peer_id) {
                return true;
            }
        }

        if let Some(m) = self.initial_membership.lock().as_ref() {
            if m.nodes().any(|(i, _)| &i.0 == peer_id) {
                return true;
            }
        }

        tracing::warn!(
            %peer_id,
            "Non-member node tried to access an internal Raft RPC"
        );

        false
    }

    pub fn is_voter(&self, peer_id: &libp2p::PeerId) -> bool {
        let membership = self.inner.metrics().membership_config;
        membership.voter_ids().any(|id| &id.0 == peer_id)
    }
}

fn unauthorized_error() -> Error<raft::ClientWriteFail<TypeConfig>> {
    Error::APIError(raft::ClientWriteFail::Local(
        raft::LocalClientWriteFail::Unauthorized,
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error(transparent)]
    Cluster(cluster::Error),

    #[error(transparent)]
    Other(Box<Error<ClientWriteFail>>),
}

impl TryFrom<ConsensusError> for irn::cluster::Error {
    type Error = ConsensusError;

    fn try_from(err: ConsensusError) -> Result<Self, Self::Error> {
        match err {
            ConsensusError::Cluster(e) => Ok(e),
            _ => Err(err),
        }
    }
}

impl irn::cluster::Consensus for Consensus {
    type Node = cluster::Node;
    type Keyspace = cluster::Keyspace;
    type Error = ConsensusError;

    fn add_node(&self, node: &Self::Node) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner
            .propose_change(raft::ProposeChangeRequest {
                change: Change::AddNode(AddNode { node: node.clone() }),
            })
            .map(|res| {
                res.map_err(|e| ConsensusError::Other(Box::new(e)))?
                    .data
                    .map(drop)
                    .map_err(ConsensusError::Cluster)
            })
    }

    fn complete_pull(
        &self,
        node_id: &PeerId,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner
            .propose_change(raft::ProposeChangeRequest {
                change: Change::CompletePull(CompletePull {
                    node_id: *node_id,
                    keyspace_version,
                }),
            })
            .map(|res| {
                res.map_err(|e| ConsensusError::Other(Box::new(e)))?
                    .data
                    .map(drop)
                    .map_err(ConsensusError::Cluster)
            })
    }

    fn shutdown_node(&self, id: &PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner
            .propose_change(raft::ProposeChangeRequest {
                change: Change::ShutdownNode(ShutdownNode { id: *id }),
            })
            .map(|res| {
                res.map_err(|e| ConsensusError::Other(Box::new(e)))?
                    .data
                    .map(drop)
                    .map_err(ConsensusError::Cluster)
            })
    }

    fn startup_node(
        &self,
        node: &Self::Node,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner
            .propose_change(raft::ProposeChangeRequest {
                change: Change::StartupNode(StartupNode { node: node.clone() }),
            })
            .map(|res| {
                res.map_err(|e| ConsensusError::Other(Box::new(e)))?
                    .data
                    .map(drop)
                    .map_err(ConsensusError::Cluster)
            })
    }

    fn decommission_node(
        &self,
        id: &PeerId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner
            .propose_change(raft::ProposeChangeRequest {
                change: Change::DecommissionNode(DecommissionNode { id: *id }),
            })
            .map(|res| {
                res.map_err(|e| ConsensusError::Other(Box::new(e)))?
                    .data
                    .map(drop)
                    .map_err(ConsensusError::Cluster)
            })
    }

    fn cluster(&self) -> Arc<Cluster> {
        self.cluster_view.cluster()
    }

    fn cluster_view(&self) -> &cluster::View {
        &self.cluster_view
    }
}

#[async_trait]
impl raft::Network<TypeConfig> for Network {
    type Client = RemoteNode<'static>;

    async fn new_client(&self, target: NodeId, node: &Node) -> Self::Client {
        self.get_peer(&target.0, &node.0).into_owned()
    }
}

#[async_trait]
impl raft::Raft<TypeConfig, raft::RpcApi> for RemoteNode<'static> {
    async fn add_member(&self, req: AddMemberRequest) -> AddMemberRpcResult {
        self.client
            .send_unary::<rpc::raft::AddMember>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }

    async fn remove_member(&self, req: RemoveMemberRequest) -> RemoveMemberRpcResult {
        self.client
            .send_unary::<rpc::raft::RemoveMember>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }

    async fn propose_change(&self, req: ProposeChangeRequest) -> ProposeChangeRpcResult {
        self.client
            .send_unary::<rpc::raft::ProposeChange>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }

    async fn append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesRpcResult {
        self.client
            .send_unary::<rpc::raft::AppendEntries>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }

    async fn install_snapshot(&self, req: InstallSnapshotRequest) -> InstallSnapshotRpcResult {
        self.client
            .send_unary::<rpc::raft::InstallSnapshot>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }

    async fn vote(&self, req: VoteRequest) -> VoteRpcResult {
        self.client
            .send_unary::<rpc::raft::Vote>(self.multiaddr.as_ref(), req)
            .await
            .map_err(|e| RpcError::Unreachable(raft::UnreachableError::new(&e)))?
            .map_err(|e| RemoteError::new(NodeId(self.id()), e).into())
    }
}

#[derive(
    Clone, Copy, Debug, Display, Hash, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq,
)]
pub struct NodeId(PeerId);

impl Default for NodeId {
    fn default() -> Self {
        Self(PeerId::from_multihash(Default::default()).unwrap())
    }
}

impl raft::TypeConfig for TypeConfig {
    type State = State;
    type Change = Change;
    type NodeId = NodeId;
    type Node = Node;
    type AddMemberPayload = AddMemberPayload;
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct AddMemberPayload {
    operator_eth_address: String,
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
