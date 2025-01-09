pub use {
    self::{
        network::Network,
        storage::{Log, Storage},
    },
    openraft::{
        error::{
            ChangeMembershipError,
            Fatal as FatalError,
            Infallible,
            NetworkError,
            NotInMembers as NotInMembersError,
            RemoteError,
            Unreachable as UnreachableError,
        },
        raft,
        testing,
        Config,
        EntryPayload as LogEntryPayload,
        Membership,
        RaftTypeConfig,
        SnapshotPolicy,
    },
    std::fmt::Debug,
};
use {
    async_trait::async_trait,
    futures::stream,
    openraft::{
        error::{RPCError, RaftError},
        RaftMetrics,
    },
    serde::{Deserialize, Serialize},
    std::{
        error::Error as StdError,
        fmt,
        future::Future,
        io::Cursor,
        result::Result as StdResult,
        sync::Arc,
        time::Duration,
    },
    tokio_stream::wrappers::WatchStream,
};

mod network;
pub mod storage;
#[cfg(test)]
mod test;

/// Configuration of types used by this crate.
pub trait TypeConfig:
    Sized + Send + Sync + Debug + Clone + Copy + Default + Eq + PartialEq + Ord + PartialOrd + 'static
{
    /// State machine.
    type State: State<Self>;

    /// Application-specific request data passed to the state machine.
    type Change: openraft::AppData + Clone + Debug + PartialEq + Unpin;

    /// A Raft node's ID.
    type NodeId: openraft::NodeId + Unpin;

    /// Raft application level node data
    type Node: openraft::Node + Unpin;

    type AddMemberPayload: fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Unpin
        + Clone
        + Send
        + Sync
        + 'static;
}

/// [`Raft`] consensus algorithm.
#[async_trait]
pub trait Raft<C: TypeConfig, A: ApiType<C> = Api>: Clone + Send + Sync + 'static {
    /// Adds a member to the [`Raft`] network.
    async fn add_member(&self, req: AddMemberRequest<C>) -> AddMemberResult<C, A>;

    /// Removes a member from the [`Raft`] network.
    async fn remove_member(&self, req: RemoveMemberRequest<C>) -> RemoveMemberResult<C, A>;

    /// Proposes a change to the [`Raft`] state.
    async fn propose_change(&self, req: ProposeChangeRequest<C>) -> ProposeChangeResult<C, A>;

    /// Appends entries to the [`Raft`] log.
    async fn append_entries(&self, req: AppendEntriesRequest<C>) -> AppendEntriesResult<C, A>;

    /// Installs a new snapshot of the [`Raft`] state.
    async fn install_snapshot(&self, req: InstallSnapshotRequest<C>)
        -> InstallSnapshotResult<C, A>;

    /// Makes a candidate vote.
    async fn vote(&self, req: VoteRequest<C>) -> VoteResult<C, A>;
}

/// Raft log entry.
pub type LogEntry<C> = openraft::Entry<OpenRaft<C>>;

/// Result of [`State::apply`].
pub type ApplyResult<C> = StdResult<
    <<C as TypeConfig>::State as State<C>>::Ok,
    <<C as TypeConfig>::State as State<C>>::Error,
>;

/// Application-provided state machine implementation.
pub trait State<C: TypeConfig>: Default + Clone + Send + Sync + 'static {
    /// Successful result of [`State::apply`].
    type Ok: Default + Debug + Serialize + for<'de> Deserialize<'de> + Unpin + Send + Sync + 'static;

    /// Error result of [`State::apply`].
    type Error: Serialize
        + for<'de> Deserialize<'de>
        + std::error::Error
        + Unpin
        + Send
        + Sync
        + 'static;

    /// Applies [`LogEntry`] to this [`State`].
    fn apply(&mut self, entry: &LogEntry<C>) -> ApplyResult<C>;

    fn snapshot(&self) -> StdResult<storage::Snapshot<C>, impl StdError + 'static>;

    fn install_snapshot(
        &mut self,
        meta: &storage::SnapshotMeta<C>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StdResult<(), impl StdError + Send + 'static>;

    /// Returns the [`LogId`] of the last applied [`LogEntry`] to this
    /// [`State`] (if any).
    fn last_applied_log_id(&self) -> Option<LogId<C>>;

    /// Returns the [`StoredMembership`] of this [`State`].
    fn stored_membership(&self) -> &StoredMembership<C>;
}

/// Creates and starts a [`Raft`] task.
pub async fn new<C: TypeConfig, N: Network<C> + Clone, S: Storage<C> + Clone>(
    node_id: C::NodeId,
    config: Config,
    initial_members: Option<impl Iterator<Item = (C::NodeId, C::Node)>>,
    network: N,
    storage: S,
) -> Result<C, RaftImpl<C, N>, InitializeError<C>> {
    use openraft::error::InitializeError as E;

    let network_adapter = network::Adapter(network.clone());
    let storage_adapter = storage::Adapter::new(storage)
        .await
        .map_err(InitializeError::Storage)
        .map_err(Error::<C, _>::APIError)?;

    let (storage_adapter, state_machine) = OpenRaftStorageHack::new(storage_adapter);

    let openraft = openraft::Raft::new(
        node_id,
        Arc::new(config),
        network_adapter,
        storage_adapter,
        state_machine,
    )
    .await?;

    if let Some(members) = initial_members {
        let members = members.collect::<std::collections::BTreeMap<_, _>>();

        match openraft.initialize(members).await {
            // `NotAllowed` means the cluster is already running, good to go.
            Ok(_) | Err(RaftError::APIError(E::NotAllowed(_))) => {}
            Err(RaftError::Fatal(f)) => return Err(RaftError::Fatal(f)),
            Err(RaftError::APIError(E::NotInMembers(e))) => {
                return Err(RaftError::APIError(InitializeError::NotInMembers(e)));
            }
        };
    }

    Ok(RaftImpl {
        raft: openraft,
        network,
    })
}

/// Error of initializing a new [`Raft`] task via [`new`] fn.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InitializeError<C: TypeConfig> {
    /// [`Raft`] [`FatalError`].
    #[error(transparent)]
    Fatal(FatalError<C::NodeId>),

    /// [`Storage`] error.
    #[error(transparent)]
    Storage(storage::Error),

    /// The node isn't in the member config.
    #[error(transparent)]
    NotInMembers(#[from] NotInMembersError<C::NodeId, C::Node>),
}

/// Sum type of all [`Raft`] [`Api`] operation requests.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request<C: TypeConfig> {
    /// [`Raft::add_member`] request.
    AddMember(AddMemberRequest<C>),

    /// [`Raft::remove_member`] request.
    RemoveMember(RemoveMemberRequest<C>),

    /// [`Raft::propose_change`] request.
    ProposeChange(ProposeChangeRequest<C>),

    /// [`Raft::append_entries`] request.
    AppendEntries(AppendEntriesRequest<C>),

    /// [`Raft::install_snapshot`] request.
    InstallSnapshot(InstallSnapshotRequest<C>),

    /// [`Raft::vote`] request.
    Vote(VoteRequest<C>),
}

impl<C: TypeConfig> Request<C> {
    pub fn name(&self) -> &'static str {
        match self {
            Self::AddMember(_) => "add_member",
            Self::RemoveMember(_) => "remove_member",
            Self::ProposeChange(_) => "propose_change",
            Self::AppendEntries(_) => "append_entries",
            Self::InstallSnapshot(_) => "install_snapshot",
            Self::Vote(_) => "vote",
        }
    }
}

/// Sum type of all [`Raft`] [`Api`] operation results.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Response<C: TypeConfig> {
    /// [`Raft::add_member`] operation result.
    AddMember(AddMemberResult<C>),

    /// [`Raft::remove_member`] operation result.
    RemoveMember(RemoveMemberResult<C>),

    /// [`Raft::propose_change`] operation request.
    ProposeChange(ProposeChangeResult<C>),

    /// [`Raft::append_entries`] operation request.
    AppendEntries(AppendEntriesResult<C>),

    /// [`Raft::install_snapshot`] operation request.
    InstallSnapshot(InstallSnapshotResult<C>),

    /// [`Raft::vote`] operation request.
    Vote(VoteResult<C>),
}

// ----------------------- `Raft::add_member` operation types.

/// Request type of [`Raft::add_member`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddMemberRequest<C: TypeConfig> {
    /// ID of the node to add as a new member.
    pub node_id: C::NodeId,

    /// Application level node data.
    pub node: C::Node,

    /// Indicator whether the node to add should only be a learner.
    /// Learner-nodes do not participate in [`Raft`] voting and just get
    /// notified about the changes.
    pub learner_only: bool,

    #[serde(default)]
    pub payload: Option<C::AddMemberPayload>,
}

/// Response type of [`Raft::add_member`] operation.
pub type AddMemberResponse<C> = ClientWriteResponse<C>;

/// Non-[`FatalError`] of [`Raft::add_member`] operation.
pub type AddMemberFail<C> = ClientWriteFail<C>;

/// Error of [`Raft::add_member`] operation.
pub type AddMemberError<C, A = Api> = Error<C, AddMemberFail<C>, A>;

/// Error of [`Raft::add_member`] RPC.
pub type AddMemberRpcError<C> = AddMemberError<C, RpcApi>;

/// Result of [`Raft::add_member`] operation.
pub type AddMemberResult<C, A = Api> = Result<C, AddMemberResponse<C>, AddMemberFail<C>, A>;

/// Result of [`Raft::add_member`] RPC.
pub type AddMemberRpcResult<C> = AddMemberResult<C, RpcApi>;

// ----------------------- `Raft::remove_member` operation types.

/// Request type of [`Raft::remove_member`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoveMemberRequest<C: TypeConfig> {
    /// ID of the node to remove.
    pub node_id: C::NodeId,

    /// Indicator whether the node to remove is a learner.
    /// Voters and learners are managed separately, so it's important to specify
    /// this correctly.
    pub is_learner: bool,
}

/// Response type of [`Raft::remove_member`] operation.
pub type RemoveMemberResponse<C> = ClientWriteResponse<C>;

/// Non-[`FatalError`] of [`Raft::remove_member`] operation.
pub type RemoveMemberFail<C> = ClientWriteFail<C>;

/// Error of [`Raft::remove_member`] operation.
pub type RemoveMemberError<C, A = Api> = Error<C, RemoveMemberFail<C>, A>;

/// Error of [`Raft::remove_member`] RPC.
pub type RemoveMemberRpcError<C> = RemoveMemberError<C, RpcApi>;

/// Result of [`Raft::remove_member`] operation.
pub type RemoveMemberResult<C, A = Api> =
    Result<C, RemoveMemberResponse<C>, RemoveMemberFail<C>, A>;

/// Result of [`Raft::remove_member`] RPC.
pub type RemoveMemberRpcResult<C> = RemoveMemberResult<C, RpcApi>;

// ----------------------- `Raft::propose_change` operation types.

/// Request type of [`Raft::propose_change`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProposeChangeRequest<C: TypeConfig> {
    pub change: C::Change,
}

/// Response type of [`Raft::propose_change`] operation.
pub type ProposeChangeResponse<C> = ClientWriteResponse<C>;

/// Non-[`FatalError`] of [`Raft::propose_change`] operation.
pub type ProposeChangeFail<C> = ClientWriteFail<C>;

/// Error of [`Raft::propose_change`] operation.
pub type ProposeChangeError<C, A = Api> = Error<C, ProposeChangeFail<C>, A>;

/// Error of [`Raft::propose_change`] RPC.
pub type ProposeChangeRpcError<C> = ProposeChangeError<C, RpcApi>;

/// Result of [`Raft::propose_change`] operation.
pub type ProposeChangeResult<C, A = Api> =
    Result<C, ProposeChangeResponse<C>, ProposeChangeFail<C>, A>;

/// Result of [`Raft::propose_change`] RPC.
pub type ProposeChangeRpcResult<C> = ProposeChangeResult<C, RpcApi>;

// ----------------------- `Raft::append_entries` operation types.

/// Request type of [`Raft::append_entries`].
pub type AppendEntriesRequest<C> = raft::AppendEntriesRequest<OpenRaft<C>>;

/// Response type of [`Raft::append_entries`] operation.
pub type AppendEntriesResponse<C> = raft::AppendEntriesResponse<<C as TypeConfig>::NodeId>;

/// Non-[`FatalError`] of [`Raft::append_entries`] operation.
pub type AppendEntriesFail = Infallible;

/// Error of [`Raft::append_entries`] operation.
pub type AppendEntriesError<C, A = Api> = Error<C, AppendEntriesFail, A>;

/// Error of [`Raft::append_entries`] RPC.
pub type AppendEntriesRpcError<C> = AppendEntriesError<C, RpcApi>;

/// Result of [`Raft::append_entries`] operation.
pub type AppendEntriesResult<C, A = Api> =
    Result<C, AppendEntriesResponse<C>, AppendEntriesFail, A>;

/// Result of [`Raft::append_entries`] RPC.
pub type AppendEntriesRpcResult<C> = AppendEntriesResult<C, RpcApi>;

// ----------------------- `Raft::install_snapshot` operation types.

/// Request type of [`Raft::install_snapshot`].
pub type InstallSnapshotRequest<C> = raft::InstallSnapshotRequest<OpenRaft<C>>;

/// Response type of [`Raft::install_snapshot`] operation.
pub type InstallSnapshotResponse<C> = raft::InstallSnapshotResponse<<C as TypeConfig>::NodeId>;

/// Non-[`FatalError`] of [`Raft::install_snapshot`] operation.
pub type InstallSnapshotFail = openraft::error::InstallSnapshotError;

/// Error of [`Raft::install_snapshot`] operation.
pub type InstallSnapshotError<C, A = Api> = Error<C, InstallSnapshotFail, A>;

/// Error of [`Raft::install_snapshot`] RPC.
pub type InstallSnapshotRpcError<C> = InstallSnapshotError<C, RpcApi>;

/// Result of [`Raft::install_snapshot`] operation.
pub type InstallSnapshotResult<C, A = Api> =
    Result<C, InstallSnapshotResponse<C>, InstallSnapshotFail, A>;

/// Result of [`Raft::install_snapshot`] RPC.
pub type InstallSnapshotRpcResult<C> = InstallSnapshotResult<C, RpcApi>;

// ----------------------- `Raft::vote` operation types.

/// Request type of [`Raft::vote`].
pub type VoteRequest<C> = raft::VoteRequest<<C as TypeConfig>::NodeId>;

/// Response type of [`Raft::vote`] operation.
pub type VoteResponse<C> = raft::VoteResponse<<C as TypeConfig>::NodeId>;

/// Non-[`FatalError`] of [`Raft::vote`] operation.
pub type VoteFail = Infallible;

/// Error of [`Raft::vote`] operation.
pub type VoteError<C, A = Api> = Error<C, VoteFail, A>;

/// Error of [`Raft::vote`] RPC.
pub type VoteRpcError<C> = VoteError<C, RpcApi>;

/// Result of [`Raft::vote`] operation.
pub type VoteResult<C, A = Api> = Result<C, VoteResponse<C>, VoteFail, A>;

/// Result of [`Raft::vote`] RPC.
pub type VoteRpcResult<C> = VoteResult<C, RpcApi>;

/// Error type constructor based on the API type.
pub trait ApiType<C> {
    /// Type of the error.
    type ErrorType<E: StdError>;
}

/// Constructs error types for local API.
pub struct Api;

impl<C: TypeConfig> ApiType<C> for Api {
    type ErrorType<E: StdError> = openraft::error::RaftError<C::NodeId, E>;
}

/// Constructs error types for RPC API.
pub struct RpcApi;

impl<C: TypeConfig> ApiType<C> for RpcApi {
    type ErrorType<E: StdError> = openraft::error::RPCError<C::NodeId, C::Node, Error<C, E>>;
}

/// Generic error type of [`Raft`] API.
pub type Error<C, E = Infallible, A = Api> = <A as ApiType<C>>::ErrorType<E>;

/// Generic error type of [`Raft`] RPC API.
pub type RpcError<C, E = Infallible> = Error<C, E, RpcApi>;

/// Generic result type of [`Raft`] API.
pub type Result<C, T, E = Infallible, A = Api> = std::result::Result<T, Error<C, E, A>>;

/// Generic result type of [`Raft`] RPC API.
pub type RpcResult<C, T, E = Infallible> = Result<C, T, E, RpcApi>;

/// Non-[`FatalError`] of [`Raft::add_member`] or [`Raft::propose_change`]
/// operations.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum ClientWriteFail<C: TypeConfig> {
    /// Error of handling the operation locally.
    #[error(transparent)]
    Local(LocalClientWriteFail<C>),

    /// Error of handling the operation via RPC to the leader.
    #[error(transparent)]
    Rpc(RpcError<C, LocalClientWriteFail<C>>),
}

/// Non-[`FatalError`] of [`Raft::add_member`] or [`Raft::propose_change`]
/// operations handled locally.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum LocalClientWriteFail<C: TypeConfig> {
    /// Error of writing a change-membership entry.
    #[error(transparent)]
    ChangeMembershipError(ChangeMembershipError<C::NodeId>),

    /// Cluster doesn't have a leader.
    #[error("no leader")]
    NoLeader,

    /// Requestor is not authorized to perform the operation.
    #[error("unauthorized")]
    Unauthorized,

    #[error("{0}")]
    Other(String),
}

/// See [`openraft::StoredMembership`].
pub type StoredMembership<C> =
    openraft::StoredMembership<<C as TypeConfig>::NodeId, <C as TypeConfig>::Node>;

/// See [`openraft::Vote`].
pub type Vote<C> = openraft::Vote<<C as TypeConfig>::NodeId>;

/// See [`openraft::LogId`].
pub type LogId<C> = openraft::LogId<<C as TypeConfig>::NodeId>;

/// See [`openraft::ClientWriteResponse`].
pub type ClientWriteResponse<C> = raft::ClientWriteResponse<OpenRaft<C>>;

/// Result of [`Raft::append_entries`] or [`Raft::propose_change`] operations.
pub type ClientWriteResult<C, A = Api> = Result<C, ClientWriteResponse<C>, ClientWriteFail<C>, A>;

// `openraft` is currently in a process of splitting the `StateMachine` out of
// the `Storage`. But the new API is only partially ready. So they ask us to
// temporary use this type for both `Storage` and `StateMachine`.
type OpenRaftStorageHack<C, S> = openraft::storage::Adaptor<OpenRaft<C>, storage::Adapter<C, S>>;

type OpenRaftImpl<C> = openraft::Raft<OpenRaft<C>>;

/// Concrete type implementing [`Raft`].
pub struct RaftImpl<C: TypeConfig, N: Network<C>> {
    raft: OpenRaftImpl<C>,
    network: N,
}

impl<C, N> Clone for RaftImpl<C, N>
where
    C: TypeConfig,
    N: Network<C>,
{
    fn clone(&self) -> Self {
        Self {
            raft: self.raft.clone(),
            network: self.network.clone(),
        }
    }
}

type RaftClientWriteError<C> =
    openraft::error::ClientWriteError<<C as TypeConfig>::NodeId, <C as TypeConfig>::Node>;

/// Stream of [`Raft`] state updates.
pub type UpdatesStream<T> = stream::Map<WatchStream<Arc<T>>, fn(Arc<T>) -> T>;

impl<C, N> RaftImpl<C, N>
where
    C: TypeConfig,
    N: Network<C>,
{
    /// Tries to perform a local client write operation, if the local node isn't
    /// the leader falls back to calling the leader via RPC.
    async fn client_write<'a, F1, F2>(
        &'a self,
        local: impl FnOnce(&'a OpenRaftImpl<C>) -> F1,
        leader: impl FnOnce(N::Client) -> F2,
    ) -> Result<C, ClientWriteResponse<C>, ClientWriteFail<C>>
    where
        F1: Future<Output = Result<C, ClientWriteResponse<C>, RaftClientWriteError<C>>>,
        F2: Future<Output = RpcResult<C, ClientWriteResponse<C>, ClientWriteFail<C>>>,
    {
        // The error of this function can conceptually be infinitely recursive, so we do
        // some flattening magic here.

        use {
            ClientWriteFail::{Local, Rpc},
            LocalClientWriteFail::{ChangeMembershipError, NoLeader},
            RPCError::{Network, PayloadTooLarge, RemoteError as Remote, Timeout, Unreachable},
            RaftError::{APIError as Api, Fatal},
        };

        let fwd = match local(&self.raft).await {
            Ok(resp) => return Ok(resp),
            Err(Api(RaftClientWriteError::<C>::ForwardToLeader(fwd))) => fwd,
            Err(Api(RaftClientWriteError::<C>::ChangeMembershipError(cm))) => {
                return Err(Api(Local(ChangeMembershipError(cm))));
            }
            Err(Fatal(f)) => return Err(Fatal(f)),
        };

        let (Some(node_id), Some(node)) = (fwd.leader_id, fwd.leader_node) else {
            return Err(Api(Local(NoLeader)));
        };

        match leader(self.network.new_client(node_id, &node).await).await {
            Ok(resp) => Ok(resp),
            Err(Timeout(t)) => Err(Api(Rpc(Timeout(t)))),
            Err(Unreachable(u)) => Err(Api(Rpc(Unreachable(u)))),
            Err(Network(n)) => Err(Api(Rpc(Network(n)))),
            Err(PayloadTooLarge(e)) => Err(Api(Rpc(PayloadTooLarge(e)))),
            Err(Remote(e)) => match e.source {
                Fatal(f) => Err(Api(Rpc(Remote(RemoteError {
                    target: node_id,
                    target_node: Some(node),
                    source: Fatal(f),
                })))),
                Api(Local(l)) => Err(Api(Rpc(Remote(RemoteError {
                    target: node_id,
                    target_node: Some(node),
                    source: Api(l),
                })))),
                Api(Rpc(r)) => Err(Api(Rpc(r))),
            },
        }
    }

    /// Shuts down the [`Raft`] detached task.
    pub async fn shutdown(&self) {
        if let Err(err) = self.raft.shutdown().await {
            tracing::warn!(?err, "failed to join Raft task");
        }
    }

    /// Waits for [`Raft`] to initialize.
    pub async fn wait_init(&self) {
        let _ = self
            .raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| metrics.membership_config.nodes().count() > 0,
                "init",
            )
            .await;
    }

    pub fn metrics(&self) -> RaftMetrics<C::NodeId, C::Node> {
        self.raft.metrics().borrow().clone()
    }
}

#[async_trait]
impl<C, N> Raft<C> for RaftImpl<C, N>
where
    C: TypeConfig,
    N: Network<C>,
{
    async fn add_member(&self, req: AddMemberRequest<C>) -> AddMemberResult<C> {
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(req.node_id, req.node.clone());

        let is_member = self
            .metrics()
            .membership_config
            .nodes()
            .any(|(id, _)| id == &req.node_id);

        let change_members = if is_member {
            openraft::ChangeMembers::SetNodes(nodes)
        } else if req.learner_only {
            openraft::ChangeMembers::AddNodes(nodes)
        } else {
            openraft::ChangeMembers::AddVoters(nodes)
        };

        self.client_write(
            |local| local.change_membership(change_members, false),
            |leader| async move { leader.add_member(req).await },
        )
        .await
    }

    async fn remove_member(&self, req: RemoveMemberRequest<C>) -> RemoveMemberResult<C> {
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(req.node_id);

        let change_members = if req.is_learner {
            openraft::ChangeMembers::RemoveNodes(nodes)
        } else {
            openraft::ChangeMembers::RemoveVoters(nodes)
        };

        self.client_write(
            |local| local.change_membership(change_members, false),
            |leader| async move { leader.remove_member(req).await },
        )
        .await
    }

    async fn propose_change(&self, req: ProposeChangeRequest<C>) -> ProposeChangeResult<C> {
        let change = req.change.clone();

        self.client_write(
            |local| local.client_write(change),
            |leader| async move { leader.propose_change(req).await },
        )
        .await
    }

    async fn append_entries(&self, req: AppendEntriesRequest<C>) -> AppendEntriesResult<C> {
        self.raft.append_entries(req).await
    }

    async fn install_snapshot(&self, req: InstallSnapshotRequest<C>) -> InstallSnapshotResult<C> {
        self.raft.install_snapshot(req).await
    }

    async fn vote(&self, req: VoteRequest<C>) -> VoteResult<C> {
        self.raft.vote(req).await
    }
}

/// Generic `openraft` type adapter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct OpenRaft<C>(C);

impl<C: TypeConfig> openraft::RaftTypeConfig for OpenRaft<C> {
    type AsyncRuntime = openraft::TokioRuntime;
    type D = C::Change;
    type Entry = LogEntry<C>;
    type Node = C::Node;
    type NodeId = C::NodeId;
    type R = ApplyResult<C>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
}

impl<C: TypeConfig> TryFrom<Response<C>> for ClientWriteResult<C> {
    type Error = Response<C>;

    fn try_from(resp: Response<C>) -> StdResult<Self, Self::Error> {
        match resp {
            Response::AddMember(resp) | Response::ProposeChange(resp) => Ok(resp),
            _ => Err(resp),
        }
    }
}

impl<C: TypeConfig> TryFrom<Response<C>> for AppendEntriesResult<C> {
    type Error = Response<C>;

    fn try_from(resp: Response<C>) -> StdResult<Self, Self::Error> {
        match resp {
            Response::AppendEntries(resp) => Ok(resp),
            _ => Err(resp),
        }
    }
}

impl<C: TypeConfig> TryFrom<Response<C>> for InstallSnapshotResult<C> {
    type Error = Response<C>;

    fn try_from(resp: Response<C>) -> StdResult<Self, Self::Error> {
        match resp {
            Response::InstallSnapshot(resp) => Ok(resp),
            _ => Err(resp),
        }
    }
}

impl<C: TypeConfig> TryFrom<Response<C>> for VoteResult<C> {
    type Error = Response<C>;

    fn try_from(resp: Response<C>) -> StdResult<Self, Self::Error> {
        match resp {
            Response::Vote(resp) => Ok(resp),
            _ => Err(resp),
        }
    }
}
