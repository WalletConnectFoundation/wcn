use {
    crate::{
        storage::{self, test::Storage as TestStorage, Snapshot, SnapshotMeta},
        AddMemberRequest,
        AddMemberRpcResult,
        AppendEntriesRequest,
        AppendEntriesRpcResult,
        Config,
        InstallSnapshotRequest,
        InstallSnapshotRpcResult,
        LogEntry,
        LogEntryPayload,
        LogId,
        Network,
        ProposeChangeRequest,
        ProposeChangeRpcResult,
        Raft,
        RaftImpl,
        RemoteError,
        RemoveMemberRequest,
        RemoveMemberRpcResult,
        Request,
        Response,
        RpcApi,
        RpcError,
        StoredMembership,
        TypeConfig,
        VoteRequest,
        VoteRpcResult,
    },
    async_trait::async_trait,
    futures::TryFutureExt,
    serde::{Deserialize, Serialize},
    std::{
        convert::Infallible,
        error::Error as StdError,
        future::Future,
        io::Cursor,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot},
        time::sleep,
    },
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct C;

impl TypeConfig for C {
    type Change = Change;
    type State = State<Self>;
    type Node = ();
    type NodeId = u64;
    type AddMemberPayload = ();
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct State<Cfg: TypeConfig = C> {
    pub counter: u64,

    pub last_applied_log: Option<LogId<Cfg>>,

    pub membership: StoredMembership<Cfg>,
}

impl crate::State<C> for State {
    type Ok = ();
    type Error = DummyError;

    /// Applies [`LogEntry`] to this [`State`].
    fn apply(&mut self, entry: &LogEntry<C>) -> Result<(), DummyError> {
        match &entry.payload {
            LogEntryPayload::Blank => {}
            LogEntryPayload::Normal(Change::Incr) => self.counter += 1,
            LogEntryPayload::Normal(Change::Decr) => self.counter -= 1,
            LogEntryPayload::Membership(membership) => {
                self.membership =
                    StoredMembership::<C>::new(Some(entry.log_id), membership.clone());
            }
        }

        self.last_applied_log = Some(entry.log_id);

        Ok(())
    }

    fn snapshot(&self) -> Result<storage::Snapshot<C>, impl StdError + 'static> {
        let last_log_id = self.last_applied_log_id();
        let meta = SnapshotMeta::<C> {
            last_log_id,
            last_membership: self.stored_membership().clone(),
            snapshot_id: last_log_id.map(|id| id.to_string()).unwrap_or_default(),
        };

        Ok::<_, Infallible>(Snapshot::<C> {
            meta,
            snapshot: Box::new(Cursor::new(self.counter.to_be_bytes().into())),
        })
    }

    fn install_snapshot(
        &mut self,
        meta: &storage::SnapshotMeta<C>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), impl StdError + Send + 'static> {
        let data = snapshot.into_inner();
        assert!(data.len() == 8);
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&data);

        self.counter = u64::from_be_bytes(bytes);
        self.last_applied_log = meta.last_log_id;
        self.membership = meta.last_membership.clone();

        Ok::<_, Infallible>(())
    }

    /// Returns the [`LogId`] of the last applied [`LogEntry`] to this
    /// [`State`] (if any).
    fn last_applied_log_id(&self) -> Option<LogId<C>> {
        self.last_applied_log
    }

    /// Returns the [`StoredMembership`] of this [`State`].
    fn stored_membership(&self) -> &StoredMembership<C> {
        &self.membership
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Change {
    Incr,
    Decr,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
#[error("dummy")]
pub struct DummyError;

#[tokio::test]
async fn cluster() {
    let network = TestNetwork::new();

    cluster_suite(
        [
            (1, (), network.clone(), TestStorage::default()),
            (2, (), network.clone(), TestStorage::default()),
            (3, (), network.clone(), TestStorage::default()),
            (4, (), network.clone(), TestStorage::default()),
            (5, (), network.clone(), TestStorage::default()),
            (6, (), network.clone(), TestStorage::default()),
        ],
        network.clone(),
    )
    .await
}

async fn cluster_suite<Sp: ServerSpawner<C> + Clone>(
    // 5 members + 1 learner.
    nodes: [(u64, (), TestNetwork, TestStorage); 6],
    server_spawner: Sp,
) {
    let members = nodes.iter().take(5).map(|(id, node, ..)| (*id, *node));

    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let run_node = |idx: usize| {
        let node_id = nodes[idx].0;
        // make only first 5 nodes to be bootstrap nodes
        let members = (idx < 5).then_some(members.clone());
        let config = config.clone();
        let network = nodes[idx].2.clone();
        let storage = nodes[idx].3.clone();
        let state = storage.state.clone();
        let sp = server_spawner.clone();

        async move {
            crate::new(node_id, config, members, network, storage.clone())
                .map_ok(|raft| {
                    sp.spawn_server(node_id, raft.clone());
                    TestNode { raft, state }
                })
                .await
        }
    };

    let ch_req = |change| ProposeChangeRequest { change };

    let n1 = run_node(0).await.unwrap();
    let n2 = run_node(1).await.unwrap();

    // quorum isn't formed yet (2/5)
    assert!(n1.raft.propose_change(ch_req(Change::Incr)).await.is_err());

    let n3 = run_node(2).await.unwrap();

    // wait for leader to be elected.
    sleep(Duration::from_millis(200)).await;

    n1.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(1).await;
    n2.assert_state(1).await;
    n3.assert_state(1).await;

    n2.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(2).await;
    n2.assert_state(2).await;
    n3.assert_state(2).await;

    n3.raft.propose_change(ch_req(Change::Decr)).await.unwrap();

    n1.assert_state(1).await;
    n2.assert_state(1).await;
    n3.assert_state(1).await;

    let n4 = run_node(3).await.unwrap();
    n4.assert_state(1).await;

    n4.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(2).await;
    n2.assert_state(2).await;
    n3.assert_state(2).await;
    n4.assert_state(2).await;

    // lets assume member 5 is dead

    // We can run a new learner node without errors, but it won't receive any state
    // updates until we explicitly add it to the cluster.
    let n6 = run_node(5).await.unwrap();

    // add learner

    let req = AddMemberRequest {
        node_id: nodes[5].0,
        node: nodes[5].1,
        learner_only: true,
        payload: None,
    };
    n4.raft.add_member(req).await.unwrap();

    // now learner can see the state
    n6.assert_state(2).await;
    // and can propose changes (through the leader)
    n6.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(3).await;
    n2.assert_state(3).await;
    n3.assert_state(3).await;
    n4.assert_state(3).await;
    n6.assert_state(3).await;

    // remove learner

    let req = RemoveMemberRequest {
        node_id: nodes[5].0,
        is_learner: true,
    };
    n3.raft.remove_member(req).await.unwrap();

    // add member

    let req = AddMemberRequest {
        node_id: nodes[5].0,
        node: nodes[5].1,
        learner_only: false,
        payload: None,
    };
    n2.raft.add_member(req).await.unwrap();

    n6.assert_state(3).await;
    n6.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(4).await;
    n2.assert_state(4).await;
    n3.assert_state(4).await;
    n4.assert_state(4).await;
    n6.assert_state(4).await;

    // remove member

    let req = RemoveMemberRequest {
        node_id: nodes[5].0,
        is_learner: false,
    };
    n1.raft.remove_member(req).await.unwrap();

    n6.raft.propose_change(ch_req(Change::Incr)).await.unwrap();

    n1.assert_state(5).await;
    n2.assert_state(5).await;
    n3.assert_state(5).await;
    n4.assert_state(5).await;
}

struct TestNode {
    raft: RaftImpl<C, TestNetwork>,
    state: Arc<std::sync::Mutex<Option<State>>>,
}

impl TestNode {
    async fn assert_state(&self, counter: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = self.state.lock().unwrap().clone();
                eprintln!("{state:?}");
                if state.map(|s| s.counter) == Some(counter) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .unwrap()
    }
}

#[derive(Debug)]
struct Rpc {
    request: Request<C>,
    response: oneshot::Sender<Response<C>>,
}

impl Rpc {
    fn new(request: Request<C>) -> (oneshot::Receiver<Response<C>>, Self) {
        let (tx, rx) = oneshot::channel();
        let rpc = Self {
            request,
            response: tx,
        };

        (rx, rpc)
    }
}

pub trait ServerSpawner<C: TypeConfig> {
    fn spawn_server(&self, node_id: C::NodeId, raft: impl Raft<C>);
}

#[derive(Clone, Debug)]
struct TestNetwork {
    nodes: Arc<Mutex<[NodeChannels; 6]>>,
}

impl TestNetwork {
    fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new([
                NodeChannels::new(),
                NodeChannels::new(),
                NodeChannels::new(),
                NodeChannels::new(),
                NodeChannels::new(),
                NodeChannels::new(),
            ])),
        }
    }
}

#[derive(Debug)]
struct NodeChannels {
    sender: mpsc::Sender<Rpc>,
    receiver: Option<mpsc::Receiver<Rpc>>,
}

impl NodeChannels {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(16);
        Self {
            sender: tx,
            receiver: Some(rx),
        }
    }
}

impl ServerSpawner<C> for TestNetwork {
    fn spawn_server(&self, node_id: u64, raft: impl Raft<C>) {
        let this = self.clone();

        tokio::spawn(async move {
            let mut rx = this.nodes.lock().unwrap()[(node_id - 1) as usize]
                .receiver
                .take()
                .unwrap();

            while let Some(rpc) = rx.recv().await {
                let resp = match rpc.request {
                    Request::AddMember(req) => Response::AddMember(raft.add_member(req).await),
                    Request::RemoveMember(req) => {
                        Response::RemoveMember(raft.remove_member(req).await)
                    }
                    Request::ProposeChange(req) => {
                        Response::ProposeChange(raft.propose_change(req).await)
                    }
                    Request::AppendEntries(req) => {
                        Response::AppendEntries(raft.append_entries(req).await)
                    }
                    Request::InstallSnapshot(req) => {
                        Response::InstallSnapshot(raft.install_snapshot(req).await)
                    }
                    Request::Vote(req) => Response::Vote(raft.vote(req).await),
                };

                rpc.response.send(resp).unwrap();
            }
        });
    }
}

#[async_trait]
impl Network<C> for TestNetwork {
    type Client = Client;

    fn new_client(&self, target: u64, _node: &()) -> Self::Client {
        Client {
            node_id: target,
            sender: self.nodes.lock().unwrap()[(target - 1) as usize]
                .sender
                .clone(),
        }
    }
}

#[derive(Clone)]
struct Client {
    node_id: u64,
    sender: mpsc::Sender<Rpc>,
}

impl Client {
    async fn send_rpc(&self, req: Request<C>) -> Response<C> {
        let (resp_rx, rpc) = Rpc::new(req);
        self.sender.send(rpc).await.unwrap();
        resp_rx.await.unwrap()
    }
}

impl Raft<C, RpcApi> for Client {
    fn add_member(
        &self,
        req: AddMemberRequest<C>,
    ) -> impl Future<Output = AddMemberRpcResult<C>> + Send {
        async move {
            match self.send_rpc(Request::AddMember(req)).await {
                Response::AddMember(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }

    fn remove_member(
        &self,
        req: RemoveMemberRequest<C>,
    ) -> impl Future<Output = RemoveMemberRpcResult<C>> {
        async move {
            match self.send_rpc(Request::RemoveMember(req)).await {
                Response::RemoveMember(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }

    fn propose_change(
        &self,
        req: ProposeChangeRequest<C>,
    ) -> impl Future<Output = ProposeChangeRpcResult<C>> + Send {
        async move {
            match self.send_rpc(Request::ProposeChange(req)).await {
                Response::ProposeChange(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }

    fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> impl Future<Output = AppendEntriesRpcResult<C>> + Send {
        async move {
            match self.send_rpc(Request::AppendEntries(req)).await {
                Response::AppendEntries(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }

    fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> impl Future<Output = InstallSnapshotRpcResult<C>> {
        async move {
            match self.send_rpc(Request::InstallSnapshot(req)).await {
                Response::InstallSnapshot(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }

    fn vote(&self, req: VoteRequest<C>) -> impl Future<Output = VoteRpcResult<C>> + Send {
        async move {
            match self.send_rpc(Request::Vote(req)).await {
                Response::Vote(resp) => resp
                    .map_err(|e| RpcError::<C, _>::RemoteError(RemoteError::new(self.node_id, e))),
                _ => unreachable!(),
            }
        }
    }
}
