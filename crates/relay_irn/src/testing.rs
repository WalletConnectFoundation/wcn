use {
    crate::{
        cluster::NodeOperationMode,
        migration,
        network::HandleRequest,
        replication::{
            CoordinatorResponse,
            DispatchReplicated,
            Read,
            ReplicatableOperation,
            ReplicatableOperationOutput,
            Write,
        },
        AlreadyRunning,
        BootingMigrations,
        Consensus,
        LeavingMigrations,
        Network,
        NodeOpts,
        PeerId,
        ShutdownReason,
    },
    derive_more::Deref,
    futures::{stream, StreamExt},
    once_cell::sync::Lazy,
    std::{
        any::{Any, TypeId},
        collections::HashMap,
        fmt,
        pin::pin,
        sync::{Arc, Mutex},
        time::Duration,
    },
};

static CLUSTERS: Lazy<Mutex<HashMap<ClusterKey, Box<dyn Any + Send + Sync + 'static>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct Cluster<S: Setup> {
    config: ClusterConfig,
    nodes: Arc<Mutex<Vec<Node<S>>>>,
}

impl<S: Setup> Clone for Cluster<S> {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            config: self.config.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClusterConfig {
    pub num_nodes: usize,
    pub num_groups: usize,
    pub node_ops: NodeOpts,

    /// All tests
    pub serial: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ClusterKey {
    type_id: TypeId,
    config: ClusterConfig,
}

impl<S: Setup> Cluster<S>
where
    migration::Manager<S::Network, S::Storage>: BootingMigrations + LeavingMigrations,
{
    pub fn get_or_bootstrap(cfg: ClusterConfig) -> Self {
        assert!(cfg.num_nodes >= cfg.num_groups);

        let key = ClusterKey {
            type_id: TypeId::of::<Self>(),
            config: cfg.clone(),
        };

        CLUSTERS
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(move || Box::new(Self::bootstrap(cfg)))
            .downcast_ref::<Self>()
            .unwrap()
            .clone()
    }

    fn bootstrap(cfg: ClusterConfig) -> Self {
        let num_nodes = cfg.num_nodes;

        let cluster = Self {
            nodes: Arc::new(Mutex::new(Vec::new())),
            config: cfg,
        };

        for _ in 0..num_nodes {
            cluster.bootup_node();
        }

        cluster
    }

    pub async fn wait_op_mode(&self, mode: NodeOperationMode, node_id: Option<PeerId>) {
        tokio::time::timeout(Duration::from_secs(60), async {
            for node in self.nodes.lock().unwrap().clone() {
                let mut changes = pin!(node.consensus.changes());

                'out: loop {
                    let view = changes.next().await.unwrap();
                    for (_, node) in view.nodes() {
                        match node_id {
                            Some(id) if node.peer_id == id && node.mode == mode => return,
                            Some(id) if node.peer_id == id => continue 'out,
                            Some(_) => continue,
                            None if node.mode != mode => continue 'out,
                            None => {}
                        }
                    }

                    // If we didn't find `Left` node it may be already deleted from the cluster
                    // view.
                    if node_id.is_some() && mode == NodeOperationMode::Left {
                        return;
                    }
                }
            }
        })
        .await
        .unwrap();
    }

    pub fn node(&self, idx: usize) -> Node<S> {
        self.nodes.lock().unwrap().get(idx).unwrap().clone()
    }

    pub fn bootup_node(&self) {
        let mut nodes = self.nodes.lock().unwrap();
        let idx = nodes.len();

        let mut node_id = PeerId::random();
        node_id.group = (idx % self.config.num_groups) as u16;

        let consensus = S::consensus(&self.config, idx, node_id);
        let network = S::network(&self.config, idx, node_id);
        let storage = S::storage(&self.config, idx, node_id);

        let node = crate::Node::new(node_id, self.config.node_ops, consensus, network, storage);
        nodes.push(Node {
            inner: node.clone(),
            task_handle: Arc::new(Mutex::new(Some(tokio::spawn(
                async move { node.run().await },
            )))),
        });
    }

    pub async fn shutdown_node(&self, idx: usize, reason: ShutdownReason) {
        let node = self.node(idx);
        node.shutdown(reason).unwrap();
        node.task_handle
            .lock()
            .unwrap()
            .take()
            .expect("already shutdown")
            .await
            .expect("join node task")
            .unwrap();
    }

    pub async fn decommission_node(&self, idx: usize) {
        self.shutdown_node(idx, ShutdownReason::Decommission).await;
        self.nodes.lock().unwrap().remove(idx);
    }

    /// Test of Theseus. If all nodes in the cluster are replaced
    /// ~~is it the same cluster?~~ does it still possess all the data?
    pub async fn full_node_rotation<SE: fmt::Debug, NE: fmt::Debug>(&self)
    where
        crate::Node<S::Consensus, S::Network, S::Storage>: HandleRequest<
                DispatchReplicated<S::ReadOperation>,
                Response = CoordinatorResponse<ReadOutput<S>, SE, NE>,
            > + HandleRequest<
                DispatchReplicated<S::WriteOperation>,
                Response = CoordinatorResponse<WriteOutput<S>, SE, NE>,
            >,
        ReadOutput<S>: fmt::Debug + PartialEq,
    {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 10;

        let nodes_num = self.config.num_nodes;

        self.wait_op_mode(NodeOperationMode::Normal, None).await;

        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async {
                let assert = S::gen_write_read_assert(None);

                self.node(0)
                    .handle_request(DispatchReplicated {
                        operation: assert.write,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                ReadAssert::<S> {
                    operation: assert.read,
                    expected_output: assert.expected_output,
                }
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        for _ in 0..nodes_num {
            // spin up new node and wait for the cluster to become `Normal`
            self.bootup_node();
            self.wait_op_mode(NodeOperationMode::Normal, None).await;

            // decomission a node and wait for it to become `Left`
            let id = self.node(0).id;
            self.decommission_node(0).await;
            self.wait_op_mode(NodeOperationMode::Left, Some(id)).await;
        }

        let node = &self.node(0);
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = node
                    .handle_request(DispatchReplicated {
                        operation: assert.operation,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                usize::from(assert.expected_output != output)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    pub async fn restart_then_decommission<SE: fmt::Debug, NE: fmt::Debug>(&self) {}
}

#[derive(Clone, Deref)]
pub struct Node<S: Setup> {
    #[deref]
    inner: crate::Node<S::Consensus, S::Network, S::Storage>,
    task_handle:
        Arc<Mutex<Option<tokio::task::JoinHandle<Result<ShutdownReason, AlreadyRunning>>>>>,
}

impl<S: Setup> Node<S> {
    pub async fn wait_op_mode(&self, peer_id: PeerId, mode: NodeOperationMode) {
        tokio::time::timeout(Duration::from_secs(60), async {
            let mut changes = pin!(self.consensus.changes());

            loop {
                let view = changes.next().await.unwrap();
                match view.nodes().get(&peer_id) {
                    Some(n) if n.mode == mode => return,
                    None if mode == NodeOperationMode::Left => return,
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
    }
}

pub trait Setup: Sized + Clone + Copy + Send + Sync + 'static {
    type Consensus: Consensus;
    type Network: Network;
    type Storage: Clone + Send + Sync + 'static;

    type ReadOperation: ReplicatableOperation<Type = Read>;
    type WriteOperation: ReplicatableOperation<Type = Write>;
    type RemoveOperation: ReplicatableOperation<Type = Write>;

    fn consensus(
        cluster_config: &ClusterConfig,
        node_idx: usize,
        node_id: PeerId,
    ) -> Self::Consensus;

    fn network(cluster_config: &ClusterConfig, node_idx: usize, node_id: PeerId) -> Self::Network;
    fn storage(cluster_config: &ClusterConfig, node_idx: usize, node_id: PeerId) -> Self::Storage;

    fn gen_write_read_assert(seed: Option<u64>) -> WriteReadAssert<Self>;
    fn gen_write_assert(seed: Option<u64>) -> WriteAssert<Self>;
    fn gen_read_assert(seed: Option<u64>) -> ReadAssert<Self>;
    fn gen_empty_read_assert(seed: Option<u64>) -> ReadAssert<Self>;
    fn gen_remove_assert(seed: Option<u64>) -> RemoveAssert<Self>;
}

/// Test case that performs the following steps:
/// - executes write operation, asserting no errors occurred
/// - executes read operation, asserting that output matches the expected one
pub struct WriteReadAssert<S: Setup> {
    pub write: S::WriteOperation,
    pub read: S::ReadOperation,
    pub expected_output: <S::ReadOperation as ReplicatableOperation>::Output,
}

pub struct WriteAssert<S: Setup> {
    pub operation: S::WriteOperation,
    pub expected_output: <S::WriteOperation as ReplicatableOperation>::Output,
}

pub struct ReadAssert<S: Setup> {
    pub operation: S::ReadOperation,
    pub expected_output: <S::ReadOperation as ReplicatableOperation>::Output,
}

pub struct RemoveAssert<S: Setup> {
    pub operation: S::RemoveOperation,
    pub expected_output: <S::RemoveOperation as ReplicatableOperation>::Output,
}

pub type ReadOutput<S> = ReplicatableOperationOutput<<S as Setup>::ReadOperation>;
pub type WriteOutput<S> = ReplicatableOperationOutput<<S as Setup>::WriteOperation>;
pub type RemoveOutput<S> = ReplicatableOperationOutput<<S as Setup>::RemoveOperation>;
