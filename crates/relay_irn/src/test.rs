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
    itertools::Itertools,
    libp2p::Multiaddr,
    rand::seq::IteratorRandom,
    std::{collections::HashMap, fmt, pin::pin, time::Duration},
    tap::Pipe,
};

pub struct Cluster<C: Context> {
    config: ClusterConfig,
    nodes: HashMap<PeerId, NodeHandle<C>>,

    bootnode_addrs: HashMap<PeerId, Multiaddr>,

    test_context: C,
    next_port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConfig {
    pub num_bootnodes: usize,
    pub num_groups: usize,
    pub node_opts: NodeOpts,
}

impl<C: Context, SE: fmt::Debug, NE: fmt::Debug> Cluster<C>
where
    migration::Manager<C::Network, C::Storage>: BootingMigrations + LeavingMigrations,
    crate::Node<C::Consensus, C::Network, C::Storage>: HandleRequest<
            DispatchReplicated<C::ReadOperation>,
            Response = CoordinatorResponse<ReadOutput<C>, SE, NE>,
        > + HandleRequest<
            DispatchReplicated<C::WriteOperation>,
            Response = CoordinatorResponse<WriteOutput<C>, SE, NE>,
        >,
    ReadOutput<C>: fmt::Debug + PartialEq,
{
    pub fn new(ctx: C, cfg: ClusterConfig) -> Self {
        let mut cluster = Self {
            nodes: HashMap::new(),
            config: cfg.clone(),
            bootnode_addrs: HashMap::new(),
            test_context: ctx,
            next_port: 42000,
        };

        cluster.bootnode_addrs = (0..cfg.num_bootnodes)
            .into_iter()
            .map(|_| cluster.next_node_identity())
            .collect();

        for idt in cluster.bootnode_addrs.clone() {
            cluster.bootup_node(Some(idt));
        }

        cluster
    }

    pub async fn run_test_suite(&mut self) {
        // wait for all nodes in the cluster to become `Normal`
        self.wait_op_mode(NodeOperationMode::Normal, None).await;

        self.full_node_rotation().await;
        self.restart_then_decommission().await;
    }

    /// Test of Theseus. If all nodes in the cluster are replaced
    /// ~~is it the same cluster?~~ does it still possess all the data?
    async fn full_node_rotation(&mut self) {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 10;

        let this = &self;
        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|idx| async move {
                let assert = C::gen_write_read_assert(Some(idx as u64));

                this.random_node()
                    .handle_request(DispatchReplicated {
                        operation: assert.write,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                ReadAssert::<C> {
                    operation: assert.read,
                    expected_output: assert.expected_output,
                }
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        for id in self.nodes.keys().copied().collect_vec() {
            // spin up new node and wait for the cluster to become `Normal`
            let new_id = self.bootup_node(None);
            self.wait_op_mode(NodeOperationMode::Normal, Some(new_id))
                .await;

            // decomission a node and wait for it to become `Left`
            self.shutdown_node(&id, ShutdownReason::Decommission).await;
            self.wait_op_mode(NodeOperationMode::Left, Some(id)).await;

            for n in self.nodes.values() {
                dbg!(&n.storage);
            }
        }

        // Although our copy of `ClusterView` is already in the right state, it may not
        // be the case for all FSMs in the cluster.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let node = &self.random_node();
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = node
                    .handle_request(DispatchReplicated {
                        operation: assert.operation,
                    })
                    .await
                    .expect("coordinator")
                    .expect("replica");

                assert_eq!(assert.expected_output, output);

                usize::from(assert.expected_output != output)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    /// Restart a node then decommission the same node. We had an FSM bug
    /// related to this.
    async fn restart_then_decommission(&mut self) {
        let (id, addr) = self.random_node().pipe(|n| (n.id, n.addr.clone()));

        self.shutdown_node(&id, ShutdownReason::Restart).await;
        self.wait_op_mode(NodeOperationMode::Restarting, Some(id))
            .await;

        self.bootup_node(Some((id, addr)));
        self.wait_op_mode(NodeOperationMode::Normal, Some(id)).await;

        panic!();

        self.shutdown_node(&id, ShutdownReason::Decommission).await;
        self.wait_op_mode(NodeOperationMode::Left, Some(id)).await;
    }

    async fn wait_op_mode(&self, mode: NodeOperationMode, node_id: Option<PeerId>) {
        tokio::time::timeout(Duration::from_secs(60), async {
            for node in self.nodes.values() {
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
                    if node_id.is_none() || mode == NodeOperationMode::Left {
                        return;
                    }
                }
            }
        })
        .await
        .unwrap();
    }

    fn random_node(&self) -> &NodeHandle<C> {
        self.nodes.values().choose(&mut rand::thread_rng()).unwrap()
    }

    fn node(&self, id: &PeerId) -> &NodeHandle<C> {
        self.nodes.get(id).unwrap()
    }

    fn node_mut(&mut self, id: &PeerId) -> &mut NodeHandle<C> {
        self.nodes.get_mut(&id).unwrap()
    }

    fn next_node_identity(&mut self) -> (PeerId, Multiaddr) {
        let port = self.next_port;
        self.next_port += 1;

        let addr = format!("/ip4/127.0.0.1/udp/{port}/quic-v1")
            .parse()
            .unwrap();

        let mut id = PeerId::random();
        id.group = port % self.config.num_groups as u16 + 1;

        (id, addr)
    }

    fn create_node(
        &mut self,
        id: PeerId,
        addr: Multiaddr,
        peers: &HashMap<PeerId, Multiaddr>,
    ) -> Node<C> {
        let consensus = self.test_context.new_consensus(id);
        let network = self.test_context.new_network(id, addr.clone(), peers);
        let storage = self.test_context.new_storage(id);

        let node = crate::Node::new(id, self.config.node_opts, consensus, network, storage);
        self.test_context.node_created(&node);
        node
    }

    fn bootup_node(&mut self, identity: Option<(PeerId, Multiaddr)>) -> PeerId {
        let (id, addr) = identity.unwrap_or_else(|| self.next_node_identity());
        let node = self.create_node(id, addr.clone(), &self.bootnode_addrs.clone());

        self.nodes.insert(id, NodeHandle {
            inner: node.clone(),
            addr,
            task_handle: Some(tokio::spawn(async move { node.run().await.unwrap() })),
        });

        id
    }

    async fn shutdown_node(&mut self, id: &PeerId, reason: ShutdownReason) {
        let node = self.node_mut(id);
        node.shutdown(reason).unwrap();
        node.task_handle
            .take()
            .expect("already shutdown")
            .await
            .expect("join node task");
        self.nodes.remove(id);
    }
}

pub type Node<S> =
    crate::Node<<S as Context>::Consensus, <S as Context>::Network, <S as Context>::Storage>;

#[derive(Deref)]
pub struct NodeHandle<S: Context> {
    #[deref]
    inner: Node<S>,
    addr: Multiaddr,
    task_handle: Option<tokio::task::JoinHandle<ShutdownReason>>,
}

impl<C: Context> NodeHandle<C> {
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

pub trait Context: Sized + Send + Sync + 'static {
    type Consensus: Consensus;
    type Network: Network;
    type Storage: fmt::Debug + Clone + Send + Sync + 'static;

    type ReadOperation: ReplicatableOperation<Type = Read>;
    type WriteOperation: ReplicatableOperation<Type = Write>;
    type RemoveOperation: ReplicatableOperation<Type = Write>;

    fn new_consensus(&mut self, node_id: PeerId) -> Self::Consensus;
    fn new_storage(&mut self, node_id: PeerId) -> Self::Storage;
    fn new_network(
        &mut self,
        node_id: PeerId,
        mutliaddr: Multiaddr,
        peers: &HashMap<PeerId, Multiaddr>,
    ) -> Self::Network;

    fn node_created(&mut self, node: &Node<Self>);

    fn gen_write_read_assert(seed: Option<u64>) -> WriteReadAssert<Self>;
    fn gen_write_assert(seed: Option<u64>) -> WriteAssert<Self>;
    fn gen_read_assert(seed: Option<u64>) -> ReadAssert<Self>;
    fn gen_empty_read_assert(seed: Option<u64>) -> ReadAssert<Self>;
    fn gen_remove_assert(seed: Option<u64>) -> RemoveAssert<Self>;
}

/// Test case that performs the following steps:
/// - executes write operation, asserting no errors occurred
/// - executes read operation, asserting that output matches the expected one
pub struct WriteReadAssert<S: Context> {
    pub write: S::WriteOperation,
    pub read: S::ReadOperation,
    pub expected_output: <S::ReadOperation as ReplicatableOperation>::Output,
}

pub struct WriteAssert<S: Context> {
    pub operation: S::WriteOperation,
    pub expected_output: <S::WriteOperation as ReplicatableOperation>::Output,
}

pub struct ReadAssert<S: Context> {
    pub operation: S::ReadOperation,
    pub expected_output: <S::ReadOperation as ReplicatableOperation>::Output,
}

pub struct RemoveAssert<S: Context> {
    pub operation: S::RemoveOperation,
    pub expected_output: <S::RemoveOperation as ReplicatableOperation>::Output,
}

pub type ReadOutput<S> = ReplicatableOperationOutput<<S as Context>::ReadOperation>;
pub type WriteOutput<S> = ReplicatableOperationOutput<<S as Context>::WriteOperation>;
pub type RemoveOutput<S> = ReplicatableOperationOutput<<S as Context>::RemoveOperation>;
