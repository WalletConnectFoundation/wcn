#![allow(async_fn_in_trait)]

use {
    crate::{
        cluster::{self, node, Node as _},
        fsm,
        replication::{self, CoordinatorError, NoRepair, StorageOperation},
        Consensus,
        NodeOpts,
    },
    derive_more::Deref,
    futures::{stream, StreamExt},
    itertools::Itertools,
    libp2p::PeerId,
    rand::seq::IteratorRandom,
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasher,
        time::Duration,
    },
};

pub struct Cluster<C: Context> {
    config: ClusterConfig,
    nodes: HashMap<node::Id<C::Node>, NodeHandle<C>>,

    test_context: C,

    // next_port: u16,
    // node_count: u16,

    // expected_view: ClusterView,
    authorized_client: libp2p::PeerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConfig {
    pub num_bootnodes: usize,
    pub node_opts: NodeOpts,
}

#[allow(clippy::trait_duplication_in_bounds)] // false positive
impl<C: Context> Cluster<C>
// where
//     migration::Manager<C::Network, C::Storage>: BootingMigrations + LeavingMigrations,
//     crate::Node<C::Consensus, C::Network, C::Storage>: Coordinator<C::ReadOperation, Output =
// ReadOutput<C>, StorageError = SE, NetworkError = NE>
//         + Coordinator< C::WriteOperation, Output = WriteOutput<C>, StorageError = SE,
//           NetworkError = NE,
//         > + Replica<C::ReadOperation, Output = ReadOutput<C>, StorageError = SE>
//         + Replica<C::WriteOperation, Output = WriteOutput<C>, StorageError = SE>,
//     ReadOutput<C>: fmt::Debug + PartialEq,
{
    pub async fn new(ctx: C, cfg: ClusterConfig) -> Self {
        let mut cluster = Self {
            nodes: HashMap::new(),
            config: cfg.clone(),
            test_context: ctx,
            // next_port: 42000,
            // node_count: 0,
            // expected_view: ClusterView::default(),
            authorized_client: libp2p::PeerId::random(),
        };

        let bootnodes = (0..cfg.num_bootnodes)
            .map(|_| cluster.test_context.next_node())
            .collect_vec();

        cluster.test_context.pre_bootstrap(&bootnodes).await;

        // cluster.expected_view.set_peers(
        //     bootnode_identities
        //         .iter()
        //         .map(|idt| {
        //             (idt.peer_id, cluster::Node {
        //                 peer_id: idt.peer_id,
        //                 addr: idt.addr.clone(),
        //                 mode: NodeOperationMode::Started,
        //             })
        //         })
        //         .collect(),
        // );

        for idt in &bootnodes {
            cluster.bootstrap_node(idt.clone(), &bootnodes).await;
        }

        // wait for all nodes in the cluster to become `Normal`
        cluster
            .wait_node_state(None, Some(node::State::Normal))
            .await;

        cluster
    }

    pub fn nodes(&self) -> &HashMap<PeerId, NodeHandle<C>> {
        &self.nodes
    }

    pub async fn run_test_suite(&mut self) {
        // self.cluster_view_version_validation().await;
        self.authorization().await;
        self.replication_and_read_repairs().await;
        self.full_node_rotation().await;
        self.restart_then_decommission().await;
        self.scaling().await;
    }

    // TODO: convert into a unit test once replication machinery is properly
    // isolated
    // async fn cluster_view_version_validation(&self) {
    //     tracing::info!("Cluster view version validation");

    //     let mismatching_version = u128::MAX;

    //     let node = self.random_node();
    //     let mut req = node.new_replicated_request(C::gen_test_ops().read).await;
    //     req.keyspace_version = mismatching_version;

    //     let resp = node.handle_replication(self.random_peer_id(), req).await;
    //     assert_eq!(resp, Err(ReplicaError::KeyspaceVersionMismatch));
    // }

    async fn authorization(&mut self) {
        tracing::info!("Authorization");

        let ops = C::gen_test_ops();
        let client = &libp2p::PeerId::random();

        // by defaut client authorization is disabled
        self.random_node()
            .coordinator()
            .replicate(client, ops.write)
            .await
            .expect("auth is disabled")
            .unwrap();

        // bootup a new node with authorization enabled
        let node = self.test_context.next_node();
        self.bootup_node_with_auth(node.clone()).await;

        let resp = self
            .node_mut(node.id())
            .coordinator()
            .replicate(client, ops.read.clone())
            .await;
        assert_eq!(resp, Err(CoordinatorError::Unauthorized));

        self.node_mut(node.id())
            .clone()
            .coordinator()
            .replicate(&self.authorized_client, ops.read.clone())
            .await
            .expect("should be authorized")
            .unwrap();

        self.shutdown_node(node.id(), fsm::ShutdownReason::Decommission)
            .await;
    }

    async fn replication_and_read_repairs(&self) {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 100;

        tracing::info!("Replication and read repairs");

        // How many peers are targeted for the request.
        // TODO: get from keyspace
        let replication_factor = 3;

        let this = &self;
        let test_cases: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async move {
                let assert = C::gen_test_ops();

                this.random_node()
                    .coordinator()
                    .replicate(&this.authorized_client, assert.write.clone())
                    .await
                    .expect("coordinator")
                    .expect("replica");

                assert
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let this = &self;
        let mismatches: usize = stream::iter(test_cases)
            .map(|c| async move {
                // find one replica and break it
                for n in this.nodes.values() {
                    let output = n
                        .replica()
                        .handle_replication(
                            this.random_peer_id(),
                            c.read.clone(),
                            n.consensus().cluster().keyspace_version(),
                        )
                        .await
                        .unwrap();

                    if c.expected_output == output {
                        n.replica()
                            .handle_replication(
                                this.random_peer_id(),
                                c.overwrite.clone(),
                                n.consensus().cluster().keyspace_version(),
                            )
                            .await
                            .unwrap();
                        break;
                    }
                }

                // make replicated request via each coordinator to surely trigger a read repair
                let coordinators: usize = stream::iter(this.nodes.values())
                    .map(|n| async {
                        let output = n
                            .coordinator()
                            .replicate(&this.authorized_client, c.read.clone())
                            .await
                            .expect("coordinator")
                            .expect("replica");

                        usize::from(c.expected_output == output)
                    })
                    .buffer_unordered(this.nodes.len())
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .sum();

                if coordinators != this.nodes.len() {
                    return 1;
                }

                // check that all replicas have the data
                let replicas: usize = stream::iter(this.nodes.values())
                    .map(|n| async {
                        let output = n
                            .replica()
                            .handle_replication(
                                this.random_peer_id(),
                                c.read.clone(),
                                n.consensus().cluster().keyspace_version(),
                            )
                            .await
                            .expect("replica");

                        usize::from(c.expected_output == output)
                    })
                    .buffer_unordered(this.nodes.len())
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .sum();

                usize::from(replicas != replication_factor)
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();

        assert_eq!(mismatches, 0);
    }

    /// Test of Theseus. If all nodes in the cluster are replaced
    /// ~~is it the same cluster?~~ does it still possess all the data?
    async fn full_node_rotation(&mut self) {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 10;

        tracing::info!("Full node rotation");

        let this = &self;
        let authorized_client = self.authorized_client;
        let read_asserts: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async move {
                let assert = C::gen_test_ops();

                this.random_node()
                    .coordinator()
                    .replicate(&authorized_client, assert.write.clone())
                    .await
                    .expect("coordinator")
                    .expect("replica");

                assert
            })
            .buffer_unordered(REQUEST_CONCURRENCY)
            .collect()
            .await;

        // There are some non-blocking requests being spawned, wait for them to finish.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        for id in self.nodes.keys().copied().collect_vec() {
            let idt = self.test_context.next_node();

            // spin up new node and wait for the cluster to become `Normal`
            self.bootup_node(idt).await;

            // decomission a node and wait for it to become `Left`
            self.shutdown_node(&id, fsm::ShutdownReason::Decommission)
                .await;
        }

        let node = &self.random_node();
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = node
                    .coordinator()
                    .replicate(&authorized_client, assert.read)
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

    /// Restart a node then decommission the same node. We had an FSM bug
    /// related to this.
    async fn restart_then_decommission(&mut self) {
        tracing::info!("Restart then decommission");
        let node = self.test_context.next_node();
        let id = node.id().clone();

        self.bootup_node(node.clone()).await;
        self.shutdown_node(&id, fsm::ShutdownReason::Restart).await;

        self.bootup_node(node).await;
        self.shutdown_node(&id, fsm::ShutdownReason::Decommission)
            .await;
    }

    /// Scale cluster 2x, then scale back down.
    async fn scaling(&mut self) {
        tracing::info!("Scaling");

        let n = self.nodes.len();
        let mut new_nodes = vec![];

        for _ in 0..n {
            let node = self.test_context.next_node();
            let id = node.id();

            self.bootup_node(node.clone()).await;

            new_nodes.push(id.clone());
        }

        for id in new_nodes {
            self.shutdown_node(&id, fsm::ShutdownReason::Decommission)
                .await;
        }
    }

    async fn wait_node_state(
        &self,
        node_id: Option<&node::Id<C::Node>>,
        state: Option<node::State<C::Node>>,
    ) {
        let node_ids = if let Some(id) = node_id {
            vec![id]
        } else {
            self.nodes.keys().collect()
        };

        tokio::time::timeout(Duration::from_secs(30), async {
            let mut cluster_version = 0;

            loop {
                'out: for node in self.nodes.values() {
                    let cluster = node.inner.consensus().cluster();

                    if cluster.version() > cluster_version {
                        cluster_version = cluster.version();
                        break;
                    }
                    if cluster.version() < cluster_version {
                        break;
                    };

                    for node_id in &node_ids {
                        if node.inner.consensus().cluster().node_state(node_id) != state {
                            break 'out;
                        }
                    }

                    return;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("inconsistent cluster");
            // panic!(
            //     "Inconsistent ClusterView!\nExpected: {:#?}\nGot: {:#?}",
            //     self.expected_view, views
            // )
        });
    }

    // fn add_expected_peer(&mut self, id: PeerId, addr: Multiaddr) {
    //     let mut expected_peers = self.expected_view.nodes().clone();
    //     expected_peers.insert(id, cluster::Node {
    //         peer_id: id,
    //         addr,
    //         mode: NodeOperationMode::Started,
    //     });
    //     self.expected_view.set_peers(expected_peers);
    // }

    // fn remove_expected_peer(&mut self, id: PeerId) {
    //     let mut expected_peers = self.expected_view.nodes().clone();
    //     expected_peers.remove(&id);
    //     self.expected_view.set_peers(expected_peers);
    // }

    // fn set_expected_mode(&mut self, id: PeerId, mode: NodeOperationMode) {
    //     self.expected_view.update_node_op_mode(id, mode).unwrap();
    // }

    fn random_node(&self) -> &NodeHandle<C> {
        self.nodes.values().choose(&mut rand::thread_rng()).unwrap()
    }

    fn random_peer_id(&self) -> &PeerId {
        &self.random_node().id()
    }

    fn node_mut(&mut self, id: &node::Id<C::Node>) -> &mut NodeHandle<C> {
        self.nodes.get_mut(id).unwrap()
    }

    // fn next_node_addr(&mut self) -> Multiaddr {
    //     let port = self.next_port;
    //     self.next_port += 1;

    //     format!("/ip4/127.0.0.1/udp/{port}/quic-v1")
    //         .parse()
    //         .unwrap()
    // }

    // fn next_node_identity(&mut self) -> NodeIdentity {
    //     let keypair = Keypair::generate_ed25519();
    //     self.node_count += 1;

    //     NodeIdentity {
    //         peer_id: keypair.public().to_peer_id(),
    //         keypair,
    //         addr: self.next_node_addr(),
    //         api_addr: self.next_node_addr(),
    //     }
    // }

    async fn create_node(
        &mut self,
        node: C::Node,
        bootnodes: Option<&[C::Node]>,
        enable_auth: bool,
    ) -> Node<C> {
        let peers = match bootnodes {
            Some(nodes) => nodes.iter().map(|n| (n.id().clone(), n.clone())).collect(),
            None => [self.random_node().cluster_node().clone()]
                .map(|n| (n.id().clone(), n))
                .into_iter()
                .collect(),
        };

        let deps = self
            .test_context
            .init_deps(node.clone(), peers, bootnodes.is_some())
            .await;

        let mut opts = self.config.node_opts.clone();
        if enable_auth {
            let mut auth = crate::AuthorizationOpts {
                allowed_coordinator_clients: HashSet::new(),
            };
            auth.allowed_coordinator_clients
                .insert(self.authorized_client);
            opts.authorization = Some(auth);
        }

        crate::Node::new(
            node,
            opts,
            deps.consensus,
            deps.network,
            deps.storage,
            deps.hasher_builder,
        )
    }

    async fn bootup_node(&mut self, node: C::Node) {
        self.bootup_node_(node, None, false).await
    }

    async fn bootstrap_node(&mut self, node: C::Node, bootstrap_nodes: &[C::Node]) {
        self.bootup_node_(node, Some(bootstrap_nodes), false).await
    }

    async fn bootup_node_with_auth(&mut self, node: C::Node) {
        self.bootup_node_(node, None, true).await
    }

    async fn bootup_node_(
        &mut self,
        cluster_node: C::Node,
        bootnodes: Option<&[C::Node]>,
        enable_auth: bool,
    ) {
        tracing::info!(?cluster_node, "Booting up node");

        let id = cluster_node.id().clone();

        let node = self
            .create_node(cluster_node.clone(), bootnodes, enable_auth)
            .await;

        self.test_context.pre_bootup(&cluster_node, &node).await;

        self.nodes.insert(node.id().clone(), NodeHandle {
            inner: node.clone(),
            cluster_node,
            task_handle: Some(tokio::spawn(async move { node.run().await.unwrap() })),
        });

        // if self.expected_view.nodes().get(&idt.peer_id).map(|n| n.mode)
        //     != Some(NodeOperationMode::Restarting)
        // {
        //     if bootnodes.is_none() {
        //         self.add_expected_peer(idt.peer_id, idt.addr);
        //     }

        //     self.set_expected_mode(idt.peer_id, NodeOperationMode::Booting);
        // }

        // self.set_expected_mode(idt.peer_id, NodeOperationMode::Normal);

        if bootnodes.is_none() {
            self.wait_node_state(Some(&id), Some(node::State::Normal))
                .await;
        }
    }

    async fn shutdown_node(&mut self, id: &node::Id<C::Node>, reason: fsm::ShutdownReason) {
        tracing::info!(%id, ?reason, "Shutting down node");

        let node = self.node_mut(id);
        node.shutdown(reason).unwrap();
        node.task_handle
            .take()
            .expect("already shutdown")
            .await
            .expect("join node task");

        let node = self.nodes.remove(id).unwrap();
        self.test_context.post_shutdown(&node, reason).await;

        self.wait_node_state(Some(id), None).await;
    }
}

// #[derive(Clone, Debug)]
// pub struct NodeIdentity {
//     pub peer_id: PeerId,
//     pub keypair: Keypair,
//     pub addr: Multiaddr,
//     pub api_addr: Multiaddr,
// }

pub type Node<S> = crate::Node<
    <S as Context>::Consensus,
    <S as Context>::Network,
    <S as Context>::Storage,
    <S as Context>::BuildHasher,
>;

#[derive(Deref)]
pub struct NodeHandle<S: Context> {
    #[deref]
    inner: Node<S>,
    cluster_node: S::Node,
    task_handle: Option<tokio::task::JoinHandle<fsm::ShutdownReason>>,
}

// impl<S: Context> NodeHandle<S> {
//     pub fn identity(&self) -> &NodeIdentity {
//         &self.identity
//     }
// }

pub trait Context: Sized + Send + Sync + 'static {
    type Node: cluster::Node<Id = PeerId>;

    type Consensus: Consensus<Node = Self::Node> + Clone + Send + Sync + 'static;

    type Network: fsm::Network<Self::Node>
        + replication::Network<Self::Node, Self::Storage, Self::ReadOperation>
        + replication::Network<Self::Node, Self::Storage, Self::WriteOperation>;

    type Storage: replication::Storage<Self::ReadOperation>
        + replication::Storage<Self::WriteOperation>
        + fsm::Storage<<Self::Network as fsm::Network<Self::Node>>::Data>
        + Clone;

    type BuildHasher: BuildHasher + Clone + Send + Sync + 'static;

    type ReadOperation: StorageOperation<RepairOperation = Self::WriteOperation>;
    type WriteOperation: StorageOperation<RepairOperation = NoRepair>;

    async fn init_deps(
        &mut self,
        node: Self::Node,
        peers: HashMap<node::Id<Self::Node>, Self::Node>,
        is_bootstrap: bool,
    ) -> Dependencies<Self::Consensus, Self::Network, Self::Storage, Self::BuildHasher>;

    fn next_node(&mut self) -> Self::Node;
    fn gen_test_ops() -> Operations<Self>;

    async fn pre_bootstrap(&mut self, bootnodes: &[Self::Node]);
    async fn pre_bootup(&mut self, cluster_node: &Self::Node, node: &Node<Self>);
    async fn post_shutdown(&mut self, node: &NodeHandle<Self>, reason: fsm::ShutdownReason);
}

pub struct Dependencies<C, N, S, H> {
    pub consensus: C,
    pub network: N,
    pub storage: S,
    pub hasher_builder: H,
}

pub struct Operations<S: Context> {
    pub write: S::WriteOperation,
    pub read: S::ReadOperation,
    pub expected_output: <S::ReadOperation as StorageOperation>::Output,

    // for corrupting the data to validate read repairs
    pub overwrite: S::WriteOperation,
}

// pub type ReadOutput<S> = ReplicatableOperationOutput<<S as
// Context>::ReadOperation>; pub type WriteOutput<S> =
// ReplicatableOperationOutput<<S as Context>::WriteOperation>;
