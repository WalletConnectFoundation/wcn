#![allow(async_fn_in_trait)]

use {
    crate::{
        cluster::{self, ClusterView, NodeOperationMode},
        migration,
        replication::{
            Coordinator,
            CoordinatorError,
            Read,
            Replica,
            ReplicaError,
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
    futures::{stream, FutureExt, StreamExt},
    itertools::Itertools,
    libp2p::{identity::Keypair, Multiaddr},
    rand::seq::IteratorRandom,
    std::{
        collections::{HashMap, HashSet},
        fmt,
        time::Duration,
    },
};

pub struct Cluster<C: Context> {
    config: ClusterConfig,
    nodes: HashMap<PeerId, NodeHandle<C>>,

    test_context: C,
    next_port: u16,
    node_count: u16,

    expected_view: ClusterView,

    authorized_client: libp2p::PeerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConfig {
    pub num_bootnodes: usize,
    pub num_groups: usize,
    pub node_opts: NodeOpts,
}

#[allow(clippy::trait_duplication_in_bounds)] // false positive
impl<C: Context, SE: fmt::Debug + PartialEq, NE: fmt::Debug + PartialEq> Cluster<C>
where
    migration::Manager<C::Network, C::Storage>: BootingMigrations + LeavingMigrations,
    crate::Node<C::Consensus, C::Network, C::Storage>: Coordinator<C::ReadOperation, Output = ReadOutput<C>, StorageError = SE, NetworkError = NE>
        + Coordinator<
            C::WriteOperation,
            Output = WriteOutput<C>,
            StorageError = SE,
            NetworkError = NE,
        > + Replica<C::ReadOperation, Output = ReadOutput<C>, StorageError = SE>
        + Replica<C::WriteOperation, Output = WriteOutput<C>, StorageError = SE>,
    ReadOutput<C>: fmt::Debug + PartialEq,
{
    pub async fn new(ctx: C, cfg: ClusterConfig) -> Self {
        let mut cluster = Self {
            nodes: HashMap::new(),
            config: cfg.clone(),
            test_context: ctx,
            next_port: 42000,
            node_count: 0,
            expected_view: ClusterView::default(),
            authorized_client: libp2p::PeerId::random(),
        };

        let bootnode_identities = (0..cfg.num_bootnodes)
            .map(|_| cluster.next_node_identity())
            .collect_vec();

        cluster
            .test_context
            .pre_bootstrap(&bootnode_identities)
            .await;

        cluster.expected_view.set_peers(
            bootnode_identities
                .iter()
                .map(|idt| {
                    (idt.peer_id, cluster::Node {
                        peer_id: idt.peer_id,
                        addr: idt.addr.clone(),
                        mode: NodeOperationMode::Started,
                    })
                })
                .collect(),
        );

        for idt in &bootnode_identities {
            cluster
                .bootstrap_node(idt.clone(), &bootnode_identities)
                .await;
        }

        // wait for all nodes in the cluster to become `Normal`
        cluster.wait_for_consistency().await;

        cluster
    }

    pub fn nodes(&self) -> &HashMap<PeerId, NodeHandle<C>> {
        &self.nodes
    }

    pub async fn run_test_suite(&mut self) {
        self.cluster_view_version_validation().await;
        self.authorization().await;
        self.replication_and_read_repairs().await;
        self.full_node_rotation().await;
        self.restart_then_decommission().await;
        self.scaling().await;
    }

    // TODO: convert into a unit test once replication machinery is properly
    // isolated
    async fn cluster_view_version_validation(&mut self) {
        tracing::info!("Cluster view version validation");

        let mismatching_version = u128::MAX;

        let node = self.random_node();
        let mut req = node.new_replicated_request(C::gen_test_ops().read).await;
        req.cluster_view_version = mismatching_version;

        let resp = node.handle_replication(self.random_peer_id(), req).await;
        assert_eq!(resp, Err(ReplicaError::ClusterViewVersionMismatch));
    }

    async fn authorization(&mut self) {
        tracing::info!("Authorization");

        let ops = C::gen_test_ops();
        let client = &libp2p::PeerId::random();

        // by defaut client authorization is disabled
        self.random_node()
            .replicate(client, ops.write)
            .await
            .expect("auth is disabled")
            .unwrap();

        // bootup a new node with authorization enabled
        let idt = self.next_node_identity();
        self.bootup_node_with_auth(idt.clone()).await;

        let resp = self
            .node_mut(&idt.peer_id)
            .replicate(client, ops.read.clone())
            .await;
        assert_eq!(resp, Err(CoordinatorError::Unauthorized));

        self.node_mut(&idt.peer_id)
            .clone()
            .replicate(&self.authorized_client, ops.read.clone())
            .await
            .expect("should be authorized")
            .unwrap();

        self.shutdown_node(&idt.peer_id, ShutdownReason::Decommission)
            .await;
    }

    async fn replication_and_read_repairs(&mut self) {
        const RECORDS_NUM: usize = 10000;
        const REQUEST_CONCURRENCY: usize = 100;

        tracing::info!("Replication and read repairs");

        let strategy = self.config.node_opts.replication_strategy.clone();

        // How many peers are targeted for the request.
        let replication_factor = strategy.replication_factor();

        let this = &self;
        let test_cases: Vec<_> = stream::iter(0..RECORDS_NUM)
            .map(|_| async move {
                let assert = C::gen_test_ops();

                this.random_node()
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
                        .handle_replication(
                            this.random_peer_id(),
                            n.new_replicated_request(c.read.clone()).await,
                        )
                        .await
                        .unwrap();

                    if c.expected_output == output {
                        n.handle_replication(
                            this.random_peer_id(),
                            n.new_replicated_request(c.overwrite.clone()).await,
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
                            .handle_replication(
                                this.random_peer_id(),
                                n.new_replicated_request(c.read.clone()).await,
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
            let idt = self.next_node_identity();

            // spin up new node and wait for the cluster to become `Normal`
            self.bootup_node(idt).await;

            // decomission a node and wait for it to become `Left`
            self.shutdown_node(&id, ShutdownReason::Decommission).await;
        }

        let node = &self.random_node();
        let mismatches: usize = stream::iter(read_asserts)
            .map(|assert| async move {
                let output = node
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
        let idt = self.next_node_identity();
        let id = idt.peer_id;

        self.bootup_node(idt.clone()).await;
        self.shutdown_node(&id, ShutdownReason::Restart).await;

        self.bootup_node(idt).await;
        self.shutdown_node(&id, ShutdownReason::Decommission).await;
    }

    /// Scale cluster 2x, then scale back down.
    async fn scaling(&mut self) {
        tracing::info!("Scaling");

        let n = self.nodes.len();
        let mut new_nodes = vec![];

        for _ in 0..n {
            let idt = self.next_node_identity();
            let id = idt.peer_id;

            self.bootup_node(idt.clone()).await;

            new_nodes.push(id);
        }

        for id in new_nodes {
            self.shutdown_node(&id, ShutdownReason::Decommission).await;
        }
    }

    async fn wait_for_consistency(&self) {
        let mut views = HashMap::new();

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                views = stream::iter(self.nodes.values())
                    .then(|n| n.cluster.read().map(|c| (n.id, c.view().clone())))
                    .collect()
                    .await;

                if views.values().all(|v| &self.expected_view == v) {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Inconsistent ClusterView!\nExpected: {:#?}\nGot: {:#?}",
                self.expected_view, views
            )
        });
    }

    fn add_expected_peer(&mut self, id: PeerId, addr: Multiaddr) {
        let mut expected_peers = self.expected_view.nodes().clone();
        expected_peers.insert(id, cluster::Node {
            peer_id: id,
            addr,
            mode: NodeOperationMode::Started,
        });
        self.expected_view.set_peers(expected_peers);
    }

    fn remove_expected_peer(&mut self, id: PeerId) {
        let mut expected_peers = self.expected_view.nodes().clone();
        expected_peers.remove(&id);
        self.expected_view.set_peers(expected_peers);
    }

    fn set_expected_mode(&mut self, id: PeerId, mode: NodeOperationMode) {
        self.expected_view.update_node_op_mode(id, mode).unwrap();
    }

    fn random_node(&self) -> &NodeHandle<C> {
        self.nodes.values().choose(&mut rand::thread_rng()).unwrap()
    }

    fn random_peer_id(&self) -> &libp2p::PeerId {
        &self.random_node().id.id
    }

    fn node_mut(&mut self, id: &PeerId) -> &mut NodeHandle<C> {
        self.nodes.get_mut(id).unwrap()
    }

    fn next_node_addr(&mut self) -> Multiaddr {
        let port = self.next_port;
        self.next_port += 1;

        format!("/ip4//udp/{port}/quic-v1")
            .parse()
            .unwrap()
    }

    fn next_node_identity(&mut self) -> NodeIdentity {
        let keypair = Keypair::generate_ed25519();

        let group = self.node_count % self.config.num_groups as u16 + 1;
        self.node_count += 1;

        NodeIdentity {
            peer_id: PeerId {
                id: keypair.public().to_peer_id(),
                group,
            },
            keypair,
            addr: self.next_node_addr(),
            api_addr: self.next_node_addr(),
        }
    }

    async fn create_node(
        &mut self,
        idt: NodeIdentity,
        bootnodes: Option<&[NodeIdentity]>,
        enable_auth: bool,
    ) -> Node<C> {
        let peers = match bootnodes {
            Some(nodes) => nodes.iter().map(|p| (p.peer_id, p.addr.clone())).collect(),
            None => {
                let node = self.random_node();
                [(node.identity.peer_id, node.identity.addr.clone())]
                    .into_iter()
                    .collect()
            }
        };

        let deps = self
            .test_context
            .init_deps(idt.clone(), peers, bootnodes.is_some())
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
            idt.peer_id,
            opts,
            deps.consensus,
            deps.network,
            deps.storage,
        )
    }

    async fn bootup_node(&mut self, idt: NodeIdentity) {
        self.bootup_node_(idt, None, false).await
    }

    async fn bootstrap_node(&mut self, idt: NodeIdentity, bootstrap_nodes: &[NodeIdentity]) {
        self.bootup_node_(idt, Some(bootstrap_nodes), false).await
    }

    async fn bootup_node_with_auth(&mut self, idt: NodeIdentity) {
        self.bootup_node_(idt, None, true).await
    }

    async fn bootup_node_(
        &mut self,
        idt: NodeIdentity,
        bootnodes: Option<&[NodeIdentity]>,
        enable_auth: bool,
    ) {
        tracing::info!(id = %idt.peer_id, addr = %idt.addr, "Booting up node");

        let node = self.create_node(idt.clone(), bootnodes, enable_auth).await;

        self.test_context.pre_bootup(&idt, &node).await;

        self.nodes.insert(idt.peer_id, NodeHandle {
            inner: node.clone(),
            identity: idt.clone(),
            task_handle: Some(tokio::spawn(async move { node.run().await.unwrap() })),
        });

        if self.expected_view.nodes().get(&idt.peer_id).map(|n| n.mode)
            != Some(NodeOperationMode::Restarting)
        {
            if bootnodes.is_none() {
                self.add_expected_peer(idt.peer_id, idt.addr);
            }

            self.set_expected_mode(idt.peer_id, NodeOperationMode::Booting);
        }

        self.set_expected_mode(idt.peer_id, NodeOperationMode::Normal);

        if bootnodes.is_none() {
            self.wait_for_consistency().await;
        }
    }

    async fn shutdown_node(&mut self, id: &PeerId, reason: ShutdownReason) {
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

        match reason {
            ShutdownReason::Restart => self.set_expected_mode(*id, NodeOperationMode::Restarting),
            ShutdownReason::Decommission => {
                self.set_expected_mode(*id, NodeOperationMode::Leaving);
                self.set_expected_mode(*id, NodeOperationMode::Left);
                self.remove_expected_peer(*id);
            }
        }

        self.wait_for_consistency().await;
    }
}

#[derive(Clone, Debug)]
pub struct NodeIdentity {
    pub peer_id: PeerId,
    pub keypair: Keypair,
    pub addr: Multiaddr,
    pub api_addr: Multiaddr,
}

pub type Node<S> =
    crate::Node<<S as Context>::Consensus, <S as Context>::Network, <S as Context>::Storage>;

#[derive(Deref)]
pub struct NodeHandle<S: Context> {
    #[deref]
    inner: Node<S>,
    identity: NodeIdentity,
    task_handle: Option<tokio::task::JoinHandle<ShutdownReason>>,
}

impl<S: Context> NodeHandle<S> {
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }
}

pub trait Context: Sized + Send + Sync + 'static {
    type Consensus: Consensus;
    type Network: Network;
    type Storage: fmt::Debug + Clone + Send + Sync + 'static;

    type ReadOperation: ReplicatableOperation<Type = Read>;
    type WriteOperation: ReplicatableOperation<Type = Write>;

    async fn init_deps(
        &mut self,
        node_identity: NodeIdentity,
        peers: HashMap<PeerId, Multiaddr>,
        is_bootstrap: bool,
    ) -> Dependencies<Self::Consensus, Self::Network, Self::Storage>;

    fn gen_test_ops() -> Operations<Self>;

    async fn pre_bootstrap(&mut self, bootnodes: &[NodeIdentity]);
    async fn pre_bootup(&mut self, idt: &NodeIdentity, node: &Node<Self>);
    async fn post_shutdown(&mut self, node: &NodeHandle<Self>, reason: ShutdownReason);
}

pub struct Dependencies<C, N, S> {
    pub consensus: C,
    pub network: N,
    pub storage: S,
}

pub struct Operations<S: Context> {
    pub write: S::WriteOperation,
    pub read: S::ReadOperation,
    pub expected_output: <S::ReadOperation as ReplicatableOperation>::Output,

    // for corrupting the data to validate read repairs
    pub overwrite: S::WriteOperation,
}

pub type ReadOutput<S> = ReplicatableOperationOutput<<S as Context>::ReadOperation>;
pub type WriteOutput<S> = ReplicatableOperationOutput<<S as Context>::WriteOperation>;
