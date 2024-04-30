use {
    libp2p::Multiaddr,
    rand::Rng,
    relay_irn::{
        cluster::{self, replication::Strategy},
        consensus,
        network,
        storage,
        test::{self, NodeIdentity},
        NodeOpts,
        PeerId,
        ShutdownReason,
    },
    std::{collections::HashMap, time::Duration},
    storage::stub::{Get, Set},
    tracing_subscriber::EnvFilter,
};

#[derive(Default)]
struct Context {
    consensus: consensus::Stub,
    network_registry: network::stub::Registry,
    storages: HashMap<PeerId, storage::Stub>,

    bootnodes: Vec<cluster::Node>,
}

impl test::Context for Context {
    type Consensus = consensus::Stub;
    type Network = network::Stub;
    type Storage = storage::Stub;

    type ReadOperation = Get;
    type WriteOperation = Set;

    async fn init_deps(
        &mut self,
        idt: NodeIdentity,
        peers: HashMap<PeerId, Multiaddr>,
        _is_bootstrap: bool,
    ) -> test::Dependencies<Self::Consensus, Self::Network, Self::Storage> {
        let peers = peers
            .iter()
            .map(|(id, addr)| (*id, [addr.clone()].into_iter().collect()))
            .collect();
        let network = self
            .network_registry
            .new_network_handle(idt.addr.clone(), peers);

        test::Dependencies {
            consensus: self.consensus.clone(),
            network,
            storage: self.storages.entry(idt.peer_id).or_default().clone(),
        }
    }

    fn gen_test_ops() -> test::Operations<Self> {
        let mut rng = rand::thread_rng();
        let key = rng.gen::<u64>();
        let value = rng.gen::<u64>();
        let value2 = rng.gen::<u64>();

        test::Operations {
            write: Set(key, value),
            read: Get(key),
            expected_output: Some(value),
            overwrite: Set(key, value2),
        }
    }

    async fn pre_bootstrap(&mut self, bootnodes: &[NodeIdentity]) {
        self.bootnodes = bootnodes
            .iter()
            .map(|n| cluster::Node {
                peer_id: n.peer_id,
                addr: n.addr.clone(),
                mode: cluster::NodeOperationMode::Started,
            })
            .collect();

        self.consensus.set_nodes(self.bootnodes.clone());
    }

    async fn pre_bootup(&mut self, _idt: &NodeIdentity, node: &test::Node<Self>) {
        self.network_registry
            .register_node(node.network().local_multiaddr(), node.clone());

        let is_bootnode = self.bootnodes.iter().any(|n| &n.peer_id == node.id());
        let is_restarting = self.consensus.get_view().nodes().contains_key(node.id());

        if !is_bootnode && !is_restarting {
            self.consensus.set_node(cluster::Node {
                peer_id: *node.id(),
                addr: node.network().local_multiaddr(),
                mode: cluster::NodeOperationMode::Started,
            })
        };
    }

    async fn post_shutdown(&mut self, node: &test::NodeHandle<Self>, reason: ShutdownReason) {
        if reason == ShutdownReason::Decommission {
            self.consensus.remove_node(node.id())
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_suite() {
    const NUM_BOOTNODES: usize = 5;

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    test::Cluster::new(Context::default(), test::ClusterConfig {
        num_bootnodes: NUM_BOOTNODES,
        num_groups: 3,
        node_opts: NodeOpts {
            replication_strategy: Strategy::new(3, cluster::replication::ConsistencyLevel::Quorum),
            replication_request_timeout: Duration::from_secs(15),
            replication_concurrency_limit: 1000,
            replication_request_queue: 4096,
            warmup_delay: Duration::from_millis(10),
        },
    })
    .await
    .run_test_suite()
    .await;
}
