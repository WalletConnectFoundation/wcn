use {
    crate::{
        cluster::{
            keyspace::{hashring::Positioned, pending_ranges::PendingRange, KeyPosition, KeyRange},
            Cluster,
            NodeOperationMode,
        },
        BootingMigrations,
        LeavingMigrations,
        Migrations,
        PeerId,
        Storage,
    },
    async_trait::async_trait,
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
    },
    tokio::sync::RwLock,
};

pub mod booting;
pub mod leaving;

/// [`Storage`] operation for importing data.
pub struct Import<Data> {
    pub key_range: KeyRange<KeyPosition>,
    pub data: Data,
}

/// [`Storage`] operation for exporting data.
pub struct Export {
    pub key_range: KeyRange<KeyPosition>,
}

/// [`Storage`] operation for storing other operations as hinted to be committed
/// at a later time when migration process is completed.
pub struct StoreHinted<Op> {
    pub operation: Op,
}

/// [`Storage`] operation for committing hinted operations previously stored
/// using [`StoreHinted`].
// TODO: Commit logic doesn't need to be in Rocks
pub struct CommitHintedOperations {
    pub key_range: KeyRange<KeyPosition>,
}

/// Migration status of a specific [`KeyRange`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyRangeStatus {
    InProgress,
    Completed,
}

type KeyRangeStatuses = HashMap<KeyRange<KeyPosition>, Arc<RwLock<KeyRangeStatus>>>;

#[derive(Clone, Debug)]
pub struct Manager<N, S> {
    id: PeerId,

    network: N,
    storage: S,
    cluster: Arc<RwLock<Cluster>>,

    // Used to determine whether hinting is required for a specific keyrange during migration
    // process.
    key_range_statuses: Arc<Mutex<KeyRangeStatuses>>,
}

impl<N, S> Manager<N, S> {
    pub fn new(id: PeerId, network: N, storage: S, cluster: Arc<RwLock<Cluster>>) -> Self {
        Self {
            id,
            network,
            storage,
            cluster,
            key_range_statuses: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn status_entry(&self, key_range: KeyRange<KeyPosition>) -> Arc<RwLock<KeyRangeStatus>> {
        self.key_range_statuses
            .lock()
            .unwrap()
            .entry(key_range)
            .or_insert_with(|| Arc::new(RwLock::new(KeyRangeStatus::InProgress)))
            .clone()
    }

    /// Stores the provided operation as hinted to be committed later after
    /// migration process is finished.
    ///
    /// Returns the operation back to the caller if hinting isn't required.
    pub(crate) async fn store_hinted<Op>(
        &self,
        operation: Positioned<Op>,
    ) -> Result<Option<Positioned<Op>>, S::Error>
    where
        S: Storage<StoreHinted<Positioned<Op>>>,
    {
        // Here we're ensuring that no new operations can be stored as hinted during the
        // execution of `CommitHintedOperations` for a specific `KeyRange` using
        // locking. By doing so, if `KeyRange` migration `Status` is
        // `Completed`, we can safely process operations normally, skipping the
        // whole hinting process.

        let Some(key_range) = self
            .cluster
            .read()
            .await
            .pending_ranges(&self.id)
            .map(|pending_ranges| pending_ranges.iter())
            .into_iter()
            .flatten()
            .filter_map(|range| match range {
                PendingRange::Push { range, destination } if destination == &self.id => Some(range),
                PendingRange::Pull { range, .. } => Some(range),
                _ => None,
            })
            .find(|range| range.contains(&operation.position))
            .copied()
        else {
            return Ok(Some(operation));
        };

        let status = self.status_entry(key_range);
        let status_guard = status.read().await;

        match *status_guard {
            KeyRangeStatus::InProgress => self
                .storage
                .exec(StoreHinted { operation })
                .await
                .map(|_| None),
            KeyRangeStatus::Completed => Ok(Some(operation)),
        }
    }

    async fn commit_hinted_operations(
        &self,
        key_range: KeyRange<KeyPosition>,
    ) -> Result<(), S::Error>
    where
        S: Storage<CommitHintedOperations>,
    {
        let status = self.status_entry(key_range);
        let mut status_guard = status.write().await;

        self.storage
            .exec(CommitHintedOperations { key_range })
            .await
            .map(|_| *status_guard = KeyRangeStatus::Completed)
    }
}

#[async_trait]
impl<N, S> Migrations for Manager<N, S>
where
    N: Send + Sync,
    S: Send + Sync,
    Self: LeavingMigrations + BootingMigrations,
{
    async fn update_pending_ranges(&self, _: ()) {
        let cluster = self.cluster.read().await;
        let is_normal = cluster
            .node_op_mode(&self.id)
            .is_some_and(|mode| mode == NodeOperationMode::Normal);

        // We only need to perform cleanup for `Normal`, `Booting` cleans itself up upon
        // completion of the booting migrations.
        if !is_normal {
            return;
        }

        let ranges: HashSet<_> = cluster.pending_push_ranges_to(self.id).collect();

        self.key_range_statuses
            .lock()
            .unwrap()
            .retain(|range, _| ranges.contains(range));
    }
}

#[cfg(test)]
pub mod stub {
    use {
        crate::{
            cluster::{self, Cluster, ClusterView, NodeOperationMode},
            network,
            storage::{self, stub::Data},
            PeerId,
        },
        std::{
            collections::{HashMap, HashSet},
            sync::Arc,
        },
        tokio::sync::RwLock,
    };

    pub type Manager = super::Manager<network::Stub, storage::Stub>;

    pub fn cluster(test_mode: NodeOperationMode) -> Vec<Manager> {
        use NodeOperationMode as M;

        let nodes: Vec<_> = (0..=4)
            .into_iter()
            .map(|idx| {
                let id = PeerId::random();
                let addr = format!("/ip4/127.0.0.1/udp/300{idx}/quic-v1")
                    .parse()
                    .unwrap();
                let mut node = cluster::Node::new(id, addr);

                node.mode = match idx {
                    4 => test_mode,
                    _ => NodeOperationMode::Normal,
                };

                (id, node)
            })
            .collect();

        let mut view = ClusterView::new();
        view.set_peers(nodes.clone().into_iter().collect());

        let peers: HashMap<_, HashSet<_>> = view
            .nodes()
            .values()
            .map(|n| (n.peer_id, [n.addr.clone()].into_iter().collect()))
            .collect();

        let data = Data::generate();
        let network_registry = network::stub::Registry::default();

        let mut cluster = Cluster::new(cluster::replication::Strategy::new(
            3,
            cluster::replication::ConsistencyLevel::Quorum,
        ));
        cluster.install_view_update(view.clone());

        nodes
            .into_iter()
            .map(|(_, n)| {
                let storage = storage::Stub::new();

                match (test_mode, n.mode) {
                    (M::Booting, M::Normal) | (M::Leaving, M::Leaving) => {
                        storage.populate(data.clone())
                    }
                    _ => {}
                };

                let network = network_registry.new_network_handle(n.addr.clone(), peers.clone());

                let manager = super::Manager::new(
                    n.peer_id,
                    network,
                    storage,
                    Arc::new(RwLock::new(cluster.clone())),
                );

                network_registry.register_migration_manager(n.addr.clone(), manager.clone());
                manager
            })
            .collect()
    }
}

#[cfg(test)]
pub type StubbedManager = stub::Manager;
