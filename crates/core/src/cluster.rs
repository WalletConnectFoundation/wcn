use {
    self::{
        keyspace::{
            pending_ranges::{PendingRange, PendingRanges},
            KeyPosition,
            KeyRange,
            Keyspace,
            RingPosition,
        },
        replication::Strategy,
    },
    crate::{cluster::keyspace::hashring, PeerId},
    derive_more::AsRef,
    libp2p::Multiaddr,
    serde::{Deserialize, Serialize},
    smallvec::SmallVec,
    std::{
        collections::{BTreeMap, HashMap},
        mem,
        time::Duration,
    },
};

pub mod error;
pub mod keyspace;
pub mod replication;
#[cfg(test)]
pub mod test_util;

pub const NODE_PRUNE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy)]
pub enum NodePruneReason {
    Decommissioned,
    Unresponsive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum NodeOperationMode {
    /// Node only started, waits for booting.
    Started = 0,

    /// Node started booting by pulling required data from existing nodes.
    Booting = 1,

    /// Node is ready and processes the requests.
    Normal = 2,

    /// Node wants to leave the cluster, so starts processing new requests by
    /// forwarding them to other nodes (which will assume the responsibility for
    /// the data). Node still processes the requests, but also starts migrating
    /// the data to other nodes. Once the data is migrated, node will transition
    /// to `Left` state.
    Leaving = 3,

    /// Node has left the cluster and is no longer processing the requests.
    Left = 4,

    /// Node is restarting.
    Restarting = 5,
}

#[derive(AsRef, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[as_ref]
    pub peer_id: PeerId,
    pub addr: Multiaddr,
    pub mode: NodeOperationMode,
}

impl Node {
    pub fn new(peer_id: PeerId, addr: Multiaddr) -> Self {
        Self {
            peer_id,
            addr,
            mode: NodeOperationMode::Started,
        }
    }

    fn is_booting(&self) -> bool {
        self.mode == NodeOperationMode::Booting
    }

    fn is_leaving(&self) -> bool {
        self.mode == NodeOperationMode::Leaving
    }

    fn is_restarting(&self) -> bool {
        self.mode == NodeOperationMode::Restarting
    }
}

/// Node's local knowledge of all the peers in the cluster. It's possible that
/// the cluster view contains information about more peers than the `Keyspace`
/// does. The reason is that not all running nodes may join the keyspace for
/// various reasons. Cluster view can also store info about decommissioned
/// nodes for a while, until pruned.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterView {
    peers: HashMap<PeerId, Node>,
    // bootstrap_nodes: HashSet<PeerId>,
    version: u128,
}

pub struct VersionedRequest<T> {
    pub inner: T,
    pub cluster_view_version: u128,
}

impl<T> VersionedRequest<T> {
    pub fn new(inner: T, cluster_view_version: u128) -> Self {
        Self {
            inner,
            cluster_view_version,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[error("Cluster view versions of requester and responder don't match")]
pub struct ViewVersionMismatch;

#[derive(Debug, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[error("Requester is not a member of the cluster according to the cluster view")]
pub struct NotMemberError;

/// Result of [`ClusterView::update_node_op_mode`].
pub type UpdateNodeOpModeResult = Result<bool, UpdateNodeOpModeError>;

/// Error of [`ClusterView::update_node_op_mode`].
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum UpdateNodeOpModeError {
    #[error("Trying to update a Node that is not in the current ClusterView")]
    UnknownNode,

    #[error("There is a data migrating node already running")]
    MigrationInProgress,

    #[error("Another node currently restarting")]
    RestartInProgress,

    #[error("Invalid OperationMode transition (from: {from:?}, to: {to:?})")]
    InvalidTransition {
        from: NodeOperationMode,
        to: NodeOperationMode,
    },
}

impl ClusterView {
    /// Creates a new empty [`ClusterView`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrites [`ClusterView::peers`] and bumps [`ClusterView::version`].
    ///
    /// Expected to be used by the consensus module.
    pub fn set_peers(&mut self, peers: HashMap<PeerId, Node>) {
        self.peers = peers;
        self.version += 1;
    }

    /// Tries to update [`NodeOperationMode`] to the provided value.
    pub fn update_node_op_mode(
        &mut self,
        id: PeerId,
        mode: NodeOperationMode,
    ) -> UpdateNodeOpModeResult {
        use self::{NodeOperationMode as M, UpdateNodeOpModeError as E};

        let old_mode = self.peers.get_mut(&id).ok_or(E::UnknownNode)?.mode;

        // We don't bump the version on restars, because they are inconsequential to
        // replication.
        match (old_mode, mode) {
            (old, new) if old == new => return Ok(false),
            (M::Started, M::Booting) | (M::Normal, M::Leaving) => {
                if self.has_data_migrating_node() {
                    return Err(E::MigrationInProgress);
                }
                self.version += 1;
            }
            (M::Booting, M::Normal) | (M::Started | M::Booting | M::Leaving, M::Left) => {
                self.version += 1;
            }
            (M::Normal, M::Restarting) => {
                if self.has_restarting_node() {
                    return Err(E::RestartInProgress);
                }
            }
            (M::Restarting, M::Normal) => {}
            (old, new) => return Err(E::InvalidTransition { from: old, to: new }),
        }

        let _ = self.peers.get_mut(&id).map(|n| n.mode = mode);
        Ok(true)
    }

    pub fn into_nodes(self) -> HashMap<PeerId, Node> {
        self.peers
    }

    pub fn nodes(&self) -> &HashMap<PeerId, Node> {
        &self.peers
    }

    pub fn version(&self) -> u128 {
        self.version
    }

    fn has_data_migrating_node(&self) -> bool {
        self.peers.values().any(Node::is_booting) | self.peers.values().any(Node::is_leaving)
    }

    fn has_restarting_node(&self) -> bool {
        self.peers.values().any(Node::is_restarting)
    }
}

/// Difference between two [`Cluster`] states.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Diff {
    /// List of added [`Node`]s.
    pub added: SmallVec<[Node; 1]>,

    /// List of updated [`Node`]s.
    pub updated: SmallVec<[NodeDiff; 1]>,

    /// List of removed [`Node`]s.
    pub removed: SmallVec<[Node; 1]>,
}

impl Diff {
    /// Returns `true` if there are no changes.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }

    /// Returns a new [`NodeOperationMode`] for a node (node added or updated).
    pub fn node_op_mode(&self, id: PeerId) -> Option<NodeOperationMode> {
        let f = |new: &Node| (new.peer_id == id).then_some(new.mode);

        self.added
            .iter()
            .find_map(f)
            .or_else(|| self.updated.iter().find_map(|d| f(&d.new)))
    }
}

/// Difference between two [`Node`] states.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeDiff {
    /// Old state of the [`Node`].
    pub old: Node,

    /// New state of the [`Node`].
    pub new: Node,
}

#[derive(Clone, Debug)]
pub struct Cluster {
    view: ClusterView,
    keyspace: hashring::Keyspace,
    pending_ranges: BTreeMap<PeerId, PendingRanges>,
}

impl Cluster {
    /// Creates a new [`Cluster`] with provided replication strategy.
    ///
    /// The cluster view and keyspace are initialized empty, to be populated by
    /// consensus module.
    pub fn new(strategy: Strategy) -> Self {
        Self {
            view: ClusterView::new(),
            keyspace: hashring::Keyspace::new(strategy),
            pending_ranges: BTreeMap::new(),
        }
    }

    pub fn view(&self) -> &ClusterView {
        &self.view
    }

    pub fn keyspace(&self) -> &hashring::Keyspace {
        &self.keyspace
    }

    pub fn node(&self, peer_id: &PeerId) -> Option<&Node> {
        self.view.peers.get(peer_id)
    }

    pub fn node_op_mode(&self, peer_id: &PeerId) -> Option<NodeOperationMode> {
        self.node(peer_id).map(|n| n.mode)
    }

    pub fn node_reached_op_mode(&self, peer_id: &PeerId, mode: NodeOperationMode) -> bool {
        self.node_op_mode(peer_id).filter(|m| m >= &mode).is_some()
    }

    pub fn booting_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.find_peers(Node::is_booting)
    }

    pub fn leaving_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.find_peers(Node::is_leaving)
    }

    pub fn pending_ranges(&self, peer_id: &PeerId) -> Option<&PendingRanges> {
        self.pending_ranges.get(peer_id)
    }

    pub fn has_pending_ranges(&self, peer_id: &PeerId) -> bool {
        self.pending_ranges(peer_id)
            .map_or(false, |ranges| !ranges.is_empty())
    }

    /// Checks the pending ranges to see if we have a peer that is booting and
    /// will be responsible for a key at a given position.
    pub fn booting_peers_owning_pos(&self, pos: RingPosition) -> impl Iterator<Item = PeerId> + '_ {
        let booting_peers = self.booting_peers();

        booting_peers.filter(move |booting_peer| {
            self.pending_ranges(booting_peer)
                .map_or(false, |pending_ranges| {
                    pending_ranges.iter().any(
                        |r| matches!(r, PendingRange::Pull { range, .. } if range.contains(&pos)),
                    )
                })
        })
    }

    /// Checks the pending ranges to see if we have a peer that is leaving and a
    /// given position is to be owned by some other peer. Returns IDs of the
    /// peers that will be responsible for the key at a given position.
    pub fn new_owners_for_pos(&self, pos: RingPosition) -> impl Iterator<Item = PeerId> + '_ {
        let leaving_peers = self.leaving_peers();

        leaving_peers
            .filter_map(|leaving_peer| self.pending_ranges(&leaving_peer))
            .flat_map(|pending_ranges| pending_ranges.iter())
            .filter_map(move |r| match r {
                PendingRange::Push { range, destination } if range.contains(&pos) => {
                    Some(*destination)
                }
                _ => None,
            })
    }

    /// Returns push [`PendingRange`]s in which the node having the provided
    /// [`PeerId`] is the destination.
    pub fn pending_push_ranges_to(
        &self,
        id: PeerId,
    ) -> impl Iterator<Item = KeyRange<KeyPosition>> + '_ {
        self.pending_ranges
            .values()
            .flatten()
            .filter_map(move |p| match p {
                PendingRange::Push { range, destination } if destination == &id => Some(*range),
                _ => None,
            })
    }

    pub fn is_new_owner_of_pos(&self, id: PeerId, pos: RingPosition) -> bool {
        self.new_owners_for_pos(pos).any(|owner_id| owner_id == id)
    }

    fn find_peers(&self, f: impl Fn(&Node) -> bool + 'static) -> impl Iterator<Item = PeerId> + '_ {
        self.view
            .peers
            .values()
            .filter_map(move |p| f(p).then_some(p.peer_id))
    }

    /// Installs updated [`ClusterView`] received from the consensus module and
    /// returns cluster [`Diff`].
    pub fn install_view_update(&mut self, new: ClusterView) -> Diff {
        let mut diff = Diff::default();
        let mut do_update = false;

        for (id, new) in &new.peers {
            match self.view.peers.remove(id) {
                Some(old) if &old == new => {}
                Some(old) => {
                    do_update |= self.node_updated(new);

                    diff.updated.push(NodeDiff {
                        old,
                        new: new.clone(),
                    });
                }
                None => {
                    do_update |= self.node_added(new);

                    diff.added.push(new.clone());
                }
            }
        }

        for old in mem::take(&mut self.view.peers).into_values() {
            do_update |= self.node_removed(&old);
            diff.removed.push(old);
        }

        self.view = new;

        if do_update {
            self.update_pending_ranges();
        }

        diff
    }

    /// Process a node that has been added to the cluster.
    ///
    /// A newly added node, depending on its current state, can be added to the
    /// keyspace. Returns `true` if `pending_ranges` need to be updated.
    fn node_added(&mut self, node: &Node) -> bool {
        match node.mode {
            NodeOperationMode::Started => {
                // Node has just started.
                // No need to add it to the keyspace or recalculate pending ranges.
                false
            }
            NodeOperationMode::Booting => {
                // Node is actively running migrations.
                // No need to add it to the keyspace up until it has transferred all the pending
                // ranges. Pending ranges recalculation is required, so that the current node is
                // aware of what parts of the keyspace are being transferred to the booting
                // node.
                true
            }
            NodeOperationMode::Normal | NodeOperationMode::Restarting => {
                // Node is fully provisioned and is serving reads and writes.
                // Add it to the keyspace (if it is not there already) and recalculate pending
                // ranges, just to make sure we have up to date ranges.
                if !self.keyspace.has_node(node) {
                    self.add_to_keyspace(node);
                }
                true
            }
            NodeOperationMode::Leaving => {
                // Node is already in the process of leaving the cluster.
                // Add it to keyspace, so that it can be used when pulling the pending ranges.
                // Recalculate pending ranges.
                if !self.keyspace.has_node(node) {
                    self.add_to_keyspace(node);
                }
                true
            }
            NodeOperationMode::Left => {
                // Do nothing. We didn't add it to the keyspace, and it should
                // not have affected the local cluster view.
                false
            }
        }
    }

    /// Process a node that has been updated.
    ///
    /// State transitions are controlled by FSM. A node can miss some updates,
    /// but it can be assumed that the state update is from one of the previous
    /// states, i.e. you will not see `Booting` after `Normal`.
    ///
    /// Returns `true` if `pending_ranges` need to be updated.
    fn node_updated(&mut self, node: &Node) -> bool {
        match node.mode {
            NodeOperationMode::Started => {
                // The `Started` state is initial state of the node. The way node FSM works it
                // only can transition forward, so such an update should not happen.
                // Make sure that the node is not in the keyspace.

                tracing::error!(?node, "node is updated to `Started` state");
                if self.keyspace.has_node(node) {
                    tracing::warn!(?node, "unexpected node in the keyspace");
                    self.remove_from_keyspace(node);
                }
                false
            }
            NodeOperationMode::Booting => {
                // The node has changed its mode from `Started` to `Booting`.
                // Migrations are in progress. Make sure that the node is not in the keyspace.

                if self.keyspace.has_node(node) {
                    tracing::warn!(?node, "unexpected node in the keyspace");
                    self.remove_from_keyspace(node);
                }
                true
            }
            NodeOperationMode::Normal => {
                // The node has changed its mode from `Booted` to `Normal`.
                // Migrations and hinted operations are done. Make sure that the node is in the
                // keyspace.

                if !self.keyspace.has_node(node) {
                    self.add_to_keyspace(node);
                }
                true
            }
            NodeOperationMode::Leaving => {
                // The node has changed its mode from `Normal` to `Leaving`.
                // Make sure that the node is in the keyspace (it should be
                // there already, but if `Booting` -> `Normal` update is missed,
                // we need to make sure we add it).

                if !self.keyspace.has_node(node) {
                    self.add_to_keyspace(node);
                }
                true
            }
            NodeOperationMode::Left => {
                // The node has changed its mode from `Leaving` to `Left`.
                // All the migrations streaming is done by this point. Make sure that the node
                // is evicted from the keyspace.

                self.remove_from_keyspace(node);
                true
            }
            NodeOperationMode::Restarting => false,
        }
    }

    /// Process a node that has been removed from the cluster.
    ///
    /// Returns `true` if `pending_ranges` need to be updated.
    fn node_removed(&mut self, node: &Node) -> bool {
        use NodeOperationMode as M;
        match node.mode {
            M::Started => false,
            M::Booting => true,
            M::Normal | M::Leaving | M::Left | M::Restarting => {
                self.remove_from_keyspace(node);
                true
            }
        }
    }

    fn add_to_keyspace(&mut self, node: &Node) {
        if let Err(err) = self.keyspace.add_node(node) {
            tracing::warn!(?err, ?node, "failed to add node to the keyspace");
        }
    }

    fn remove_from_keyspace(&mut self, node: &Node) {
        if let Err(err) = self.keyspace.remove_node(node) {
            tracing::warn!(?err, ?node, "failed to remove node from the keyspace");
        }
    }

    fn update_pending_ranges(&mut self) {
        let _ = self
            .keyspace
            .pending_ranges(
                self.find_peers(Node::is_booting).collect(),
                self.find_peers(Node::is_leaving).collect(),
            )
            .map(|ranges| self.pending_ranges = ranges)
            .map_err(|err| tracing::warn!(?err, "failed to update pending ranges"));
    }

    pub fn validate_version(
        &self,
        cluster_view_version: u128,
    ) -> Result<&Self, ViewVersionMismatch> {
        if self.view.version != cluster_view_version {
            return Err(ViewVersionMismatch);
        }

        Ok(self)
    }

    pub fn validate_member(&self, id: &libp2p::PeerId) -> Result<&Self, NotMemberError> {
        if !self.view.peers.keys().any(|i| &i.id == id) {
            return Err(NotMemberError);
        }

        Ok(self)
    }
}

impl Node {
    /// Generate a node with empty addresses and provided peer id.
    ///
    /// This is used for testing purposes.
    pub fn generate(peer_id: PeerId) -> Self {
        Node {
            peer_id,
            addr: Multiaddr::empty(),
            mode: NodeOperationMode::Normal,
        }
    }
}

#[cfg(test)]
#[allow(clippy::reversed_empty_ranges)]
mod tests {
    use {
        super::*,
        crate::cluster::{
            keyspace::HashRing,
            replication::ConsistencyLevel,
            test_util::{pulls_from_pushes, push, push_range, PEERS},
        },
        std::collections::BTreeSet,
    };

    struct InstallViewTestCase {
        name: &'static str,
        replication_strategy: Strategy,
        vnodes_count: usize,
        // If cluster has some initial state, it's represented by `local_node` + `known_nodes`.
        known_nodes: Vec<(usize, NodeOperationMode)>,
        // Updated cluster state. The difference between initial and updated state triggers the
        // pending ranges recalculation.
        updated_view: ClusterView,
        expected_ranges: Vec<(usize, Vec<PendingRange<RingPosition>>)>,
    }

    impl From<(usize, NodeOperationMode)> for Node {
        fn from((i, mode): (usize, NodeOperationMode)) -> Self {
            Node {
                peer_id: PEERS[i].peer_id,
                addr: Multiaddr::empty(),
                mode,
            }
        }
    }

    impl From<(u128, Vec<(usize, NodeOperationMode)>)> for ClusterView {
        fn from((version, nodes): (u128, Vec<(usize, NodeOperationMode)>)) -> Self {
            let peers = nodes
                .into_iter()
                .map(Into::into)
                .map(|n: Node| (n.peer_id, n));
            ClusterView {
                peers: peers.collect(),
                version,
            }
        }
    }

    fn assert_cluster_view_update(test: InstallViewTestCase) {
        let mut cluster = Cluster::new(test.replication_strategy.clone());

        // Replace keyspace with a new one, so that we can control number of virtual
        // nodes on the ring.
        let mut ring = HashRing::new(test.vnodes_count);
        for (id, mode) in test.known_nodes {
            let node: Node = (id, mode).into();
            cluster.view.peers.insert(PEERS[id].peer_id, node.clone());
            if node.mode == NodeOperationMode::Normal {
                ring.add_node(PEERS[id].peer_id).unwrap();
            }
        }
        cluster.keyspace = hashring::Keyspace::with_ring(ring, test.replication_strategy);

        // Make sure that pending ranges are calculates for the initial state.
        cluster.update_pending_ranges();

        // Transform expected ranges from the format that we use in the test.
        let expected_ranges = test
            .expected_ranges
            .into_iter()
            .map(|(idx, ranges)| (PEERS[idx].peer_id, BTreeSet::from_iter(ranges)))
            .collect::<BTreeMap<_, _>>();
        let expected_ranges = pulls_from_pushes(expected_ranges);

        // Install view update and verify the result.
        cluster.install_view_update(test.updated_view);
        assert_eq!(
            &cluster.pending_ranges,
            &expected_ranges,
            "{}: {}",
            test.name,
            {
                // Pretty print the difference.
                difference::Changeset::new(
                    &format!("{:#?}", &expected_ranges),
                    &format!("{:#?}", &cluster.pending_ranges),
                    " ",
                )
            }
        );
    }

    #[test]
    fn install_view_update() {
        use NodeOperationMode::*;

        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        let mut tests = vec![
            InstallViewTestCase {
                name: "no peers, local node started (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Started)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node started (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Started)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node started (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Started), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node started (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Started), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node booting (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Booting)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node booting (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Booting)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node booting (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Booting), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![(1, vec![push(3..0, 0)])],
            },
            InstallViewTestCase {
                name: "existing cluster, local node booting (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Booting), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![(1, vec![push(3..0, 0)]), (3, vec![push(3..0, 0)])],
            },
            InstallViewTestCase {
                name: "local node normal, discovers started nodes (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal)],
                updated_view: (1, vec![
                    (0, Normal),
                    (1, Normal),
                    (2, Started),
                    (3, Started),
                ])
                    .into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "local node normal, discovers started nodes (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal)],
                updated_view: (1, vec![
                    (0, Normal),
                    (1, Normal),
                    (2, Started),
                    (3, Started),
                ])
                    .into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "local node normal, discovers booting node (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal)],
                updated_view: (1, vec![
                    (0, Normal),
                    (1, Normal),
                    (2, Booting),
                    (3, Started),
                ])
                    .into(),
                expected_ranges: vec![(0, vec![push(1..2, 2)])],
            },
            InstallViewTestCase {
                name: "local node normal, discovers booting node (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (4, Normal)],
                updated_view: (1, vec![
                    (0, Normal),
                    (1, Normal),
                    (2, Booting),
                    (3, Started),
                    (4, Normal),
                ])
                    .into(),
                expected_ranges: vec![
                    (0, vec![push(1..2, 2)]),
                    (1, vec![push(1..2, 2)]),
                    (4, vec![push(1..2, 2)]),
                ],
            },
            InstallViewTestCase {
                name: "no peers, local node done booting (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node done booting (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![],
                updated_view: (1, vec![(0, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node done booting (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node done booting (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "local node is normal, other node done booting (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Booting), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "local node is normal, other node done booting (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(1, Normal), (2, Booting), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node is leaving (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal)],
                updated_view: (1, vec![(0, Leaving)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node is leaving (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Normal)],
                updated_view: (1, vec![(0, Leaving)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node is leaving (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Leaving), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![(0, vec![push(3..0, 1)])],
            },
            InstallViewTestCase {
                name: "existing cluster, local node is leaving (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Leaving), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![(0, vec![push(3..0, 3)]), (1, vec![push(3..0, 3)])],
            },
            InstallViewTestCase {
                name: "local node is normal, another node is leaving (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Leaving), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![(1, vec![push(0..1, 2)])],
            },
            InstallViewTestCase {
                name: "local node is normal, another node is leaving (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Normal), (1, Leaving), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![
                    (0, vec![push(3..0, 2)]),
                    (1, vec![push(0..1, 2), push(2..3, 2), push(3..0, 2)]),
                    (3, vec![push(2..3, 2), push(0..1, 2)]),
                ],
            },
            InstallViewTestCase {
                name: "no peers, local node has left (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Leaving)],
                updated_view: (1, vec![(0, Left)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "no peers, local node has left (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Leaving)],
                updated_view: (1, vec![(0, Left)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node has left (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Leaving), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Left), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, local node has left (replicated)",
                vnodes_count: 1,
                replication_strategy: replicated.clone(),
                known_nodes: vec![(0, Leaving), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(0, Left), (1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, normal local node removed (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated.clone(),
                known_nodes: vec![(0, Normal), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
            InstallViewTestCase {
                name: "existing cluster, booting local node removed (non-replicated)",
                vnodes_count: 1,
                replication_strategy: non_replicated,
                known_nodes: vec![(0, Booting), (1, Normal), (2, Normal), (3, Normal)],
                updated_view: (1, vec![(1, Normal), (2, Normal), (3, Normal)]).into(),
                expected_ranges: vec![],
            },
        ];

        // More complex cases.

        {
            // Local node is booting, cluster is replicated and has vnodes.
            let all_ranges = vec![
                push_range(444400609862258242..555500550042804847, 0),
                push_range(555500550042804847..2979629758535287163, 0),
                push_range(2979629758535287163..4080287171546526274, 0),
                push_range(5962916429082114754..7079604129657860890, 0),
                push_range(7079604129657860890..10990937191143821813, 0),
                push_range(10990937191143821813..11319066114435793127, 0),
                push_range(11481432248301971266..13937008202598308873, 0),
                push_range(13937008202598308873..111100983615508364, 0),
            ];
            tests.push(InstallViewTestCase {
                name: "local node starts booting, cluster is replicated and has vnodes",
                vnodes_count: 3,
                replication_strategy: replicated,
                known_nodes: vec![(0, Started), (1, Normal), (3, Normal), (4, Normal)],
                updated_view: (1, vec![(0, Booting), (1, Normal), (3, Normal), (4, Normal)]).into(),
                expected_ranges: vec![
                    (1, all_ranges.clone()),
                    (3, all_ranges.clone()),
                    (4, all_ranges),
                ],
            });
        }

        for test in tests {
            assert_cluster_view_update(test);
        }
    }

    #[test]
    fn mode_cmp() {
        let mode = NodeOperationMode::Normal;
        assert!(mode > NodeOperationMode::Started);
        assert!(mode > NodeOperationMode::Booting);
        assert_eq!(mode, NodeOperationMode::Normal);
        assert!(mode <= NodeOperationMode::Normal);
        assert!(mode < NodeOperationMode::Leaving);
        assert!(mode <= NodeOperationMode::Leaving);
    }
}
