use {
    crate::cluster::replication::Strategy,
    itertools::Itertools,
    rand::Rng,
    std::collections::HashSet,
};

#[cfg(test)]
mod tests;

use {
    super::partitioner::{DefaultPartitioner, Partitioner, DEFAULT_SEED1, DEFAULT_SEED2},
    crate::{
        cluster::{
            error::{ClusterError, HashRingError},
            keyspace::{
                self,
                hashring::RingDirection::{Clockwise, CounterClockwise},
                pending_ranges::{PendingRanges, PendingRangesCalculator},
                range::KeyRange,
            },
            replication::ReplicaSet,
        },
        PeerId,
    },
    std::{
        collections::{BTreeMap, HashMap, VecDeque},
        fmt::Debug,
        hash::Hash,
        ops::Bound::{Excluded, Unbounded},
    },
};

/// Defines the default number of tokens per node.
///
/// Multiple tokens per node allows to distribute the load across the cluster in
/// a more balanced way. Since positions on the ring are assigned based on the
/// node hash, multiple instances of such positions split the key space more
/// evenly.
///
/// This improves the load distribution on updates as well: when a node is
/// removed from the cluster, its tokens affect *multiple* remaining nodes.
pub const DEFAULT_TOKENS_PER_NODE: usize = 64;

/// Defines an arbitrary position on the ring.
pub type RingPosition = u64;

#[derive(Clone, Debug)]
pub struct Positioned<T> {
    pub position: RingPosition,
    pub inner: T,
}

/// Node that can be assigned a position on the ring.
pub trait RingNode:
    Hash + Clone + Copy + Debug + Eq + PartialEq + Ord + PartialOrd + 'static
{
}

/// Blanket implementation of `RingNode` for all types that implement the
/// necessary traits.
impl<T> RingNode for T where
    T: Hash + Clone + Copy + Debug + Eq + PartialEq + Ord + PartialOrd + 'static
{
}

/// Defines an ownership over a position on the ring.
pub type RingToken<'a, T = PeerId> = (&'a RingPosition, &'a T);

/// Defines the direction in which the ring is traversed.
#[derive(Clone, Copy)]
pub enum RingDirection {
    Clockwise,
    CounterClockwise,
}

/// Defines an iterator over a range of ring token positions.
pub enum HashRingIter<T, U> {
    Clockwise(T),
    CounterClockwise(U),
}

impl<T, U, V> Iterator for HashRingIter<T, U>
where
    T: Iterator<Item = V>,
    U: Iterator<Item = V>,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Clockwise(iter) => iter.next(),
            Self::CounterClockwise(iter) => iter.next(),
        }
    }
}

impl<T, U, V> DoubleEndedIterator for HashRingIter<T, U>
where
    T: Iterator<Item = V> + DoubleEndedIterator,
    U: Iterator<Item = V> + DoubleEndedIterator,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Clockwise(iter) => iter.next_back(),
            Self::CounterClockwise(iter) => iter.next_back(),
        }
    }
}

/// Represents a consistent hash ring. Nodes are assigned positions on the ring,
/// effectively becoming responsible for a range of keys (counter-clockwise from
/// their positions and till the previous node).
#[derive(Clone)]
pub struct HashRing<N = PeerId, P = DefaultPartitioner> {
    /// Partitioner used to compute ring positions.
    partitioner: P,

    /// List of nodes in the ring. The key is the node's ID.
    nodes: HashMap<u16, N>,

    /// Counter used to generate unique IDs for nodes.
    node_counter: u16,

    /// Once a node is removed from the ring, its ID is added to this queue, so
    /// that it can be reused.
    free_node_ids: VecDeque<u16>,

    /// How many tokens are assigned for each node.
    tokens_per_node: usize,

    /// The ring positions assigned to nodes (sorted in ascending order).
    tokens: BTreeMap<RingPosition, u16>,
}

impl<T: RingNode> Default for HashRing<T> {
    fn default() -> Self {
        Self::new(DEFAULT_TOKENS_PER_NODE)
    }
}

/// Allows to create a hash ring from a set of ring positions.
///
/// Useful for testing.
#[cfg(test)]
impl<N: RingNode> From<Vec<(RingPosition, N)>> for HashRing<N> {
    fn from(positions: Vec<(RingPosition, N)>) -> Self {
        let mut ring = Self::new(1);

        // Extract unique node ids, and populate ring nodes list.
        for (_, node) in &positions {
            ring.nodes.insert(ring.node_counter, *node);
            ring.node_counter += 1;
        }

        for (pos, node) in positions {
            ring.tokens.insert(
                pos,
                ring.nodes
                    .iter()
                    .find_map(|(id, n)| if *n == node { Some(*id) } else { None })
                    .unwrap(),
            );
        }

        ring
    }
}

impl<N: RingNode> Debug for HashRing<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashRing")
            .field("nodes", &self.nodes)
            .field("node_counter", &self.node_counter)
            .field("free_node_ids", &self.free_node_ids)
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

impl<N: RingNode> HashRing<N> {
    /// Creates a new hash ring.
    pub fn new(tokens_per_node: usize) -> Self {
        Self {
            partitioner: DefaultPartitioner::default(),
            nodes: HashMap::new(),
            node_counter: 0,
            free_node_ids: VecDeque::new(),
            tokens_per_node,
            tokens: BTreeMap::new(),
        }
    }

    /// Checks if the ring contains a given node.
    pub fn has_node(&self, node: &N) -> bool {
        self.nodes.values().any(|n| n == node)
    }

    /// Registers a new node on the ring.
    ///
    /// Position of virtual nodes is computed deterministically using keyspace
    /// partitioner.
    pub fn add_node(&mut self, node: N) -> Result<(), ClusterError> {
        // Make sure that the node is not already in the ring.
        if self.has_node(&node) {
            return Err(HashRingError::NodeAlreadyExists.into());
        }

        // Either reuse a freed ID or allocate a new one.
        let node_id = match self.free_node_ids.pop_front() {
            Some(free_id) => free_id,
            None => {
                let id = self.node_counter;
                self.node_counter = self
                    .node_counter
                    .checked_add(1)
                    .ok_or(HashRingError::OutOfIds)?;
                id
            }
        };
        self.nodes.insert(node_id, node);

        // Make sure that the position is not already taken.
        let positions = self.node_positions(&node);
        if positions.iter().any(|pos| self.tokens.contains_key(pos)) {
            return Err(HashRingError::TokenAlreadyExists.into());
        }

        // Save ring positions assigned to the new node.
        for pos in positions {
            self.tokens.insert(pos, node_id);
        }

        Ok(())
    }

    /// Assigns node to a given ring positions.
    ///
    /// Useful for testing and simulation.
    #[cfg(test)]
    pub fn set_node(&mut self, node: N, positions: Vec<RingPosition>) {
        let node_id = self.node_counter;
        self.nodes.insert(node_id, node);
        self.node_counter += 1;
        for pos in positions {
            self.tokens.insert(pos, node_id);
        }
    }

    /// Removes a node from the ring.
    ///
    /// All ring virtual node positions assigned to the node are removed as
    /// well.
    pub fn remove_node(&mut self, node: &N) -> Result<(), ClusterError> {
        // Remove ring positions assigned to the node.
        for pos in self.node_positions(node) {
            self.tokens.remove(&pos);
        }

        // Remove the node itself.
        let node_id = self
            .nodes
            .iter()
            .find_map(|(id, n)| if n == node { Some(*id) } else { None })
            .ok_or(HashRingError::NodeNotFound)?;
        self.nodes.remove(&node_id);

        // Add the node ID to the free list.
        self.free_node_ids.push_back(node_id);

        Ok(())
    }

    /// Returns number of nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the primary node responsible for the given key.
    ///
    /// Due to replication key may land on several nodes, but the primary
    /// destination is the node controlling a ring position that comes
    /// immediately after the key.
    pub fn primary_node<K: Hash>(&self, key: &K) -> Option<N> {
        self.primary_token(key).map(|token| *token.1)
    }

    pub fn natural_replicas(&self, pos: RingPosition) -> Vec<N> {
        let mut nodes: Vec<N> = Vec::with_capacity(3);

        for (_, &node) in self.tokens(pos.wrapping_add(1), Clockwise) {
            nodes.push(node);
            if nodes.len() >= 3 {
                break;
            }
        }

        nodes
    }

    /// Returns the token of a node that owns a range for the given key.
    ///
    /// Double hashing of multiple virtual nodes (per node) is used to avoid
    /// non-uniform distribution of keys across the ring.
    pub fn primary_token<K: Hash>(&self, key: &K) -> Option<RingToken<N>> {
        self.tokens(self.partitioner.key_position(key), Clockwise)
            .next()
    }

    /// Returns assigned node positions (tokens) starting from the given
    /// location on the ring.
    ///
    /// To start from the beginning, use `0` as the `start` position.
    /// Each node gets several virtual nodes, where each virtual node is placed
    /// on the ring. Therefore a node can get multiple tokens on the ring.
    ///
    /// One can go in both directions, clockwise and counter-clockwise, allowing
    /// to see both the next assigned positions and the previous ones.
    ///
    /// Note: since we semantically treat the ordered set as a circle, the
    /// returned iterator will be chained with another iterator allowing
    /// wrapping around the ring. See tests for examples.
    #[must_use]
    pub fn tokens(
        &self,
        start: RingPosition,
        dir: RingDirection,
    ) -> impl DoubleEndedIterator<Item = RingToken<N>> {
        match dir {
            Clockwise => HashRingIter::Clockwise(
                self.tokens
                    .range(start..)
                    .chain(self.tokens.range(0..start)),
            ),
            CounterClockwise => HashRingIter::CounterClockwise(
                self.tokens
                    .range(..=start)
                    .rev()
                    // We must exclude start position i.e. `(start..)`.
                    .chain(self.tokens.range((Excluded(start), Unbounded)).rev()),
            ),
        }
        .map(|(pos, node_id)| (pos, self.nodes.get(node_id).expect("node not found")))
    }

    /// Returns ring position to which a given key will be assigned.
    pub fn key_position<K: Hash>(&self, key: &K) -> RingPosition {
        self.partitioner.key_position(key)
    }

    /// Returns calculated ring positions for each virtual node assigned to a
    /// given node.
    pub fn node_positions(&self, node: &N) -> Vec<RingPosition> {
        let h1 = self.partitioner.key_position_seeded(node, DEFAULT_SEED1);
        let h2 = self.partitioner.key_position_seeded(node, DEFAULT_SEED2);

        (0..self.tokens_per_node)
            .map(|i| h1.wrapping_add((i as RingPosition).wrapping_mul(h2)))
            .collect()
    }

    /// Returns the key range owned by a node, if the node is located at a given
    /// position.
    ///
    /// If range is available, it always ends at the given position, and starts
    /// at the position to the left (counter-clockwise) of the provided `pos`.
    /// If range is not available (e.g. on empty ring) `None` is returned.
    ///
    /// Note: since we semantically treat the ordered set as a circle, the key
    /// range wraps around.
    pub fn key_range(&self, pos: RingPosition) -> Option<KeyRange<RingPosition>> {
        if self.tokens.is_empty() {
            return None;
        }
        let prev_pos = self.tokens(pos, Clockwise).next_back();
        let start = prev_pos.map_or(0, |token| *token.0);
        Some(KeyRange::new(start, pos))
    }

    /// Returns size of the ring, i.e. number of contained tokens.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns `true` if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct Keyspace {
    ring: HashRing,
    strategy: Strategy,
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl Keyspace {
    pub fn new(strategy: Strategy) -> Self {
        Self {
            ring: HashRing::new(DEFAULT_TOKENS_PER_NODE),
            strategy,
        }
    }

    pub fn with_ring(ring: HashRing, strategy: Strategy) -> Self {
        Self { ring, strategy }
    }
}

impl keyspace::Keyspace for Keyspace {
    type Node = PeerId;

    fn replication_strategy(&self) -> &Strategy {
        &self.strategy
    }

    fn key_position<K: Hash>(&self, key: &K) -> RingPosition {
        self.ring.key_position(key)
    }

    fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    fn has_node<N: AsRef<Self::Node>>(&self, node: N) -> bool {
        self.ring.has_node(node.as_ref())
    }

    fn add_node<N: AsRef<Self::Node>>(&mut self, node: N) -> Result<(), ClusterError> {
        self.ring.add_node(*node.as_ref())
    }

    fn remove_node<T: AsRef<Self::Node>>(&mut self, node: T) -> Result<(), ClusterError> {
        self.ring.remove_node(node.as_ref())
    }

    fn replica_set(&self, pos: RingPosition) -> ReplicaSet<Vec<Self::Node>, Self::Node> {
        let natural_replicas = self.strategy.natural_replicas(&self.ring, pos);
        ReplicaSet::new(natural_replicas, self.strategy.clone())
    }

    fn pending_ranges(
        &self,
        joining_peers: Vec<Self::Node>,
        leaving_peers: Vec<Self::Node>,
    ) -> Result<BTreeMap<Self::Node, PendingRanges>, ClusterError> {
        PendingRangesCalculator::new(&self.ring, self.strategy.clone())
            .with_joining_peers(joining_peers)
            .with_leaving_peers(leaving_peers)
            .ranges()
    }
}

#[test]
fn test_variance() {
    use {core::panic, rand::random, std::ops::Add};

    fn case(nodes: usize, tokens_per_node: usize) {
        let mut ring = HashRing::new(64);
        for _ in 0..nodes {
            ring.add_node(PeerId::random()).unwrap();
        }

        let mut hits_per_node = HashMap::<PeerId, u64>::new();
        let mut rng = rand::thread_rng();

        for _ in 0..10_000_000 {
            let peer_id = ring.primary_node(&rng.gen::<u64>()).unwrap();

            *hits_per_node.entry(peer_id).or_default() += 1;
        }

        let distr = hits_per_node.values().collect_vec();

        let mut keys_per_node = HashMap::<u16, u64>::new();

        let mut prev_key = 0;
        for (key, node_id) in &ring.tokens {
            assert!(prev_key < *key);

            *keys_per_node.entry(*node_id).or_default() += key - prev_key;
            prev_key = *key;
        }

        let key_distr = keys_per_node
            .values()
            .map(|&n| n as f64 / u64::MAX as f64)
            .collect_vec();

        println!(
            "nodes: {nodes}, tokens: {tokens_per_node}, distribution: {distr:?}, \
             key_distribution: {key_distr:?}"
        );
    }

    for _ in 0..10 {
        case(12, 1024);
    }

    panic!();
}
