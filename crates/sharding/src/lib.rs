use {
    core::panic,
    indexmap::IndexSet,
    itertools::Itertools,
    std::{
        array,
        char::MAX,
        collections::{BTreeSet, HashMap, HashSet},
        default,
        fmt,
        hash::{DefaultHasher, Hash, Hasher, SipHasher},
        iter,
        marker::PhantomData,
        ops::{Range, RangeInclusive},
    },
    xxhash_rust::{
        const_xxh3::{xxh3_128, xxh3_64},
        xxh3::{self, xxh3_64_with_seed, Xxh3},
    },
};

const MAX_NODES: usize = 256;

/// Identifier of a shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardId(u16);

impl ShardId {
    /// Creates a [`ShardId`] identifying the shard resposible for the given
    /// key.
    pub fn from_key(key: u64) -> Self {
        let mut bytes = [0; 2];
        bytes.copy_from_slice(&key.to_be_bytes()[..2]);
        Self(u16::from_be_bytes(bytes))
    }

    /// Returns the keyrange the shard with this [`ShardId`] is resposible for.
    pub fn key_range(&self) -> RangeInclusive<u64> {
        let start = (self.0 as u64) << 48;
        let end = if self.0 == u16::MAX {
            u64::MAX
        } else {
            (((self.0 as u64) + 1) << 48) - 1
        };
        RangeInclusive::new(start, end)
    }
}

/// A space of keys divided into a set of shards.
#[derive(Clone)]
pub struct Keyspace<const RF: usize, NodeId> {
    nodes: IndexSet<NodeId>,
    shards: Vec<Shard<RF>>,
}

#[derive(Clone, Copy, Debug)]
struct Shard<const RF: usize> {
    replicas: [usize; RF],
}

impl<const RF: usize, N> Keyspace<RF, N> {
    /// Creates a new [`Keyspace`].
    pub fn new(
        nodes: impl IntoIterator<Item = N>,
        hasher_factory: &impl HasherFactory,
        mut sharding_strategy: impl Strategy<N>,
    ) -> Result<Self, Error>
    where
        N: Hash + Eq + Ord,
    {
        if RF == 0 || RF > MAX_NODES {
            return Err(Error::InvalidReplicationFactor {
                min: 1,
                max: MAX_NODES,
            });
        }

        let nodes: IndexSet<_> = nodes.into_iter().collect();
        let nodes_count = nodes.len();
        if nodes_count < RF || nodes_count > MAX_NODES {
            return Err(Error::InvalidNodesCount {
                min: RF,
                max: MAX_NODES,
            });
        }

        let replicas = [0; RF];
        let n_shards = u16::MAX as usize + 1;
        let mut shards: Vec<_> = (0..n_shards).map(|_| Shard { replicas }).collect();

        let mut node_ranking: Vec<_> = nodes
            .iter()
            .enumerate()
            .map(|(idx, id)| (0, id, idx))
            .collect();

        for (shard_idx, shard) in shards.iter_mut().enumerate() {
            for (score, node_id, _) in &mut node_ranking {
                let mut hasher = hasher_factory.new_hasher();
                (node_id, shard_idx).hash(&mut hasher);
                *score = hasher.finish();
            }
            node_ranking.sort_unstable();

            let mut cursor = 0;
            for replica_idx in &mut shard.replicas {
                loop {
                    if cursor == node_ranking.len() {
                        return Err(Error::IncompleteReplicaSet);
                    }

                    let (_, node_id, node_idx) = node_ranking[cursor];

                    if sharding_strategy.is_suitable_replica(shard_idx, node_id) {
                        *replica_idx = node_idx;
                        cursor += 1;
                        break;
                    }
                }
            }
        }

        Ok(Self { nodes, shards })
    }

    /// Returns an [`Iterator`] of replicas responsible for the shard with the
    /// given [`ShardId`].
    pub fn shard_replicas(&self, shard_id: ShardId) -> impl Iterator<Item = &N> {
        self.shards[shard_id.0 as usize]
            .replicas
            .iter()
            .map(|idx| &self.nodes[*idx])
    }

    /// Returns an [`Iterator`] of nodes in the [`Keyspace`].
    pub fn nodes(&self) -> impl Iterator<Item = &N> {
        self.nodes.iter()
    }
}

/// [`Hasher`] factory.
pub trait HasherFactory {
    type Hasher: Hasher;

    /// Creates a new [`Hasher`].
    fn new_hasher(&self) -> Self::Hasher;
}

impl<F, H> HasherFactory for F
where
    F: Fn() -> H,
    H: Hasher,
{
    type Hasher = H;

    fn new_hasher(&self) -> Self::Hasher {
        (self)()
    }
}

/// Sharding strategy.
pub trait Strategy<N> {
    /// Indicates whether the specified node is suitable to be used as a
    /// replica for the specified shard.
    ///
    /// This function can only be called once per every combination of
    /// `shard_idx` and `node_id`.
    fn is_suitable_replica(&mut self, shard_idx: usize, node_id: &N) -> bool;
}

impl<F, N> Strategy<N> for F
where
    F: FnMut(usize, &N) -> bool,
{
    fn is_suitable_replica(&mut self, shard_idx: usize, node_id: &N) -> bool {
        (self)(shard_idx, node_id)
    }
}

/// Default sharding [`Strategy`] that considers every node to be equally
/// suitable to be a replica for any shard.
#[derive(Clone, Copy, Debug)]
pub struct DefaultStrategy;

impl<N> Strategy<N> for DefaultStrategy {
    fn is_suitable_replica(&mut self, _shard_idx: usize, _node_id: &N) -> bool {
        true
    }
}

/// Error of [`Keyspace::new`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Replication factor should be in range [{}, {}]", min, max)]
    InvalidReplicationFactor { min: usize, max: usize },

    #[error("Nodes count should be in range [{}, {}]", min, max)]
    InvalidNodesCount { min: usize, max: usize },

    #[error("Sharding strategy didn't select enough nodes to fill a replica set")]
    IncompleteReplicaSet,
}

#[cfg(any(test, feature = "testing"))]
impl<const RF: usize, N> Keyspace<RF, N>
where
    N: Eq + Hash + fmt::Debug,
{
    fn assert_variance_and_stability(
        &self,
        old: &Self,
        expected_variance: &ExpectedDistributionVariance,
    ) {
        dbg!(&self.nodes);
        let mut shards_per_node = HashMap::<usize, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas.iter() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        let mean = self.shards.len() * RF / self.nodes.len();
        let max_deviation = shards_per_node
            .values()
            .copied()
            .map(|n| (mean as isize - n as isize).abs() as usize)
            .max()
            .unwrap();
        let coefficient = max_deviation as f64 / mean as f64;

        dbg!(mean, max_deviation, coefficient);

        let allowed_coefficient = expected_variance.get(self.nodes.len());
        assert!(coefficient <= allowed_coefficient);

        let old_nodes: HashSet<_> = old.nodes().collect();
        let new_nodes: HashSet<_> = self.nodes().collect();

        let mut allowed_shard_movements = 0;

        let removed: HashSet<_> = old_nodes.difference(&new_nodes).collect();
        let added: HashSet<_> = new_nodes.difference(&old_nodes).collect();

        if !removed.is_empty() {
            for shard in &old.shards {
                for replica_idx in &shard.replicas {
                    if removed.contains(&old.nodes.get_index(*replica_idx).unwrap()) {
                        allowed_shard_movements += 1;
                    }
                }
            }
        }

        allowed_shard_movements +=
            (((added.len() * RF * mean) as f64) * (1.0 + allowed_coefficient)) as usize;

        let mut shard_movements = 0;
        let mut replicas: HashSet<_>;
        for shard_idx in 0..old.shards.len() {
            let shard_id = ShardId(shard_idx as u16);

            replicas = self.shard_replicas(shard_id).collect();

            for node_id in old.shard_replicas(shard_id) {
                if !replicas.contains(node_id) {
                    shard_movements += 1;
                }
            }
        }

        dbg!(shard_movements, removed, added);
        assert!(
            shard_movements <= allowed_shard_movements,
            "{shard_movements} <= {allowed_shard_movements}"
        );
    }
}

/// Specifies the expected shard distribution variance.
///
/// Each element of the [`Vec`] should be an `(N, C)` tuple, where `N` is number
/// of nodes in the cluster and `C` is a maximum allowed mean absolute deviation
/// coefficient.
///
/// For example, coefficient of `0.1` means that the maximum and the minimum
/// amount of shards per node are allowed to deviate from the mean (the average)
/// by 10%.
/// Let's say the expected mean is 100 shards per node, then coefficient of
/// `0.1` will allow the number of shards per node to deviate in the range of
/// `90..=110`.
///
/// Not every cluster size needs to be specified, one may specify
/// the expected variance as a range:
/// ```
/// sharding::ExpectedDistributionVariance(vec![(4, 0.01), (8, 0.02), (256, 0.1)]);
/// ```
/// This would mean that for a cluster size of up to 4 nodes the coefficient is
/// `0.01`, for a cluster size in the range of `5..8` the coefficient is `0.02`
/// and so on.
pub struct ExpectedDistributionVariance(pub Vec<(usize, f64)>);

impl ExpectedDistributionVariance {
    fn get(&self, nodes_count: usize) -> f64 {
        for (count, coef) in self.0.iter().copied() {
            if nodes_count <= count {
                return coef;
            }
        }

        panic!("Missing coefficient for nodes_count: {nodes_count}");
    }
}

#[cfg(any(test, feature = "testing"))]
pub fn test_suite<const RF: usize, NodeId, S>(
    hasher_factory: impl HasherFactory,
    sharding_strategy_factory: impl FnMut() -> S,
    node_id_factory: impl FnMut() -> NodeId,
    expected_variance: ExpectedDistributionVariance,
) where
    NodeId: Eq + Hash + Ord + Clone + fmt::Debug,
    S: Strategy<NodeId>,
{
    use rand::random;

    let new_hasher = hasher_factory;
    let mut new_strategy = sharding_strategy_factory;
    let mut new_node_id = node_id_factory;

    let initial_nodes = (0..RF).map(|_| new_node_id());

    let mut keyspace: Keyspace<RF, _> =
        Keyspace::new(initial_nodes, &new_hasher, new_strategy()).unwrap();
    keyspace.assert_variance_and_stability(&keyspace, &expected_variance);

    for _ in 0..10 {
        let nodes = keyspace.nodes().cloned().chain([new_node_id()]);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    for _ in 0..10 {
        let mut nodes = keyspace.nodes.clone();
        nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());

        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // let nodes = keyspace.nodes().cloned().chain(17..256);
    // let new_keyspace = Keyspace::new(nodes, hasher, strategy).unwrap();
    // new_keyspace.assert_variance_and_stability(&keyspace,
    // &expected_variance); keyspace = new_keyspace;
}

#[test]
fn t() {
    let mut next_node_id = 0;
    test_suite::<3, _, _>(
        || DefaultHasher::new(),
        || DefaultStrategy,
        || {
            next_node_id += 1;
            next_node_id
        },
        ExpectedDistributionVariance(vec![(16, 0.03), (256, 0.13)]),
    )
}

#[test]
fn test_shard_id() {
    fn assert_from_key(u: [u8; 8], shard_id: u16) {
        assert_eq!(ShardId::from_key(u64::from_be_bytes(u)), ShardId(shard_id));
    }

    assert_from_key([0, 0, 0, 0, 0, 0, 0, 0], 0);
    assert_from_key([0, 0, 1, 2, 3, 4, 5, 6], 0);
    assert_from_key([0, 1, 1, 2, 3, 4, 5, 6], 1);
    assert_from_key([0, 255, 1, 2, 3, 4, 5, 6], 255);
    assert_from_key([1, 0, 1, 2, 3, 4, 5, 6], 256);
    assert_from_key([255, 255, 1, 2, 3, 4, 5, 6], u16::MAX);

    fn assert_key_range(shard_id: u16, start: [u8; 8], end: [u8; 8]) {
        let start = u64::from_be_bytes(start);
        let end = u64::from_be_bytes(end);
        assert_eq!(
            ShardId(shard_id).key_range(),
            RangeInclusive::new(start, end)
        );
    }

    assert_key_range(0, [0, 0, 0, 0, 0, 0, 0, 0], [
        0, 0, 255, 255, 255, 255, 255, 255,
    ]);
    assert_key_range(1, [0, 1, 0, 0, 0, 0, 0, 0], [
        0, 1, 255, 255, 255, 255, 255, 255,
    ]);
    assert_key_range(u16::MAX, [255, 255, 0, 0, 0, 0, 0, 0], [
        255, 255, 255, 255, 255, 255, 255, 255,
    ]);
}
