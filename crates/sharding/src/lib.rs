use {
    core::panic,
    indexmap::IndexSet,
    itertools::Itertools,
    std::{
        char::MAX,
        collections::{BTreeSet, HashMap, HashSet},
        default,
        hash::{DefaultHasher, Hash, Hasher, SipHasher},
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
    /// Creates a new [`Keyspace`] with the required amount of nodes.
    pub fn new(nodes: [N; RF]) -> Self
    where
        N: Hash + Eq,
    {
        let mut replicas = [0; RF];
        for idx in 0..nodes.len() {
            replicas[idx] = idx;
        }

        let n_shards = u16::MAX as usize + 1;

        Self {
            nodes: nodes.into_iter().collect(),
            shards: (0..n_shards).map(|_| Shard { replicas }).collect(),
        }
    }

    /// Returns an [`Iterator`] of replicas responsible for the given
    /// [`ShardId`].
    pub fn replicas(&self, shard_id: ShardId) -> impl Iterator<Item = &N> {
        self.shards[shard_id.0 as usize]
            .replicas
            .iter()
            .map(|idx| &self.nodes[*idx])
    }

    /// Starts a [`Resharding`] process.
    pub fn reshard<H>(self, hasher_factory: H) -> Resharding<RF, N, H, DefaultStrategy>
    where
        H: HasherFactory,
    {
        Resharding {
            keyspace: self,
            hasher_factory,
            strategy: DefaultStrategy,
        }
    }

    fn assert_variance(&self, max_deviasion_coefficient: f64) {
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
        assert!(coefficient <= max_deviasion_coefficient);
    }

    // fn assert_data_movement(&self, old: &Self) {
    //     let to_move = shards_to_migrate(old, self);
    //     dbg!(to_move);
    // }
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
    /// Indicates whether the specified node is suitable to be used as a replica
    /// for the specified shard.
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
pub struct DefaultStrategy;

impl<N> Strategy<N> for DefaultStrategy {
    fn is_suitable_replica(&mut self, _shard_idx: usize, _node_id: &N) -> bool {
        true
    }
}

/// [`Keyspace`] resharding process.
#[must_use]
pub struct Resharding<const RF: usize, N, H, S> {
    keyspace: Keyspace<RF, N>,
    hasher_factory: H,
    strategy: S,
}

impl<const RF: usize, N, H, S> Resharding<RF, N, H, S>
where
    N: Eq + Hash,
{
    /// Specifies a sharding [`Strategy`] to use.
    pub fn with_strategy<NewS>(self, strategy: NewS) -> Resharding<RF, N, H, NewS>
    where
        NewS: Strategy<N>,
    {
        Resharding {
            keyspace: self.keyspace,
            hasher_factory: self.hasher_factory,
            strategy,
        }
    }

    /// Adds a node to the [`Keyspace`].
    pub fn add_node(&mut self, id: N) -> Result<&mut Self, AddNodeError> {
        self.add_nodes([id])
    }

    /// Adds multiple nodes to the [`Keyspace`].
    ///
    /// This function won't try to add any nodes if the resulting number of
    /// nodes is going to get over the allowed limit.
    pub fn add_nodes(
        &mut self,
        ids: impl IntoIterator<Item = N>,
    ) -> Result<&mut Self, AddNodeError> {
        // if self.keyspace.nodes.len() == MAX_NODES {
        //     return Err(AddNodeError::TooManyNodes(MAX_NODES));
        // }

        for id in ids {
            self.keyspace.nodes.insert(id);
        }

        Ok(self)
    }

    pub fn remove_node(&mut self, id: impl AsRef<N>) -> &mut Self {
        self.remove_nodes([id])
    }

    pub fn remove_nodes(&mut self, ids: impl IntoIterator<Item = impl AsRef<N>>) -> &mut Self {
        for id in ids {
            self.keyspace.nodes.shift_remove(id.as_ref());
        }
        self
    }

    pub fn finish(&mut self) -> Result<(), ReshardingError>
    where
        N: Ord,
        H: HasherFactory,
        S: Strategy<N>,
    {
        let nodes = self.keyspace.nodes.iter();
        let mut node_ranking: Vec<_> = nodes.enumerate().map(|(idx, id)| (0, id, idx)).collect();

        for (shard_idx, shard) in self.keyspace.shards.iter_mut().enumerate() {
            for (score, node_id, _) in &mut node_ranking {
                let mut hasher = self.hasher_factory.new_hasher();
                (node_id, shard_idx).hash(&mut hasher);
                *score = hasher.finish();
            }
            node_ranking.sort_unstable();

            let mut cursor = 0;
            for replica_idx in &mut shard.replicas {
                loop {
                    if cursor == node_ranking.len() {
                        return Err(ReshardingError::IncompleteReplicaSet);
                    }

                    let (_, node_id, node_idx) = node_ranking[cursor];

                    if self.strategy.is_suitable_replica(shard_idx, node_id) {
                        *replica_idx = node_idx;
                        cursor += 1;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Error of [`Keyspace::new`].
#[derive(Debug, thiserror::Error)]
pub enum KeyspaceCreationError {
    #[error("Replication factor should be in range [1, {}]", max)]
    InvalidReplicationFactor { max: usize },
}

/// Error of [`Resharding::add_node`] and [`Resharding::add_nodes`].
#[derive(Debug, thiserror::Error)]
pub enum AddNodeError {
    #[error("Limit of {} nodes reached", _0)]
    TooManyNodes(usize),
}

/// Error of [`Resharding::remove_node`] and [`Resharding::remove_node`].
#[derive(Debug, thiserror::Error)]
pub enum RemoveNodeError {
    #[error("Cluster size can not be smaller than the replication factor ({})", _0)]
    NotEnoughNodes(usize),
}

/// Error of [`Resharding`].
#[derive(Debug, thiserror::Error)]
pub enum ReshardingError {
    #[error("Sharding strategy didn't select enough nodes to fill a replica set")]
    IncompleteReplicaSet,
}

// fn shards_to_migrate<const RF: usize, N>(a: &Keyspace<RF, N>, b:
// &Keyspace<RF, N>) -> usize {     let mut n = 0;

//     for (shard_idx, shard) in a.hashring.iter().enumerate() {
//         let new_replicas = b.hashring[shard_idx].replicas;

//         for replica_idx in shard.replicas {
//             if !new_replicas.iter().any(|idx| idx == &replica_idx) {
//                 n += 1;
//             }
//         }

//         // if shard.replicas[0] != new_replicas[0] {
//         //     n += 1;
//         // }
//     }

//     n
// }

fn test_suite() {}

#[test]
fn t() {
    let nodes = [1, 2, 3];
    let mut keyspace = Keyspace::new(nodes);

    for id in 4..=16 {
        // let snapshot = keyspace.clone();

        keyspace
            .reshard(|| DefaultHasher::new())
            .add_node(id)
            .finish()
            .unwrap();

        keyspace.assert_variance(0.03);
    }

    keyspace
        .reshard(|| DefaultHasher::new())
        .add_nodes(17..256)
        .finish()
        .unwrap();
    keyspace.assert_variance(0.13);

    // for id in 4..=100 {
    //     let snapshot = ring.clone();

    //     ring.remove_node(id);
    //     ring.assert_variance();
    //     ring.assert_data_movement(&snapshot);
    // }

    // dbg!(&keyspace.nodes);

    // let mut shards_per_node = HashMap::<usize, usize>::new();

    // for shard in &keyspace.hashring {
    //     for node_idx in shard.replicas() {
    //         *shards_per_node.entry(*node_idx).or_default() += 1;
    //     }
    // }

    // dbg!(&shards_per_node);

    // let mut hits_per_node = HashMap::<usize, usize>::new();

    // for _ in 0..1_000_000 {
    //     let idx = rand::random::<u16>();
    //     let shard = keyspace.get_shard(ShardId(idx));
    //     for node_idx in shard.replicas() {
    //         *hits_per_node.entry(*node_idx).or_default() += 1;
    //     }
    // }

    // dbg!(&hits_per_node);

    // println!(
    //     "hits min: {}, max: {}",
    //     hits_per_node.values().min().unwrap(),
    //     hits_per_node.values().max().unwrap(),
    // );

    panic!();
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
