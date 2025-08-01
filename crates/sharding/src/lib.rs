use std::{
    collections::HashSet,
    fmt,
    hash::{BuildHasher, Hash},
    ops::RangeInclusive,
};

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// Identifier of a shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardId(pub u16);

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
#[derive(Clone, PartialEq, Eq)]
pub struct Keyspace<N, const RF: usize> {
    nodes_count: usize,
    shards: Vec<Shard<N, RF>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Shard<N, const RF: usize> {
    replicas: [N; RF],
}

impl<N, const RF: usize> fmt::Debug for Keyspace<N, RF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keyspace")
            .field("nodes_count", &self.nodes_count)
            .finish_non_exhaustive()
    }
}

impl<N, const RF: usize> Keyspace<N, RF>
where
    N: Copy + Default + Hash + Eq + PartialEq + Ord + PartialOrd,
{
    /// Creates a new [`Keyspace`].
    pub fn new<S: ReplicationStrategy<N>>(
        nodes: impl IntoIterator<Item = N>,
        build_hasher: &impl BuildHasher,
        build_replication_strategy: impl Fn() -> S,
    ) -> Result<Self, Error> {
        const { assert!(RF > 0) };

        let nodes: HashSet<_> = nodes.into_iter().collect();
        let nodes_count = nodes.len();
        if nodes_count < RF {
            return Err(Error::InvalidNodesCount(RF));
        }

        let replicas = [N::default(); RF];
        let n_shards = u16::MAX as usize + 1;
        let mut shards: Vec<_> = (0..n_shards).map(|_| Shard { replicas }).collect();

        // using [Rendezvous](https://en.wikipedia.org/wiki/Rendezvous_hashing) hashing to calculate
        // nodes' priority of being replicas per shard.

        let mut node_priority_queue: Vec<_> = nodes.iter().map(|idx| (0, idx)).collect();

        for (shard_idx, shard) in shards.iter_mut().enumerate() {
            let mut replication_strategy = build_replication_strategy();

            for (score, node) in &mut node_priority_queue {
                *score = build_hasher.hash_one((node, shard_idx));
            }
            node_priority_queue.sort_unstable();

            let mut cursor = 0;
            for replica_id in &mut shard.replicas {
                loop {
                    if cursor == node_priority_queue.len() {
                        return Err(Error::IncompleteReplicaSet);
                    }

                    let (_, node) = node_priority_queue[cursor];
                    cursor += 1;

                    if replication_strategy.is_suitable_replica(node) {
                        *replica_id = *node;
                        break;
                    }
                }
            }
        }

        Ok(Self {
            nodes_count,
            shards,
        })
    }

    /// Returns an [`Iterator`] of replicas responsible for the shard with the
    /// given [`ShardId`].
    pub fn shard_replicas(&self, shard_id: ShardId) -> &[N; RF] {
        &self.shards[shard_id.0 as usize].replicas
    }

    pub fn shards(&self) -> impl Iterator<Item = (ShardId, &'_ [N; RF])> + '_ {
        self.shards
            .iter()
            .enumerate()
            .map(|(idx, s)| (ShardId(idx as u16), &s.replicas))
    }

    pub fn nodes_count(&self) -> usize {
        self.nodes_count
    }
}

/// Strategy of how a shard should be replicated across nodes.
///
/// One instance of [`ReplicationStrategy`] is only responsible for determining
/// replicas of a single shard.
pub trait ReplicationStrategy<N> {
    /// Indicates whether the specified node is suitable to be used as a
    /// replica for the current shard.
    ///
    /// This function will never be called with the same `node` again on the
    /// same instance of [`ReplicationStrategy`].
    fn is_suitable_replica(&mut self, node: &N) -> bool;
}

/// Default sharding [`Strategy`] that considers every node to be equally
/// suitable to be a replica for any shard.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultReplicationStrategy;

impl<N> ReplicationStrategy<N> for DefaultReplicationStrategy {
    fn is_suitable_replica(&mut self, _node: &N) -> bool {
        true
    }
}

/// Error of [`Keyspace::new`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Nodes count should be larger than the replication factor ({_0})")]
    InvalidNodesCount(usize),

    #[error("Sharding strategy didn't select enough nodes to fill a replica set")]
    IncompleteReplicaSet,
}

#[cfg(test)]
#[test]
fn test_keyspace() {
    use std::hash::{BuildHasherDefault, DefaultHasher};

    let expected_distribution_and_stability = |nodes_count: usize| match nodes_count {
        _ if nodes_count <= 16 => (0.96, 0.74),
        _ if nodes_count <= 32 => (0.93, 0.94),
        _ if nodes_count <= 64 => (0.90, 0.96),
        _ if nodes_count <= 128 => (0.86, 0.98),
        _ if nodes_count <= 256 => (0.82, 0.99),
        _ => unreachable!(),
    };

    let mut nodes: HashSet<_> = (0u8..3).collect();

    let mut keyspace: Keyspace<u8, 3> = Keyspace::new(
        nodes.iter().copied(),
        &BuildHasherDefault::<DefaultHasher>::default(),
        || DefaultReplicationStrategy,
    )
    .unwrap();
    keyspace.assert_distribution(1.0);

    // the test is very slow in debug builds
    let max_nodes = if cfg!(debug_assertions) { 16 } else { u8::MAX };

    // scale cluster up
    for idx in 3..max_nodes {
        nodes.insert(idx);
        let new_keyspace = Keyspace::new(
            nodes.iter().copied(),
            &BuildHasherDefault::<DefaultHasher>::default(),
            || DefaultReplicationStrategy,
        )
        .unwrap();

        dbg!(nodes.len());

        let (min_max_ratio, stability_coef) = expected_distribution_and_stability(nodes.len());
        new_keyspace.assert_distribution(min_max_ratio);
        new_keyspace.assert_stability(&keyspace, stability_coef);
        keyspace = new_keyspace;
    }

    let mut nodes_vec: Vec<_> = nodes.iter().copied().collect();

    // scale cluster down to RF
    loop {
        let i: usize = rand::random::<usize>() % nodes_vec.len();
        nodes_vec.swap_remove(i);
        nodes = nodes_vec.iter().copied().collect();

        let new_keyspace = Keyspace::new(
            nodes.iter().copied(),
            &BuildHasherDefault::<DefaultHasher>::default(),
            || DefaultReplicationStrategy,
        )
        .unwrap();

        dbg!(nodes.len());

        let (min_max_ratio, stability_coef) = expected_distribution_and_stability(nodes.len());
        new_keyspace.assert_distribution(min_max_ratio);
        new_keyspace.assert_stability(&keyspace, stability_coef);
        keyspace = new_keyspace;

        if nodes.len() == 3 {
            break;
        }
    }
}

#[cfg(test)]
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
