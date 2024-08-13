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
    N: Copy + Default + Hash + Eq + PartialEq + Ord + PartialOrd + 'static,
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

        let mut node_priority_queue: Vec<_> = nodes
            .iter()
            .map(|node| ReplicaCandidate { priority: 0, node })
            .collect();

        'out: for (shard_idx, shard) in shards.iter_mut().enumerate() {
            for item in &mut node_priority_queue {
                item.priority = build_hasher.hash_one((item.node, shard_idx));
            }
            node_priority_queue.sort_unstable();

            for (idx, node) in build_replication_strategy()
                .choose_replicas(&node_priority_queue)
                .enumerate()
            {
                shard.replicas[idx] = *node;

                if idx == shard.replicas.len() - 1 {
                    continue 'out;
                }
            }

            return Err(Error::IncompleteReplicaSet);
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

/// Node priority in [`NodePriorityQueue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct ReplicaCandidate<'a, N> {
    priority: u64,
    node: &'a N,
}

impl<'a, N> ReplicaCandidate<'a, N> {
    pub fn node(&self) -> &'a N {
        self.node
    }
}

// /// Priority queue of nodes, where the position of a node is the priority of
// it /// being a replica for some shard.
// pub trait ReplicaPriorityQueue<N: 'static> {
//     /// [`Iterator`] of nodes representing the queue.
//     fn iter(&self) -> impl Iterator<Item = &N>;
// }

// impl<N: 'static> ReplicaPriorityQueue<N> for Vec<(u64, &N)> {
//     fn iter(&self) -> impl Iterator<Item = &N> {
//         self.as_slice().iter().map(|(_, n)| *n)
//     }
// }

/// Strategy of how shards are being replicated across nodes.
pub trait ReplicationStrategy<N: 'static> {
    /// Given a list of [`ReplicaCandidate`]s order by priority returns an
    /// [`Iterator`] of nodes being chosen.
    fn choose_replicas<'a>(
        &'a mut self,
        candidates: &'a [ReplicaCandidate<'a, N>],
    ) -> impl Iterator<Item = &'a N>;
}

/// Default [`ReplicationStrategy`] that considers every node to be equally
/// suitable to be a replica for any shard.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultReplicationStrategy;

impl<N: 'static> ReplicationStrategy<N> for DefaultReplicationStrategy {
    fn choose_replicas<'a>(
        &'a mut self,
        candidates: &'a [ReplicaCandidate<'a, N>],
    ) -> impl Iterator<Item = &'a N> {
        candidates.iter().map(ReplicaCandidate::node)
    }
}

/// Error of [`Keyspace::new`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Nodes count should be larges than the replication factor ({_0})")]
    InvalidNodesCount(usize),

    #[error("Sharding strategy didn't select enough nodes to fill a replica set")]
    IncompleteReplicaSet,
}

#[cfg(test)]
#[test]
fn test_keyspace() {
    use std::hash::{BuildHasherDefault, DefaultHasher};

    let expected_variance = testing::ExpectedDistributionVariance(vec![
        (16, 0.03),
        (32, 0.05),
        (64, 0.07),
        (128, 0.1),
        (256, 0.15),
    ]);

    let mut nodes: HashSet<_> = (0u8..3).collect();

    let mut keyspace: Keyspace<u8, 3> = Keyspace::new(
        nodes.iter().copied(),
        &BuildHasherDefault::<DefaultHasher>::default(),
        || DefaultReplicationStrategy,
    )
    .unwrap();
    keyspace.assert_variance_and_stability(&keyspace, &expected_variance);

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
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
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
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
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
