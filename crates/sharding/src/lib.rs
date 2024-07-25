use {
    indexmap::IndexSet,
    std::{
        hash::{BuildHasher, Hash},
        ops::RangeInclusive,
    },
};

#[cfg(any(test, feature = "testing"))]
pub mod testing;

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
        build_hasher: &impl BuildHasher,
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

        // using [Randevouz](https://en.wikipedia.org/wiki/Rendezvous_hashing) hashing to assign nodes to shards.

        let mut node_ranking: Vec<_> = nodes
            .iter()
            .enumerate()
            .map(|(idx, id)| (0, id, idx))
            .collect();

        for (shard_idx, shard) in shards.iter_mut().enumerate() {
            for (score, node_id, _) in &mut node_ranking {
                *score = build_hasher.hash_one((node_id, shard_idx));
            }
            node_ranking.sort_unstable();

            let mut cursor = 0;
            for replica_idx in &mut shard.replicas {
                loop {
                    if cursor == node_ranking.len() {
                        return Err(Error::IncompleteReplicaSet);
                    }

                    let (_, node_id, node_idx) = node_ranking[cursor];
                    cursor += 1;

                    if sharding_strategy.is_suitable_replica(shard_idx, node_id) {
                        *replica_idx = node_idx;
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

    /// Returns the number of nodes in the [`Keyspace`].
    pub fn nodes_count(&self) -> usize {
        self.nodes.len()
    }
}

/// Sharding strategy.
pub trait Strategy<N> {
    /// Indicates whether the specified node is suitable to be used as a
    /// replica for the specified shard.
    ///
    /// In consecutive calls to this function `shard_idx` is going to either
    /// stay the same or increase, but never decrease. Also, this function will
    /// never be called for the same combination of `shard_idx` and
    /// `node_id` again.
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

#[cfg(test)]
#[test]
fn test_keyspace() {
    use std::hash::{BuildHasherDefault, DefaultHasher};

    testing::keyspace_test_suite::<3, _, _>(
        BuildHasherDefault::<DefaultHasher>::default(),
        || DefaultStrategy,
        rand::random::<usize>,
        testing::ExpectedDistributionVariance(vec![
            (16, 0.03),
            (32, 0.05),
            (64, 0.07),
            (128, 0.1),
            (256, 0.15),
        ]),
        10,
    )
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
