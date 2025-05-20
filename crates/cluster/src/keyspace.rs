use {crate::node, sharding::ShardId, tap::TapOptional};

const REPLICATION_FACTOR: usize = 5;

/// Strategy of replicating data within a [`Keyspace`] across a set of
/// [`node::Operator`]s.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum ReplicationStrategy {
    /// Keys are being uniformly distributed across [`node::Operator`]s.
    #[default]
    UniformDistribution = 0,
}

/// Continuous space of `u64` keys, where each key is assigned to a set of
/// [`node::Operator`]s.
pub struct Keyspace {
    operators: node::Operators,
    sharding: sharding::Keyspace<node::OperatorIdx, REPLICATION_FACTOR>,
    replication_strategy: ReplicationStrategy,

    version: u64,
}

impl Keyspace {
    /// Required number of [`node::Operator`]s a data should be replicated to.
    pub fn replication_factor(&self) -> usize {
        REPLICATION_FACTOR
    }

    /// Returns the set of [`node::Operator`]s the data under the specified key
    /// should be replicated to.
    pub fn replicas(&self, key: u64) -> impl Iterator<Item = &node::Operator> + '_ {
        self.sharding
            .shard_replicas(ShardId::from_key(key))
            .iter()
            .filter_map(|&idx| {
                self.operators
                    .get(idx as usize)?
                    .as_ref()
                    .tap_none(|| tracing::warn!("Missing node operator {idx}"))
            })
    }

    /// Get [`node::Operators`] map of this [`Kespace`].
    pub(super) fn operators(&self) -> &node::Operators {
        &self.operators
    }
}

struct Snapshot {}
