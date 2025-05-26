use {
    crate::{
        node_operator::{self, NodeOperators},
        NodeOperator,
    },
    derive_more::TryFrom,
    sharding::ShardId,
    tap::TapOptional,
};

const REPLICATION_FACTOR: usize = 5;

/// Strategy of replicating data within a [`Keyspace`] across a set of
/// [`node::Operator`]s.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, TryFrom)]
#[try_from(repr)]
pub enum ReplicationStrategy {
    /// Keys are being uniformly distributed across [`node::Operator`]s.
    #[default]
    UniformDistribution = 0,
}

// impl ReplicationStrategy {
//     pub(super) fn try_from(repr: u8) -> Option<Self> {
//         match repr {
//             0 => Some(Self::UniformDistribution),
//             _ => None,
//         }
//     }
// }

/// Continuous space of `u64` keys, where each key is assigned to a set of
/// [`node::Operator`]s.
pub struct Keyspace {
    operators: NodeOperators,
    sharding: sharding::Keyspace<node_operator::Idx, REPLICATION_FACTOR>,
    replication_strategy: ReplicationStrategy,

    version: u64,
}

impl Keyspace {
    pub(super) fn new(
        operators: NodeOperators,
        replication_strategy: ReplicationStrategy,
        version: u64,
    ) -> Self {
        todo!()
    }

    /// Required number of [`NodeOperator`]s a data should be replicated to.
    pub fn replication_factor(&self) -> usize {
        REPLICATION_FACTOR
    }

    /// Returns the set of [`NodeOperator`]s the data under the specified key
    /// should be replicated to.
    pub fn replicas(&self, key: u64) -> impl Iterator<Item = &NodeOperator> + '_ {
        self.sharding
            .shard_replicas(ShardId::from_key(key))
            .iter()
            .filter_map(|&idx| {
                self.operators
                    .get_by_idx(idx)
                    .tap_none(|| tracing::warn!("Missing node operator {idx}"))
            })
    }

    /// Get [`NodeOperators`] of this [`Keyspace`].
    pub(super) fn operators(&self) -> &NodeOperators {
        &self.operators
    }
}

struct Snapshot {}
