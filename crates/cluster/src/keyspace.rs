use {
    crate::{
        node_operator::{self, NodeOperators},
        VersionedNodeOperator,
    },
    arc_swap::ArcSwap,
    derive_more::TryFrom,
    sharding::ShardId,
    std::sync::Arc,
    tap::TapOptional,
};

const REPLICATION_FACTOR: usize = 5;

/// [`Keyspace`] version.
pub type Version = u64;

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
    /// Creates a new [`Keyspace`].
    ///
    /// It's highly CPU intensive to construct a new [`Keyspace`] (order of
    /// seconds), so the task is being [spawned](tokio::task::spawn_blocking)
    /// to the [`tokio`] threadpool.
    ///
    /// May return `None` if the [`tokio`] task gets canceled for some reason.
    pub(super) async fn new(
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
    pub fn replicas(&self, key: u64) -> impl Iterator<Item = &VersionedNodeOperator> + '_ {
        self.sharding
            .shard_replicas(ShardId::from_key(key))
            .iter()
            .filter_map(|&idx| {
                self.operators
                    .get_by_idx(idx)
                    .tap_none(|| tracing::warn!("Missing node operator {idx}"))
            })
    }

    /// Returns [`Version`] of this [`Keyspace`].
    pub fn version(&self) -> Version {
        self.version
    }

    /// Get [`NodeOperators`] of this [`Keyspace`].
    pub(super) fn operators(&self) -> &NodeOperators {
        &self.operators
    }

    /// Returns a mutable reference to [`NodeOperator`] data.
    pub(super) fn operator_data_mut(
        &mut self,
        id: &node_operator::Id,
    ) -> Option<&mut node_operator::VersionedData> {
        self.operators.get_data_mut(id)
    }
}

struct Snapshot {}

pub struct Replica {
    idx: node_operator::Idx,
    node_operator: Arc<VersionedNodeOperator>,
}
