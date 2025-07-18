use {
    crate::{
        node_operator::{self},
        NodeOperators,
    },
    derivative::Derivative,
    derive_more::TryFrom,
    serde::{Deserialize, Serialize},
    sharding::ShardId,
    std::collections::HashSet,
    xxhash_rust::xxh3::Xxh3Builder,
};

/// Maximum number of [`node_operator`]s within a [`Keyspace`].
pub const MAX_OPERATORS: usize = 256;

/// Number of [`node_operator`]s within a [`ReplicaSet`].
pub const REPLICATION_FACTOR: u8 = 5;

/// Continuous space of `u64` keys.
///
/// [`Keyspace`] is being split into a set of equally sized [`Shards`],
/// and each [`Shard`] is being assigned to a set of [`node_operator`]s.
#[derive(Clone, Derivative, Serialize, Deserialize)]
#[derivative(Debug)]
pub struct Keyspace<S = ()> {
    operators: HashSet<node_operator::Idx>,

    #[derivative(Debug = "ignore")]
    shards: S,

    replication_strategy: ReplicationStrategy,

    version: u64,
}

/// All [`Shard`]s within a [`Keyspace`].
pub struct Shards(sharding::Keyspace<node_operator::Idx, { REPLICATION_FACTOR as usize }>);

/// A single [`Shard`] within a [`Keyspace`].
#[derive(Clone, Copy, Debug)]
pub struct Shard {
    replica_set: [node_operator::Idx; REPLICATION_FACTOR as usize],
}

/// Strategy of distributing [`Shard`]s to [`node_operator`]s.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, TryFrom, Eq, PartialEq, Serialize, Deserialize)]
#[try_from(repr)]
pub enum ReplicationStrategy {
    /// [`Shard`]s are being uniformly distributed across [`node_operator`]s.
    #[default]
    UniformDistribution = 0,
}

/// Set of [`node_operator`]s assigned to a [`Shard`].
pub type ReplicaSet<T = node_operator::Idx> = [T; REPLICATION_FACTOR as usize];

/// [`Keyspace`] version.
pub type Version = u64;

impl Keyspace {
    /// Creates a new [`Keyspace`].
    pub fn new(
        operators: HashSet<node_operator::Idx>,
        replication_strategy: ReplicationStrategy,
        version: u64,
    ) -> Result<Keyspace, CreationError> {
        if operators.len() < REPLICATION_FACTOR as usize {
            return Err(CreationError::TooFewOperators(operators.len()));
        }

        if operators.len() > MAX_OPERATORS {
            return Err(CreationError::TooManyOperators(operators.len()));
        }

        Ok(Keyspace {
            operators,
            shards: (),
            replication_strategy,
            version,
        })
    }

    pub(crate) async fn calculate<Shards>(self) -> Keyspace<Shards>
    where
        Self: sealed::Calculate<Shards>,
    {
        sealed::Calculate::<Shards>::calculate_shards(self).await
    }
}

impl Keyspace<Shards> {
    /// Returns the [`Shard`] that contains the specified key.
    pub fn shard(&self, key: u64) -> Shard {
        Shard {
            replica_set: *self.shards.0.shard_replicas(ShardId::from_key(key)),
        }
    }
}

impl<S> Keyspace<S> {
    /// Returns an [`Iterator`] over [`node_operator`]s of this [`Keyspace`].
    pub fn operators(&self) -> impl Iterator<Item = node_operator::Idx> + '_ {
        self.operators.iter().copied()
    }

    /// Returns [`ReplicationStrategy`] of this [`Keyspace`].
    pub fn replication_strategy(&self) -> ReplicationStrategy {
        self.replication_strategy
    }

    /// Returns [`Version`] of this [`Keyspace`].
    pub fn version(&self) -> Version {
        self.version
    }

    pub(super) fn contains_operator(&self, idx: node_operator::Idx) -> bool {
        self.operators.contains(&idx)
    }

    pub(super) fn require_diff<T>(&self, other: &Keyspace<T>) -> Result<(), SameKeyspaceError> {
        if self.operators == other.operators
            && self.replication_strategy == other.replication_strategy
        {
            return Err(SameKeyspaceError);
        }

        Ok(())
    }

    pub(super) fn validate<N>(
        &self,
        node_operators: &NodeOperators<N>,
    ) -> Result<(), UnknownNodeOperator> {
        for &operator_idx in &self.operators {
            if !node_operators.contains_idx(operator_idx) {
                return Err(UnknownNodeOperator(operator_idx));
            }
        }

        Ok(())
    }
}

impl Shard {
    /// Returns [`ReplicaSet`] assigned to this [`Shard`].
    pub fn replica_set(&self) -> ReplicaSet {
        self.replica_set
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreationError {
    #[error("Too few operators within a Keyspace: {_0} < {REPLICATION_FACTOR}")]
    TooFewOperators(usize),

    #[error("Too many operators within a Keyspace: {_0} > {MAX_OPERATORS}")]
    TooManyOperators(usize),
}

#[derive(Debug, thiserror::Error)]
#[error("The new Keyspace doesn't differ from the old one")]
pub struct SameKeyspaceError;

#[derive(Debug, thiserror::Error)]
#[error("The new Keyspace contains unknown NodeOperator(idx: {_0})")]
pub struct UnknownNodeOperator(node_operator::Idx);

pub(crate) mod sealed {
    use std::future::Future;

    #[allow(unused_imports)]
    use super::*;

    /// Trait to make `Keyspace<()>` and `Keyspace<Shards>` polymorphic.
    pub trait Calculate<Shards> {
        /// Calculates [`Shards`] of a [`Keyspace`].
        ///
        /// It's highly CPU intensive task (order of seconds), so the task is
        /// being [spawned](tokio::task::spawn_blocking) to the
        /// [`tokio`] threadpool.
        fn calculate_shards(self) -> impl Future<Output = Keyspace<Shards>> + Send;
    }
}

impl sealed::Calculate<Shards> for Keyspace {
    async fn calculate_shards(self) -> Keyspace<Shards> {
        let operators = self.operators.clone();
        let res = tokio::task::spawn_blocking(move || {
            sharding::Keyspace::new(operators.into_iter(), &Xxh3Builder::default(), || {
                sharding::DefaultReplicationStrategy
            })
        })
        .await
        .expect("`calculate_shards` task panicked"); // we don't expect the task to panic

        match res {
            Ok(sharding) => Keyspace {
                operators: self.operators,
                shards: Shards(sharding),
                replication_strategy: ReplicationStrategy::UniformDistribution,
                version: self.version,
            },

            // we checked this in the `Keyspace` constructor
            Err(sharding::Error::InvalidNodesCount(_)) => unreachable!(),

            // impossible with the default replication strategy
            Err(sharding::Error::IncompleteReplicaSet) => unreachable!(),
        }
    }
}

impl sealed::Calculate<()> for Keyspace {
    async fn calculate_shards(self) -> Keyspace {
        self
    }
}
