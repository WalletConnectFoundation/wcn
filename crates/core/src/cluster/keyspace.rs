//! Keyspace functionality required by an IRN cluster.

use {
    super::{node, Node, Nodes},
    derivative::Derivative,
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    sharding::ShardId,
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet},
        hash::BuildHasher,
        marker::PhantomData,
        ops::RangeInclusive,
    },
};

/// Keyspace functionality required by a [`Cluster`](super::Cluster).
pub trait Keyspace<N: Node>: Clone + Send + Sync + 'static {
    /// Snapshot of the [`Keyspace`] state suitable for network transmission and
    /// persistent storage.
    type Snapshot<'a>: Serialize + DeserializeOwned;

    /// Creates a new [`Keyspace`], distributing the keys across the list of
    /// the provided [`Nodes`].
    fn new(nodes: &Nodes<N>) -> super::Result<Self>;

    /// Updates the [`Keyspace`], redistributing the keys across the list of the
    /// provided [`Nodes`].
    ///
    /// Returns `false` if the [`Keyspace`] state hasn't changed.
    fn update(&mut self, nodes: &Nodes<N>) -> super::Result<bool>;

    /// Returns all the key [`Range`]s in this [`Keyspace`].
    fn ranges(&self) -> impl Iterator<Item = Range<&[node::Idx]>>;

    /// Returns replicas responsible for the provided `key`.
    fn replicas(&self, key: u64) -> &[node::Idx];

    /// Builds a [`Keyspace::Snapshot`] of this [`Keyspace`].
    fn snapshot(&self) -> Self::Snapshot<'_>;

    /// Re-constructs a [`Keyspace`] out of the provided [`Keyspace::Snapshot`]
    /// and [`Nodes`].
    fn from_snapshot(nodes: &Nodes<N>, snapshot: Self::Snapshot<'_>) -> super::Result<Self>;

    /// Returns the version of this [`Keyspace`].
    ///
    /// The version is expected to be increased after each [`Keyspace`]
    /// modification.
    fn version(&self) -> u64;
}

/// Key range within a [`Keyspace`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range<R> {
    /// The range of keys this [`Range`] contains.
    pub keys: RangeInclusive<u64>,

    /// List of replicas responsible for this [`Range`].
    pub replicas: R,
}

impl<R> Range<R> {
    pub(super) fn map_replicas<U>(self, f: impl FnOnce(R) -> U) -> Range<U> {
        Range {
            keys: self.keys,
            replicas: f(self.replicas),
        }
    }
}

fn split_range(range: &RangeInclusive<u64>, at: u64) -> (RangeInclusive<u64>, RangeInclusive<u64>) {
    debug_assert!(&at < range.end());

    (
        RangeInclusive::new(*range.start(), at),
        RangeInclusive::new(at + 1, *range.end()),
    )
}

type PendingRange<N> = Range<HashSet<<N as Node>::Id>>;

/// [`Keyspace`] migration plan.
///
/// Represents a subset of keys to re-distribute after a [`Keyspace`] state has
/// been changed.
// TODO: merge continuous ranges
#[derive(Derivative, PartialEq, Eq)]
#[derivative(Debug)]
pub struct MigrationPlan<N: Node> {
    #[derivative(Debug = "ignore")]
    pending_ranges: HashMap<N::Id, Vec<PendingRange<N>>>,
    keyspace_version: u64,
}

impl<N: Node> MigrationPlan<N> {
    pub(super) fn new<'a>(
        old_ranges: impl IntoIterator<Item = Range<impl IntoIterator<Item = &'a N::Id>>> + 'a,
        new_ranges: impl IntoIterator<Item = Range<impl IntoIterator<Item = &'a N::Id>>> + 'a,
        keyspace_version: u64,
    ) -> Result<MigrationPlan<N>, MigrationPlanError> {
        MigrationPlanner {
            old_replicas: HashSet::new(),
            new_replicas: HashSet::new(),
            plan: MigrationPlan {
                pending_ranges: HashMap::new(),
                keyspace_version,
            },
        }
        .pending_ranges(old_ranges.into_iter(), new_ranges.into_iter())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending_ranges.is_empty()
    }

    pub(super) fn pulling_nodes(&self) -> impl Iterator<Item = &N::Id> {
        self.pending_ranges.keys()
    }

    /// Returns a list of [`Range`]s that needs to be pulled by the [`Node`]
    /// with the provided [`Node::Id`].
    pub fn pending_ranges(&self, node_id: &N::Id) -> &[Range<HashSet<N::Id>>] {
        self.pending_ranges
            .get(node_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// Returns the version of the new [`Keyspace`].
    pub fn keyspace_version(&self) -> u64 {
        self.keyspace_version
    }
}

struct MigrationPlanner<'a, N: Node> {
    old_replicas: HashSet<&'a N::Id>,
    new_replicas: HashSet<&'a N::Id>,

    plan: MigrationPlan<N>,
}

impl<'a, N: Node> MigrationPlanner<'a, N> {
    fn pending_ranges(
        mut self,
        mut old_ranges: impl Iterator<Item = Range<impl IntoIterator<Item = &'a N::Id>>> + 'a,
        mut new_ranges: impl Iterator<Item = Range<impl IntoIterator<Item = &'a N::Id>>> + 'a,
    ) -> Result<MigrationPlan<N>, MigrationPlanError> {
        let mut next_old_range = |this: &mut Self| {
            old_ranges.next().map(|range| {
                this.old_replicas = range.replicas.into_iter().collect();
                range.keys
            })
        };

        let mut next_new_range = |this: &mut Self| {
            new_ranges.next().map(|range| {
                this.new_replicas = range.replicas.into_iter().collect();
                range.keys
            })
        };

        let mut old_range = match next_old_range(&mut self) {
            Some(range) if *range.start() == 0 => Some(range),
            _ => return Err(MigrationPlanError::InvalidRange),
        };

        let mut new_range = match next_new_range(&mut self) {
            Some(range) if *range.start() == 0 => Some(range),
            _ => return Err(MigrationPlanError::InvalidRange),
        };

        loop {
            (old_range, new_range) = match (old_range, new_range) {
                (None, None) => return Ok(self.plan),
                (Some(old), Some(new)) => {
                    if old.start() != new.start() {
                        return Err(MigrationPlanError::InvalidRange);
                    }

                    match old.end().cmp(new.end()) {
                        Ordering::Equal => {
                            self.process_range(new);
                            (next_old_range(&mut self), next_new_range(&mut self))
                        }
                        Ordering::Less => {
                            let (new_left, new_right) = split_range(&new, *old.end());
                            self.process_range(new_left);
                            (next_old_range(&mut self), Some(new_right))
                        }
                        Ordering::Greater => {
                            let (old_left, old_right) = split_range(&old, *new.end());
                            self.process_range(old_left);
                            (Some(old_right), next_new_range(&mut self))
                        }
                    }
                }
                _ => return Err(MigrationPlanError::InvalidRange),
            };
        }
    }

    fn process_range(&mut self, range: RangeInclusive<u64>) {
        if self.old_replicas == self.new_replicas {
            return;
        }

        for id in self.new_replicas.difference(&self.old_replicas).copied() {
            let range = Range {
                keys: range.clone(),
                replicas: self.old_replicas.iter().map(|&id| id.clone()).collect(),
            };

            // HashMap::entry forces clone
            if let Some(ranges) = self.plan.pending_ranges.get_mut(id) {
                ranges.push(range);
            } else {
                let ranges = vec![range];
                let _ = self.plan.pending_ranges.insert(id.clone(), ranges);
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub(super) enum MigrationPlanError {
    #[error("Both ranges should cover all of the Keyspace - [0, u64::MAX]")]
    InvalidRange,
}

/// Sharded [`Keyspace`].
///
/// [`Keyspace`] implementation based on [`sharding`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sharded<const RF: usize, B, S> {
    keyspace: sharding::Keyspace<node::Idx, RF>,
    _marker: PhantomData<(B, S)>,
    version: u64,
}

/// Snapshot of a [`Sharded`] keyspace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardedSnapshot {
    version: u64,
}

/// [`sharding::ReplicationStrategy`] of a [`Sharded`] [`Keyspace`].
pub struct ReplicationStrategy<'a, S, N: Node> {
    inner: S,
    nodes: &'a Nodes<N>,
}

impl<'a, S, N> sharding::ReplicationStrategy<node::Idx> for ReplicationStrategy<'a, S, N>
where
    S: sharding::ReplicationStrategy<N>,
    N: Node,
{
    fn is_suitable_replica(&mut self, node_idx: &node::Idx) -> bool {
        let Some(node) = self.nodes.get_by_idx(*node_idx) else {
            tracing::warn!(%node_idx, "ReplicationStrategy: missing node");
            return false;
        };

        self.inner.is_suitable_replica(node)
    }
}

impl<const RF: usize, B, S> Sharded<RF, B, S> {
    fn new_inner<N>(nodes: &Nodes<N>, version: u64) -> super::Result<Self>
    where
        N: Node,
        B: BuildHasher + Default + Clone + Send + Sync + 'static,
        S: sharding::ReplicationStrategy<N> + Default + Clone + Send + Sync + 'static,
    {
        let strategy = || ReplicationStrategy {
            inner: S::default(),
            nodes,
        };

        sharding::Keyspace::new(nodes.iter().map(|(idx, _)| idx), &B::default(), strategy)
            .map(|keyspace| Sharded {
                keyspace,
                _marker: PhantomData,
                version,
            })
            .map_err(|e| match e {
                sharding::Error::InvalidNodesCount(_) => super::Error::TooFewNodes,
                e @ sharding::Error::IncompleteReplicaSet => super::Error::Bug(e.to_string()),
            })
    }

    /// Returns the underlying [`sharding::Keyspace`].
    pub fn sharding(&self) -> &sharding::Keyspace<u8, RF> {
        &self.keyspace
    }
}

impl<const RF: usize, N, B, S> Keyspace<N> for Sharded<RF, B, S>
where
    N: Node,
    B: BuildHasher + Default + Clone + Send + Sync + 'static,
    S: sharding::ReplicationStrategy<N> + Default + Clone + Send + Sync + 'static,
{
    type Snapshot<'a> = ShardedSnapshot;

    fn new(nodes: &Nodes<N>) -> super::Result<Self> {
        Self::new_inner(nodes, 0)
    }

    fn update(&mut self, nodes: &Nodes<N>) -> super::Result<bool> {
        *self = Self::new_inner(nodes, self.version + 1)?;
        Ok(true)
    }

    fn ranges(&self) -> impl Iterator<Item = Range<&[node::Idx]>> {
        self.keyspace.shards().map(|(shard_id, replicas)| Range {
            keys: shard_id.key_range(),
            replicas: replicas.as_slice(),
        })
    }

    fn replicas(&self, key_hash: u64) -> &[node::Idx] {
        self.keyspace.shard_replicas(ShardId::from_key(key_hash))
    }

    fn snapshot(&self) -> Self::Snapshot<'_> {
        ShardedSnapshot {
            version: self.version,
        }
    }

    fn from_snapshot(nodes: &Nodes<N>, snapshot: Self::Snapshot<'_>) -> super::Result<Self> {
        Self::new_inner(nodes, snapshot.version)
    }

    fn version(&self) -> u64 {
        self.version
    }
}
