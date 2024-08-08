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

pub trait Keyspace<N: Node>: Clone + Send + Sync + 'static {
    type Snapshot<'a>: Serialize + DeserializeOwned;

    fn new(nodes: &Nodes<N>) -> super::Result<Self>;
    fn update(&mut self, nodes: &Nodes<N>) -> super::Result<bool>;
    fn ranges(&self) -> impl Iterator<Item = Range<&[node::Idx]>>;

    fn replicas(&self, key_hash: u64) -> &[node::Idx];

    fn snapshot(&self) -> Self::Snapshot<'_>;
    fn from_snapshot(nodes: &Nodes<N>, snapshot: Self::Snapshot<'_>) -> super::Result<Self>;

    fn version(&self) -> u64;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range<R> {
    pub keys: RangeInclusive<u64>,
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

// TODO: merge continuous ranges
#[derive(Derivative, PartialEq, Eq)]
#[derivative(Debug)]
pub struct MigrationPlan<N: Node> {
    #[derivative(Debug = "ignore")]
    pending_ranges: HashMap<N::Id, Vec<Range<HashSet<N::Id>>>>,
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

    pub fn pending_ranges(&self, node_id: &N::Id) -> &[Range<HashSet<N::Id>>] {
        self.pending_ranges
            .get(node_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

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
                            self.process_range(new)?;
                            (next_old_range(&mut self), next_new_range(&mut self))
                        }
                        Ordering::Less => {
                            let (new_left, new_right) = split_range(&new, *old.end());
                            self.process_range(new_left)?;
                            (next_old_range(&mut self), Some(new_right))
                        }
                        Ordering::Greater => {
                            let (old_left, old_right) = split_range(&old, *new.end());
                            self.process_range(old_left)?;
                            (Some(old_right), next_new_range(&mut self))
                        }
                    }
                }
                _ => return Err(MigrationPlanError::InvalidRange),
            };
        }
    }

    fn process_range(&mut self, range: RangeInclusive<u64>) -> Result<(), MigrationPlanError> {
        if self.old_replicas == self.new_replicas {
            return Ok(());
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

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum MigrationPlanError {
    #[error("Both ranges should cover all of the Keyspace - [0, u64::MAX]")]
    InvalidRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sharded<const RF: usize, B, S> {
    keyspace: sharding::Keyspace<node::Idx, RF>,
    _marker: PhantomData<(B, S)>,
    version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    version: u64,
}

pub struct ShardingStrategy<'a, S, N: Node> {
    inner: S,
    nodes: &'a Nodes<N>,
}

impl<'a, S, N> sharding::Strategy<node::Idx> for ShardingStrategy<'a, S, N>
where
    S: sharding::Strategy<N>,
    N: Node,
{
    fn is_suitable_replica(&mut self, shard_idx: usize, node_idx: &node::Idx) -> bool {
        let Some(node) = self.nodes.get_by_idx(*node_idx) else {
            tracing::warn!(%node_idx, "sharding::Strategy: missing node");
            return false;
        };

        self.inner.is_suitable_replica(shard_idx, node)
    }
}

impl<const RF: usize, B, S> Sharded<RF, B, S> {
    fn new_inner<N>(nodes: &Nodes<N>, version: u64) -> super::Result<Self>
    where
        N: Node,
        B: BuildHasher + Default + Clone + Send + Sync + 'static,
        S: sharding::Strategy<N> + Default + Clone + Send + Sync + 'static,
    {
        let strategy = ShardingStrategy {
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
}

impl<const RF: usize, N, B, S> Keyspace<N> for Sharded<RF, B, S>
where
    N: Node,
    B: BuildHasher + Default + Clone + Send + Sync + 'static,
    S: sharding::Strategy<N> + Default + Clone + Send + Sync + 'static,
{
    type Snapshot<'a> = Snapshot;

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
        Snapshot {
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
