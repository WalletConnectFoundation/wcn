pub use {
    consensus::Consensus,
    keyspace::Keyspace,
    node::Node,
    snapshot::Snapshot,
    view::{View, Viewable},
};
use {
    derivative::Derivative,
    itertools::Itertools,
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeSet, HashMap},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
    tap::TapOptional,
};

pub mod consensus;
pub mod keyspace;
pub mod node;
pub mod snapshot;
pub mod view;

#[cfg(test)]
mod test;

pub type Nodes<N> = node::SlotMap<N>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cluster<N: Node, K> {
    nodes: Nodes<N>,
    restarting_node: Option<N::Id>,

    keyspace: Option<Arc<K>>,

    migration: Option<Migration<N, K>>,

    version: u128,
}

impl<N: Node, K> Default for Cluster<N, K> {
    fn default() -> Self {
        Self {
            nodes: Nodes::new(),
            restarting_node: None,
            keyspace: None,
            migration: None,
            version: 0,
        }
    }
}

#[derive(Clone, Derivative, PartialEq, Eq)]
#[derivative(Debug)]
struct Migration<N: Node, K> {
    nodes: Nodes<N>,
    pulling_nodes: BTreeSet<N::Id>,

    keyspace: Arc<K>,

    #[derivative(Debug = "ignore")]
    plan: Arc<keyspace::MigrationPlan<N>>,
}

impl<N: Node, K: Keyspace<N>> Cluster<N, K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_viewable(self) -> Viewable<N, K> {
        Viewable::new(self)
    }

    pub fn add_node(&mut self, node: N) -> Result<()> {
        if self.migration.is_some() {
            return Err(Error::MigrationInProgress);
        }

        if self.nodes.contains(node.id()) {
            return Err(Error::NodeAlreadyExists);
        }

        node.can_add(&self.nodes)
            .map_err(|e| Error::InvalidNode(e.to_string()))?;

        let mut new_nodes = self.nodes.clone();
        let _ = new_nodes.insert(node)?;

        self.begin_migration(new_nodes)?;
        self.incr_version();

        Ok(())
    }

    pub fn complete_pull(&mut self, node_id: &N::Id, keyspace_version: u64) -> Result<bool> {
        let Some(migration) = self.migration.as_mut() else {
            return Err(Error::NoMigration);
        };

        if !migration.nodes.contains(node_id) {
            return Err(Error::UnknownNode);
        }

        if migration.keyspace.version() != keyspace_version {
            return Err(Error::KeyspaceVersionMismatch);
        }

        if !migration.pulling_nodes.remove(node_id) {
            return Ok(false);
        }

        if migration.pulling_nodes.is_empty() {
            let _ = migration;

            if let Some(migration) = self.migration.take() {
                self.nodes = migration.nodes;
                self.keyspace = Some(migration.keyspace);
            }
        }

        self.incr_version();

        Ok(true)
    }

    pub fn shutdown_node(&mut self, id: &N::Id) -> Result<bool> {
        if !self.contains_node(id) {
            return Err(Error::UnknownNode);
        }

        if self.migration.is_some() {
            return Err(Error::MigrationInProgress);
        }

        match self.restarting_node.as_ref() {
            None => {}
            Some(node_id) if node_id == id => return Ok(false),
            Some(_) => return Err(Error::AnotherNodeRestarting),
        };

        self.restarting_node = Some(id.clone());
        self.incr_version();

        Ok(true)
    }

    pub fn startup_node(&mut self, node: N) -> Result<()> {
        let Some(old_node) = self.nodes.get(node.id()) else {
            return Err(Error::UnknownNode);
        };

        match self.restarting_node.as_ref() {
            Some(node_id) if node_id == node.id() => {}
            _ => return Err(Error::NodeAlreadyStarted),
        };

        // Can't happen under normal circumstances, as nodes are only allowed to restart
        // when there is no migration.
        if self.migration.is_some() {
            return Err(Error::MigrationInProgress);
        }

        if old_node != &node {
            let old_node = old_node.clone();

            old_node
                .can_update(&self.nodes, &node)
                .map_err(|e| Error::InvalidNode(e.to_string()))?;

            let mut new_nodes = self.nodes.clone();
            let _ = new_nodes.insert(node)?;

            self.begin_migration(new_nodes)?;
        }

        self.restarting_node = None;
        self.incr_version();

        Ok(())
    }

    pub fn decommission_node(&mut self, id: &N::Id) -> Result<()> {
        if self.migration.is_some() {
            return Err(Error::MigrationInProgress);
        }

        if !self.nodes.contains(id) {
            return Err(Error::UnknownNode);
        }

        let mut new_nodes = self.nodes.clone();
        new_nodes.remove(id);

        self.begin_migration(new_nodes)?;
        self.incr_version();

        Ok(())
    }

    pub fn node(&self, id: &N::Id) -> Option<&N> {
        // prioritize the new version of the `Node`
        if let Some(migration) = &self.migration {
            return migration.nodes.get(id).or_else(|| self.nodes.get(id));
        }

        self.nodes.get(id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &N> {
        use itertools::EitherOrBoth;

        let old_slots = self.nodes.slots();
        let new_slots = self
            .migration
            .as_ref()
            .map(|m| m.nodes.slots())
            .unwrap_or_default();

        old_slots
            .iter()
            .zip_longest(new_slots.iter())
            .filter_map(|e| match e {
                // prioritize the new version of the `Node`
                EitherOrBoth::Both(Some(_), Some(new)) => Some(new),
                EitherOrBoth::Both(Some(old), None) => Some(old),
                EitherOrBoth::Both(_, new) => new.as_ref(),
                EitherOrBoth::Left(old) => old.as_ref(),
                EitherOrBoth::Right(new) => new.as_ref(),
            })
    }

    pub fn contains_node(&self, id: &N::Id) -> bool {
        if self.nodes.contains(id) {
            return true;
        }

        self.migration
            .as_ref()
            .map(|m| m.nodes.contains(id))
            .unwrap_or_default()
    }

    pub fn node_state(&self, id: &N::Id) -> Option<node::State<N>> {
        if self.restarting_node.as_ref() == Some(id) {
            return Some(node::State::Restarting);
        }

        let Some(migration) = &self.migration else {
            return self.nodes.get(id).map(|_| node::State::Normal);
        };

        if migration.pulling_nodes.contains(id) {
            return Some(node::State::Pulling(migration.plan.clone()));
        }

        match (self.nodes.get(id), migration.nodes.get(id)) {
            (None, None) => None,
            (Some(_), None) => Some(node::State::Decommissioning),
            _ => Some(node::State::Normal),
        }
    }

    pub fn replica_set(
        &self,
        key_hash: u64,
        is_write: bool,
    ) -> Result<ReplicaSet<impl Iterator<Item = &N> + '_>> {
        let Some(keyspace) = &self.keyspace else {
            return Err(Error::NotBootstrapped);
        };

        let old_replicas = keyspace.replicas(key_hash);
        let new_replicas = self
            .migration
            .as_ref()
            .filter(|_| is_write)
            .map(|m| m.keyspace.replicas(key_hash))
            .unwrap_or_default();

        let mut required_count = old_replicas.len() / 2 + 1;

        // TODO: replace with no-alloc version
        let mut nodes = HashMap::with_capacity(old_replicas.len() + new_replicas.len());

        for node_idx in old_replicas {
            let Some(node) = self.nodes.get_by_idx(*node_idx) else {
                tracing::warn!(%node_idx, "Cluster::replica_set: missing old node");
                return Err(Error::Bug("Missing node".to_string()));
            };

            let _ = nodes.insert(node.id(), node);
        }

        if let Some(migration) = &self.migration {
            for node_idx in new_replicas {
                let Some(node) = migration.nodes.get_by_idx(*node_idx) else {
                    tracing::warn!(%node_idx, "Cluster::replica_set: missing new node");
                    return Err(Error::Bug("Missing node".to_string()));
                };

                if nodes.insert(node.id(), node).is_none() {
                    required_count += 1;
                }
            }
        }

        Ok(ReplicaSet {
            required_count,
            nodes: nodes.into_values(),
        })
    }

    pub fn keyspace_version(&self) -> u64 {
        if let Some(migration) = &self.migration {
            migration.keyspace.version()
        } else {
            self.keyspace
                .as_ref()
                .map(|k| k.version())
                .unwrap_or_default()
        }
    }

    pub fn version(&self) -> u128 {
        self.version
    }

    fn begin_migration(&mut self, new_nodes: Nodes<N>) -> Result<()> {
        // if cluster is not bootstrapped yet we don't need a migration, just try to
        // initialize the keyspace
        let Some(keyspace) = &self.keyspace else {
            return match K::new(&new_nodes) {
                Ok(keyspace) => {
                    self.nodes = new_nodes;
                    self.keyspace = Some(Arc::new(keyspace));
                    Ok(())
                }
                // keyspace can't be initialized yet, we need more nodes
                Err(Error::TooFewNodes) => {
                    self.nodes = new_nodes;
                    Ok(())
                }
                Err(err) => Err(err),
            };
        };

        let mut new_keyspace = Arc::clone(keyspace);
        Arc::make_mut(&mut new_keyspace).update(&new_nodes)?;

        let plan = migration_plan(
            &self.nodes,
            keyspace.as_ref(),
            &new_nodes,
            new_keyspace.as_ref(),
        )?;

        if plan.is_empty() {
            self.nodes = new_nodes;
            return Ok(());
        }

        self.migration = Some(Migration {
            nodes: new_nodes,
            pulling_nodes: plan.pulling_nodes().cloned().collect(),
            keyspace: new_keyspace,
            plan: Arc::new(plan),
        });

        Ok(())
    }

    fn incr_version(&mut self) {
        self.version += 1;
    }
}

fn migration_plan<N: Node, K: Keyspace<N>>(
    old_nodes: &Nodes<N>,
    old_keyspace: &K,
    new_nodes: &Nodes<N>,
    new_keyspace: &K,
) -> Result<keyspace::MigrationPlan<N>> {
    let corrupted = AtomicBool::new(false);

    let plan = keyspace::MigrationPlan::new(
        complete_keyspace_ranges(old_nodes, old_keyspace, &corrupted),
        complete_keyspace_ranges(new_nodes, new_keyspace, &corrupted),
        new_keyspace.version(),
    )
    .map_err(|e| Error::Bug(e.to_string()))?;

    if corrupted.load(Ordering::Relaxed) {
        return Err(Error::Bug("Node is missing".to_string()));
    }

    Ok(plan)
}

fn complete_keyspace_ranges<'a, N: Node, K: Keyspace<N>>(
    nodes: &'a Nodes<N>,
    keyspace: &'a K,
    missing: &'a AtomicBool,
) -> impl Iterator<Item = keyspace::Range<impl Iterator<Item = &'a N::Id>>> + 'a {
    keyspace.ranges().map(move |range| {
        range.map_replicas(move |r| {
            r.iter().filter_map(|&idx| {
                nodes
                    .get_by_idx(idx)
                    .map(Node::id)
                    .tap_none(|| missing.store(true, Ordering::Relaxed))
            })
        })
    })
}

pub struct ReplicaSet<I> {
    pub required_count: usize,
    pub nodes: I,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize, PartialEq)]
pub enum Error {
    #[error("Cluster is not bootstrapped yet")]
    NotBootstrapped,

    #[error("Node is already a member of the Cluster")]
    NodeAlreadyExists,

    #[error("Node is already started")]
    NodeAlreadyStarted,

    #[error("Node is not a member of the Cluster")]
    UnknownNode,

    #[error("Max amount of nodes is reached")]
    TooManyNodes,

    #[error("Min amount of nodes is reached")]
    TooFewNodes,

    #[error("Cluster migration is in progress")]
    MigrationInProgress,

    #[error("Cluster doesn't have an ongoing migration")]
    NoMigration,

    #[error("Keyspace version mismatch")]
    KeyspaceVersionMismatch,

    #[error("Another node is currently restarting")]
    AnotherNodeRestarting,

    #[error("Invalid Node: {_0}")]
    InvalidNode(String),

    #[error("Bug: {_0}")]
    Bug(String),
}
