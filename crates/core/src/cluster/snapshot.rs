//! Cluster snapshot machinery.

use {
    super::{
        node::{self, SlotMap},
        Cluster,
        Keyspace,
        Node,
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{borrow::Cow, collections::BTreeSet, sync::Arc},
};

/// Snapshot of a [`Cluster`] state suitable for network transmission and
/// persistent storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "N: Serialize + DeserializeOwned")]
pub struct Snapshot<'a, N: Node, K: Keyspace<N>> {
    nodes: Cow<'a, [node::Slot<N>]>,
    restarting_node: Option<Cow<'a, N::Id>>,

    keyspace: Option<K::Snapshot<'a>>,

    migration: Option<MigrationSnapshot<'a, N, K>>,

    version: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationSnapshot<'a, N: Node, K: Keyspace<N>> {
    nodes: Cow<'a, [node::Slot<N>]>,
    pulling_nodes: Cow<'a, BTreeSet<N::Id>>,

    keyspace: K::Snapshot<'a>,
}

impl<N: Node, K: Keyspace<N>> Cluster<N, K> {
    /// Builds a [`Snapshot`] of this [`Cluster`].
    pub fn snapshot(&self) -> Snapshot<'_, N, K> {
        Snapshot {
            nodes: self.nodes.slots().into(),
            restarting_node: self.restarting_node.as_ref().map(Cow::Borrowed),
            keyspace: self.keyspace.as_ref().map(|k| k.snapshot()),
            migration: self.migration.as_ref().map(|m| MigrationSnapshot {
                nodes: m.nodes.slots().into(),
                pulling_nodes: Cow::Borrowed(&m.pulling_nodes),
                keyspace: m.keyspace.snapshot(),
            }),
            version: self.version,
        }
    }

    /// Re-constructs a [`Cluster`] out of the provided [`Snapshot`].
    pub fn from_snapshot(s: Snapshot<'_, N, K>) -> super::Result<Self> {
        let nodes = SlotMap::from_slots(s.nodes.into_owned())?;
        let keyspace = s
            .keyspace
            .map(|k| K::from_snapshot(&nodes, k).map(Arc::new))
            .transpose()?;

        let migration = s
            .migration
            .map(|m| {
                let Some(keyspace) = &keyspace else {
                    return Err(super::Error::Bug(
                        "Keyspace is `None`, but Migration is `Some`".to_string(),
                    ));
                };

                let new_nodes = SlotMap::from_slots(m.nodes.into_owned())?;
                let new_keyspace = Arc::new(K::from_snapshot(&new_nodes, m.keyspace)?);

                let plan = super::migration_plan(
                    &nodes,
                    keyspace.as_ref(),
                    &new_nodes,
                    new_keyspace.as_ref(),
                )?;

                Ok::<_, super::Error>(super::Migration {
                    nodes: new_nodes,
                    pulling_nodes: m.pulling_nodes.into_owned(),
                    keyspace: new_keyspace,
                    plan: Arc::new(plan),
                })
            })
            .transpose()?;

        Ok(Self {
            nodes,
            restarting_node: s.restarting_node.map(Cow::into_owned),
            keyspace,
            migration,
            version: s.version,
        })
    }
}
