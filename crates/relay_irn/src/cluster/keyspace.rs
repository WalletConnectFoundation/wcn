use {
    super::replication::Strategy,
    crate::cluster::{
        error::ClusterError,
        keyspace::pending_ranges::PendingRanges,
        replication::ReplicaSet,
    },
    range::MergedRanges,
    std::{collections::BTreeMap, fmt::Debug, hash::Hash},
};
pub use {
    hashring::{HashRing, RingPosition},
    range::KeyRange,
};

pub mod hashring;
pub mod partitioner;
pub mod pending_ranges;
pub mod range;

pub type DefaultMergedRanges = MergedRanges<RingPosition, Vec<KeyRange<RingPosition>>>;

/// Defines key position within a keyspace.
pub type KeyPosition = u64;

/// Defines a keyspace over which data is distributed.
pub trait Keyspace {
    /// The type of the node living in this keyspace.
    type Node: Hash + Clone + Copy + Debug + Eq + PartialEq + Ord + PartialOrd + 'static;

    /// Returns the replication strategy for this keyspace.
    fn replication_strategy(&self) -> &Strategy;

    /// Returns ring position to which a given key will be assigned.
    fn key_position<K: Hash>(&self, key: &K) -> RingPosition;

    /// Returns `true` if the keyspace is empty.
    fn is_empty(&self) -> bool;

    /// Returns `true` if the keyspace contains the given node.
    fn has_node<N: AsRef<Self::Node>>(&self, node: N) -> bool;

    /// Adds the given node to the keyspace.
    fn add_node<N: AsRef<Self::Node>>(&mut self, node: N) -> Result<(), ClusterError>;

    /// Removes the given node from the keyspace.
    fn remove_node<N: AsRef<Self::Node>>(&mut self, node: N) -> Result<(), ClusterError>;

    /// Returns the replica set configuration for the given key position.
    fn replica_set(&self, pos: RingPosition) -> ReplicaSet<Vec<Self::Node>, Self::Node>;

    /// Calculates the pending ranges for the given set of joining and leaving
    /// peers.
    fn pending_ranges(
        &self,
        joining_peers: Vec<Self::Node>,
        leaving_peers: Vec<Self::Node>,
    ) -> Result<BTreeMap<Self::Node, PendingRanges>, ClusterError>;
}
