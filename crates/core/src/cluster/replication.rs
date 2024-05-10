pub use strategy::Strategy;
use {
    crate::{cluster::keyspace::hashring::RingNode, PeerId},
    serde::{Deserialize, Serialize},
    std::marker::PhantomData,
};

pub mod strategy;

/// Consistency level for replicated persistence operations.
///
/// Defines how many replicas must be updated before the operation is considered
/// successful.
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ConsistencyLevel {
    One,
    #[default]
    Quorum,
    All,
}

/// A replica set.
///
/// The replica set is a set of peers that are responsible for the same key.
/// Coordinator node might or might not be in the replica set, so when
/// requesting replica list adapter supply local node id, so it can be filtered
/// out.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSet<T, P = PeerId>
where
    P: 'static,
    T: AsRef<[P]>,
{
    replicas: T,
    strategy: Strategy,
    _marker: PhantomData<P>,
}

impl<T, P> ReplicaSet<T, P>
where
    P: RingNode + 'static,
    T: AsRef<[P]>,
{
    /// Creates a new replica set.
    pub fn new(replicas: T, strategy: Strategy) -> Self {
        ReplicaSet {
            replicas,
            strategy,
            _marker: PhantomData,
        }
    }

    /// Returns `true` if a given node is in the replica set.
    pub fn contains(&self, node: &P) -> bool {
        self.replicas.as_ref().contains(node)
    }

    /// Returns remote peers in replica set.
    ///
    /// If coordinator is in the replica set, it is filtered out, as requests to
    /// it should be processed locally.
    pub fn remote_replicas<'a>(&'a self, coordinator: &'a P) -> impl Iterator<Item = &P> + '_ {
        self.replicas
            .as_ref()
            .iter()
            .filter(move |peer| *peer != coordinator)
    }

    /// Returns copy of all peers in replica set.
    pub fn replicas(&self) -> Vec<P> {
        self.replicas.as_ref().to_vec()
    }

    /// Returns selected replication strategy.
    pub fn strategy(&self) -> &Strategy {
        &self.strategy
    }

    /// Checks if the replica set is valid.
    pub fn is_valid(&self) -> bool {
        self.replicas.as_ref().len() >= self.strategy.required_replica_count()
    }
}

pub type PeerIdReplicaSet = ReplicaSet<Vec<PeerId>, PeerId>;

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        crate::cluster::keyspace::{
            hashring::{HashRing, RingPosition},
            KeyRange,
        },
    };

    pub fn assert_replicas(
        strategy: &Strategy,
        ring: &HashRing,
        pos: RingPosition,
        mut expected: Vec<PeerId>,
    ) {
        let mut replicas = strategy.natural_replicas(ring, pos);
        replicas.sort();
        expected.sort();
        assert_eq!(replicas, expected);
    }

    #[track_caller]
    pub fn assert_ranges(
        strategy: &Strategy,
        positions: &[(RingPosition, PeerId)],
        start_pos: RingPosition,
        expected: Vec<KeyRange<RingPosition>>,
    ) {
        let ring = HashRing::from(positions.to_owned());
        assert_eq!(strategy.key_ranges(&ring, start_pos, true), expected);
    }

    #[test]
    fn remote_replicas_with_coordinator() {
        let peers = vec![
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
        ];

        let replica_set = ReplicaSet::new(peers.clone(), Strategy::default());

        let coordinator = &peers[0].clone();
        let remote_replicas: Vec<_> = replica_set.remote_replicas(coordinator).collect();

        // Assert that replica set contains coordinator, but remote replicas do not.
        assert_eq!(remote_replicas.len(), 4);
        assert!(replica_set.contains(coordinator));
        assert!(!remote_replicas.contains(&coordinator));
    }

    #[test]
    fn remote_replicas_without_coordinator() {
        let peers = vec![
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
        ];

        let replica_set = ReplicaSet::new(peers, Strategy::default());

        let coordinator = &PeerId::random();
        let remote_replicas: Vec<_> = replica_set.remote_replicas(coordinator).collect();

        // Assert that replica set does not contain coordinator.
        assert_eq!(remote_replicas.len(), 5);
        assert!(!replica_set.contains(coordinator));
        assert!(!remote_replicas.contains(&coordinator));
    }
}
