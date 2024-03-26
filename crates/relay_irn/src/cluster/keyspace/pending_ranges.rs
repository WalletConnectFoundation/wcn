use {
    crate::{
        cluster::{
            error::ClusterError,
            keyspace::{
                hashring::RingDirection::Clockwise,
                range::KeyRange,
                HashRing,
                RingPosition,
            },
            replication::Strategy,
        },
        PeerId,
    },
    num_traits::Bounded,
    std::{
        cmp::Ordering,
        collections::{BTreeMap, BTreeSet},
        fmt::Debug,
    },
};

/// Key range that is pending to be pushed or pulled.
/// Pulling can be done from multiple peers.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub enum PendingRange<T>
where
    T: Bounded,
{
    Push {
        range: KeyRange<T>,
        destination: PeerId,
    },
    Pull {
        range: KeyRange<T>,
        sources: BTreeSet<PeerId>, // One can pull from multiple peers.
    },
}

pub type PendingRanges = BTreeSet<PendingRange<RingPosition>>;

impl<T> PartialOrd<Self> for PendingRange<T>
where
    T: Bounded + PartialEq + Ord + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Pending ranges are stored in sorted set, so we need to implement `Ord`.
impl<T> Ord for PendingRange<T>
where
    T: Bounded + PartialEq + Ord + Debug,
{
    /// Compares two ranges by their start position. If start position is the
    /// same, then comparison is done by the destination/source peers (for
    /// consistency).
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::Push { range, destination },
                Self::Push {
                    range: other,
                    destination: other_destination,
                },
            ) => {
                if range.start == other.start {
                    destination.cmp(other_destination)
                } else {
                    range.start.cmp(&other.start)
                }
            }
            (
                Self::Pull {
                    range,
                    sources: source,
                },
                Self::Pull {
                    range: other,
                    sources: other_source,
                },
            ) => {
                if range.start == other.start {
                    source.cmp(other_source)
                } else {
                    range.start.cmp(&other.start)
                }
            }
            (Self::Push { .. }, Self::Pull { .. }) => Ordering::Less,
            (Self::Pull { .. }, Self::Push { .. }) => Ordering::Greater,
        }
    }
}

/// Calculates pending ranges for a given keyspace.
pub struct PendingRangesCalculator<'a> {
    ring: &'a HashRing,
    strategy: Strategy,
    joining_peers: Vec<PeerId>,
    leaving_peers: Vec<PeerId>,
    min_required_nodes: usize,
}

impl<'a> PendingRangesCalculator<'a> {
    pub fn new(ring: &'a HashRing, strategy: Strategy) -> Self {
        let min_required_nodes = strategy.replication_factor();
        Self {
            ring,
            strategy,
            joining_peers: Vec::new(),
            leaving_peers: Vec::new(),
            min_required_nodes,
        }
    }

    pub fn with_joining_peers(mut self, joining_peers: Vec<PeerId>) -> Self {
        self.joining_peers = joining_peers;
        self
    }

    pub fn with_leaving_peers(mut self, leaving_peers: Vec<PeerId>) -> Self {
        self.leaving_peers = leaving_peers;
        self
    }

    pub fn with_min_required_nodes(mut self, count: usize) -> Self {
        self.min_required_nodes = count;
        self
    }

    /// Given list of joining and leaving peers, calculates pending ranges.
    ///
    /// For convenience, both push and pull ranges are returned. Note, that pull
    /// ranges can be calculated from push ranges deterministically.
    pub fn ranges(self) -> Result<BTreeMap<PeerId, PendingRanges>, ClusterError> {
        if self.no_calculation_needed() {
            return Ok(BTreeMap::new());
        }

        let mut pending_ranges = BTreeMap::new();
        let mut ring = self.ring.clone();

        // Remove leaving peers from the cloned ring. All the ranges that were owned by
        // leaving peers will be transferred to the new owners.
        for leaving_peer in &self.leaving_peers {
            ring.remove_node(leaving_peer)?;
        }

        // Joining peers are added to the cloned ring, so that we can calculate the new
        // assigned key ranges precisely. When calculating from where to pull
        // the keys, the original ring is used.
        for joining_peer in self.joining_peers.iter().copied() {
            ring.add_node(joining_peer)?;
        }

        // Calculate affected ranges that will be pushed to new owners from leaving
        // peers (and peers that have those ranges as replicated).
        for leaving_peer in &self.leaving_peers {
            // If leaving peer is also joining, then it will be removed from the ring and
            // added back again, so we don't need to process it here.
            if self.joining_peers.contains(leaving_peer) {
                continue;
            }
            // Process key ranges that are owned by the leaving peer (use original ring).
            for pos in self.ring.node_positions(leaving_peer) {
                self.process_leaving_peer(&mut pending_ranges, &ring, pos);
            }
        }

        // Calculate ranges that will be pulled from existing peers to joining peers.
        for joining_peer in &self.joining_peers {
            if self.leaving_peers.contains(joining_peer) {
                continue;
            }
            // Process key ranges that will be targeting the joining peer.
            for pos in ring.node_positions(joining_peer) {
                self.process_joining_peer(&mut pending_ranges, &ring, joining_peer, pos);
            }
        }

        Ok(pending_ranges)
    }

    fn process_joining_peer(
        &self,
        pending_ranges: &mut BTreeMap<PeerId, PendingRanges>,
        ring: &HashRing,
        joining_peer: &PeerId,
        pos: RingPosition,
    ) {
        for range in self.strategy.key_ranges(ring, pos, false) {
            // Since the range is obtained from the new ring, it may contain leaving peers
            // (which are already removed from the new ring). So, to cover all points of the
            // range, we need to obtain sub-ranges that are contained within it (on the
            // original ring).
            let mut start_pos = range.start;
            for (&token_pos, _) in self.ring.tokens(start_pos.wrapping_add(1), Clockwise) {
                let sub_range = if range.contains(&token_pos) && token_pos != start_pos {
                    KeyRange::new(start_pos, token_pos)
                } else {
                    KeyRange::new(start_pos, range.end)
                };

                // Get peers that currently own the range.
                let source_peers = self.strategy.natural_replicas(self.ring, sub_range.start);
                for source_peer in &source_peers {
                    // Add a pending range to the source peer.
                    pending_ranges
                        .entry(*source_peer)
                        .or_default()
                        .insert(PendingRange::Push {
                            range: sub_range,
                            destination: *joining_peer,
                        });
                }

                pending_ranges
                    .entry(*joining_peer)
                    .or_default()
                    .insert(PendingRange::Pull {
                        range: sub_range,
                        sources: BTreeSet::from_iter(source_peers),
                    });

                // If peer at token position is not within the range, we are done.
                if !range.contains(&token_pos) {
                    break;
                }
                start_pos = token_pos;
            }
        }
    }

    fn process_leaving_peer(
        &self,
        pending_ranges: &mut BTreeMap<PeerId, PendingRanges>,
        ring: &HashRing,
        pos: RingPosition,
    ) {
        for range in self.strategy.key_ranges(self.ring, pos, false) {
            // Since range is obtained from the original ring, the new ring might contain
            // joining peers within that range, so we should process the range as split into
            // sub-ranges created by such joining peers.
            let mut start_pos = range.start;
            for (&token_pos, _) in ring.tokens(start_pos.wrapping_add(1), Clockwise) {
                let sub_range = if range.contains(&token_pos) && token_pos != start_pos {
                    KeyRange::new(start_pos, token_pos)
                } else {
                    KeyRange::new(start_pos, range.end)
                };

                // Get peers that currently own the range.
                let source_peers = self.strategy.natural_replicas(self.ring, sub_range.start);
                // Get peers that should own the range (on a new ring).
                let destination_peers = self.strategy.natural_replicas(ring, sub_range.start);
                // Find destination peers that are not already owners.
                let destination_peers = destination_peers
                    .into_iter()
                    .filter(|peer| !source_peers.contains(peer))
                    .collect::<Vec<_>>();

                for destination_peer in destination_peers {
                    // Pull the range from the leaving peer (and any other peers that have
                    // it) to the new owner.
                    pending_ranges.entry(destination_peer).or_default().insert(
                        PendingRange::Pull {
                            range: sub_range,
                            sources: BTreeSet::from_iter(source_peers.clone()),
                        },
                    );
                    // Log the range that will be pushed to the new owner (by leaving node,
                    // and any nodes that currently own the
                    // range).
                    for source_peer in &source_peers {
                        pending_ranges.entry(*source_peer).or_default().insert(
                            PendingRange::Push {
                                range: sub_range,
                                destination: destination_peer,
                            },
                        );
                    }
                }
                // If peer at token position is not within the range, we are done.
                if !range.contains(&token_pos) {
                    break;
                }
                start_pos = token_pos;
            }
        }
    }

    /// Returns true if there is no need to calculate pending ranges.
    fn no_calculation_needed(&self) -> bool {
        // No need to calculate anything if there are no current peers in the ring, or
        // if no peers are joining or leaving.
        let empty_ring = self.ring.is_empty();
        let no_changing_peers = self.joining_peers.is_empty() && self.leaving_peers.is_empty();
        if empty_ring || no_changing_peers {
            return true;
        }

        // If there are no peers left after the joining and leaving peers, then there
        // should not be any pending ranges reported.
        let not_enough_peers_left =
            (self.ring.len() + self.joining_peers.len()) <= self.leaving_peers.len();
        if not_enough_peers_left {
            return true;
        }

        // The number of source peers is too small (cluster is only forming).
        if self.ring.node_count() < self.min_required_nodes {
            return true;
        }

        false
    }
}

#[cfg(test)]
#[allow(clippy::reversed_empty_ranges)]
mod tests {
    use {
        super::*,
        crate::cluster::{
            replication::ConsistencyLevel,
            test_util::{pulls_from_pushes, push, push_range, PeerPosition, PEERS},
        },
    };

    // Set of current, joining and leaving peers.
    #[derive(Debug, Clone)]
    struct PeerSet(Vec<usize>, Vec<usize>, Vec<usize>);

    impl PeerSet {
        fn current_peers(&self) -> Vec<PeerPosition> {
            self.0.iter().map(|idx| PEERS[*idx]).collect()
        }

        fn joining_peers(&self) -> Vec<PeerPosition> {
            self.1.iter().map(|idx| PEERS[*idx]).collect()
        }

        fn leaving_peers(&self) -> Vec<PeerPosition> {
            self.2.iter().map(|idx| PEERS[*idx]).collect()
        }
    }

    fn peer_id(idx: usize) -> PeerId {
        PEERS[idx].peer_id
    }

    #[track_caller]
    fn assert_ranges(
        strategy: Strategy,
        peers: PeerSet,
        expected_ranges: Vec<(usize, Vec<PendingRange<RingPosition>>)>,
    ) {
        let mut ring = HashRing::new(1);
        for PeerPosition { peer_id, position } in peers.current_peers() {
            ring.set_node(peer_id, vec![position]);
        }

        let joining_peers: Vec<_> = peers.joining_peers().iter().map(|p| p.peer_id).collect();
        let leaving_peers: Vec<_> = peers.leaving_peers().iter().map(|p| p.peer_id).collect();

        // Convert expected ranges to a map.
        let expected_ranges = expected_ranges
            .into_iter()
            .map(|(peer_idx, ranges)| (peer_id(peer_idx), BTreeSet::from_iter(ranges)))
            .collect::<BTreeMap<_, _>>();

        // Extract pulls from pushes.
        let expected_ranges = pulls_from_pushes(expected_ranges);

        let calculated_ranges = PendingRangesCalculator::new(&ring, strategy)
            .with_min_required_nodes(peers.current_peers().len())
            .with_joining_peers(joining_peers)
            .with_leaving_peers(leaving_peers)
            .ranges()
            .unwrap();

        assert_eq!(&calculated_ranges, &expected_ranges, "{}", {
            // Pretty print the difference.
            difference::Changeset::new(
                &format!("{:#?}", &expected_ranges),
                &format!("{:#?}", &calculated_ranges),
                " ",
            )
        });
    }

    #[test]
    fn empty_ring() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        let peers = PeerSet(vec![], vec![], vec![]); // No current peers.

        // No joining or leaving peers.
        assert_ranges(non_replicated.clone(), peers.clone(), vec![]);
        assert_ranges(replicated.clone(), peers, vec![]);

        // Has joining peers.
        let peers = PeerSet(vec![], vec![1, 2, 3], vec![]);
        assert_ranges(non_replicated.clone(), peers.clone(), vec![]);
        assert_ranges(replicated.clone(), peers, vec![]);

        // Has leaving peers.
        let peers = PeerSet(vec![], vec![], vec![1, 2, 3]);
        assert_ranges(non_replicated.clone(), peers.clone(), vec![]);
        assert_ranges(replicated.clone(), peers, vec![]);

        // Has both joining and leaving peers.
        let peers = PeerSet(vec![], vec![1, 5, 7], vec![2, 3]);
        assert_ranges(non_replicated, peers.clone(), vec![]);
        assert_ranges(replicated, peers, vec![]);
    }

    #[test]
    fn one_exists_add_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // Ring has a single peer. No joining or leaving peers.
            // ____0____p0____|
            let peers = PeerSet(vec![0], vec![], vec![]);
            assert_ranges(non_replicated.clone(), peers.clone(), vec![]);
            assert_ranges(replicated.clone(), peers, vec![]);
        }

        {
            // ____0____p0____+p1____|
            let peers = PeerSet(vec![0], vec![1], vec![]);
            let expected_ranges = vec![(0, vec![push(0..1, 1)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![(0, vec![push(0..1, 1), push(1..0, 1)])];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn one_exists_remove_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // Degenerate case: p0 is the only peer, no ranges are pending if it is removed.
            // ____0____-p0____|
            let peers = PeerSet(vec![0], vec![], vec![0]);
            assert_ranges(non_replicated, peers.clone(), vec![]);
            assert_ranges(replicated, peers, vec![]);
        }
    }

    #[test]
    fn one_exists_add_two() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____+p1____+p2____|
            let peers = PeerSet(vec![0], vec![1, 2], vec![]);

            let expected_ranges = vec![(0, vec![push(0..1, 1), push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![(0, vec![push(0..1, 1), push(2..0, 1), push(1..2, 2)])];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____+p2____|
            let peers = PeerSet(vec![1], vec![0, 2], vec![]);

            let expected_ranges = vec![(1, vec![push(2..0, 0), push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![(1, vec![
                push(0..1, 0),
                push(1..2, 0),
                push(2..0, 0),
                push(1..2, 2),
            ])];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____+p1____p2____|
            let peers = PeerSet(vec![2], vec![0, 1], vec![]);

            let expected_ranges = vec![(2, vec![push(2..0, 0), push(0..1, 1)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![(2, vec![
                push(0..1, 0),
                push(1..2, 0),
                push(2..0, 0),
                push(0..1, 1),
                push(2..0, 1),
            ])];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn two_exist_add_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____+p2____|
            let peers = PeerSet(vec![0, 1], vec![2], vec![]);

            let expected_ranges = vec![(0, vec![push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![(0, vec![push(1..2, 2)]), (1, vec![push(1..2, 2)])];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____|
            let peers = PeerSet(vec![0, 2], vec![1], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(0..1, 1), push(2..0, 1)]),
                (2, vec![push(0..1, 1), push(2..0, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____|
            let peers = PeerSet(vec![1, 2], vec![0], vec![]);

            let expected_ranges = vec![(1, vec![push(2..0, 0)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(2..0, 0)]),
                (2, vec![push(1..2, 0)]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn two_exist_remove_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____-p1____|
            let peers = PeerSet(vec![0, 1], vec![], vec![1]);

            let expected_ranges = vec![(1, vec![push(0..1, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            // Nothing to move in replicated case (since destination node has all the
            // ranges already).
            assert_ranges(replicated.clone(), peers, vec![]);
        }

        {
            // ____0____-p0____p1____|
            let peers = PeerSet(vec![0, 1], vec![], vec![0]);

            let expected_ranges = vec![(0, vec![push(1..0, 1)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);
            assert_ranges(replicated, peers, vec![]);
        }
    }

    #[test]
    fn two_exist_add_two() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____+p2____+p3____|
            let peers = PeerSet(vec![0, 1], vec![2, 3], vec![]);

            let expected_ranges = vec![(0, vec![push(1..2, 2), push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let push_to_peer2_and_peer3 =
                vec![push(1..2, 2), push(0..1, 3), push(1..2, 3), push(2..3, 3)];
            let expected_ranges = vec![
                (0, push_to_peer2_and_peer3.clone()),
                (1, push_to_peer2_and_peer3),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____+p3____|
            let peers = PeerSet(vec![0, 2], vec![1, 3], vec![]);

            let expected_ranges = vec![(0, vec![push(2..3, 3)]), (2, vec![push(0..1, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let push_to_peer1_and_peer3 = vec![
                push(0..1, 1),
                push(3..0, 1),
                push(2..3, 1),
                push(0..1, 3),
                push(1..2, 3),
                push(2..3, 3),
            ];
            let expected_ranges = vec![
                (0, push_to_peer1_and_peer3.clone()),
                (2, push_to_peer1_and_peer3),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____+p3____|
            let peers = PeerSet(vec![1, 2], vec![0, 3], vec![]);

            // Note: peer2 doesn't control any ranges that need to be transferred.
            let expected_ranges = vec![(1, vec![push(3..0, 0), push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(3..0, 0), push(2..3, 3), push(0..1, 3)]),
                (2, vec![push(1..2, 3)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____+p2____p3____|
            let peers = PeerSet(vec![0, 3], vec![1, 2], vec![]);

            // Note: peer0 doesn't control any ranges that need to be transferred.
            let expected_ranges = vec![(3, vec![push(1..2, 2), push(0..1, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..0, 1)]),
                (3, vec![push(0..1, 1), push(2..3, 1), push(1..2, 2)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____+p2____p3____|
            let peers = PeerSet(vec![1, 3], vec![0, 2], vec![]);

            let expected_ranges = vec![(1, vec![push(3..0, 0)]), (3, vec![push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(3..0, 0), push(1..2, 2)]),
                (3, vec![push(3..0, 0), push(1..2, 2)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____+p1____p2____p3____|
            let peers = PeerSet(vec![2, 3], vec![0, 1], vec![]);

            // Note: peer3 doesn't control any ranges that need to be transferred.
            let expected_ranges = vec![(2, vec![push(0..1, 1), push(3..0, 0)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let push_to_peer0_and_peer1 =
                vec![push(3..0, 0), push(0..1, 1), push(3..0, 1), push(2..3, 1)];
            let expected_ranges = vec![
                (2, push_to_peer0_and_peer1.clone()),
                (3, push_to_peer0_and_peer1),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn five_existing_add_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![]);

            let expected_ranges = vec![(0, vec![push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(3..4, 5)]),
                (1, vec![push(4..5, 5), push(3..4, 5), push(2..3, 5)]),
                // (2, vec![push(4..5, 5)]),
                (3, vec![push(2..3, 5)]),
                (4, vec![push(3..4, 5), push(2..3, 5), push(4..5, 5)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____p3____+p4____p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 5], vec![4], vec![]);

            let expected_ranges = vec![(5, vec![push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(5..0, 4), push(4..5, 4), push(3..4, 4)]),
                (1, vec![push(0..1, 4), push(5..0, 4)]),
                (2, vec![push(1..2, 4)]),
                (3, vec![push(2..3, 4), push(1..2, 4), push(0..1, 4)]),
                (5, vec![push(4..5, 4), push(3..4, 4), push(2..3, 4)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____+p3____p4____p5____|
            let peers = PeerSet(vec![0, 1, 2, 4, 5], vec![3], vec![]);

            let expected_ranges = vec![(4, vec![push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(0..1, 3), push(1..2, 3), push(2..3, 3)]),
                (1, vec![push(0..1, 3)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![push(0..1, 3), push(1..2, 3), push(2..3, 3)]),
                (5, vec![push(2..3, 3)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____p3____p4____p5____|
            let peers = PeerSet(vec![0, 1, 3, 4, 5], vec![2], vec![]);

            let expected_ranges = vec![(3, vec![push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (3, vec![push(1..2, 2)]),
                (4, vec![push(1..2, 2)]),
                (5, vec![push(1..2, 2)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____p3____p4____p5____|
            let peers = PeerSet(vec![0, 2, 3, 4, 5], vec![1], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(5..0, 1)]),
                (2, vec![push(0..1, 1), push(5..0, 1)]),
                (3, vec![push(0..1, 1)]),
                (4, vec![push(0..1, 1), push(5..0, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____p4____p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![]);

            let expected_ranges = vec![(1, vec![push(5..0, 0)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![push(3..4, 0), push(4..5, 0), push(5..0, 0)]),
                (4, vec![push(3..4, 0), push(4..5, 0), push(5..0, 0)]),
                (5, vec![push(3..4, 0), push(4..5, 0)]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn five_existing_remove_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____p2____p3____-p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![4]);

            let expected_ranges = vec![(4, vec![push(3..4, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![]; // sic!
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____-p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![3]);

            let expected_ranges = vec![(3, vec![push(2..3, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(2..3, 0)]),
                (2, vec![push(1..2, 0)]),
                (3, vec![push(0..1, 0), push(1..2, 0), push(2..3, 0)]),
                (4, vec![push(0..1, 0), push(1..2, 0), push(2..3, 0)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____-p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![2]);

            let expected_ranges = vec![(2, vec![push(1..2, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (2, vec![push(1..2, 1)]),
                (3, vec![push(1..2, 1)]),
                (4, vec![push(1..2, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____-p1____p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![1]);

            let expected_ranges = vec![(1, vec![push(0..1, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(4..0, 2), push(3..4, 2)]),
                (1, vec![
                    push(4..0, 2),
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                ]),
                (3, vec![push(0..1, 2), push(2..3, 2)]),
                (4, vec![
                    push(4..0, 2),
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![0]);

            let expected_ranges = vec![(0, vec![push(4..0, 1)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(4..0, 3), push(3..4, 3)]),
                (1, vec![push(4..0, 3), push(3..4, 3)]),
                (4, vec![push(4..0, 3), push(3..4, 3)]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn five_existing_add_two() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____p2____p3____p4____+p5____+p6____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5, 6], vec![]);

            let expected_ranges = vec![(0, vec![push(4..5, 5), push(5..6, 6)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 5),
                    push(3..4, 6),
                    push(4..5, 5),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 5),
                    push(3..4, 6),
                    push(4..5, 5),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
                (3, vec![push(2..3, 5)]),
                (4, vec![
                    push(2..3, 5),
                    push(3..4, 5),
                    push(3..4, 6),
                    push(4..5, 5),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____p3____+p4____p5____+p6____|
            let peers = PeerSet(vec![0, 1, 2, 3, 5], vec![4, 6], vec![]);

            let expected_ranges = vec![(0, vec![push(5..6, 6)]), (5, vec![push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 4),
                    push(3..4, 6),
                    push(4..5, 4),
                    push(4..5, 6),
                    push(5..6, 4),
                    push(5..6, 6),
                    push(6..0, 4),
                ]),
                (1, vec![
                    push(0..1, 4),
                    push(5..6, 4),
                    push(5..6, 6),
                    push(6..0, 4),
                ]),
                (2, vec![push(1..2, 4)]),
                (3, vec![push(0..1, 4), push(1..2, 4), push(2..3, 4)]),
                (5, vec![
                    push(2..3, 4),
                    push(3..4, 4),
                    push(3..4, 6),
                    push(4..5, 4),
                    push(4..5, 6),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____+p3____p4____p5____+p6____|
            let peers = PeerSet(vec![0, 1, 2, 4, 5], vec![3, 6], vec![]);

            let expected_ranges = vec![(0, vec![push(5..6, 6)]), (4, vec![push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
                (1, vec![push(0..1, 3), push(5..6, 6)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
                (5, vec![push(2..3, 3), push(3..4, 6), push(4..5, 6)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____p3____p4____p5____+p6____|
            let peers = PeerSet(vec![0, 1, 3, 4, 5], vec![2, 6], vec![]);

            let expected_ranges = vec![(0, vec![push(5..6, 6)]), (3, vec![push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 6), push(4..5, 6), push(5..6, 6)]),
                (1, vec![push(5..6, 6)]),
                (3, vec![push(1..2, 2)]),
                (4, vec![
                    push(1..2, 2),
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 6),
                ]),
                (5, vec![push(1..2, 2), push(3..4, 6), push(4..5, 6)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____p3____p4____p5____+p6____|
            let peers = PeerSet(vec![0, 2, 3, 4, 5], vec![1, 6], vec![]);

            let expected_ranges = vec![(0, vec![push(5..6, 6)]), (2, vec![push(0..1, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 1),
                    push(5..6, 6),
                    push(6..0, 1),
                ]),
                (2, vec![
                    push(0..1, 1),
                    push(5..6, 1),
                    push(5..6, 6),
                    push(6..0, 1),
                ]),
                (3, vec![push(0..1, 1)]),
                (4, vec![
                    push(0..1, 1),
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 1),
                    push(5..6, 6),
                    push(6..0, 1),
                ]),
                (5, vec![push(3..4, 6), push(4..5, 6)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____p4____p5____+p6____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0, 6], vec![]);

            let expected_ranges = vec![(1, vec![push(6..0, 0), push(5..6, 6)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..6, 6), push(6..0, 0)]),
                (3, vec![
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 6),
                    push(6..0, 0),
                ]),
                (4, vec![
                    push(3..4, 6),
                    push(4..5, 6),
                    push(5..6, 6),
                    push(6..0, 0),
                ]),
                (5, vec![push(3..4, 6), push(4..5, 6)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____p3____+p4____+p5____p6____|
            let peers = PeerSet(vec![0, 1, 2, 3, 6], vec![4, 5], vec![]);

            let expected_ranges = vec![(6, vec![push(3..4, 4), push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 4)]),
                (1, vec![
                    push(0..1, 4),
                    push(2..3, 4),
                    push(2..3, 5),
                    push(3..4, 4),
                    push(3..4, 5),
                    push(4..5, 4),
                    push(4..5, 5),
                    push(5..6, 4),
                    push(6..0, 4),
                ]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 4),
                    push(2..3, 5),
                ]),
                (6, vec![
                    push(3..4, 4),
                    push(3..4, 5),
                    push(4..5, 4),
                    push(4..5, 5),
                    push(5..6, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____+p3____p4____+p5____p6____|
            let peers = PeerSet(vec![0, 1, 2, 4, 6], vec![3, 5], vec![]);

            let expected_ranges = vec![(4, vec![push(2..3, 3)]), (6, vec![push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(0..1, 3),
                    push(2..3, 3),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
                (6, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____p3____p4____+p5____p6____|
            let peers = PeerSet(vec![0, 1, 3, 4, 6], vec![2, 5], vec![]);

            let expected_ranges = vec![(3, vec![push(1..2, 2)]), (6, vec![push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(1..2, 2),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
                (3, vec![push(1..2, 2), push(2..3, 5)]),
                (4, vec![
                    push(1..2, 2),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
                (6, vec![push(3..4, 5), push(4..5, 5)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____p3____p4____+p5____p6____|
            let peers = PeerSet(vec![0, 2, 3, 4, 6], vec![1, 5], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)]), (6, vec![push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 1)]),
                (2, vec![
                    push(0..1, 1),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (3, vec![push(0..1, 1), push(2..3, 5)]),
                (4, vec![
                    push(0..1, 1),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (6, vec![push(3..4, 5), push(4..5, 5), push(5..6, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____p4____+p5____p6____|
            let peers = PeerSet(vec![1, 2, 3, 4, 6], vec![0, 5], vec![]);

            let expected_ranges = vec![(1, vec![push(6..0, 0)]), (6, vec![push(4..5, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(6..0, 0),
                ]),
                (3, vec![push(2..3, 5), push(6..0, 0)]),
                (4, vec![
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(6..0, 0),
                ]),
                (6, vec![push(3..4, 5), push(4..5, 5)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____+p3____+p4____p5____p6____|
            let peers = PeerSet(vec![0, 1, 2, 5, 6], vec![3, 4], vec![]);

            let expected_ranges = vec![(5, vec![push(2..3, 3), push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 4)]),
                (1, vec![
                    push(0..1, 3),
                    push(0..1, 4),
                    push(5..6, 4),
                    push(6..0, 4),
                ]),
                (2, vec![push(1..2, 3), push(1..2, 4)]),
                (5, vec![
                    push(2..3, 3),
                    push(2..3, 4),
                    push(3..4, 4),
                    push(4..5, 4),
                ]),
                (6, vec![
                    push(0..1, 3),
                    push(0..1, 4),
                    push(1..2, 3),
                    push(1..2, 4),
                    push(2..3, 3),
                    push(2..3, 4),
                    push(3..4, 4),
                    push(4..5, 4),
                    push(5..6, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____p3____+p4____p5____p6____|
            let peers = PeerSet(vec![0, 1, 3, 5, 6], vec![2, 4], vec![]);

            let expected_ranges = vec![(3, vec![push(1..2, 2)]), (5, vec![push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 4)]),
                (1, vec![push(0..1, 4), push(5..6, 4), push(6..0, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 2),
                    push(1..2, 4),
                    push(2..3, 4),
                ]),
                (5, vec![
                    push(1..2, 2),
                    push(1..2, 4),
                    push(2..3, 4),
                    push(3..4, 4),
                    push(4..5, 4),
                ]),
                (6, vec![push(3..4, 4), push(4..5, 4), push(5..6, 4)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____p3____+p4____p5____p6____|
            let peers = PeerSet(vec![0, 2, 3, 5, 6], vec![1, 4], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)]), (5, vec![push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 1), push(6..0, 4)]),
                (2, vec![
                    push(0..1, 1),
                    push(0..1, 4),
                    push(1..2, 4),
                    push(5..6, 1),
                    push(5..6, 4),
                    push(6..0, 1),
                    push(6..0, 4),
                ]),
                (3, vec![
                    push(0..1, 1),
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 4),
                ]),
                (5, vec![push(2..3, 4), push(3..4, 4), push(4..5, 4)]),
                (6, vec![
                    push(3..4, 4),
                    push(4..5, 4),
                    push(5..6, 1),
                    push(5..6, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____+p4____p5____p6____|
            let peers = PeerSet(vec![1, 2, 3, 5, 6], vec![0, 4], vec![]);

            let expected_ranges = vec![(1, vec![push(6..0, 0)]), (5, vec![push(3..4, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(0..1, 4),
                    push(5..6, 4),
                    push(6..0, 0),
                    push(6..0, 4),
                ]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 4),
                    push(6..0, 0),
                    push(6..0, 4),
                ]),
                (5, vec![push(2..3, 4), push(3..4, 4), push(4..5, 4)]),
                (6, vec![push(3..4, 4), push(4..5, 4), push(5..6, 4)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____+p3____p4____p5____p6____|
            let peers = PeerSet(vec![0, 1, 4, 5, 6], vec![2, 3], vec![]);

            let expected_ranges = vec![(4, vec![push(1..2, 2), push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 2),
                    push(1..2, 3),
                    push(2..3, 3),
                ]),
                (5, vec![push(1..2, 2), push(1..2, 3), push(2..3, 3)]),
                (6, vec![
                    push(0..1, 3),
                    push(1..2, 2),
                    push(1..2, 3),
                    push(2..3, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____+p3____p4____p5____p6____|
            let peers = PeerSet(vec![0, 2, 4, 5, 6], vec![1, 3], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)]), (4, vec![push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 1)]),
                (2, vec![
                    push(0..1, 1),
                    push(0..1, 3),
                    push(1..2, 3),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (4, vec![
                    push(0..1, 1),
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (5, vec![push(2..3, 3)]),
                (6, vec![
                    push(0..1, 1),
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(5..6, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____+p3____p4____p5____p6____|
            let peers = PeerSet(vec![1, 2, 4, 5, 6], vec![0, 3], vec![]);

            let expected_ranges = vec![(1, vec![push(6..0, 0)]), (4, vec![push(2..3, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 3), push(6..0, 0)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(6..0, 0),
                ]),
                (5, vec![push(2..3, 3)]),
                (6, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 3),
                    push(6..0, 0),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____+p2____p3____p4____p5____p6____|
            let peers = PeerSet(vec![0, 3, 4, 5, 6], vec![1, 2], vec![]);

            let expected_ranges = vec![(3, vec![push(0..1, 1), push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(6..0, 1)]),
                (3, vec![push(0..1, 1), push(1..2, 2)]),
                (4, vec![
                    push(0..1, 1),
                    push(1..2, 2),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (5, vec![
                    push(0..1, 1),
                    push(1..2, 2),
                    push(5..6, 1),
                    push(6..0, 1),
                ]),
                (6, vec![push(5..6, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____+p2____p3____p4____p5____p6____|
            let peers = PeerSet(vec![1, 3, 4, 5, 6], vec![0, 2], vec![]);

            let expected_ranges = vec![(1, vec![push(6..0, 0)]), (3, vec![push(1..2, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(6..0, 0)]),
                (3, vec![push(1..2, 2), push(6..0, 0)]),
                (4, vec![push(1..2, 2), push(6..0, 0)]),
                (5, vec![push(1..2, 2)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____+p1____p2____p3____p4____p5____p6____|
            let peers = PeerSet(vec![2, 3, 4, 5, 6], vec![0, 1], vec![]);

            let expected_ranges = vec![(2, vec![push(0..1, 1), push(6..0, 0)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (2, vec![
                    push(0..1, 1),
                    push(5..6, 1),
                    push(6..0, 0),
                    push(6..0, 1),
                ]),
                (3, vec![push(0..1, 1), push(6..0, 0), push(6..0, 1)]),
                (4, vec![
                    push(0..1, 1),
                    push(5..6, 1),
                    push(6..0, 0),
                    push(6..0, 1),
                ]),
                (6, vec![push(5..6, 1)]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn five_existing_remove_two() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____p2____-p3____-p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![3, 4]);

            let expected_ranges = vec![(3, vec![push(2..3, 0)]), (4, vec![push(3..4, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(2..3, 0)]),
                (2, vec![push(1..2, 0)]),
                (3, vec![push(0..1, 0), push(1..2, 0), push(2..3, 0)]),
                (4, vec![push(0..1, 0), push(1..2, 0), push(2..3, 0)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____-p2____p3____-p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![2, 4]);

            let expected_ranges = vec![(2, vec![push(1..2, 3)]), (4, vec![push(3..4, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (2, vec![push(1..2, 1)]),
                (3, vec![push(1..2, 1)]),
                (4, vec![push(1..2, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____-p1____p2____p3____-p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![1, 4]);

            let expected_ranges = vec![(1, vec![push(0..1, 2)]), (4, vec![push(3..4, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 2), push(4..0, 2)]),
                (1, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(4..0, 2),
                ]),
                (3, vec![push(0..1, 2), push(2..3, 2)]),
                (4, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(4..0, 2),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____p3____-p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![0, 4]);

            let expected_ranges = vec![(0, vec![push(4..0, 1)]), (4, vec![push(3..4, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 3), push(4..0, 3)]),
                (1, vec![push(3..4, 3), push(4..0, 3)]),
                (4, vec![push(3..4, 3), push(4..0, 3)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____-p2____-p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![2, 3]);

            let expected_ranges = vec![(2, vec![push(1..2, 4)]), (3, vec![push(2..3, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(2..3, 0)]),
                (2, vec![push(1..2, 0), push(1..2, 1)]),
                (3, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(1..2, 1),
                    push(2..3, 0),
                ]),
                (4, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(1..2, 1),
                    push(2..3, 0),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____-p1____p2____-p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![1, 3]);

            let expected_ranges = vec![(1, vec![push(0..1, 2)]), (3, vec![push(2..3, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 2), push(4..0, 2)]),
                (1, vec![
                    push(0..1, 0),
                    push(0..1, 2),
                    push(2..3, 0),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(4..0, 2),
                ]),
                (2, vec![push(1..2, 0)]),
                (3, vec![
                    push(0..1, 0),
                    push(0..1, 2),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 2),
                ]),
                (4, vec![
                    push(0..1, 0),
                    push(0..1, 2),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(4..0, 2),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____-p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![0, 3]);

            let expected_ranges = vec![(0, vec![push(4..0, 1)]), (3, vec![push(2..3, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____-p1____-p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![1, 2]);

            let expected_ranges = vec![(1, vec![push(0..1, 3)]), (2, vec![push(1..2, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____-p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![0, 2]);

            let expected_ranges = vec![(0, vec![push(4..0, 1)]), (2, vec![push(1..2, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 3), push(4..0, 3)]),
                (1, vec![push(3..4, 3), push(4..0, 3)]),
                (2, vec![push(1..2, 1)]),
                (3, vec![push(1..2, 1)]),
                (4, vec![push(1..2, 1), push(3..4, 3), push(4..0, 3)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____-p1____p2____p3____p4____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![], vec![0, 1]);

            let expected_ranges = vec![(0, vec![push(4..0, 2)]), (1, vec![push(0..1, 2)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 2),
                    push(3..4, 3),
                    push(4..0, 2),
                    push(4..0, 3),
                ]),
                (1, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(3..4, 3),
                    push(4..0, 3),
                    push(4..0, 2),
                ]),
                (3, vec![push(0..1, 2), push(2..3, 2)]),
                (4, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 2),
                    push(3..4, 3),
                    push(4..0, 3),
                    push(4..0, 2),
                ]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn some_nodes_unaffected() {
        // The existing cluster is of such a size that there are nodes that are not
        // affected by the change.
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____-p2____p3____p4____p5____p6____-p7____p8____p9____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], vec![], vec![2, 7]);

            // peer0 do not have any ranges to move (pull or push).
            let expected_ranges = vec![
                (1, vec![push(5..6, 9), push(6..7, 9)]),
                (2, vec![push(1..2, 5)]),
                (3, vec![push(1..2, 5)]),
                (4, vec![push(1..2, 5)]),
                (5, vec![push(4..5, 9)]),
                (6, vec![push(4..5, 9), push(5..6, 9)]),
                (7, vec![push(4..5, 9), push(5..6, 9), push(6..7, 9)]),
                (8, vec![push(6..7, 9)]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn add_one_remove_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____p0____p1____p2____p3____-p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![4]);

            let expected_ranges = vec![(0, vec![push(4..5, 5)]), (4, vec![push(3..4, 5)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 5), push(4..5, 5)]),
                (1, vec![push(2..3, 5), push(3..4, 5), push(4..5, 5)]),
                (3, vec![push(2..3, 5)]),
                (4, vec![push(2..3, 5), push(3..4, 5), push(4..5, 5)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____-p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![3]);

            let expected_ranges = vec![(0, vec![push(4..5, 5)]), (3, vec![push(2..3, 4)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 5), push(4..5, 5)]),
                (1, vec![
                    push(0..1, 0),
                    push(2..3, 0),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
                (2, vec![push(1..2, 0)]),
                (3, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 5),
                ]),
                (4, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____-p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![2]);

            let expected_ranges = vec![(0, vec![push(4..5, 5)]), (2, vec![push(1..2, 3)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 5), push(4..5, 5)]),
                (1, vec![push(2..3, 5), push(3..4, 5), push(4..5, 5)]),
                (2, vec![push(1..2, 5)]),
                (3, vec![push(1..2, 5), push(2..3, 5)]),
                (4, vec![
                    push(1..2, 5),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____-p1____p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![1]);

            let expected_ranges = vec![(0, vec![push(4..5, 5)]), (1, vec![push(0..1, 2)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 5), push(4..5, 5), push(5..0, 2)]),
                (1, vec![
                    push(0..1, 2),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(5..0, 2),
                ]),
                (3, vec![push(0..1, 2), push(2..3, 5)]),
                (4, vec![
                    push(0..1, 2),
                    push(2..3, 5),
                    push(3..4, 5),
                    push(4..5, 5),
                    push(5..0, 2),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![0]);

            let expected_ranges = vec![(0, vec![push(4..5, 5), push(5..0, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (3, vec![push(2..3, 5)]),
                (4, vec![
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____p3____+p4____-p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 5], vec![4], vec![5]);

            let expected_ranges = vec![(5, vec![push(3..4, 4), push(4..5, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 1),
                    push(3..4, 4),
                    push(4..5, 1),
                    push(4..5, 4),
                    push(5..0, 4),
                ]),
                (1, vec![push(0..1, 4), push(5..0, 4)]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 1),
                    push(2..3, 4),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(2..3, 4),
                    push(3..4, 1),
                    push(3..4, 4),
                    push(4..5, 1),
                    push(4..5, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____p2____+p3____p4____-p5____|
            let peers = PeerSet(vec![0, 1, 2, 4, 5], vec![3], vec![5]);

            let expected_ranges = vec![(4, vec![push(2..3, 3)]), (5, vec![push(4..5, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
                (1, vec![push(0..1, 3)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____p1____+p2____p3____p4____-p5____|
            let peers = PeerSet(vec![0, 1, 3, 4, 5], vec![2], vec![5]);

            let expected_ranges = vec![(3, vec![push(1..2, 2)]), (5, vec![push(4..5, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 1), push(4..5, 1)]),
                (3, vec![push(1..2, 2), push(2..3, 1)]),
                (4, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
                (5, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____p0____+p1____p2____p3____p4____-p5____|
            let peers = PeerSet(vec![0, 2, 3, 4, 5], vec![1], vec![5]);

            let expected_ranges = vec![(2, vec![push(0..1, 1)]), (5, vec![push(4..5, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 1), push(4..5, 1), push(5..0, 1)]),
                (2, vec![push(0..1, 1), push(5..0, 1)]),
                (3, vec![push(0..1, 1), push(2..3, 1)]),
                (4, vec![
                    push(0..1, 1),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(4..5, 1),
                    push(5..0, 1),
                ]),
                (5, vec![push(2..3, 1), push(3..4, 1), push(4..5, 1)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![5]);

            let expected_ranges = vec![(1, vec![push(5..0, 0)]), (5, vec![push(4..5, 0)])];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn add_one_remove_two() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____-p0____p1____p2____p3____+p4____-p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 5], vec![4], vec![0, 5]);

            let expected_ranges = vec![
                (0, vec![push(5..0, 1)]),
                (5, vec![push(3..4, 4), push(4..5, 1)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 1),
                    push(3..4, 3),
                    push(3..4, 4),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(4..5, 4),
                    push(5..0, 3),
                    push(5..0, 4),
                ]),
                (1, vec![push(0..1, 4), push(5..0, 3), push(5..0, 4)]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 1),
                    push(2..3, 4),
                ]),
                (5, vec![
                    push(2..3, 4),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(3..4, 4),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(4..5, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____+p3____p4____-p5____|
            let peers = PeerSet(vec![0, 1, 2, 4, 5], vec![3], vec![0, 5]);

            let expected_ranges = vec![
                (0, vec![push(5..0, 1)]),
                (4, vec![push(2..3, 3)]),
                (5, vec![push(4..5, 1)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 3),
                ]),
                (1, vec![push(0..1, 3), push(5..0, 3)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 3),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____+p2____p3____p4____-p5____|
            let peers = PeerSet(vec![0, 1, 3, 4, 5], vec![2], vec![0, 5]);

            let expected_ranges = vec![
                (0, vec![push(5..0, 1)]),
                (3, vec![push(1..2, 2)]),
                (5, vec![push(4..5, 1)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 3),
                ]),
                (1, vec![push(5..0, 3)]),
                (3, vec![push(1..2, 2), push(2..3, 1)]),
                (4, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 3),
                ]),
                (5, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____+p1____p2____p3____p4____-p5____|
            let peers = PeerSet(vec![0, 2, 3, 4, 5], vec![1], vec![0, 5]);

            let expected_ranges = vec![
                (0, vec![push(5..0, 1)]),
                (2, vec![push(0..1, 1)]),
                (5, vec![push(4..5, 1)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 1),
                    push(5..0, 3),
                ]),
                (2, vec![push(0..1, 1), push(5..0, 1), push(5..0, 3)]),
                (3, vec![push(0..1, 1), push(2..3, 1)]),
                (4, vec![
                    push(0..1, 1),
                    push(2..3, 1),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                    push(5..0, 1),
                    push(5..0, 3),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(3..4, 1),
                    push(3..4, 3),
                    push(4..5, 1),
                    push(4..5, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____-p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![4, 5]);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (4, vec![push(3..4, 0)]),
                (5, vec![push(4..5, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____-p3____p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![3, 5]);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![push(2..3, 4)]),
                (5, vec![push(4..5, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(5..0, 0)]),
                (2, vec![push(1..2, 0)]),
                (3, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (5, vec![
                    push(2..3, 0),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____-p2____p3____p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![2, 5]);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (2, vec![push(1..2, 3)]),
                (5, vec![push(4..5, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (2, vec![push(1..2, 1)]),
                (3, vec![
                    push(1..2, 1),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(1..2, 1),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____-p1____p2____p3____p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 4, 5], vec![0], vec![1, 5]);

            let expected_ranges = vec![
                (1, vec![push(0..1, 2), push(5..0, 0)]),
                (5, vec![push(4..5, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 2), push(5..0, 0), push(5..0, 2)]),
                (3, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 0),
                    push(3..4, 2),
                    push(4..5, 0),
                    push(4..5, 2),
                    push(5..0, 0),
                    push(5..0, 2),
                ]),
                (4, vec![
                    push(0..1, 2),
                    push(2..3, 2),
                    push(3..4, 0),
                    push(3..4, 2),
                    push(4..5, 0),
                    push(4..5, 2),
                    push(5..0, 0),
                    push(5..0, 2),
                ]),
                (5, vec![
                    push(2..3, 2),
                    push(3..4, 0),
                    push(3..4, 2),
                    push(4..5, 0),
                    push(4..5, 2),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____p3____-p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![0, 4]);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(5..0, 1)]),
                (4, vec![push(3..4, 5)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (3, vec![push(2..3, 5)]),
                (4, vec![
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____-p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![0, 3]);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(5..0, 1)]),
                (3, vec![push(2..3, 4)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![push(3..4, 5), push(4..5, 5)]),
                (1, vec![push(2..3, 5), push(3..4, 5), push(4..5, 5)]),
                (3, vec![push(2..3, 5)]),
                (4, vec![push(2..3, 5), push(3..4, 5), push(4..5, 5)]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____-p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![0, 2]);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(5..0, 1)]),
                (2, vec![push(1..2, 3)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (2, vec![push(1..2, 5)]),
                (3, vec![push(1..2, 5), push(2..3, 5)]),
                (4, vec![
                    push(1..2, 5),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____-p1____p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4], vec![5], vec![0, 1]);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(5..0, 2)]),
                (1, vec![push(0..1, 2)]),
            ];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 2),
                    push(5..0, 3),
                ]),
                (1, vec![
                    push(0..1, 2),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 2),
                    push(5..0, 3),
                ]),
                (3, vec![push(0..1, 2), push(2..3, 5)]),
                (4, vec![
                    push(0..1, 2),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 2),
                    push(5..0, 3),
                ]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn add_two_remove_one() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // ____0____+p0____p1____p2____p3____-p4____+p5____|
            let peers = PeerSet(vec![1, 2, 3, 4], vec![0, 5], vec![4]);

            let expected_ranges = vec![
                (1, vec![push(4..5, 5), push(5..0, 0)]),
                (4, vec![push(3..4, 5)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
                (3, vec![
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____p2____p3____+p4____+p5____|
            let peers = PeerSet(vec![0, 1, 2, 3], vec![4, 5], vec![0]);

            let expected_ranges = vec![(0, vec![push(3..4, 4), push(4..5, 5), push(5..0, 1)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 4),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 4),
                    push(4..5, 5),
                    push(5..0, 3),
                    push(5..0, 4),
                ]),
                (1, vec![
                    push(0..1, 4),
                    push(2..3, 4),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 4),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 4),
                    push(4..5, 5),
                    push(5..0, 3),
                    push(5..0, 4),
                ]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 4),
                    push(2..3, 5),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____p2____p3____+p4____-p5____|
            let peers = PeerSet(vec![1, 2, 3, 5], vec![0, 4], vec![5]);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (5, vec![push(3..4, 4), push(4..5, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 4), push(5..0, 0), push(5..0, 4)]),
                (2, vec![push(1..2, 4)]),
                (3, vec![
                    push(0..1, 4),
                    push(1..2, 4),
                    push(2..3, 1),
                    push(2..3, 4),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(3..4, 4),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(4..5, 4),
                    push(5..0, 0),
                    push(5..0, 4),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(2..3, 4),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(3..4, 4),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(4..5, 4),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____-p2____p3____p4____+p5____|
            let peers = PeerSet(vec![1, 2, 3, 4], vec![0, 5], vec![2]);

            let expected_ranges = vec![
                (1, vec![push(4..5, 5), push(5..0, 0)]),
                (2, vec![push(1..2, 3)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
                (2, vec![push(1..2, 5)]),
                (3, vec![
                    push(1..2, 5),
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(1..2, 5),
                    push(2..3, 5),
                    push(3..4, 0),
                    push(3..4, 5),
                    push(4..5, 0),
                    push(4..5, 5),
                    push(5..0, 0),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____-p0____p1____+p2____p3____p4____+p5____|
            let peers = PeerSet(vec![0, 1, 3, 4], vec![2, 5], vec![0]);

            let expected_ranges = vec![
                (0, vec![push(4..5, 5), push(5..0, 1)]),
                (3, vec![push(1..2, 2)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (1, vec![
                    push(1..2, 2),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
                (3, vec![push(1..2, 2), push(2..3, 5)]),
                (4, vec![
                    push(1..2, 2),
                    push(2..3, 5),
                    push(3..4, 3),
                    push(3..4, 5),
                    push(4..5, 3),
                    push(4..5, 5),
                    push(5..0, 3),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // ____0____+p0____p1____+p2____p3____p4____-p5____|
            let peers = PeerSet(vec![1, 3, 4, 5], vec![0, 2], vec![5]);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![push(1..2, 2)]),
                (5, vec![push(4..5, 0)]),
            ];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(5..0, 0)]),
                (3, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (4, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..0, 0),
                ]),
                (5, vec![
                    push(1..2, 2),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn add_and_remove_corner_cases() {
        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        {
            // Same peer in and out -- actions cancel each other out.
            // ____0____p0____p1____p2____p3____-+p4____p5____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4, 5], vec![4], vec![4]);

            assert_ranges(non_replicated.clone(), peers.clone(), vec![]);
            assert_ranges(replicated.clone(), peers, vec![]);
        }

        {
            // Joining and leaving sets intersection is not empty.
            // Intersecting elements are ignored.
            // ____0____p0____p1____p2____+p3____-+p4____-p5____|
            let peers = PeerSet(vec![0, 1, 2, 4, 5], vec![3, 4], vec![4, 5]);

            let expected_ranges = vec![(4, vec![push(2..3, 3)]), (5, vec![push(4..5, 0)])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (0, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
                (1, vec![push(0..1, 3)]),
                (2, vec![push(1..2, 3)]),
                (4, vec![
                    push(0..1, 3),
                    push(1..2, 3),
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
                (5, vec![
                    push(2..3, 1),
                    push(2..3, 3),
                    push(3..4, 1),
                    push(4..5, 1),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // Many consecutive joins.
            // ____0____p0____p1____+p2____+p3____+p4____+p5____+p6____p7____|
            let peers = PeerSet(vec![0, 1, 7], vec![2, 3, 4, 5, 6], vec![]);

            let expected_ranges = vec![(7, vec![
                push(1..2, 2),
                push(2..3, 3),
                push(3..4, 4),
                push(4..5, 5),
                push(5..6, 6),
            ])];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let from_current_peers = vec![
                push(0..1, 3),
                push(0..1, 4),
                push(1..2, 2),
                push(1..2, 3),
                push(1..2, 4),
                push(2..3, 3),
                push(2..3, 4),
                push(2..3, 5),
                push(3..4, 4),
                push(3..4, 5),
                push(3..4, 6),
                push(4..5, 5),
                push(4..5, 6),
                push(5..6, 6),
                push(7..0, 4),
            ];
            let expected_ranges = vec![
                (0, from_current_peers.clone()),
                (1, from_current_peers.clone()),
                (7, from_current_peers),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // Many consecutive leaves.
            // ____0____p0____p1____p2____-p3____-p4____-p5____-p6____|
            let peers = PeerSet(vec![0, 1, 2, 3, 4, 5, 6], vec![], vec![3, 4, 5, 6]);

            let expected_ranges = vec![
                (3, vec![push(2..3, 0)]),
                (4, vec![push(3..4, 0)]),
                (5, vec![push(4..5, 0)]),
                (6, vec![push(5..6, 0)]),
            ];
            assert_ranges(non_replicated.clone(), peers.clone(), expected_ranges);

            let expected_ranges = vec![
                (1, vec![push(0..1, 0), push(5..6, 0)]),
                (2, vec![push(1..2, 0)]),
                (3, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 1),
                ]),
                (4, vec![
                    push(0..1, 0),
                    push(1..2, 0),
                    push(2..3, 0),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..6, 0),
                ]),
                (5, vec![
                    push(2..3, 0),
                    push(2..3, 1),
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                ]),
                (6, vec![
                    push(3..4, 0),
                    push(3..4, 1),
                    push(4..5, 0),
                    push(4..5, 1),
                    push(5..6, 0),
                ]),
            ];
            assert_ranges(replicated.clone(), peers, expected_ranges);
        }

        {
            // Many consecutive joins and leaves.
            // ____0____-p0____-p1____+p2____+p3____+p4____+p5____+p6____-p7____|
            let peers = PeerSet(vec![0, 1, 7], vec![2, 3, 4, 5, 6], vec![0, 1, 7]);

            let expected_ranges = vec![
                (7, vec![
                    push(1..2, 2),
                    push(2..3, 3),
                    push(3..4, 4),
                    push(4..5, 5),
                    push(5..6, 6),
                    push(6..7, 2),
                ]),
                (0, vec![push(7..0, 2)]),
                (1, vec![push(0..1, 2)]),
            ];
            assert_ranges(non_replicated, peers.clone(), expected_ranges);

            let from_existing_peers = vec![
                push(0..1, 2),
                push(0..1, 3),
                push(0..1, 4),
                push(1..2, 2),
                push(1..2, 3),
                push(1..2, 4),
                push(2..3, 3),
                push(2..3, 4),
                push(2..3, 5),
                push(3..4, 4),
                push(3..4, 5),
                push(3..4, 6),
                push(4..5, 4),
                push(4..5, 5),
                push(4..5, 6),
                push(5..6, 2),
                push(5..6, 4),
                push(5..6, 6),
                push(6..7, 2),
                push(6..7, 3),
                push(6..7, 4),
                push(7..0, 2),
                push(7..0, 3),
                push(7..0, 4),
            ];
            let expected_ranges = vec![
                (0, from_existing_peers.clone()),
                (1, from_existing_peers.clone()),
                (7, from_existing_peers),
            ];
            assert_ranges(replicated, peers, expected_ranges);
        }
    }

    #[test]
    fn virtual_nodes() {
        struct TestCase<'a> {
            name: &'static str,
            ring: &'a HashRing,
            strategy: Strategy,
            joining_peers: Vec<PeerId>,
            leaving_peers: Vec<PeerId>,
            expected_ranges: Vec<(usize, Vec<PendingRange<RingPosition>>)>,
        }

        fn assert_case(t: TestCase) {
            // Wrap in BTreeSet and augment with pull (calculated from pushes).
            let expected = pulls_from_pushes(
                t.expected_ranges
                    .into_iter()
                    .map(|(peer_idx, ranges)| (peer_id(peer_idx), BTreeSet::from_iter(ranges)))
                    .collect::<BTreeMap<_, _>>(),
            );

            let got = PendingRangesCalculator::new(t.ring, t.strategy)
                .with_joining_peers(t.joining_peers)
                .with_leaving_peers(t.leaving_peers)
                .ranges()
                .unwrap();

            assert_eq!(
                &got,
                &expected,
                "{}\nTest case: {}",
                {
                    // Pretty print the difference.
                    difference::Changeset::new(
                        &format!("{:#?}", &expected),
                        &format!("{:#?}", &got),
                        " ",
                    )
                },
                t.name
            );
        }

        let non_replicated = Strategy::new(1, ConsistencyLevel::default());
        let replicated = Strategy::new(3, ConsistencyLevel::default());

        // Ring with 5 virtual nodes per physical node.
        let mut ring = HashRing::new(5);
        ring.add_node(peer_id(0)).unwrap();
        ring.add_node(peer_id(3)).unwrap();
        ring.add_node(peer_id(5)).unwrap();
        ring.add_node(peer_id(7)).unwrap();
        ring.add_node(peer_id(9)).unwrap();

        let tests = vec![
            TestCase {
                name: "add one (non-replicated)",
                ring: &ring,
                strategy: non_replicated.clone(),
                joining_peers: vec![peer_id(1)],
                leaving_peers: vec![],
                expected_ranges: vec![
                    (3, vec![
                        push_range(111100983615508364..222200056717412907, 1),
                        push_range(2157149190045013229..2347668201829205240, 1),
                    ]),
                    (5, vec![
                        push_range(6262852972106561918..7079604129657860890, 1),
                        push_range(11481432248301971266..13937008202598308873, 1),
                        push_range(11481432248301971266..13937008202598308873, 1),
                    ]),
                    (9, vec![push_range(
                        8802913300059853569..9205072274769653223,
                        1,
                    )]),
                ],
            },
            TestCase {
                name: "add one (replicated)",
                ring: &ring,
                strategy: replicated.clone(),
                joining_peers: vec![peer_id(1)],
                leaving_peers: vec![],
                expected_ranges: vec![
                    (0, vec![
                        push_range(104302449933739290..111100983615508364, 1),
                        push_range(4071719813032132674..4080287171546526274, 1),
                        push_range(5962916429082114754..6262852972106561918, 1),
                        push_range(6262852972106561918..7079604129657860890, 1),
                        push_range(8802913300059853569..9205072274769653223, 1),
                        push_range(9205072274769653223..9775525813282526520, 1),
                        push_range(9775525813282526520..10620416794376564822, 1),
                        push_range(10620416794376564822..11319066114435793127, 1),
                        push_range(11481432248301971266..13937008202598308873, 1),
                        push_range(13958129153739724505..14611137494956920135, 1),
                        push_range(14611137494956920135..14977980616646567726, 1),
                        push_range(14977980616646567726..15288252302366811037, 1),
                        push_range(16999948067521827778..104302449933739290, 1),
                    ]),
                    (3, vec![
                        push_range(111100983615508364..222200056717412907, 1),
                        push_range(2157149190045013229..2347668201829205240, 1),
                        push_range(2347668201829205240..4071719813032132674, 1),
                        push_range(4080287171546526274..4939914131608132905, 1),
                        push_range(4939914131608132905..5962916429082114754, 1),
                        push_range(11319066114435793127..11481432248301971266, 1),
                        push_range(15288252302366811037..16999948067521827778, 1),
                    ]),
                    (5, vec![
                        push_range(104302449933739290..111100983615508364, 1),
                        push_range(111100983615508364..222200056717412907, 1),
                        push_range(2157149190045013229..2347668201829205240, 1),
                        push_range(2347668201829205240..4071719813032132674, 1),
                        push_range(4071719813032132674..4080287171546526274, 1),
                        push_range(4080287171546526274..4939914131608132905, 1),
                        push_range(4939914131608132905..5962916429082114754, 1),
                        push_range(5962916429082114754..6262852972106561918, 1),
                        push_range(6262852972106561918..7079604129657860890, 1),
                        push_range(8802913300059853569..9205072274769653223, 1),
                        push_range(9205072274769653223..9775525813282526520, 1),
                        push_range(9775525813282526520..10620416794376564822, 1),
                        push_range(10620416794376564822..11319066114435793127, 1),
                        push_range(11319066114435793127..11481432248301971266, 1),
                        push_range(11481432248301971266..13937008202598308873, 1),
                        push_range(13958129153739724505..14611137494956920135, 1),
                        push_range(14611137494956920135..14977980616646567726, 1),
                        push_range(14977980616646567726..15288252302366811037, 1),
                        push_range(15288252302366811037..16999948067521827778, 1),
                        push_range(16999948067521827778..104302449933739290, 1),
                    ]),
                    (7, vec![
                        push_range(104302449933739290..111100983615508364, 1),
                        push_range(111100983615508364..222200056717412907, 1),
                        push_range(4939914131608132905..5962916429082114754, 1),
                        push_range(5962916429082114754..6262852972106561918, 1),
                        push_range(9775525813282526520..10620416794376564822, 1),
                        push_range(14611137494956920135..14977980616646567726, 1),
                    ]),
                    (9, vec![
                        push_range(2157149190045013229..2347668201829205240, 1),
                        push_range(2347668201829205240..4071719813032132674, 1),
                        push_range(4071719813032132674..4080287171546526274, 1),
                        push_range(4080287171546526274..4939914131608132905, 1),
                        push_range(6262852972106561918..7079604129657860890, 1),
                        push_range(8802913300059853569..9205072274769653223, 1),
                        push_range(9205072274769653223..9775525813282526520, 1),
                        push_range(10620416794376564822..11319066114435793127, 1),
                        push_range(11319066114435793127..11481432248301971266, 1),
                        push_range(11481432248301971266..13937008202598308873, 1),
                        push_range(13958129153739724505..14611137494956920135, 1),
                        push_range(14977980616646567726..15288252302366811037, 1),
                        push_range(15288252302366811037..16999948067521827778, 1),
                        push_range(16999948067521827778..104302449933739290, 1),
                    ]),
                ],
            },
            TestCase {
                name: "remove one (non-replicated)",
                ring: &ring,
                strategy: non_replicated.clone(),
                joining_peers: vec![],
                leaving_peers: vec![peer_id(0)],
                expected_ranges: vec![(0, vec![
                    push_range(104302449933739290..111100983615508364, 3),
                    push_range(4071719813032132674..4080287171546526274, 9),
                    push_range(7312365043724884165..8049473359477544184, 5),
                    push_range(10620416794376564822..11319066114435793127, 3),
                    push_range(14977980616646567726..15288252302366811037, 3),
                    push_range(14977980616646567726..15288252302366811037, 3),
                ])],
            },
            TestCase {
                name: "remove one (replicated)",
                ring: &ring,
                strategy: replicated.clone(),
                joining_peers: vec![],
                leaving_peers: vec![peer_id(0)],
                expected_ranges: vec![
                    (0, vec![
                        push_range(104302449933739290..111100983615508364, 3),
                        push_range(4071719813032132674..4080287171546526274, 3),
                        push_range(5962916429082114754..6262852972106561918, 3),
                        push_range(6262852972106561918..7312365043724884165, 3),
                        push_range(7312365043724884165..8049473359477544184, 3),
                        push_range(8049473359477544184..8802913300059853569, 3),
                        push_range(8802913300059853569..9775525813282526520, 3),
                        push_range(9775525813282526520..10620416794376564822, 3),
                        push_range(10620416794376564822..11319066114435793127, 3),
                        push_range(11481432248301971266..13958129153739724505, 3),
                        push_range(13958129153739724505..14611137494956920135, 3),
                        push_range(14611137494956920135..14977980616646567726, 3),
                        push_range(14977980616646567726..15288252302366811037, 3),
                        push_range(16999948067521827778..104302449933739290, 3),
                    ]),
                    (5, vec![
                        push_range(104302449933739290..111100983615508364, 3),
                        push_range(4071719813032132674..4080287171546526274, 3),
                        push_range(5962916429082114754..6262852972106561918, 3),
                        push_range(6262852972106561918..7312365043724884165, 3),
                        push_range(7312365043724884165..8049473359477544184, 3),
                        push_range(8049473359477544184..8802913300059853569, 3),
                        push_range(8802913300059853569..9775525813282526520, 3),
                        push_range(9775525813282526520..10620416794376564822, 3),
                        push_range(10620416794376564822..11319066114435793127, 3),
                        push_range(11481432248301971266..13958129153739724505, 3),
                        push_range(13958129153739724505..14611137494956920135, 3),
                        push_range(14611137494956920135..14977980616646567726, 3),
                        push_range(14977980616646567726..15288252302366811037, 3),
                        push_range(16999948067521827778..104302449933739290, 3),
                    ]),
                    (7, vec![
                        push_range(104302449933739290..111100983615508364, 3),
                        push_range(5962916429082114754..6262852972106561918, 3),
                        push_range(9775525813282526520..10620416794376564822, 3),
                        push_range(14611137494956920135..14977980616646567726, 3),
                    ]),
                    (9, vec![
                        push_range(4071719813032132674..4080287171546526274, 3),
                        push_range(6262852972106561918..7312365043724884165, 3),
                        push_range(7312365043724884165..8049473359477544184, 3),
                        push_range(8049473359477544184..8802913300059853569, 3),
                        push_range(8802913300059853569..9775525813282526520, 3),
                        push_range(10620416794376564822..11319066114435793127, 3),
                        push_range(11481432248301971266..13958129153739724505, 3),
                        push_range(13958129153739724505..14611137494956920135, 3),
                        push_range(14977980616646567726..15288252302366811037, 3),
                        push_range(16999948067521827778..104302449933739290, 3),
                    ]),
                ],
            },
            TestCase {
                name: "remove two add three (non-replicated)",
                ring: &ring,
                strategy: non_replicated,
                joining_peers: vec![peer_id(2), peer_id(6), peer_id(8)],
                leaving_peers: vec![peer_id(7), peer_id(9)],
                expected_ranges: vec![
                    (3, vec![
                        push_range(111100983615508364..333300128304974679, 2),
                        push_range(2157149190045013229..2444298396908704876, 2),
                    ]),
                    (5, vec![
                        push_range(6262852972106561918..6339531408554059866, 6),
                        push_range(6339531408554059866..6666294934116165270, 2),
                        push_range(8049473359477544184..8777293202719895467, 2),
                        push_range(11481432248301971266..11901362416697421172, 6),
                    ]),
                    (7, vec![
                        push_range(666600933710043825..777700400410698560, 6),
                        push_range(777700400410698560..888800365207019014, 8),
                        push_range(1000005102921762134..1905289149836559014, 5),
                        push_range(5962916429082114754..6262852972106561918, 6),
                        push_range(9775525813282526520..10620416794376564822, 0),
                        push_range(14611137494956920135..14977980616646567726, 0),
                        push_range(14611137494956920135..14977980616646567726, 0),
                    ]),
                    (9, vec![
                        push_range(53400938724242007..104302449933739290, 0),
                        push_range(888800365207019014..999900572057947227, 8),
                        push_range(999900572057947227..1000005102921762134, 5),
                        push_range(4080287171546526274..4555296665512435073, 2),
                        push_range(4555296665512435073..4578280359274592168, 6),
                        push_range(4578280359274592168..4939914131608132905, 3),
                        push_range(8802913300059853569..8803523158912165205, 8),
                        push_range(8803523158912165205..9750022792245870425, 8),
                        push_range(9750022792245870425..9775525813282526520, 0),
                        push_range(13958129153739724505..14611137494956920135, 0),
                        push_range(16999948067521827778..17463193424840782478, 6),
                        push_range(17463193424840782478..17553645379100088403, 8),
                        push_range(17553645379100088403..53400938724242007, 8),
                    ]),
                ],
            },
            TestCase {
                name: "remove two add three (replicated)",
                ring: &ring,
                strategy: replicated,
                joining_peers: vec![peer_id(2), peer_id(6), peer_id(8)],
                leaving_peers: vec![peer_id(7), peer_id(9)],
                expected_ranges: vec![
                    (0, vec![
                        push_range(53400938724242007..104302449933739290, 2),
                        push_range(104302449933739290..111100983615508364, 2),
                        push_range(4071719813032132674..4080287171546526274, 2),
                        push_range(5962916429082114754..6262852972106561918, 6),
                        push_range(5962916429082114754..6262852972106561918, 2),
                        push_range(6262852972106561918..6339531408554059866, 6),
                        push_range(6262852972106561918..6339531408554059866, 2),
                        push_range(6339531408554059866..6666294934116165270, 2),
                        push_range(7312365043724884165..8049473359477544184, 2),
                        push_range(8049473359477544184..8777293202719895467, 8),
                        push_range(8049473359477544184..8777293202719895467, 2),
                        push_range(8777293202719895467..8802913300059853569, 8),
                        push_range(8802913300059853569..8803523158912165205, 8),
                        push_range(8803523158912165205..9750022792245870425, 8),
                        push_range(11481432248301971266..11901362416697421172, 6),
                        push_range(13958129153739724505..14611137494956920135, 2),
                        push_range(14611137494956920135..14977980616646567726, 2),
                        push_range(14977980616646567726..15288252302366811037, 2),
                        // push_range(15288252302366811037..16999948067521827778, 2),
                        push_range(16999948067521827778..17463193424840782478, 6),
                        push_range(16999948067521827778..17463193424840782478, 2),
                        push_range(17463193424840782478..17553645379100088403, 8),
                        push_range(17463193424840782478..17553645379100088403, 2),
                        push_range(17553645379100088403..53400938724242007, 8),
                        push_range(17553645379100088403..53400938724242007, 2),
                    ]),
                    (3, vec![
                        push_range(111100983615508364..333300128304974679, 2),
                        push_range(444400609862258242..666600933710043825, 6),
                        push_range(666600933710043825..777700400410698560, 6),
                        push_range(777700400410698560..888800365207019014, 8),
                        push_range(888800365207019014..999900572057947227, 8),
                        push_range(2157149190045013229..2444298396908704876, 2),
                        push_range(2444298396908704876..4071719813032132674, 2),
                        push_range(4080287171546526274..4555296665512435073, 6),
                        push_range(4080287171546526274..4555296665512435073, 2),
                        push_range(4555296665512435073..4578280359274592168, 6),
                        push_range(4555296665512435073..4578280359274592168, 2),
                        push_range(4578280359274592168..4939914131608132905, 2),
                        push_range(4939914131608132905..5962916429082114754, 2),
                        push_range(15288252302366811037..16999948067521827778, 2),
                    ]),
                    (5, vec![
                        push_range(53400938724242007..104302449933739290, 2),
                        push_range(104302449933739290..111100983615508364, 2),
                        push_range(111100983615508364..333300128304974679, 2),
                        push_range(444400609862258242..666600933710043825, 6),
                        push_range(666600933710043825..777700400410698560, 6),
                        push_range(777700400410698560..888800365207019014, 8),
                        push_range(888800365207019014..999900572057947227, 8),
                        push_range(2157149190045013229..2444298396908704876, 2),
                        push_range(2444298396908704876..4071719813032132674, 2),
                        push_range(4071719813032132674..4080287171546526274, 2),
                        push_range(4080287171546526274..4555296665512435073, 6),
                        push_range(4080287171546526274..4555296665512435073, 2),
                        push_range(4555296665512435073..4578280359274592168, 6),
                        push_range(4555296665512435073..4578280359274592168, 2),
                        push_range(4578280359274592168..4939914131608132905, 2),
                        push_range(4939914131608132905..5962916429082114754, 2),
                        push_range(5962916429082114754..6262852972106561918, 6),
                        push_range(5962916429082114754..6262852972106561918, 2),
                        push_range(6262852972106561918..6339531408554059866, 6),
                        push_range(6262852972106561918..6339531408554059866, 2),
                        push_range(6339531408554059866..6666294934116165270, 2),
                        push_range(7312365043724884165..8049473359477544184, 2),
                        push_range(8049473359477544184..8777293202719895467, 8),
                        push_range(8049473359477544184..8777293202719895467, 2),
                        push_range(8777293202719895467..8802913300059853569, 8),
                        push_range(8802913300059853569..8803523158912165205, 8),
                        push_range(8803523158912165205..9750022792245870425, 8),
                        push_range(11481432248301971266..11901362416697421172, 6),
                        push_range(13958129153739724505..14611137494956920135, 2),
                        push_range(14611137494956920135..14977980616646567726, 2),
                        push_range(14977980616646567726..15288252302366811037, 2),
                        push_range(15288252302366811037..16999948067521827778, 2),
                        push_range(16999948067521827778..17463193424840782478, 6),
                        push_range(16999948067521827778..17463193424840782478, 2),
                        push_range(17463193424840782478..17553645379100088403, 8),
                        push_range(17463193424840782478..17553645379100088403, 2),
                        push_range(17553645379100088403..53400938724242007, 8),
                        push_range(17553645379100088403..53400938724242007, 2),
                    ]),
                    (7, vec![
                        push_range(104302449933739290..111100983615508364, 2),
                        push_range(111100983615508364..333300128304974679, 2),
                        push_range(444400609862258242..666600933710043825, 6),
                        push_range(666600933710043825..777700400410698560, 6),
                        push_range(777700400410698560..888800365207019014, 8),
                        push_range(4939914131608132905..5962916429082114754, 2),
                        push_range(5962916429082114754..6262852972106561918, 6),
                        push_range(5962916429082114754..6262852972106561918, 2),
                        push_range(14611137494956920135..14977980616646567726, 2),
                    ]),
                    (9, vec![
                        push_range(53400938724242007..104302449933739290, 2),
                        push_range(888800365207019014..999900572057947227, 8),
                        push_range(2157149190045013229..2444298396908704876, 2),
                        push_range(2444298396908704876..4071719813032132674, 2),
                        push_range(4071719813032132674..4080287171546526274, 2),
                        push_range(4080287171546526274..4555296665512435073, 6),
                        push_range(4080287171546526274..4555296665512435073, 2),
                        push_range(4555296665512435073..4578280359274592168, 6),
                        push_range(4555296665512435073..4578280359274592168, 2),
                        push_range(4578280359274592168..4939914131608132905, 2),
                        push_range(6262852972106561918..6339531408554059866, 6),
                        push_range(6262852972106561918..6339531408554059866, 2),
                        push_range(6339531408554059866..6666294934116165270, 2),
                        push_range(7312365043724884165..8049473359477544184, 2),
                        push_range(8049473359477544184..8777293202719895467, 8),
                        push_range(8049473359477544184..8777293202719895467, 2),
                        push_range(8777293202719895467..8802913300059853569, 8),
                        push_range(8802913300059853569..8803523158912165205, 8),
                        push_range(8803523158912165205..9750022792245870425, 8),
                        push_range(11481432248301971266..11901362416697421172, 6),
                        push_range(13958129153739724505..14611137494956920135, 2),
                        push_range(14977980616646567726..15288252302366811037, 2),
                        push_range(15288252302366811037..16999948067521827778, 2),
                        push_range(16999948067521827778..17463193424840782478, 6),
                        push_range(16999948067521827778..17463193424840782478, 2),
                        push_range(17463193424840782478..17553645379100088403, 8),
                        push_range(17463193424840782478..17553645379100088403, 2),
                        push_range(17553645379100088403..53400938724242007, 8),
                        push_range(17553645379100088403..53400938724242007, 2),
                    ]),
                ],
            },
        ];

        for test in tests {
            assert_case(test);
        }
    }
}
