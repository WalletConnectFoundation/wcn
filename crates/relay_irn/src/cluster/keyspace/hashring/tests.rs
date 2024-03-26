use {
    super::*,
    crate::cluster::{keyspace::range::merge_ranges, test_util::preset_peers},
    hex::FromHex,
    std::{
        collections::{BTreeSet, HashSet},
        hash::Hasher,
    },
};

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Node {
    id: &'static str,
}

impl Node {
    fn new(id: &'static str) -> Self {
        Self { id }
    }
}

#[test]
fn empty_ring() {
    let ring: HashRing<PeerId> = HashRing::new(1);
    assert_eq!(ring.primary_node(&0), None);
}

#[test]
fn single_peer() {
    fn assert_case(ring: &HashRing<PeerId>, peer: PeerId) {
        assert_eq!(ring.primary_node(&0), Some(peer));
        assert_eq!(ring.primary_node(&1), Some(peer));
        assert_eq!(ring.primary_node(&2), Some(peer));
    }

    {
        // Single peer, single token per peer.
        let mut ring = HashRing::new(1);
        let peer = PeerId::random();
        ring.add_node(peer).unwrap();
        assert_case(&ring, peer);
    }

    {
        // Single peer, multiple tokens per peer.
        let mut ring = HashRing::new(3);
        let peer = PeerId::random();
        ring.add_node(peer).unwrap();
        assert_case(&ring, peer);
    }
}

#[test]
fn multiple_peers() {
    let mut ring = HashRing::new(1);

    let num_peers = 10;
    let mut peers = Vec::with_capacity(num_peers);
    for i in 0..num_peers {
        peers.insert(i, PeerId::random());
        ring.add_node(peers[i]).unwrap();
    }

    let num_keys = 100;
    for i in 0..num_keys {
        let peer = ring.primary_node(&i).unwrap();
        assert!(peers.contains(&peer));
    }
}

#[test]
fn unregister_peer() {
    let mut ring = HashRing::new(DEFAULT_TOKENS_PER_NODE);
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    ring.add_node(peer1).unwrap();
    ring.add_node(peer2).unwrap();

    // Make sure that peer1 and peer2 are assigned to the ring.
    assert_eq!(ring.len() / DEFAULT_TOKENS_PER_NODE, 2);

    // Unregister peer1, and make sure that it is no longer assigned to the ring.
    ring.remove_node(&peer1).unwrap();
    assert_eq!(ring.len() / DEFAULT_TOKENS_PER_NODE, 1);
    assert_eq!(ring.primary_node(&0), Some(peer2));
}

#[test]
fn node_id_reuse() {
    let mut ring = HashRing::<Node>::new(1);

    let nodes = [Node::new("node1"), Node::new("node2"), Node::new("node3")];

    for node in nodes.iter() {
        ring.add_node(*node).unwrap();
    }

    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0, &1, &2])
    );

    // Remove node, make sure we have a "gap" in ids.
    ring.remove_node(&nodes[1]).unwrap();
    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0, &2])
    );

    // Add a new node, make sure it reuses the id.
    ring.add_node(Node { id: "node4" }).unwrap();
    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0, &1, &2])
    );
}

#[test]
fn duplicate_node() {
    let mut ring = HashRing::<Node>::new(1);

    let node = Node::new("node1");
    ring.add_node(node).unwrap();
    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0])
    );

    // Add the same node again, make sure it fails.
    assert_eq!(
        ring.add_node(node).unwrap_err().to_string(),
        ClusterError::RingError(HashRingError::NodeAlreadyExists).to_string()
    );

    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0])
    );
}

#[test]
fn remove_non_existent_node() {
    let mut ring = HashRing::<Node>::new(1);

    let node = Node::new("node1");
    ring.add_node(node).unwrap();
    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0])
    );

    // Remove a non-existent node, make sure it fails.
    assert_eq!(
        ring.remove_node(&Node::new("node2"))
            .unwrap_err()
            .to_string(),
        ClusterError::RingError(HashRingError::NodeNotFound).to_string()
    );

    assert_eq!(
        HashSet::<_>::from_iter(ring.nodes.keys()),
        HashSet::from_iter(vec![&0])
    );
}

#[test]
fn tokens_basic() {
    let mut ring = HashRing::new(1);
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();
    ring.add_node(peer1).unwrap();
    ring.add_node(peer2).unwrap();
    ring.add_node(peer3).unwrap();

    // Traverse from the beginning (clockwise).
    let positions = ring
        .tokens(0, Clockwise)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 3);
    assert!(positions.contains(&peer1));
    assert!(positions.contains(&peer2));
    assert!(positions.contains(&peer3));

    // Traverse from the beginning (counter-clockwise).
    let positions = ring
        .tokens(0, CounterClockwise)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 3);
    assert!(positions.contains(&peer1));
    assert!(positions.contains(&peer2));
    assert!(positions.contains(&peer3));

    // Unregister peer2, and make sure that it is no longer assigned to the ring.
    ring.remove_node(&peer2).unwrap();
    let positions = ring
        .tokens(0, Clockwise)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 2);
    assert!(positions.contains(&peer1));
    assert!(!positions.contains(&peer2));
    assert!(positions.contains(&peer3));
}

#[test]
fn tokens_wrap_around() {
    let mut ring = HashRing::new(1);
    let peers = vec![PeerId::random(), PeerId::random(), PeerId::random()];
    peers.iter().for_each(|peer| ring.add_node(*peer).unwrap());

    // Start from position near the end of the ring (wrap around, clockwise).
    let positions = ring
        .tokens(u64::MAX - 1, Clockwise)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(
        BTreeSet::from_iter(positions),
        BTreeSet::from_iter(peers.clone())
    );

    // Start from position near zero of the ring (wrap around, counter-clockwise).
    let positions = ring
        .tokens(1, CounterClockwise)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(BTreeSet::from_iter(positions), BTreeSet::from_iter(peers));
}

#[track_caller]
fn assert_peers(ring: &HashRing, start: u64, dir: RingDirection, expected: Vec<PeerId>) {
    let positions = ring
        .tokens(start, dir)
        .map(|token| *token.1)
        .collect::<Vec<_>>();
    assert_eq!(positions, expected);
}

#[test]
fn tokens_corner_cases() {
    let mut ring = HashRing::new(1);
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();

    // Inject three nodes.
    ring.nodes.insert(0, peer1);
    ring.nodes.insert(1, peer2);
    ring.nodes.insert(2, peer3);

    // Peers at zero, max/2, and max.
    ring.tokens.insert(0, 0); // peer1
    ring.tokens.insert(u64::MAX / 2, 1); // peer2
    ring.tokens.insert(u64::MAX, 2); // peer3

    let test_cases = vec![
        // [0, 0)
        (0, Clockwise, vec![peer1, peer2, peer3]),
        (0, CounterClockwise, vec![peer1, peer3, peer2]),
        // [1, 1)
        (1, Clockwise, vec![peer2, peer3, peer1]),
        (1, CounterClockwise, vec![peer1, peer3, peer2]),
        // [max/2, max/2)
        (u64::MAX / 2, Clockwise, vec![peer2, peer3, peer1]),
        (u64::MAX / 2, CounterClockwise, vec![peer2, peer1, peer3]),
        // [max/2 + 1, max/2 + 1)
        (u64::MAX / 2 + 1, Clockwise, vec![peer3, peer1, peer2]),
        (u64::MAX / 2 + 1, CounterClockwise, vec![
            peer2, peer1, peer3,
        ]),
        // [max, max)
        (u64::MAX, Clockwise, vec![peer3, peer1, peer2]),
        (u64::MAX, CounterClockwise, vec![peer3, peer2, peer1]),
    ];
    for (start, dir, expected) in test_cases {
        assert_peers(&ring, start, dir, expected);
    }
}

#[test]
fn key_range_simple() {
    let mut ring = HashRing::new(1);
    // Empty ring doesn't produce any ranges.
    assert_eq!(ring.key_range(42), None);
    assert_eq!(ring.key_range(u64::MAX), None);
    assert_eq!(ring.key_range(0), None);

    // For a single peer, the range is the whole ring.
    let peer1 = PeerId::random();
    ring.add_node(peer1).unwrap();
    let (&peer_pos, _) = ring.primary_token(&42).unwrap();

    // Go in counter-clockwise direction up until the only peer is reached.
    assert_eq!(ring.key_range(42).unwrap(), KeyRange::new(peer_pos, 42));
    // The same thing, but start from the maximum key.
    assert_eq!(
        ring.key_range(u64::MAX).unwrap(),
        KeyRange::new(peer_pos, u64::MAX)
    );
    // If we use peer position as the start key, then the range is the whole ring.
    assert_eq!(
        ring.key_range(peer_pos).unwrap(),
        KeyRange::new(peer_pos, peer_pos)
    );
}

#[test]
fn key_range_multiple_peers() {
    let mut ring = HashRing::new(DEFAULT_TOKENS_PER_NODE);
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();
    ring.add_node(peer1).unwrap();
    ring.add_node(peer2).unwrap();
    ring.add_node(peer3).unwrap();

    let mut ranges = Vec::new();

    for (pos, _) in ring.tokens(0, Clockwise) {
        let range = ring.key_range(*pos).unwrap();
        ranges.push(range);
    }

    assert_eq!(ranges.len() / DEFAULT_TOKENS_PER_NODE, 3);
    let merged = merge_ranges(ranges).collect::<Vec<_>>();
    assert_eq!(merged.len(), 1);
    let combined_range = merged.first().unwrap();
    // Combined range should be the whole ring.
    assert_eq!(combined_range.start, combined_range.end);
}

#[test]
fn tokens_iter() {
    // In order to support wrapping around the origin (zero), we need to chain
    // another iterator, which will be wrapped around the origin.

    let p1 = PeerId::random();
    let p2 = PeerId::random();
    let p3 = PeerId::random();

    {
        // Single token.
        let t1 = (10000, p1);
        let positions = vec![t1];

        // -----A-----|-----A-----|
        //   K->
        assert_tokens_iter(0, positions.clone(), vec![p1], Clockwise);

        // -----A-----|-----A-----|
        //  <-K
        assert_tokens_iter(0, positions.clone(), vec![p1], CounterClockwise);

        // -----A-----|-----A-----|
        //      K->
        assert_tokens_iter(1000, positions.clone(), vec![p1], Clockwise);

        // -----A-----|-----A-----|
        //    <-K
        assert_tokens_iter(1000, positions, vec![p1], CounterClockwise);
    }

    {
        // Two tokens.
        let t1 = (10000, p1);
        let t2 = (200000, p2);
        let positions = vec![t1, t2];

        // -----A--B--|-----A--B--|
        //   K->
        assert_tokens_iter(0, positions.clone(), vec![p1, p2], Clockwise);

        // -----A--B--|-----A--B--|
        //  <-K
        assert_tokens_iter(0, positions.clone(), vec![p2, p1], CounterClockwise);

        // ----A---B--|----A---B--|
        //     K->
        assert_tokens_iter(10000, positions.clone(), vec![p1, p2], Clockwise);

        // ----A---B--|----A---B--|
        //   <-K
        assert_tokens_iter(10000, positions.clone(), vec![p1, p2], CounterClockwise);

        // --A-----B--|--A-----B--|
        //     K->
        assert_tokens_iter(15000, positions.clone(), vec![p2, p1], Clockwise);

        // --A-----B--|--A-----B--|
        //     <-K
        assert_tokens_iter(15000, positions.clone(), vec![p1, p2], CounterClockwise);

        // --A---B----|--A---B----|
        //       K->
        assert_tokens_iter(200000, positions.clone(), vec![p2, p1], Clockwise);

        // --A-----B--|--A-----B--|
        //       <-K
        assert_tokens_iter(200000, positions.clone(), vec![p2, p1], CounterClockwise);

        // --A---B----|--A---B----|
        //         K->
        assert_tokens_iter(250000, positions.clone(), vec![p1, p2], Clockwise);

        // --A---B----|--A---B----|
        //        <-K
        assert_tokens_iter(250000, positions.clone(), vec![p2, p1], CounterClockwise);

        // --A---B----|--A---B----|
        //            K-> (at the max position)
        assert_tokens_iter(u64::MAX, positions.clone(), vec![p1, p2], Clockwise);

        // --A---B----|--A---B----|
        //          <-K (at the max position)
        assert_tokens_iter(u64::MAX, positions.clone(), vec![p2, p1], CounterClockwise);

        // --A---B----|--A---B----|
        //            K-> (at the min position)
        assert_tokens_iter(0, positions.clone(), vec![p1, p2], Clockwise);

        // --A---B----|--A---B----|
        //          <-K (at the min position)
        assert_tokens_iter(0, positions, vec![p2, p1], CounterClockwise);
    }

    {
        // 2+ tokens and corner cases.
        let t1 = (10000, p1);
        let t2 = (200000, p2);
        let t3 = (500000, p3);
        let positions = vec![t1, t2, t3];

        // -----A--B--C--|-----A--B--C--|
        //   K->
        assert_tokens_iter(0, positions.clone(), vec![p1, p2, p3], Clockwise);

        // -----A--B--C--|-----A--B--C--|
        //  <-K
        assert_tokens_iter(0, positions.clone(), vec![p3, p2, p1], CounterClockwise);

        // -----A--B--C--|-----A--B--C--|
        //         K->
        assert_tokens_iter(200000, positions.clone(), vec![p2, p3, p1], Clockwise);

        // -----A--B--C--|-----A--B--C--|
        //       <-K
        assert_tokens_iter(200000, positions, vec![p2, p1, p3], CounterClockwise);
    }
}

#[track_caller]
fn assert_tokens_iter(
    start: RingPosition,
    positions: Vec<(RingPosition, PeerId)>,
    expected: Vec<PeerId>,
    dir: RingDirection,
) {
    let ring = HashRing::from(positions);
    let tokens: Vec<_> = ring.tokens(start, dir).map(|t| *t.1).collect();
    assert_eq!(tokens, expected);
}

#[derive(Debug, Clone, Copy)]
struct VarianceExpectation {
    max_variance: f64,
    success_rate: f64,
}

struct VarianceTestCase {
    num_peers: usize,
    num_keys: usize,
    expectations: Vec<VarianceExpectation>,
}

/// Asserts that the hash ring key distribution is balanced.
///
/// Given a number of peers and keys, the function will check that the
/// expectations (success rate for a given variance) are met.
#[track_caller]
fn assert_variance(test_case: VarianceTestCase) {
    let mut ring = HashRing::new(DEFAULT_TOKENS_PER_NODE);
    let num_peers = test_case.num_peers;
    let num_keys = test_case.num_keys;

    // Populate the ring with random peers.
    let mut peers = Vec::with_capacity(num_peers);
    for i in 0..num_peers {
        peers.insert(i, PeerId::random());
        ring.add_node(peers[i]).unwrap();
    }

    // Count the number of keys that land on each peer.
    let mut counts = HashMap::with_capacity(num_peers);
    for i in 0..num_keys {
        let key = ring.partitioner.key_position(&i);
        let peer = ring.primary_node(&key).unwrap();
        counts.entry(peer).and_modify(|e| *e += 1).or_insert(1);
    }

    // Expected count for each peer, if the distribution was perfectly balanced.
    let expected_count = num_keys as f64 / num_peers as f64;

    for expectation in test_case.expectations {
        // `success_rate` percentage of nodes must to be within the expected `variance`.
        let variance = expectation.max_variance;
        let success_rate = expectation.success_rate;

        // Upper bound for the number of keys that can land on a peer.
        let upper_bound = (expected_count * (1.0 + variance)) as u32;

        let mut overflowed_peers = 0f64;
        for count in counts.values() {
            if *count > upper_bound {
                overflowed_peers += 1.0;
            }
        }
        let percent_within_range = 1.0 - overflowed_peers / (num_peers as f64);

        assert!(
            percent_within_range >= success_rate,
            "Less than {}% peers are within the expected {}% ({} keys) variance range. Actual: {}%",
            success_rate * 100.0,
            variance * 100.0,
            upper_bound,
            percent_within_range * 100.0,
        );
    }
}

#[cfg_attr(not(feature = "randomized-tests"), ignore)]
#[test]
fn variance() {
    let expectations = vec![
        VarianceExpectation {
            max_variance: 0.1,
            success_rate: 0.5,
        },
        VarianceExpectation {
            max_variance: 0.25,
            success_rate: 0.75,
        },
        VarianceExpectation {
            max_variance: 0.5,
            success_rate: 1.0,
        },
    ];
    assert_variance(VarianceTestCase {
        num_peers: 10,
        num_keys: 100_000,
        expectations: expectations.clone(),
    });

    assert_variance(VarianceTestCase {
        num_peers: 100,
        num_keys: 100_000,
        expectations: expectations.clone(),
    });

    assert_variance(VarianceTestCase {
        num_peers: 250,
        num_keys: 100_000,
        expectations,
    });
}

#[cfg_attr(not(feature = "randomized-tests"), ignore)]
#[test]
fn key_remapping() {
    fn assert_key_remaps(
        ring: &HashRing,
        num_keys: usize,
        expected_moves: usize,
        mapping: &mut HashMap<usize, PeerId>,
    ) {
        let mut moves = 0;
        for i in 0..num_keys {
            let previous_peer = mapping[&i];
            let key = ring.partitioner.key_position(&i);
            let peer = ring.primary_node(&key).unwrap();
            if peer != previous_peer {
                moves += 1;
            }
            // Update the mapping, to be used in the next test.
            mapping.entry(i).and_modify(|e| *e = peer).or_insert(peer);
        }
        assert!(
            moves <= expected_moves,
            "Too many key moves when adding a peer (expected: {}, actual: {})",
            expected_moves,
            moves,
        );
    }

    let mut ring = HashRing::new(DEFAULT_TOKENS_PER_NODE);
    let num_peers = 10;
    let num_keys = 10_000;
    // The number of keys that should be moved when a peer is added or removed. On
    // perfectly balanced ring, where each node gets equal number of keys, this is
    // `num_keys / num_peers`. We add some extra moves to cover for a variance that
    // exists in the current, close to perfect distribution.
    let expected_moves = num_keys / num_peers + num_keys / num_peers / 5;

    // Populate the ring with random peers.
    let mut peers = Vec::with_capacity(num_peers);
    for i in 0..num_peers {
        peers.insert(i, PeerId::random());
        ring.add_node(peers[i]).unwrap();
    }

    // Map each key to its primary peer.
    let mut mapping = HashMap::with_capacity(num_peers);
    for i in 0..num_keys {
        let key = ring.partitioner.key_position(&i);
        let peer = ring.primary_node(&key).unwrap();
        mapping.entry(i).and_modify(|e| *e = peer).or_insert(peer);
    }

    // Make sure keys-peers mapping is bijective.
    assert_eq!(mapping.len(), num_keys);

    // Remove a peer and count necessary key moves.
    ring.remove_node(&peers[0]).unwrap();
    assert_key_remaps(&ring, num_keys, expected_moves, &mut mapping);

    // Add a peer and count necessary key moves.
    ring.add_node(PeerId::random()).unwrap();
    assert_key_remaps(&ring, num_keys, expected_moves, &mut mapping);
}

#[test]
fn custom_peer() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct CustomPeer {
        // This id comes from the outside, from the swarm, for instance.
        external_id: PeerId,
        // This is the id that is used for hashing. This way we can have deterministic hashing
        // and therefore deterministic node placement on the ring. We can always extract the
        // external id, when needed.
        internal_id: PeerId,
    }

    impl From<CustomPeer> for PeerId {
        fn from(peer: CustomPeer) -> PeerId {
            peer.external_id
        }
    }

    impl Hash for CustomPeer {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.internal_id.hash(state);
        }
    }

    // These peers are pre-selected and are not random.
    let peers = preset_peers();

    // External peer id comes from the outside, we don't have control over it.
    let external_id = PeerId::random();

    // Internal peer id is used for hashing and is deterministic.
    let peer = CustomPeer {
        external_id,
        internal_id: peers[0].peer_id,
    };

    let mut ring = HashRing::new(1);
    ring.add_node(peer).unwrap();

    // Make sure that peer is placed on an expected position.
    assert_eq!(
        *ring.node_positions(&peer).first().unwrap(),
        peers[0].position
    );
    assert_eq!(ring.primary_node(&0), Some(peer));

    // Make sure that when searching for key owner, we can extract the
    // external id.
    let extracted_peer: PeerId = ring.primary_node(&0).unwrap().into();
    assert_eq!(extracted_peer, external_id);
}

#[test]
fn hasher_sanity_check() {
    // Make sure the hasher algorithm doesn't change between releases.
    let partitioner = DefaultPartitioner::new();
    assert_eq!(partitioner.key_position(&0u64), 0x1424aa9885a1d5c7);
    assert_eq!(partitioner.key_position(&1u64), 0xcfb732e08be9ec0);
    assert_eq!(partitioner.key_position(&123456u64), 0x4879daae426e14fb);

    // Make sure that peer_id can be positioned on the ring.
    let hex_string = "002032e23b848ba9fb974c30c417122cfe3ccf53e1c84c2d81d93754f67697259e18";
    let node_bytes = <[u8; 34]>::from_hex(hex_string).expect("invalid hex string");
    let node_id = &PeerId::from_bytes(&node_bytes, 3).unwrap();
    assert_eq!(partitioner.key_position(node_id), 811111111692949100);
    // Make sure that ring position hasher can bee seeded.
    assert_eq!(
        partitioner.key_position_seeded(&node_id, DEFAULT_SEED2),
        7625271849173840990
    );
    assert_eq!(node_id.group, 3);
}
