use {
    super::*,
    crate::cluster::keyspace::HashRing,
    hex::FromHex,
    once_cell::sync::Lazy,
    std::{collections::BTreeSet, ops::Range},
};

#[derive(Debug, Clone, Copy)]
pub struct PeerPosition {
    pub peer_id: PeerId,
    pub position: RingPosition,
}

impl PeerPosition {
    pub fn new(peer_id: PeerId, position: RingPosition) -> Self {
        Self { peer_id, position }
    }
}

impl From<(PeerId, RingPosition)> for PeerPosition {
    fn from((peer_id, position): (PeerId, RingPosition)) -> Self {
        Self { peer_id, position }
    }
}

impl From<PeerPosition> for (PeerId, RingPosition) {
    fn from(PeerPosition { peer_id, position }: PeerPosition) -> Self {
        (peer_id, position)
    }
}

/// Returns a list of peers with their positions in the ring. Preset peers are
/// sorted by their ring position, and are well distributed across the ring.
pub fn preset_peers() -> Vec<PeerPosition> {
    let peer_bytes = vec![
        (
            // 1AnkJG8WK93kUQ7hG7ZVPwq7cfaZoca3pPss7nGt511ewd
            111100983615508364,
            "0020f3a9fa9bc4fd9827cd83c3d8b7b83fd6f8c79cee7e33d756000f6c4956612654",
            1,
        ),
        (
            // 1AjGX6xnkcBhsFUVQR2cMCGaW4DJjRj8dbKdonspb8AFEv
            222200056717412907,
            "0020bff92d09f41a77daf04c194b7e33c815fab950000498fa0a4a5123448e532cf7",
            2,
        ),
        (
            // 1AmhX2sdZyHTCide2BSYApohQtZpzo6EGGpyngYsWArsXT
            333300128304974679,
            "0020e417fd8441e42eb61eb42b8dedc5272fd29c411045bfc01c8bc92fc629a00146",
            2,
        ),
        (
            // 1Ao42X6jKtvA3WJfBabu8w9XwkPqfpeoah1NPGfqwrLoL9
            444400609862258242,
            "0020f834a252835978d2418cbd4e0d1747fafa1729e4ace1a0add746b3a21eb75b96",
            1,
        ),
        (
            // 1AeUufpcYfxTEQ98AdeDK9J43uiQgJuUBCEXEQhYfvigVs
            555500550042804847,
            "002078db3a217210285247ae56550b2b2e57150a705dad0e1a934a9c54bb1513c03e",
            3,
        ),
        (
            // 1AYFHFfcQ5oooZyshR4MgKXW1JPa4f24mB9dzxfTE6xiMD
            666600933710043825,
            "00201c3760f6a85cbf496a4b70c62534656b911491c0d9f8744e3ef0d288a9db9ae0",
            2,
        ),
        (
            // 1AjAtYs8CP9Brp6rVziD2t8rGR1Xg8Et5Wx634QWTNaeBd
            777700400410698560,
            "0020be87f0d96a1bd61e2dfd793fb236aa045c033f13a8e07be3915cdf0446b7a614",
            1,
        ),
        (
            // 1AXsDPomoXTdaqU8tXPnqRsjgpXrTggnH5H9gTUFZChYpp
            888800365207019014,
            "002016903ca2586c4aa1f951f386c247470a251e6212a90d4f9233f6b3e5052d7f21",
            3,
        ),
        (
            // 1AiCeUoeryexqoeLF5u8N2aNBxS9DDgGXDfXPaTEh726yX
            999900572057947227,
            "0020b01f81aacd72e108525b8f90019278b4bae1b5464ef9409cd60fe74d44881f4a",
            1,
        ),
        (
            // 1Ao4ixKKyuTQNzTgmtBwpZGycBmUuZnd6DePyoNeXGVioT
            1000005102921762134,
            "0020f8625a94182a9f2cde678d2a5be3bfeb22cbf7d4f9d31b711c93b7bf3858501a",
            3,
        ),
    ];

    let mut peers = vec![];
    for (pos, hex_string, group) in peer_bytes {
        let peer_bytes = <[u8; 34]>::from_hex(hex_string).expect("invalid hex string");
        peers.push(PeerPosition {
            peer_id: PeerId::from_bytes(&peer_bytes, group).unwrap(),
            position: pos,
        });
    }

    peers
}

pub static PEERS: Lazy<Vec<PeerPosition>> = Lazy::new(preset_peers);

/// Creates PUSH pending ranges for a given peer *ids* (both range and
/// destination are indexes in `PEERS`).
pub fn push(range: Range<usize>, destination: usize) -> PendingRange<u64> {
    PendingRange::Push {
        range: KeyRange::new(PEERS[range.start].position, PEERS[range.end].position),
        destination: PEERS[destination].peer_id,
    }
}

/// Creates PUSH pending range for a given peer *ids* without transforming
/// ranges.
pub fn push_range(range: Range<u64>, destination: usize) -> PendingRange<u64> {
    PendingRange::Push {
        range: range.into(),
        destination: PEERS[destination].peer_id,
    }
}

pub fn pulls_from_pushes(
    mut input_ranges: BTreeMap<PeerId, PendingRanges>,
) -> BTreeMap<PeerId, PendingRanges> {
    let mut pulls: BTreeMap<PeerId, HashMap<KeyRange<RingPosition>, Vec<PeerId>>> = BTreeMap::new();

    // Process pushes, and create corresponding pulls.
    for (source_peer, ranges) in &input_ranges {
        for range in ranges {
            if let PendingRange::Push { range, destination } = range {
                pulls
                    .entry(*destination)
                    .or_default()
                    .entry(*range)
                    .or_default()
                    .push(*source_peer);
            }
        }
    }

    // Convert to the expected format.
    let mut pull_ranges: BTreeMap<PeerId, PendingRanges> = pulls
        .into_iter()
        .map(|(idx, ranges)| {
            (
                idx,
                ranges
                    .into_iter()
                    .map(|(range, sources)| PendingRange::Pull {
                        range,
                        sources: BTreeSet::from_iter(sources),
                    })
                    .collect(),
            )
        })
        .collect();

    // Merge ranges and pushes.
    for (idx, ranges) in &mut pull_ranges {
        if let Some(existing) = input_ranges.get_mut(idx) {
            existing.extend(ranges.clone());
        } else {
            input_ranges.insert(*idx, ranges.clone());
        }
    }

    input_ranges
}

#[test]
fn verify_preset_peers() {
    let peers = preset_peers();
    let ring = HashRing::new(1);
    for peer in peers {
        assert_eq!(
            ring.key_position(&peer.peer_id),
            peer.position,
            "peer: {:?}",
            peer.position
        );
        assert_eq!(
            *ring.node_positions(&peer.peer_id).first().unwrap(),
            peer.position,
            "peer: {:?}",
            peer.position
        );
    }
}
