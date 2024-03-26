use {
    super::ConsistencyLevel,
    crate::{
        cluster::keyspace::{
            hashring::RingDirection::{Clockwise, CounterClockwise},
            range::{merge_presorted_ranges, KeyRange},
            HashRing,
            RingPosition,
        },
        PeerId,
    },
    serde::{Deserialize, Serialize},
    smallvec::SmallVec,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Strategy {
    factor: usize,
    level: ConsistencyLevel,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            factor: 3,
            level: ConsistencyLevel::default(),
        }
    }
}

impl Strategy {
    pub fn new(factor: usize, level: ConsistencyLevel) -> Self {
        Self { factor, level }
    }

    /// Returns the number of replicas for each key.
    pub fn replication_factor(&self) -> usize {
        self.factor
    }

    /// Returns the consistency level for the given replication strategy.
    pub fn consistency_level(&self) -> ConsistencyLevel {
        self.level.clone()
    }

    /// Returns the number of replicas required for a replicated operation to
    /// succeed, using given replication strategy.
    pub fn required_replica_count(&self) -> usize {
        match self.level {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => self.factor / 2 + 1,
            ConsistencyLevel::All => self.factor,
        }
    }

    /// Returns peers that serve as target replicas for the given ring position.
    ///
    /// Before locating the replicas, we move the position one step forward, so
    /// that if provided position is at some peer's position, we skip that peer
    /// (a position exactly at peer belongs to the next clockwise peer).
    ///
    /// Within this strategy, we move forward until we find `factor` number of
    /// distinct peers from different groups.
    pub fn natural_replicas(&self, ring: &HashRing, pos: RingPosition) -> Vec<PeerId> {
        let mut nodes: Vec<PeerId> = Vec::with_capacity(self.factor);

        for (_, &node) in ring.tokens(pos.wrapping_add(1), Clockwise) {
            if nodes
                .iter()
                .any(|p| node.group != 0 && p.group == node.group)
            {
                continue;
            }

            nodes.push(node);
            if nodes.len() >= self.factor {
                break;
            }
        }

        nodes
    }

    /// The key is written to the next several nodes on the ring: replication
    /// factor number of distinct nodes, with at most one node per group
    /// selected. Therefore, for key ranges, we need to iterate enough nodes
    /// backwards (so that we meet replication factor number of distinct nodes,
    /// from different groups).
    ///
    /// Since the provided position is where the node is located, we need to
    /// make sure that no key range that is targeting the node of the same group
    /// is logged.
    pub fn key_ranges(
        &self,
        ring: &HashRing,
        pos: RingPosition,
        try_merge: bool,
    ) -> Vec<KeyRange<RingPosition>> {
        let mut ids = SmallVec::<[u16; 3]>::new(); // track group IDs
        let mut ranges = Vec::new();

        // Obtain the target node and its group id. If the node is not found at the
        // given position, the first node after the position is used.
        let mut tokens = ring.tokens(pos, Clockwise);
        let (pos, target_id, mut prev_node) = match tokens.next() {
            Some((&pos, &node)) => (pos, node.group, node),
            None => return ranges,
        };
        let mut prev_pos = pos;

        // Traverse possible key ranges.
        for (&pos, &node) in ring.tokens(pos.wrapping_sub(1), CounterClockwise) {
            let range = KeyRange::new(pos, prev_pos);

            // Process unique ranges only.
            if ranges.contains(&range) {
                (prev_pos, prev_node) = (pos, node);
                continue;
            }

            // Make sure that target group id is in list at most once.
            if !ids.is_empty() && prev_node.group == target_id {
                (prev_pos, prev_node) = (pos, node);
                continue;
            }

            ranges.push(range);

            // Process only distinct groups (if group id > 0).
            if !ids.contains(&prev_node.group) {
                ids.push(prev_node.group);
                // When we reached number of distinct groups, we stop, unless incoming node is
                // of the same group -- then we continue, since we are moving backwards and a
                // node with the same group id will intercept all the keys that are targeting
                // the same group.
                if ids.len() >= self.factor && !ids.contains(&node.group) {
                    break;
                }
            }

            // When a node with the same group id is found, we stop, as such a node will
            // intercept all the keys that are targeting the same group.
            if target_id != 0 && node.group == target_id {
                break;
            }

            (prev_pos, prev_node) = (pos, node);
        }

        if try_merge {
            merge_presorted_ranges(ranges).collect()
        } else {
            ranges
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::cluster::{
            replication::tests::{assert_ranges, assert_replicas},
            test_util::{preset_peers, PEERS},
        },
        std::collections::BTreeMap,
    };

    #[test]
    fn natural_replicas() {
        let peers = preset_peers();

        let mut ring = HashRing::new(1);
        let strategy = Strategy::new(3, ConsistencyLevel::default());

        {
            // Empty keyspace.
            assert_replicas(&strategy, &ring, 0, vec![]);
        }

        {
            // Single peer keyspace.
            ring.add_node(peers[0].peer_id).unwrap(); // group 1
            assert_replicas(&strategy, &ring, 0, vec![peers[0].peer_id]);
        }

        {
            // Replication factor sized keyspace (with the same group duplicates).
            ring.add_node(peers[1].peer_id).unwrap(); // group 2
            ring.add_node(peers[2].peer_id).unwrap(); // group 2

            // Only a single (first) peer from the same group is returned.
            assert_eq!(peers[1].peer_id.group, peers[2].peer_id.group);

            assert_replicas(&strategy, &ring, 0, vec![
                peers[0].peer_id,
                peers[1].peer_id,
            ]);
        }

        {
            // Replication factor sized keyspace (with replication factor nodes selectable).
            ring.add_node(peers[3].peer_id).unwrap(); // group 1
            ring.add_node(peers[4].peer_id).unwrap(); // group 3

            assert_eq!(peers[1].peer_id.group, peers[2].peer_id.group);
            assert_eq!(peers[0].peer_id.group, peers[3].peer_id.group);

            assert_replicas(&strategy, &ring, 0, vec![
                peers[0].peer_id,
                peers[1].peer_id,
                peers[4].peer_id,
            ]);
        }

        {
            // More nodes than replication factor.
            ring.add_node(peers[5].peer_id).unwrap(); // group 3
            ring.add_node(peers[6].peer_id).unwrap(); // group 1

            assert_replicas(&strategy, &ring, 0, vec![
                peers[0].peer_id,
                peers[1].peer_id,
                peers[4].peer_id,
            ]);

            // `peer[0]` (group 1) is out, so this time `peer[3]` (group 1), is added to
            // replica set.
            assert_replicas(&strategy, &ring, peers[0].position, vec![
                peers[1].peer_id,
                peers[3].peer_id,
                peers[4].peer_id,
            ]);
        }

        {
            // Erase all groups ids, make sure that we can still find replicas (basically,
            // this will default to simple replication strategy).
            let mut ring = HashRing::new(1);

            fn erase_group_id(id: &PeerId) -> PeerId {
                PeerId {
                    id: id.id,
                    group: 0,
                }
            }

            let mut no_group_peers = BTreeMap::new();
            for peer in &peers {
                let node_id = erase_group_id(&peer.peer_id);
                no_group_peers.insert(ring.key_position(&node_id), node_id);
                ring.add_node(node_id).unwrap();
            }

            assert_eq!(ring.len(), 10);

            assert_replicas(
                &strategy,
                &ring,
                0,
                no_group_peers.values().copied().take(3).collect(),
            );

            assert_replicas(
                &strategy,
                &ring,
                *no_group_peers.first_key_value().unwrap().0,
                no_group_peers.values().copied().skip(1).take(3).collect(),
            );
        }
    }

    #[test]
    fn key_ranges_empty_keyspace() {
        let ring: HashRing = HashRing::new(1);
        let strategy = Strategy::new(3, ConsistencyLevel::default());
        let ranges = strategy.key_ranges(&ring, 0, true);
        assert_eq!(ranges.len(), 0);
    }

    #[test]
    fn key_ranges() {
        {
            // Single token, single expected replica (replication factor = 1).
            let s = Strategy::new(1, ConsistencyLevel::default());
            let positions = vec![(1000, PEERS[0].peer_id)];

            //      K
            // -----A-----
            assert_ranges(&s, &positions, 1000, vec![KeyRange::new(1000, 1000)]);

            //   K
            // -----A-----
            // Position is adjusted to the next node.
            assert_ranges(&s, &positions, 100, vec![KeyRange::new(1000, 1000)]);

            //         K
            // -----A-----
            // Position is adjusted to the next node.
            assert_ranges(&s, &positions, 1500, vec![KeyRange::new(1000, 1000)]);
        }

        {
            // Single token (replication factor = 2). This is a degenerate case: on search
            // for second replica we always will wrap around the ring, which is expected
            // as we have an abnormal situation when replica factor is higher than
            // number of tokens on the ring.
            let s = Strategy::new(2, ConsistencyLevel::default());
            let positions = vec![(1000, PEERS[0].peer_id)];

            //      K
            // -----A-----
            assert_ranges(&s, &positions, 1000, vec![KeyRange::new(1000, 1000)]);

            //   K
            // -----A-----
            assert_ranges(&s, &positions, 100, vec![KeyRange::new(1000, 1000)]);

            //         K
            // -----A-----
            assert_ranges(&s, &positions, 1500, vec![KeyRange::new(1000, 1000)]);
        }

        let s = Strategy::new(3, ConsistencyLevel::default());
        let p0 = PEERS[0].peer_id;
        let p1 = PEERS[1].peer_id;
        let p2 = PEERS[2].peer_id;
        let p3 = PEERS[3].peer_id;
        let p4 = PEERS[4].peer_id;

        {
            // Multiple tokens (replication factor = 3).
            let positions = vec![
                (10000, p0),
                (20000, p1),
                (30000, p2),
                (40000, p3),
                (50000, p4),
            ];

            //    K               *   *
            // ---0---A---B---C---D---E---|
            // g:     1   2   2   1   3
            // =======)           [=======
            // Position is adjusted to the next node.
            // Only two ranges are returned, as C has the same group id as A.
            assert_ranges(&s, &positions, 0, vec![KeyRange::new(40000, 10000)]);

            //      K             *   *
            // ---0---A---B---C---D---E---|
            // g:     1   2   2   1   3
            // =======)           [=======
            assert_ranges(&s, &positions, 5000, vec![KeyRange::new(40000, 10000)]);

            //        K           *   *
            // ---0---A---B---C---D---E---|
            // g:     1   2   2   1   3
            // =======)           [=======
            assert_ranges(&s, &positions, 10000, vec![KeyRange::new(40000, 10000)]);

            //        * K     *   *   *
            // ---0---A---B---C---D---E---|
            // ===========)   [===========
            // g:     1   2   2   1   3
            // Duplicate group ids are not allowed, therefore, any key in the range C-D will
            // als0 land on target node B. Hence, the key range is `[30000..20000)`, and not
            // `40000..20000`.
            assert_ranges(&s, &positions, 15000, vec![KeyRange::new(30000, 20000)]);

            //        * K   *    *   *
            // ---0---A---B---C---D---E---|
            // ===========)   [===========
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 20000, vec![KeyRange::new(30000, 20000)]);

            //        * * K         *
            // ---0---A---B---C---D---E---|
            //            [===)
            // g:     1   2   2   1   3
            // Both B and C are of the same group, so B will intercept any keys that come
            // before it, and therefor only `[B..C)` is returned.
            assert_ranges(&s, &positions, 25000, vec![KeyRange::new(20000, 30000)]);

            //        * *   K       *
            // ---0---A---B---C---D---E---|
            //            [===)
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 30000, vec![KeyRange::new(20000, 30000)]);

            //        * *   * K
            // ---0---A---B---C---D---E---|
            //        [===========)
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 35000, vec![KeyRange::new(10000, 40000)]);

            //        * *   *   K
            // ---0---A---B---C---D---E---|
            //        [===========)
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 40000, vec![KeyRange::new(10000, 40000)]);

            //            * *   * K
            // ---0---A---B---C---D---E---|
            // ==)[=======================
            // g:     1   2   2   1   3
            // E is only node from the group 3, so whole ring is returned (our ring has only
            // 3 groups, so a single node with unique group will be selected as target for
            // any key range -- when replication factor of 3 is used).
            assert_ranges(&s, &positions, 45000, vec![KeyRange::new(0, 0)]);

            //            * *   *   K
            // ==)[=======================
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 50000, vec![KeyRange::new(0, 0)]);

            //                * *   * K
            // ---0---A---B---C---D---E---|
            // =======)           [=======
            // g:     1   2   2   1   3
            assert_ranges(&s, &positions, 55000, vec![KeyRange::new(40000, 10000)]);
        }
    }
}
