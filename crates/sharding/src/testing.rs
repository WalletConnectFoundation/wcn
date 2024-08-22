use {
    crate::{Keyspace, ShardId},
    std::{
        collections::{HashMap, HashSet},
        fmt,
        hash::Hash,
    },
};

impl<N, const RF: usize> Keyspace<N, RF>
where
    N: Copy + fmt::Debug + Default + Hash + Eq + PartialEq + Ord + PartialOrd + 'static,
{
    /// Asserts that the `min/max` ratio is larger than the provided value.
    /// Where `min` is the minimum amount of shards being assigned to a node,
    /// and `max` is the maximum.
    ///
    /// Ratio of `0.7`, for example, means that the minimum is 70% of the
    /// maximum. So, when the highest loaded node has 100% cpu load, the lowest
    /// loaded node would have 70%.
    pub fn assert_distribution(&self, expected_min_max_ratio: f64) {
        let mut shards_per_node = HashMap::<N, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas.iter() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        let min = shards_per_node.values().min().copied().unwrap();
        let max = shards_per_node.values().max().copied().unwrap();

        let actual_min_max_ratio = min as f64 / max as f64;

        dbg!(
            expected_min_max_ratio,
            actual_min_max_ratio,
            min,
            max,
            shards_per_node,
        );
        assert!(actual_min_max_ratio >= expected_min_max_ratio)
    }

    /// Asserts that the stability coefficient between 2 keyspaces is
    /// larger than the provided value.
    ///
    /// The coefficient represents how many shards remained assigned to the same
    /// nodes. So `0.7`, for example, would mean that 70% of shards haven't been
    /// moved, and 30% moved from one node to another.
    pub fn assert_stability(&self, old_self: &Self, expected_coefficient: f64) {
        let mut shards_moved = 0;
        let mut replicas: HashSet<_>;
        for shard_idx in 0..old_self.shards.len() {
            let shard_id = ShardId(shard_idx as u16);

            replicas = self.shard_replicas(shard_id).iter().collect();

            for node_id in old_self.shard_replicas(shard_id) {
                if !replicas.contains(&node_id) {
                    shards_moved += 1;
                }
            }
        }

        let shards_total = self.shards.len() * RF;
        let actual_coefficient = 1.0 - shards_moved as f64 / shards_total as f64;

        dbg!(
            expected_coefficient,
            actual_coefficient,
            shards_total,
            shards_moved,
        );
        assert!(actual_coefficient >= expected_coefficient)
    }
}
