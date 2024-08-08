use {
    crate::{Keyspace, ShardId},
    std::{
        collections::{HashMap, HashSet},
        hash::Hash,
    },
};

impl<N, const RF: usize> Keyspace<N, RF>
where
    N: Copy + Default + Hash + Eq + PartialEq + Ord + PartialOrd,
{
    pub fn assert_variance_and_stability(
        &self,
        old_keyspace: &Self,
        expected_variance: &ExpectedDistributionVariance,
    ) {
        let nodes: HashSet<_> = self.shards().flat_map(|(_, id)| id).collect();
        let old_nodes: HashSet<_> = old_keyspace.shards().flat_map(|(_, id)| id).collect();

        println!();

        let mut shards_per_node = HashMap::<N, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas.iter() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        let mean = self.shards.len() * RF / self.nodes_count;
        let max_deviation = shards_per_node
            .values()
            .copied()
            .map(|n| (mean as isize - n as isize).unsigned_abs())
            .max()
            .unwrap();
        let coefficient = max_deviation as f64 / mean as f64;

        let allowed_coefficient = expected_variance.get(self.nodes_count);

        dbg!(mean, max_deviation, coefficient, allowed_coefficient);
        assert!(coefficient <= allowed_coefficient);

        let mut allowed_shard_movements = 0;

        let removed: HashSet<_> = old_nodes.difference(&nodes).copied().collect();
        let added: HashSet<_> = nodes.difference(&old_nodes).copied().collect();

        if !removed.is_empty() {
            for shard in &old_keyspace.shards {
                for node_id in &shard.replicas {
                    if removed.contains(node_id) {
                        allowed_shard_movements += 1;
                    }
                }
            }
        }

        allowed_shard_movements +=
            (((added.len() * RF * mean) as f64) * (1.0 + allowed_coefficient)) as usize;

        let mut shard_movements = 0;
        let mut replicas: HashSet<_>;
        for shard_idx in 0..old_keyspace.shards.len() {
            let shard_id = ShardId(shard_idx as u16);

            replicas = self.shard_replicas(shard_id).into_iter().collect();

            for node_id in old_keyspace.shard_replicas(shard_id) {
                if !replicas.contains(&node_id) {
                    shard_movements += 1;
                }
            }
        }

        dbg!(
            shard_movements,
            allowed_shard_movements,
            removed.len(),
            added.len()
        );
        assert!(shard_movements <= allowed_shard_movements);
    }
}

/// Specifies the expected shard distribution variance.
///
/// Each element of the [`Vec`] should be an `(N, C)` tuple, where `N` is number
/// of nodes in the cluster and `C` is a maximum allowed mean absolute deviation
/// coefficient.
///
/// For example, coefficient of `0.1` means that the maximum and the minimum
/// amount of shards per node are allowed to deviate from the mean (the average)
/// by 10%.
/// Let's say the expected mean is 100 shards per node, then coefficient of
/// `0.1` will allow the number of shards per node to deviate in the range of
/// `90..=110`.
///
/// Not every cluster size needs to be specified, one may specify
/// the expected variance as a range:
/// ```
/// sharding::testing::ExpectedDistributionVariance(vec![(4, 0.01), (8, 0.02), (256, 0.1)]);
/// ```
/// This would mean that for a cluster size of up to 4 nodes the coefficient is
/// `0.01`, for a cluster size in the range of `5..8` the coefficient is `0.02`
/// and so on.
pub struct ExpectedDistributionVariance(pub Vec<(usize, f64)>);

impl ExpectedDistributionVariance {
    fn get(&self, nodes_count: usize) -> f64 {
        for (count, coef) in self.0.iter().copied() {
            if nodes_count <= count {
                return coef;
            }
        }

        panic!("Missing coefficient for nodes_count: {nodes_count}");
    }
}
