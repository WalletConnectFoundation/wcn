use {
    crate::{HasherFactory, Keyspace, ShardId, Strategy},
    rand::random,
    std::{
        collections::{HashMap, HashSet},
        fmt,
        hash::Hash,
    },
};

#[cfg(any(test, feature = "testing"))]
impl<const RF: usize, N> Keyspace<RF, N>
where
    N: Eq + Hash + fmt::Debug,
{
    fn assert_variance_and_stability(
        &self,
        old: &Self,
        expected_variance: &ExpectedDistributionVariance,
    ) {
        println!();
        dbg!(self.nodes.len());

        let mut shards_per_node = HashMap::<usize, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas.iter() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        let mean = self.shards.len() * RF / self.nodes.len();
        let max_deviation = shards_per_node
            .values()
            .copied()
            .map(|n| (mean as isize - n as isize).unsigned_abs())
            .max()
            .unwrap();
        let coefficient = max_deviation as f64 / mean as f64;

        let allowed_coefficient = expected_variance.get(self.nodes.len());

        dbg!(mean, max_deviation, coefficient, allowed_coefficient);
        assert!(coefficient <= allowed_coefficient);

        let old_nodes: HashSet<_> = old.nodes().collect();
        let new_nodes: HashSet<_> = self.nodes().collect();

        let mut allowed_shard_movements = 0;

        let removed: HashSet<_> = old_nodes.difference(&new_nodes).collect();
        let added: HashSet<_> = new_nodes.difference(&old_nodes).collect();

        if !removed.is_empty() {
            for shard in &old.shards {
                for replica_idx in &shard.replicas {
                    if removed.contains(&old.nodes.get_index(*replica_idx).unwrap()) {
                        allowed_shard_movements += 1;
                    }
                }
            }
        }

        allowed_shard_movements +=
            (((added.len() * RF * mean) as f64) * (1.0 + allowed_coefficient)) as usize;

        let mut shard_movements = 0;
        let mut replicas: HashSet<_>;
        for shard_idx in 0..old.shards.len() {
            let shard_id = ShardId(shard_idx as u16);

            replicas = self.shard_replicas(shard_id).collect();

            for node_id in old.shard_replicas(shard_id) {
                if !replicas.contains(node_id) {
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

#[cfg(any(test, feature = "testing"))]
pub fn keyspace_test_suite<const RF: usize, NodeId, S>(
    hasher_factory: impl HasherFactory,
    sharding_strategy_factory: impl FnMut() -> S,
    node_id_factory: impl FnMut() -> NodeId,
    expected_variance: ExpectedDistributionVariance,
    fuzzy_iterations: usize,
) where
    NodeId: Eq + Hash + Ord + Clone + fmt::Debug,
    S: Strategy<NodeId>,
{
    let new_hasher = hasher_factory;
    let mut new_strategy = sharding_strategy_factory;
    let mut new_node_id = node_id_factory;

    let initial_nodes = (0..RF).map(|_| new_node_id());

    let mut keyspace: Keyspace<RF, _> =
        Keyspace::new(initial_nodes, &new_hasher, new_strategy()).unwrap();
    keyspace.assert_variance_and_stability(&keyspace, &expected_variance);

    // add 10 nodes by 1
    for _ in 0..10 {
        let nodes = keyspace.nodes().cloned().chain([new_node_id()]);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // remove 10 nodes by 1
    for _ in 0..10 {
        let mut nodes = keyspace.nodes.clone();
        nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());

        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // add 10 nodes at once
    {
        let ids = (0..10).map(|_| new_node_id());
        let nodes = keyspace.nodes().cloned().chain(ids);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // remove 10 nodes at once
    {
        let mut nodes = keyspace.nodes.clone();
        for _ in 0..10 {
            nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());
        }
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // scale to 32 nodes
    {
        let add = 32usize
            .checked_sub(keyspace.nodes_count())
            .unwrap_or_default();
        let ids = (0..add).map(|_| new_node_id());
        let nodes = keyspace.nodes().cloned().chain(ids);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // scale to 64 nodes
    {
        let add = 64usize
            .checked_sub(keyspace.nodes_count())
            .unwrap_or_default();
        let ids = (0..add).map(|_| new_node_id());
        let nodes = keyspace.nodes().cloned().chain(ids);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // scale to 128 nodes
    {
        let add = 128usize
            .checked_sub(keyspace.nodes_count())
            .unwrap_or_default();
        let ids = (0..add).map(|_| new_node_id());
        let nodes = keyspace.nodes().cloned().chain(ids);
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // remove 1, add 1
    {
        let mut nodes = keyspace.nodes.clone();
        nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());
        nodes.insert(new_node_id());

        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // remove 10, add 10
    {
        let mut nodes = keyspace.nodes.clone();
        for _ in 0..10 {
            nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());
            nodes.insert(new_node_id());
        }
        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }

    // add / remove random amount of nodes
    for _ in 0..fuzzy_iterations {
        let mut nodes = keyspace.nodes.clone();

        for _ in 0..2 {
            let nodes_count = random::<usize>() % (crate::MAX_NODES - RF) + RF;
            match nodes_count as isize - nodes.len() as isize {
                n if n > 0 => {
                    for _ in 0..n {
                        nodes.insert(new_node_id());
                    }
                }
                n => {
                    for _ in 5..n.abs() {
                        nodes.shift_remove_index(random::<usize>() % keyspace.nodes.len());
                    }
                }
            }
        }

        let new_keyspace = Keyspace::new(nodes, &new_hasher, new_strategy()).unwrap();
        new_keyspace.assert_variance_and_stability(&keyspace, &expected_variance);
        keyspace = new_keyspace;
    }
}
