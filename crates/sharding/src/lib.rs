use std::{
    collections::{BTreeSet, HashMap},
    default,
    hash::SipHasher,
};

#[derive(Clone)]
struct Keyspace<const RF: usize, N> {
    nodes: Vec<Option<N>>,
    shards: Vec<Shard<RF>>,

    resharding: HashMap<u16, Shard<RF>>,
}

#[derive(Clone, Copy, Debug)]
struct Shard<const RF: usize> {
    replicas: [usize; RF],
}

struct ShardId(u16);

impl ShardId {
    const MAX: Self = Self(u16::MAX);
}

impl<const RF: usize> Shard<RF> {
    fn replicas(&self) -> &[usize; RF] {
        &self.replicas
    }

    fn contains_replica(&self, node_idx: usize) -> bool {
        self.replicas.iter().any(|idx| *idx == node_idx)
    }
}

impl<const R: usize, N> Keyspace<R, N> {
    pub fn new(nodes: [N; R]) -> Self {
        // TODO: compile error
        assert!(R > 0);

        let replicas = [0; R];

        let mut nodes = nodes.into_iter();
        let first_node = nodes.next().unwrap();

        let n_shards = ShardId::MAX.0 + 1;

        let mut this = Self {
            shards: (0..n_shards).map(|_| Shard { replicas }).collect(),
            nodes: [Some(first_node)].into_iter().collect(),
            resharding: HashMap::new(),
        };

        for node in nodes {
            this.add_node(node);
        }

        this
    }

    pub fn get_shard(&self, id: ShardId) -> Shard<R> {
        self.shards[id.0 as usize]
    }

    pub fn add_node(&mut self, new_node: N) {
        // Find the position where to put new node.
        // Use first free slot if any, otherwise append to the end.
        let new_node_idx = self
            .nodes
            .iter()
            .position(|n| n.is_none())
            .unwrap_or_else(|| {
                self.nodes.push(None);
                self.nodes.len() - 1
            });

        self.nodes[new_node_idx] = Some(new_node);

        let mut resharding = self.resharding();

        for shard in &mut self.shards {
            resharding.try_reassign_primary_shard(&mut shard.replicas[0], new_node_idx);
        }

        self.reassign_secondary_shards();
    }

    pub fn remove_node(&mut self, find: impl Fn(&N) -> bool) {
        let Some(node_idx_to_remove) = self
            .nodes
            .iter()
            .position(|n| n.as_ref().map(|n| find(n)).unwrap_or_default())
        else {
            return;
        };

        let mut resharding = self.resharding();

        let mut nodes_cursor = 0;

        for shard in &mut self.shards {
            let primary_replica_idx = &mut shard.replicas[0];

            if *primary_replica_idx != node_idx_to_remove {
                continue;
            }

            loop {
                if !resharding.try_reassign_primary_shard(primary_replica_idx, nodes_cursor) {
                    nodes_cursor += 1;
                    continue;
                }

                break;
            }
        }

        self.nodes[node_idx_to_remove] = None;

        self.reassign_secondary_shards()
    }

    fn reassign_secondary_shards(&mut self) {
        for shard in &mut self.shards {
            let mut secondary_replica_idx = 1;
            let mut nodes_cursor = shard.replicas[0];

            loop {
                if secondary_replica_idx == R {
                    break;
                }

                if nodes_cursor == 0 {
                    nodes_cursor = self.nodes.len() - 1;
                } else {
                    nodes_cursor -= 1;
                }

                if nodes_cursor == shard.replicas[0] {
                    break;
                }

                if self.nodes[nodes_cursor].is_none() {
                    continue;
                }

                shard.replicas[secondary_replica_idx] = nodes_cursor;
                secondary_replica_idx += 1;
            }
        }
    }

    fn resharding(&self) -> Resharding {
        let nodes_count = self.nodes.iter().filter_map(Option::as_ref).count();

        let primary_shards_per_node = self.shards.len() / nodes_count;
        let mut shards_remainder = self.shards.len() % nodes_count;

        let mut primary_replicas_counts = vec![0, self.nodes.len()];
        for shard in &self.shards {
            primary_replicas_counts[shard.replicas[0]] += 1;
        }

        let mut nodes = self.nodes.iter().enumerate();

        Resharding {
            primary_shards_counts: primary_replicas_counts,
            primary_shards_per_node,
            first_node_idx_without_surplus: nodes
                .find_map(|(idx, node)| {
                    if shards_remainder == 0 {
                        return Some(idx);
                    }

                    if node.is_some() {
                        shards_remainder -= 1;
                    }

                    None
                })
                .unwrap_or(0),
        }
    }

    fn assert_variance(&self) {
        let mut shards_per_node = HashMap::<usize, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        let min = shards_per_node.values().min().unwrap();
        let max = shards_per_node.values().max().unwrap();

        assert!(max - min <= R, "{shards_per_node:?}");
    }

    fn assert_data_movement(&self, old: &Self) {
        let to_move = shards_to_migrate(old, self);
        dbg!(to_move);
    }
}

enum Difference {
    Positive,
    Negative,
}

// struct ReshardingPlan<const RF: usize> {
//     sources: []
// }

struct Resharding {
    /// Desired amount of primary shards per node after resharding.
    primary_shards_per_node: usize,

    /// Most of the times the amount of shards we have can't be dived by the
    /// amount of nodes without a remainder, so some nodes are going to have +1
    /// shard.
    ///
    /// This parameter indicates the first position in [`Keyspace::nodes`] that
    /// doesn't have this "surplus".
    first_node_idx_without_surplus: usize,

    /// Returns a [`Vec`] in which each element is the amount of primary
    /// [`Shard`]s the node with the respective index is assigned to.
    primary_shards_counts: Vec<usize>,
}

impl Resharding {
    fn desired_primary_shards_count(&self, node_idx: usize) -> usize {
        if node_idx < self.first_node_idx_without_surplus {
            self.primary_shards_per_node + 1
        } else {
            self.primary_shards_per_node
        }
    }

    fn try_reassign_primary_shard(&mut self, src_idx: &mut usize, dst_idx: usize) -> bool {
        if self.primary_shards_counts[*src_idx] == self.desired_primary_shards_count(*src_idx) {
            return false;
        }

        if self.primary_shards_counts[dst_idx] == self.desired_primary_shards_count(dst_idx) {
            return false;
        }

        self.primary_shards_counts[*src_idx] -= 1;
        self.primary_shards_counts[dst_idx] += 1;

        *src_idx = dst_idx;

        true
    }
}

fn shards_to_migrate<const N: usize>(a: &Keyspace<N>, b: &Keyspace<N>) -> usize {
    let mut n = 0;

    for (shard_idx, shard) in a.shards.iter().enumerate() {
        let new_replicas = b.shards[shard_idx].replicas;

        for replica_idx in shard.replicas {
            if !new_replicas.iter().any(|idx| idx == &replica_idx) {
                n += 1;
            }
        }

        // if shard.replicas[0] != new_replicas[0] {
        //     n += 1;
        // }
    }

    n
}

#[test]
fn t() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let nodes = [1, 2, 3];
    let mut ring = Keyspace::new(nodes);

    for id in 4..=100 {
        let snapshot = ring.clone();

        ring.add_node(4);
        ring.assert_variance();
        ring.assert_data_movement(&snapshot);
    }

    // for id in 4..=100 {
    //     let snapshot = ring.clone();

    //     ring.remove_node(id);
    //     ring.assert_variance();
    //     ring.assert_data_movement(&snapshot);
    // }

    dbg!(&ring.nodes);

    let mut shards_per_node = HashMap::<usize, usize>::new();

    for shard in &ring.shards {
        for node_idx in shard.replicas() {
            *shards_per_node.entry(*node_idx).or_default() += 1;
        }
    }

    dbg!(&shards_per_node);

    let mut hits_per_node = HashMap::<usize, usize>::new();

    for _ in 0..1_000_000 {
        let idx = rand::random::<u16>();
        let shard = ring.get_shard(idx as usize);
        for node_idx in shard.replicas() {
            *hits_per_node.entry(*node_idx).or_default() += 1;
        }
    }

    dbg!(&hits_per_node);

    println!(
        "hits min: {}, max: {}",
        hits_per_node.values().min().unwrap(),
        hits_per_node.values().max().unwrap(),
    );

    panic!();
}
