use {
    core::panic,
    itertools::Itertools,
    std::{
        collections::{BTreeSet, HashMap},
        default,
        hash::SipHasher,
    },
    xxhash_rust::{
        const_xxh3::{xxh3_128, xxh3_64},
        xxh3::xxh3_64_with_seed,
    },
};

#[derive(Clone)]
struct Keyspace<const RF: usize, N> {
    nodes: Vec<Option<N>>,
    hashring: HashRing<RF>,

    resharding: HashMap<u16, Shard<RF>>,
}

type HashRing<const RF: usize> = Vec<Shard<RF>>;

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

impl<const RF: usize, N> Keyspace<RF, N> {
    pub fn new(nodes: [N; RF]) -> Self {
        // TODO: compile error
        assert!(RF > 0);

        let replicas = [0; RF];

        let mut nodes = nodes.into_iter();
        let first_node = nodes.next().unwrap();

        let n_shards = ShardId::MAX.0 as usize + 1;

        let mut this = Self {
            nodes: [Some(first_node)].into_iter().collect(),
            hashring: (0..n_shards).map(|_| Shard { replicas }).collect(),
            resharding: HashMap::new(),
        };

        for node in nodes {
            this.add_node(node);
        }

        this
    }

    pub fn get_shard(&self, id: ShardId) -> Shard<RF> {
        self.hashring[id.0 as usize]
    }

    pub fn add_node(&mut self, new_node: N) {
        tracing::info!("add_node");

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

        let mut ctx = self.resharding();

        let mut idx = new_node_idx;
        let mut i = 0;
        loop {
            if i == self.hashring.len() {
                break;
            }

            ctx.try_reassign_primary_replica(&mut self.hashring[idx].replicas[0], new_node_idx);

            idx = wrap_around(idx, self.nodes.len(), self.hashring.len() - 1);
            i += 1;
        }
        dbg!(&ctx);

        fn wrap_around(a: usize, b: usize, max: usize) -> usize {
            if a + b < max {
                return a + b;
            }

            if max % b == 0 {
                a + b - max + 1
            } else {
                a + b - max
            }
        }

        self.reassign_secondary_replicas();
        // dbg!(&self.hashring);
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

        for shard in &mut self.hashring {
            let primary_replica_idx = &mut shard.replicas[0];

            if *primary_replica_idx != node_idx_to_remove {
                continue;
            }

            loop {
                if !resharding.try_reassign_primary_replica(primary_replica_idx, nodes_cursor) {
                    nodes_cursor += 1;
                    continue;
                }

                break;
            }
        }

        self.nodes[node_idx_to_remove] = None;

        self.reassign_secondary_replicas();
    }

    fn reassign_secondary_replicas(&mut self) {
        if self.nodes.len() < RF {
            return;
        }

        for shard_idx in 0..self.hashring.len() {
            let replicas = self.hashring[shard_idx].replicas;

            let mut secondary_replica_idx = 1;
            // let mut shards_cursor = shard_idx;

            // loop {
            //     if shards_cursor == self.hashring.len() - 1 {
            //         shards_cursor = 0;
            //     } else {
            //         shards_cursor += 1;
            //     };

            //     debug_assert!(shards_cursor != shard_idx);

            //     if secondary_replica_idx == RF {
            //         break;
            //     }

            //     let replica_idx = self.hashring[shards_cursor].replicas[0];

            //     if replicas[0..secondary_replica_idx].contains(&replica_idx) {
            //         continue;
            //     }

            //     self.hashring[shard_idx].replicas[secondary_replica_idx] = replica_idx;
            //     secondary_replica_idx += 1;
            // }

            let mut idx = shard_idx;

            // tracing::info!(idx);

            loop {
                if secondary_replica_idx == RF {
                    break;
                }

                idx = xxh3_64(&idx.to_be_bytes()) as usize;

                // idx = rand::random();

                let replica_idx = self.hashring[idx as u16 as usize].replicas[0];
                // tracing::info!(idx, replica_idx, ?replicas);

                if replicas[0..secondary_replica_idx].contains(&replica_idx) {
                    continue;
                }

                self.hashring[shard_idx].replicas[secondary_replica_idx] = replica_idx;
                secondary_replica_idx += 1;
            }
        }
    }

    fn resharding(&self) -> ReshardingContext {
        let nodes_count = self.nodes.iter().filter_map(Option::as_ref).count();

        let primary_shards_per_node = self.hashring.len() / nodes_count;
        let mut shards_remainder = self.hashring.len() % nodes_count;

        let mut primary_replicas_counts = vec![0; self.nodes.len()];
        for shard in &self.hashring {
            primary_replicas_counts[shard.replicas[0]] += 1;
        }

        let mut nodes = self.nodes.iter().enumerate();

        ReshardingContext {
            nodes_count,
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
        let mut primary_shards_per_node = HashMap::<usize, usize>::new();

        for shard in &self.hashring {
            for (i, node_idx) in shard.replicas().iter().enumerate() {
                *shards_per_node.entry(*node_idx).or_default() += 1;
                if i == 0 {
                    *primary_shards_per_node.entry(*node_idx).or_default() += 1;
                }
            }
        }

        let min = shards_per_node.values().min().unwrap();
        let max = shards_per_node.values().max().unwrap();
        dbg!(min, max);

        let min = primary_shards_per_node.values().min().unwrap();
        let max = primary_shards_per_node.values().max().unwrap();
        assert!(max - min <= 1, "{primary_shards_per_node:?}");

        // assert!(
        //     max - min <= RF,
        //     "{shards_per_node:?} {primary_shards_per_node:?}"
        // );
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

#[derive(Debug)]
struct ReshardingContext {
    nodes_count: usize,

    /// Desired amount of primary shards per node after resharding.
    primary_shards_per_node: usize,

    /// Most of the times the amount of shards we have can't be dived by the
    /// amount of nodes without a remainder, so some nodes are going to have +1
    /// shard.
    ///
    /// This parameter indicates the first position in [`Keyspace::nodes`] that
    /// doesn't have this "surplus".
    first_node_idx_without_surplus: usize,

    /// [`Vec`] in which each element is the amount of primary [`Shard`]s the
    /// node with the respective index is assigned to.
    primary_shards_counts: Vec<usize>,
}

impl ReshardingContext {
    fn desired_primary_shards_count(&self, node_idx: usize) -> usize {
        if node_idx < self.first_node_idx_without_surplus {
            self.primary_shards_per_node + 1
        } else {
            self.primary_shards_per_node
        }
    }

    fn try_reassign_primary_replica(&mut self, src_idx: &mut usize, dst_idx: usize) -> bool {
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

fn shards_to_migrate<const RF: usize, N>(a: &Keyspace<RF, N>, b: &Keyspace<RF, N>) -> usize {
    let mut n = 0;

    for (shard_idx, shard) in a.hashring.iter().enumerate() {
        let new_replicas = b.hashring[shard_idx].replicas;

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

        ring.add_node(id);
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

    for shard in &ring.hashring {
        for node_idx in shard.replicas() {
            *shards_per_node.entry(*node_idx).or_default() += 1;
        }
    }

    dbg!(&shards_per_node);

    let mut hits_per_node = HashMap::<usize, usize>::new();

    for _ in 0..1_000_000 {
        let idx = rand::random::<u16>();
        let shard = ring.get_shard(ShardId(idx));
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

#[test]
fn randevou() {
    let mut shards_per_node: HashMap<usize, usize> = Default::default();

    let nodes: Vec<_> = (0..10).map(|_| rand::random::<usize>()).collect();

    for shard_idx in 0..=64 * 1024usize {
        let mut replicas = [(0, 0); 3];

        for replica_idx in nodes.iter().copied() {
            let new = (
                xxh3_64(&(shard_idx * 100000000 + replica_idx).to_be_bytes()),
                replica_idx,
            );

            if new > replicas[0] {
                replicas[0] = new;
            }
            replicas.sort_unstable();
        }

        for (_, n) in replicas.iter().take(3) {
            *shards_per_node.entry(*n).or_default() += 1;
        }
    }

    dbg!(&shards_per_node);

    println!(
        "shards min: {}, max: {}",
        shards_per_node.values().min().unwrap(),
        shards_per_node.values().max().unwrap(),
    );

    panic!();
}

fn maglev(N: u64, M: u64) -> Vec<isize> {
    let mut next: Vec<_> = (0..N).map(|_| 0u64).collect();
    let mut entry: Vec<_> = (0..M).map(|_| -1isize).collect();

    let mut n = 0;
    loop {
        for i in 0..N as usize {
            let offset = xxh3_64_with_seed(&i.to_be_bytes(), 0) % M;
            let skip = xxh3_64_with_seed(&i.to_be_bytes(), 1) % (M - 1) + 1;

            let mut c = (offset + next[i] * skip) % M;
            loop {
                if entry[c as usize] < 0 {
                    break;
                }

                next[i] += 1;
                c = (offset + next[i] * skip) % M;
            }
            entry[c as usize] = i as isize;
            next[i] += 1;
            n += 1;
            if n == M {
                return entry;
            }
        }
    }
}

#[test]
fn maglev_test() {
    const M: u64 = 65537;

    let shards_a = maglev(1024, M);

    let mut shards_per_node: HashMap<usize, usize> = Default::default();
    for n in &shards_a {
        *shards_per_node.entry((*n) as usize).or_default() += 1;
    }

    println!(
        "shards min: {}, max: {}",
        shards_per_node.values().min().unwrap(),
        shards_per_node.values().max().unwrap(),
    );

    let shards_b = maglev(1025, M);

    let mut shards_per_node: HashMap<usize, usize> = Default::default();
    for n in &shards_b {
        *shards_per_node.entry((*n) as usize).or_default() += 1;
    }

    println!(
        "shards min: {}, max: {}",
        shards_per_node.values().min().unwrap(),
        shards_per_node.values().max().unwrap(),
    );

    let mut diff = 0;

    for i in 0..shards_a.len() {
        if shards_a[i] != shards_b[i] {
            diff += 1;
        }
    }

    dbg!(diff);

    panic!();
}
