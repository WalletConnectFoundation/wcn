use std::{
    collections::{BTreeSet, HashMap},
    default,
    hash::SipHasher,
};

#[derive(Clone)]
struct Keyspace<const N: usize> {
    nodes: Vec<Option<Node>>,
    shards: Vec<Shard<N>>,
}

impl<const N: usize> Drop for Keyspace<N> {
    fn drop(&mut self) {
        let mut shards_per_node = HashMap::<usize, usize>::new();

        for shard in &self.shards {
            for node_idx in shard.replicas().iter().take(1) {
                *shards_per_node.entry(*node_idx).or_default() += 1;
            }
        }

        dbg!(&shards_per_node);
    }
}

#[derive(Clone, Copy, Debug)]
struct Shard<const N: usize> {
    replicas: [usize; N],
}

impl<const N: usize> Shard<N> {
    fn replicas(&self) -> &[usize; N] {
        &self.replicas
    }

    fn contains_replica(&self, node_idx: usize) -> bool {
        self.replicas.iter().any(|idx| *idx == node_idx)
    }
}

#[derive(Clone, Debug)]
struct Node {
    id: u64,
    shards_count: usize,
}

impl Node {
    fn new(id: u64) -> Self {
        Self {
            id,
            shards_count: 0,
        }
    }
}

impl<const N: usize> Keyspace<N> {
    pub fn new(nodes: [Node; N], n_shards: usize) -> Self {
        let replicas = [0; N];

        let first_node = Node {
            id: nodes[0].id,
            shards_count: n_shards,
        };

        let mut this = Self {
            shards: (0..n_shards).map(|_| Shard { replicas }).collect(),
            nodes: [Some(first_node)].into_iter().collect(),
        };

        for node in nodes.into_iter().skip(1) {
            this.add_node(node);
        }

        this
    }

    pub fn add_node(&mut self, mut new_node: Node) {
        let new_node_idx = self
            .nodes
            .iter()
            .position(|n| n.is_none())
            .unwrap_or_else(|| {
                self.nodes.push(None);
                self.nodes.len() - 1
            });

        let nodes_count = self.nodes.iter().filter_map(Option::as_ref).count();

        let shards_per_node = self.shards.len() / (nodes_count + 1);
        let mut shards_remainder = self.shards.len() % (nodes_count + 1);

        let first_node_idx_without_surplus = self
            .nodes
            .iter()
            .enumerate()
            .find_map(|(idx, node)| {
                if shards_remainder == 0 {
                    return Some(idx);
                }

                if node.is_some() {
                    shards_remainder -= 1;
                }

                None
            })
            .unwrap_or(0);

        for shard in &mut self.shards {
            let primary_replica_idx = &mut shard.replicas[0];
            let primary_replica = self.nodes[*primary_replica_idx].as_mut().unwrap();

            let desired_shards_count = if *primary_replica_idx < first_node_idx_without_surplus {
                shards_per_node + 1
            } else {
                shards_per_node
            };

            if primary_replica.shards_count <= desired_shards_count {
                continue;
            }

            *primary_replica_idx = new_node_idx;
            primary_replica.shards_count -= 1;
            new_node.shards_count += 1;
        }

        self.nodes[new_node_idx] = Some(new_node);
        self.recalculate_secondary_replicas();

        // dbg!(&self.nodes);
        // dbg!(&self.shards);
    }

    fn recalculate_secondary_replicas(&mut self) {
        for shard in &mut self.shards {
            let mut secondary_replica_idx = 1;
            let mut nodes_cursor = shard.replicas[0];

            loop {
                if secondary_replica_idx == N {
                    break;
                }

                if nodes_cursor == 0 {
                    nodes_cursor = self.nodes.len() - 1;
                } else {
                    nodes_cursor -= 1;
                }
                // dbg!(nodes_cursor);

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

    pub fn remove_node(&mut self, id: u64) {
        let Some(node_idx_to_remove) = self
            .nodes
            .iter()
            .position(|n| n.as_ref().map(|n| n.id) == Some(id))
        else {
            return;
        };

        self.nodes[node_idx_to_remove] = None;

        let nodes_count = self.nodes.iter().filter_map(Option::as_ref).count();

        let shards_per_node = self.shards.len() / nodes_count;
        let mut shards_remainder = self.shards.len() % nodes_count;

        let first_node_idx_without_surplus = self
            .nodes
            .iter()
            .enumerate()
            .find_map(|(idx, node)| {
                if shards_remainder == 0 {
                    return Some(idx);
                }

                if node.is_some() {
                    shards_remainder -= 1;
                }

                None
            })
            .unwrap_or(0);

        let mut nodes_cursor = 0;

        for shard in &mut self.shards {
            let primary_replica_idx = &mut shard.replicas[0];

            if *primary_replica_idx != node_idx_to_remove {
                continue;
            }

            loop {
                let Some(new_owner) = self.nodes[nodes_cursor].as_mut() else {
                    nodes_cursor += 1;
                    continue;
                };

                let desired_shards_count = if nodes_cursor < first_node_idx_without_surplus {
                    shards_per_node + 1
                } else {
                    shards_per_node
                };

                if new_owner.shards_count >= desired_shards_count {
                    nodes_cursor += 1;
                    continue;
                }

                *primary_replica_idx = nodes_cursor;
                new_owner.shards_count += 1;
                break;
            }
        }

        self.recalculate_secondary_replicas()
    }

    pub fn get_shard(&self, shard_idx: usize) -> Shard<N> {
        *self.shards.get(shard_idx).unwrap()
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

        assert!(max - min <= N, "{shards_per_node:?}");
    }

    fn assert_data_movement(&self, old: &Self) {
        let to_move = shards_to_migrate(old, self);
        dbg!(to_move);
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

    let nodes = [Node::new(1), Node::new(2), Node::new(3)];
    let mut ring = Keyspace::new(nodes, u16::MAX as usize + 1);

    for id in 4..=100 {
        let snapshot = ring.clone();

        ring.add_node(Node::new(id));
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

// struct NodeRemoval<'a, const N: usize> {
//     node_idx: usize,
//     shards_per_node: usize,
//     first_node_idx_without_surplus: usize,

//     keyspace: &'a mut HashRing<N>,
// }

// impl<'a, const N: usize> NodeRemoval<'a, N> {
//     fn remove_node_from_shards(&mut self, current_shard_idx: usize) -> bool {
//         // tracing::info!(current_shard_idx);

//         if current_shard_idx >= self.keyspace.shards.len() {
//             return true;
//         }

//         let replicas = self.keyspace.shards[current_shard_idx].replicas;

//         for (replica_idx, node_idx) in replicas.into_iter().enumerate() {
//             if self.node_idx != node_idx {
//                 continue;
//             }

//             for new_owner_idx in 0..self.keyspace.nodes.len() {
//                 let Some(ref mut new_owner) = &mut
// self.keyspace.nodes[new_owner_idx] else {                     continue;
//                 };

//                 if replicas.iter().any(|idx| *idx == new_owner_idx) {
//                     continue;
//                 };

//                 let desired_shards_count = if new_owner_idx <
// self.first_node_idx_without_surplus {
// self.shards_per_node + 1                 } else {
//                     self.shards_per_node
//                 };

//                 if new_owner.shards_count >= desired_shards_count {
//                     continue;
//                 }

//                 self.keyspace.shards[current_shard_idx].replicas[replica_idx]
// = new_owner_idx;                 new_owner.shards_count += 1;
//                 let _ = new_owner;

//                 if self.remove_node_from_shards(current_shard_idx + 1) {
//                     return true;
//                 }

//                 self.keyspace.shards[current_shard_idx].replicas[replica_idx]
// = node_idx;                 self.keyspace.nodes[new_owner_idx]
//                     .as_mut()
//                     .unwrap()
//                     .shards_count -= 1;
//             }

//             return false;
//         }

//         self.remove_node_from_shards(current_shard_idx + 1)
//     }
// }

// pub fn remove_node_sorted(&mut self, id: u64) {
//     let Some(node_idx_to_remove) = self
//         .nodes
//         .iter()
//         .position(|n| n.as_ref().map(|n| n.id) == Some(id))
//     else {
//         return;
//     };

//     self.nodes[node_idx_to_remove] = None;

//     let nodes_count = self.nodes.iter().filter_map(Option::as_ref).count();

//     let shards_per_node = self.shards.len() * N / nodes_count;
//     dbg!(shards_per_node);
//     let mut shards_remainder = self.shards.len() * N % nodes_count;
//     dbg!(shards_remainder);

//     let mut node_balances: Vec<_> = self
//         .nodes
//         .iter()
//         .enumerate()
//         .filter_map(|(idx, node)| {
//             let node = node.as_ref()?;

//             let desired_shards_count = if shards_remainder == 0 {
//                 shards_per_node
//             } else {
//                 shards_remainder -= 1;
//                 shards_per_node + 1
//             };

//             Some((desired_shards_count - node.shards_count, idx))
//         })
//         .collect();
//     node_balances.sort_unstable();

//     for shard in &mut self.shards {
//         let replicas = shard.replicas;

//         'replicas: for node_idx in &mut shard.replicas {
//             if *node_idx != node_idx_to_remove {
//                 continue;
//             }

//             let mut offset = 0;

//             loop {
//                 let balances_idx = node_balances.len() - 1 - offset;

//                 let (new_owner_balance, new_owner_idx) = &mut
// node_balances[balances_idx];

//                 if replicas.iter().any(|idx| idx == new_owner_idx) {
//                     offset += 1;
//                     continue;
//                 }

//                 // if *new_owner_balance == 0 {
//                 //     dbg!(&self.nodes);
//                 //     dbg!(node_idx_to_remove);
//                 //     dbg!(*node_idx);
//                 //     dbg!(&new_owner_idx);
//                 //     dbg!(replicas);
//                 // }

//                 *node_idx = *new_owner_idx;
//                 *new_owner_balance -= 1;
//                 if *new_owner_balance == 0 {
//                     drop((new_owner_idx, new_owner_balance));
//                     node_balances.remove(balances_idx);
//                 }
//                 node_balances.sort_unstable();

//                 break 'replicas;
//             }
//         }
//     }
// }
