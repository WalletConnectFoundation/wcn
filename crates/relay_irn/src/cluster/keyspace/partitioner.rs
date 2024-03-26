use {
    super::KeyPosition,
    std::{
        fmt::Debug,
        hash::{BuildHasher, Hash},
    },
    xxhash_rust::xxh3::Xxh3Builder,
};

/// Seeds for double hashing. Essentially, we can use any seeds, to initialize
/// the hasher (by default XXH3 uses `0`).
pub const DEFAULT_SEED1: u64 = 12345;
pub const DEFAULT_SEED2: u64 = 67890;

pub type DefaultPartitioner = Xxh3Partitioner;

pub static DEFAULT_PARTITIONER: DefaultPartitioner = DefaultPartitioner::new();

/// Defines a key that can be used to partition data.
pub trait PartitionerKey: Hash {}

impl<T: Hash> PartitionerKey for T {}

/// Defines strategies for partitioning data across a cluster.
///
/// Partitioner is responsible for mapping keys to positions on the ring.
pub trait Partitioner {
    /// Returns ring position for a given key (using default seed).
    fn key_position<K: PartitionerKey>(&self, key: &K) -> KeyPosition;

    /// Returns ring position for a given key (using a given seed).
    ///
    /// In order to allow different sets of positions for a given key, seed
    /// value can be specified. This is particularly useful for multi-probing
    /// (where two pre-calculated hashes are used).
    fn key_position_seeded<K: PartitionerKey>(&self, key: &K, seed: u64) -> KeyPosition;
}

/// A partitioner that uses a XXH3 hash function to partition data. Since the
/// hash function produces uniformly distributed output, this partitioner is
/// capable (given enough points on the ring) of distributing data evenly across
/// the cluster.
#[derive(Clone)]
pub struct Xxh3Partitioner {
    hash_builder: Xxh3Builder,
}

impl Default for Xxh3Partitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for Xxh3Partitioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Xxh3Partitioner").finish()
    }
}

impl Xxh3Partitioner {
    pub const fn new() -> Self {
        Self {
            hash_builder: Xxh3Builder::new(),
        }
    }

    pub fn hash<K: PartitionerKey>(&self, key: &K, seed: u64) -> KeyPosition {
        self.hash_builder.with_seed(seed).hash_one(key)
    }
}

impl Partitioner for Xxh3Partitioner {
    fn key_position<K: PartitionerKey>(&self, key: &K) -> KeyPosition {
        self.hash(key, DEFAULT_SEED1)
    }

    fn key_position_seeded<K: PartitionerKey>(&self, key: &K, seed: u64) -> KeyPosition {
        self.hash(key, seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hasher_sanity_check() {
        // Make sure the hasher algorithm doesn't change between releases.
        let partitioner = Xxh3Partitioner::new();
        assert_eq!(partitioner.key_position(&0u64), 0x1424aa9885a1d5c7);
        assert_eq!(partitioner.key_position(&1u64), 0xcfb732e08be9ec0);
        assert_eq!(partitioner.key_position(&123456u64), 0x4879daae426e14fb);
    }
}
