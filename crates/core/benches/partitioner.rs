use {
    alloc_counter::{count_alloc, AllocCounterSystem},
    criterion::{black_box, criterion_group, criterion_main, Criterion},
    irn_core::cluster::keyspace::partitioner::{Partitioner, Xxh3Partitioner},
    std::time::{Duration, Instant},
};

#[global_allocator]
static A: AllocCounterSystem = AllocCounterSystem;

fn bench_key_position(c: &mut Criterion) {
    let partitioner = Xxh3Partitioner::new();
    let mut total_iters = 0;
    let mut total_allocs = 0;

    c.bench_function("key_position", |b| {
        b.iter_custom(|iters| {
            total_iters += iters;
            let mut elapsed = Duration::new(0, 0);
            let (allocs, _) = count_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    partitioner.key_position(black_box(&random_key(u64::MAX)));
                }
                elapsed = start.elapsed();
            });
            total_allocs += allocs.0;
            elapsed
        });
    });
    if total_iters > 0 {
        eprintln!(
            "key_position: iterations={}, allocations={}; allocations per iter={:.2}\n",
            total_iters,
            total_allocs,
            total_allocs as f64 / total_iters as f64
        );
    }
}

fn bench_key_position_seeded(c: &mut Criterion) {
    let partitioner = Xxh3Partitioner::new();
    let mut total_iters = 0;
    let mut total_allocs = 0;
    const SEED: u64 = 12345;

    c.bench_function("key_position_seeded", |b| {
        b.iter_custom(|iters| {
            total_iters += iters;
            let mut elapsed = Duration::new(0, 0);
            let (allocs, _) = count_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    partitioner
                        .key_position_seeded(black_box(&random_key(u64::MAX)), black_box(SEED));
                }
                elapsed = start.elapsed();
            });
            total_allocs += allocs.0;
            elapsed
        });
    });
    if total_iters > 0 {
        eprintln!(
            "key_position_seeded: iterations={}, allocations={}; allocations per iter={:.2}\n",
            total_iters,
            total_allocs,
            total_allocs as f64 / total_iters as f64
        );
    }
}

/// Returns a random key in the range `[0, n)`.
fn random_key(n: u64) -> u64 {
    use std::{cell::Cell, num::Wrapping};

    thread_local! {
        static RNG: Cell<Wrapping<u64>> = const { Cell::new(Wrapping(1234567890123456789u64)) };
    }

    RNG.with(|rng| {
        // This is the 64-bit variant of the Xorshift+ algorithm.
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        rng.set(x);

        // This is a fast alternative to `x % n`.
        // See: [A fast alternative to the modulo reduction](https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/)
        ((x.0 as u128).wrapping_mul(n as u128) >> 64) as u64
    })
}

criterion_group!(benches, bench_key_position, bench_key_position_seeded);
criterion_main!(benches);
