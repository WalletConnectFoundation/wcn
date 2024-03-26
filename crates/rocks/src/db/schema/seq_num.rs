use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

/// Monotonically increasing sequence numbers generator.
#[derive(Clone)]
pub(crate) struct SeqNumGenerator {
    next: Arc<AtomicU16>,
}

impl SeqNumGenerator {
    pub fn new() -> Self {
        Self {
            next: Arc::new(AtomicU16::new(0)),
        }
    }

    /// Returns a unique monotonically increasing time-based number.
    ///
    /// Please note that monotonicity here is only referring to the fact that
    /// function creates values in an increasing order. However, since only
    /// generator's `cnt` is atomically incremented, it is possible that a
    /// thread A, which called `next` before thread B, will get a number that is
    /// greater than the one returned to thread B.
    ///
    /// Since only 42 bits are used for the timestamp, this function can shift
    /// the timestamp w/o any data loss, and then apply a mask to inject a
    /// counter bits into the id.
    ///
    /// The `cnt` is 16 bits, so it can accommodate 65536 unique ids within a
    /// millisecond (65M+ per second) before wrapping around.
    ///
    /// The original idea comes from [Sharding & IDs at Instagram](https://instagram-engineering.com/sharding-ids-at-instagram-1cf5a71e5a5c)
    pub fn next(&self) -> u64 {
        let cnt = self.next.fetch_add(1, Ordering::Relaxed) as u64;
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        (timestamp << 16) | cnt
    }
}
