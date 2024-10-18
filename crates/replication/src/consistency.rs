use {derive_more::derive::Deref, smallvec::SmallVec, storage_api::Multiaddr};

/// Replication consistency mechanism guaranteeing majority of replicas
/// (quorum threshold) return the same result.
pub struct MajorityQuorum<T> {
    threshold: usize,
    results: ReplicationResults<T>,
    reached_idx: Option<usize>,
}

impl<'a, T: Clone + Eq> MajorityQuorum<T> {
    /// Creates a new [`Quorum`] with the provided quorum `threshold`.
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            results: SmallVec::new(),
            reached_idx: None,
        }
    }

    /// Pushes the provided replication result.
    pub fn push(&mut self, replica_addr: Multiaddr, result: storage_api::client::Result<T>) {
        if let Some(res) = self.is_reached() {
            return self.results.push(ReplicationResult {
                replica_addr,
                within_quorum: res == &result,
                inner: result,
            });
        }

        let mut count = 1;
        for res in &mut self.results {
            res.within_quorum = res.inner == result;
            if res.within_quorum {
                count += 1;
            }
        }

        self.results.push(ReplicationResult {
            replica_addr,
            within_quorum: true,
            inner: result,
        });

        if count >= self.threshold {
            self.reached_idx = Some(self.results.len() - 1);
        }
    }

    pub fn is_reached(&self) -> Option<&storage_api::client::Result<T>> {
        self.reached_idx.map(|idx| &self.results[idx].inner)
    }

    pub fn minority_replicas(&self) -> impl Iterator<Item = &Multiaddr> + '_ {
        let quorum_reached = self.reached_idx.is_some();

        self.results.iter().filter_map(move |res| {
            (!res.within_quorum || !quorum_reached).then_some(&res.replica_addr)
        })
    }

    pub fn into_results(self) -> ReplicationResults<T> {
        self.results
    }
}

pub type ReplicationResults<T> = SmallVec<[ReplicationResult<T>; 3]>;

#[derive(Deref)]
pub(super) struct ReplicationResult<T> {
    #[deref]
    pub inner: storage_api::client::Result<T>,
    pub replica_addr: Multiaddr,
    within_quorum: bool,
}
