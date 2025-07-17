use {
    crate::OperationResults,
    cluster::keyspace::REPLICATION_FACTOR,
    derive_more::derive::Deref,
    smallvec::SmallVec,
    storage_api::{operation, PeerAddr},
};

/// Replication consistency mechanism guaranteeing majority of replicas
/// (quorum threshold) return the same result.
pub enum MajorityQuorum<'a> {
    Pending { results: &'a mut OperationResults },
    Reached { idx: usize },
}

impl MajorityQuorum {
    /// Creates a new [`Quorum`] with the provided quorum `threshold`.
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            results: SmallVec::new(),
            reached_idx: None,
        }
    }

    /// Pushes the provided replication result.
    pub fn push(
        &mut self,
        replica_addr: PeerAddr,
        result: storage_api::Result<operation::Output<'static>>,
    ) {
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

    /// Indicates whether the [`MajorityQuorum`] is reached by returning the
    /// replication [`Result`] the quorum agreed upon.
    pub fn is_reached(&self) -> Option<&storage_api::Result<operation::Output>> {
        self.reached_idx.map(|idx| &self.results[idx].inner)
    }

    /// Returns an [`Iterator`] of replicas replication results of which do not
    /// match with the [`MajorityQuorum`].
    pub fn minority_replicas(&self) -> impl Iterator<Item = &PeerAddr> + '_ {
        let quorum_reached = self.reached_idx.is_some();

        self.results.iter().filter_map(move |res| {
            (!res.within_quorum || !quorum_reached).then_some(&res.replica_addr)
        })
    }

    /// Returns threshould of this [`MajorityQuorum`].
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Converts [`MajorityQuorum`] into the underlying [`ReplicationResults`].
    pub fn into_results(self) -> ReplicationResults {
        self.results
    }
}

pub type ReplicationResults = SmallVec<[ReplicationResult; REPLICATION_FACTOR as usize]>;

#[derive(Deref)]
pub(super) struct ReplicationResult {
    #[deref]
    pub inner: storage_api::Result<operation::Output<'static>>,
    pub replica_addr: PeerAddr,
    within_quorum: bool,
}

#[cfg(test)]
mod test {
    use {
        super::*,
        std::time::Duration,
        storage_api::{PeerId, Record, RecordExpiration, RecordVersion},
    };

    impl ReplicationResult {
        pub(crate) fn new_test(inner: storage_api::Result<operation::Output<'static>>) -> Self {
            Self {
                inner,
                replica_addr: "12D3KooWDJrGKPuU1vJLBZv2UXfcZvdBprUgAkjvkUET7q2PzwPp-/ip4/10.0.0.1/\
                               udp/3010/quic-v1"
                    .parse()
                    .unwrap(),
                within_quorum: true,
            }
        }
    }

    #[test]
    fn test_majority_quorum() {
        let addr1 = PeerAddr::new(
            PeerId::random(),
            "/ip4/10.0.0.1/udp/3010/quic-v1".parse().unwrap(),
        );
        let addr2 = PeerAddr::new(
            PeerId::random(),
            "/ip4/10.0.0.2/udp/3010/quic-v1".parse().unwrap(),
        );
        let addr3 = PeerAddr::new(
            PeerId::random(),
            "/ip4/10.0.0.3/udp/3010/quic-v1".parse().unwrap(),
        );

        let record1 = Record::<'static> {
            value: b"record1".into(),
            expiration: Duration::from_secs(30).into(),
            version: RecordVersion::now(),
        };

        let record2 = Record::<'static> {
            value: b"record2".into(),
            expiration: Duration::from_secs(30).into(),
            version: RecordVersion::now(),
        };

        let mut quorum = MajorityQuorum::new(2);
        quorum.push(addr1.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr2.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), Some(&Ok(42)));
        quorum.push(addr3.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), Some(&Ok(42)));
        assert!(quorum.minority_replicas().count() == 0);

        let mut quorum = MajorityQuorum::<u8>::new(2);
        quorum.push(addr1.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr2.clone(), Ok(0));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr3.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), Some(&Ok(42)));
        assert_eq!(quorum.minority_replicas().collect::<Vec<_>>(), vec![&addr2]);

        let mut quorum = MajorityQuorum::<u8>::new(2);
        quorum.push(addr1.clone(), Ok(42));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr2.clone(), Ok(0));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr3.clone(), Ok(1));
        assert_eq!(quorum.is_reached(), None);
        assert!(quorum.minority_replicas().count() == 3);

        let mut quorum = MajorityQuorum::<u8>::new(2);
        quorum.push(addr1, Err(storage_api::client::Error::Timeout));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr2, Ok(0));
        assert_eq!(quorum.is_reached(), None);
        quorum.push(addr3, Err(storage_api::client::Error::Timeout));
        assert_eq!(
            quorum.is_reached(),
            Some(&Err(storage_api::client::Error::Timeout))
        );
    }
}
