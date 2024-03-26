//! Replicated persistence tests.

use {
    irn::{
        cluster::replication::ConsistencyLevel,
        replication::shared_tests::{ReplicationTestCase, ReplicationTestSuite},
    },
    irn_bin::test_cluster,
};

mod basic;

#[rstest::rstest]
#[case(5, 3, ConsistencyLevel::One, false)]
#[case(5, 3, ConsistencyLevel::One, true)]
#[case(5, 3, ConsistencyLevel::Quorum, false)]
#[case(5, 3, ConsistencyLevel::Quorum, true)]
#[case(5, 3, ConsistencyLevel::All, false)]
#[case(5, 3, ConsistencyLevel::All, true)]
#[case(3, 3, ConsistencyLevel::One, true)]
#[case(3, 3, ConsistencyLevel::Quorum, true)]
#[case(3, 3, ConsistencyLevel::All, true)]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn string_basic_replicated_ops(
    #[case] nodes_count: usize,
    #[case] replication_factor: usize,
    #[case] consistency_level: ConsistencyLevel,
    #[case] coordinator_in_replica_set: bool,
) {
    let test_case = ReplicationTestCase::new(
        nodes_count,
        replication_factor,
        consistency_level,
        coordinator_in_replica_set,
    );
    ReplicationTestSuite::<test_cluster::Cluster, _, _>::string_basic_ops(test_case).await
}

#[rstest::rstest]
#[case(5, 3, ConsistencyLevel::Quorum, false)]
#[case(5, 3, ConsistencyLevel::Quorum, true)]
#[case(4, 3, ConsistencyLevel::Quorum, false)]
#[case(4, 3, ConsistencyLevel::Quorum, true)]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn string_read_repair(
    #[case] nodes_count: usize,
    #[case] replication_factor: usize,
    #[case] consistency_level: ConsistencyLevel,
    #[case] coordinator_in_replica_set: bool,
) {
    let test_case = ReplicationTestCase::new(
        nodes_count,
        replication_factor,
        consistency_level,
        coordinator_in_replica_set,
    );
    ReplicationTestSuite::<test_cluster::Cluster, _, _>::string_read_repair(test_case).await
}
