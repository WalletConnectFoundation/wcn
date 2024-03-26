use {
    irn::{cluster::replication::Strategy, ShutdownReason},
    irn_bin::test_cluster::Cluster,
    std::time::Duration,
    test_log::test,
};

async fn add_node(is_member: bool) -> Cluster {
    let mut cluster = Cluster::setup(3, Strategy::default()).await.unwrap();

    cluster.add_node(None, is_member).await.unwrap();

    // Wait for the cluster view to be updated
    tokio::time::sleep(Duration::from_secs(1)).await;

    for peer in &cluster.peers {
        let count = peer.node().consensus().cluster_view().nodes().len();
        assert_eq!(count, 4);
    }

    cluster
}

async fn remove_node(mut cluster: Cluster) {
    let node = cluster.peers.pop().unwrap();
    node.shutdown(ShutdownReason::Decommission).await;

    // Wait for the cluster view to be updated
    tokio::time::sleep(Duration::from_secs(1)).await;

    for peer in &cluster.peers {
        let count = peer.node().consensus().cluster_view().nodes().len();
        assert_eq!(count, 3);
    }
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn add_learner_node() {
    add_node(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn add_member_node() {
    add_node(true).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn remove_learner_node() {
    let cluster = add_node(false).await;
    remove_node(cluster).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn remove_member_node() {
    let cluster = add_node(true).await;
    remove_node(cluster).await;
}
