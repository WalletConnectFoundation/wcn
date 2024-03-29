use {lib::test_cluster::Cluster, test_log::test};

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn full_node_rotation() {
    irn::test::full_node_rotation::<Cluster, _, _>().await;
}
