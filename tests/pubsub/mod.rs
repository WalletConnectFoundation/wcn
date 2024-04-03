use {
    futures::{
        stream::{self, FuturesUnordered},
        FutureExt,
        StreamExt,
    },
    irn::cluster::replication::{ConsistencyLevel, Strategy},
    lib::test_cluster::Cluster,
    std::time::Duration,
    test_log::test,
};

#[allow(dead_code)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn pubsub_as_mailbox_gossip() {
    let num_nodes = 6;
    let cluster = Cluster::setup(num_nodes, Strategy::new(3, ConsistencyLevel::Quorum))
        .await
        .unwrap();
    let cluster = &cluster;
    let channel1 = b"channel1";
    let channel2 = b"channel2";
    let payload1 = b"payload1";
    let payload2 = b"payload2";

    // Subscribe 2 groups of clients to different channels.
    let mut subscriptions1 = stream::iter(0..num_nodes)
        .then(|i| async move {
            cluster
                .irn_api_client(i)
                .subscribe([channel1.to_vec()].into_iter().collect())
                .await
                .boxed()
        })
        .collect::<Vec<_>>()
        .await;
    let mut subscriptions2 = stream::iter(0..num_nodes)
        .then(|i| async move {
            cluster
                .irn_api_client(i)
                .subscribe([channel2.to_vec()].into_iter().collect())
                .await
                .boxed()
        })
        .collect::<Vec<_>>()
        .await;

    let tasks = FuturesUnordered::new();

    // Publish a message to each channel.
    // Subscribers need to be polled in order to connect to the IRN backend, so we
    // wait a bit before publishing.
    tasks.push(
        async {
            let publisher = cluster.irn_api_client(0);

            tokio::time::sleep(Duration::from_secs(3)).await;

            publisher
                .publish(channel1.to_vec(), payload1.to_vec())
                .await
                .unwrap();
            publisher
                .publish(channel2.to_vec(), payload2.to_vec())
                .await
                .unwrap();
        }
        .boxed_local(),
    );

    // All subscriber groups should receive their messages.
    for sub in subscriptions1.iter_mut() {
        tasks.push(
            async {
                let data = sub.next().await.unwrap();

                assert_eq!(&data.channel, channel1);
                assert_eq!(&data.payload, payload1);
            }
            .boxed_local(),
        )
    }

    for sub in subscriptions2.iter_mut() {
        tasks.push(
            async {
                let data = sub.next().await.unwrap();

                assert_eq!(&data.channel, channel2);
                assert_eq!(&data.payload, payload2);
            }
            .boxed_local(),
        )
    }

    // Await all tasks.
    tasks.count().await;

    // Verify there's no more messages to receive.
    stream::iter(subscriptions1)
        .chain(stream::iter(subscriptions2))
        .for_each_concurrent(None, |mut subscriber| async move {
            let result = tokio::time::timeout(Duration::from_secs(3), subscriber.next()).await;

            // We should receive a timeout error, since there's no more messages.
            assert!(result.is_err());
        })
        .await;
}
