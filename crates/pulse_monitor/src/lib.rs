use {
    arc_swap::ArcSwap,
    futures::{Stream, StreamExt as _},
    monitor::ClusterMonitor,
    std::{collections::HashSet, sync::Arc, time::Duration},
    tokio::sync::RwLock,
    transport::EchoApiTransportFactory,
    wc::metrics::{self, StringLabel},
    wcn_rpc::PeerAddr,
};

pub mod monitor;
pub mod transport;

// Note: Requires a stream that would immediately yield the initial cluster
// state.
pub async fn run<C>(mut cluster_stream: C)
where
    C: Stream<Item = Arc<domain::Cluster>>,
{
    let mut cluster_stream = std::pin::pin!(cluster_stream);

    let Some(cluster) = cluster_stream.next().await else {
        tracing::warn!("cluster state stream ended unexpectedly");
        return;
    };

    let cluster = ArcSwap::new(cluster);
    let registry = RwLock::new(ClusterMonitor::default());

    let cluster_update_fut = async {
        let mut current_peers = HashSet::new();

        loop {
            current_peers = update_registry(
                &mut *registry.write().await,
                &cluster.load_full(),
                &current_peers,
            );

            if let Some(next_cluster) = cluster_stream.next().await {
                cluster.store(next_cluster);
            } else {
                break;
            }
        }
    };

    let metrics_update_fut = async {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let curr_cluster = cluster.load_full();
            let registry = registry.read().await;
            let nodes = registry.nodes();

            for id in nodes {
                let Some(status) = registry.status(id) else {
                    continue;
                };

                let Some(node) = curr_cluster.node(id) else {
                    continue;
                };

                let Ok(address) = node.addr().quic_socketaddr() else {
                    continue;
                };

                let addr_str = address.to_string();
                let available = status.is_monitoring && status.is_available;
                let available = if available { 1.0 } else { 0.0 };

                metrics::gauge!("wcn_pulse_monitor_availability", StringLabel<"destination"> => &addr_str)
                    .set(available);

                // Failure detector can't calculate suspicion level unless it's received at
                // least one heartbeat.
                if status.is_monitoring {
                    metrics::gauge!("wcn_pulse_monitor_failure_suspicion", StringLabel<"destination"> => &addr_str)
                        .set(status.suspicion_score);
                    metrics::histogram!("wcn_pulse_monitor_latency", StringLabel<"destination"> => &addr_str)
                        .record(status.latency);
                }
            }
        }
    };

    tokio::join!(cluster_update_fut, metrics_update_fut);
}

fn update_registry(
    registry: &mut ClusterMonitor,
    cluster: &domain::Cluster,
    current_peers: &HashSet<PeerAddr>,
) -> HashSet<PeerAddr> {
    let next_peers = cluster
        .nodes()
        .map(|node| node.addr())
        .collect::<HashSet<_>>();

    // Removed nodes.
    for addr in current_peers.difference(&next_peers) {
        registry.remove(&addr.id);
    }

    // Added nodes.
    for addr in next_peers.difference(current_peers) {
        // Echo server is currently hosted on the same address we're using for storage
        // API, but it's TCP instead of UDP.
        if let Ok(socketaddr) = addr.quic_socketaddr() {
            registry.insert(addr.id, EchoApiTransportFactory(socketaddr));
        } else {
            tracing::warn!(?addr, "failed to parse socket address for peer");
        }
    }

    next_peers
}
