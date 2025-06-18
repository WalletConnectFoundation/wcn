use {
    arc_swap::ArcSwap,
    futures::{Stream, StreamExt as _},
    monitor::ClusterMonitor,
    std::{collections::HashSet, sync::Arc, time::Duration},
    tokio::sync::RwLock,
    transport::{EchoApiTransportFactory, PulseApiTransportFactory},
    wc::metrics::{self, enum_ordinalize::Ordinalize, EnumLabel, StringLabel},
    wcn_rpc::PeerAddr,
};

pub mod monitor;
pub mod transport;

// Note: Requires a stream that would immediately yield the initial cluster
// state.
pub async fn run<C>(cluster_stream: C)
where
    C: Stream<Item = Arc<domain::Cluster>>,
{
    let mut cluster_stream = std::pin::pin!(cluster_stream);

    let Some(cluster) = cluster_stream.next().await else {
        tracing::warn!("cluster state stream ended unexpectedly");
        return;
    };

    let cluster = ArcSwap::new(cluster);
    let tcp_monitor = RwLock::new(ClusterMonitor::default());
    let quic_monitor = RwLock::new(ClusterMonitor::default());

    let cluster_update_fut = async {
        let mut current_peers = HashSet::new();

        loop {
            current_peers = update_registry(
                &mut *tcp_monitor.write().await,
                &mut *quic_monitor.write().await,
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
            produce_metrics(
                &curr_cluster,
                &*tcp_monitor.read().await,
                TransportType::Tcp,
            );
            produce_metrics(
                &curr_cluster,
                &*quic_monitor.read().await,
                TransportType::Quic,
            );
        }
    };

    tokio::join!(cluster_update_fut, metrics_update_fut);
}

fn produce_metrics(cluster: &domain::Cluster, monitor: &ClusterMonitor, transport: TransportType) {
    let nodes = monitor.nodes();

    for id in nodes {
        let Some(status) = monitor.status(id) else {
            continue;
        };

        let Some(node) = cluster.node(id) else {
            continue;
        };

        let Ok(address) = node.addr().quic_socketaddr() else {
            continue;
        };

        let addr_str = address.to_string();
        let available = status.is_monitoring && status.is_available;
        let available = if available { 1.0 } else { 0.0 };

        metrics::gauge!("wcn_pulse_monitor_availability",
            EnumLabel<"transport", TransportType> => transport,
            StringLabel<"destination"> => &addr_str
        )
        .set(available);

        // Failure detector can't calculate suspicion level unless it's received at
        // least one heartbeat.
        if status.is_monitoring {
            metrics::gauge!("wcn_pulse_monitor_failure_suspicion",
                EnumLabel<"transport", TransportType> => transport,
                StringLabel<"destination"> => &addr_str
            )
            .set(status.suspicion_score);

            metrics::histogram!("wcn_pulse_monitor_latency",
                EnumLabel<"transport", TransportType> => transport,
                StringLabel<"destination"> => &addr_str
            )
            .record(status.latency);
        }
    }
}

fn update_registry(
    tcp_registry: &mut ClusterMonitor,
    quic_registry: &mut ClusterMonitor,
    cluster: &domain::Cluster,
    current_peers: &HashSet<PeerAddr>,
) -> HashSet<PeerAddr> {
    let next_peers = cluster
        .nodes()
        .map(|node| node.addr())
        .collect::<HashSet<_>>();

    // Removed nodes.
    for addr in current_peers.difference(&next_peers) {
        tcp_registry.remove(&addr.id);
        quic_registry.remove(&addr.id);
    }

    // Added nodes.
    for addr in next_peers.difference(current_peers) {
        tcp_registry.insert(addr.id, EchoApiTransportFactory(addr.clone()));

        if let Ok(client) = pulse_api::Client::new(addr.clone())
            .map_err(|err| tracing::warn!(?err, "pulse_api::Client::new"))
        {
            quic_registry.insert(addr.id, PulseApiTransportFactory {
                addr: addr.clone(),
                client,
            });
        }
    }

    next_peers
}

#[derive(Clone, Copy, Ordinalize)]
enum TransportType {
    Tcp = 0,
    Quic = 1,
}

impl metrics::Enum for TransportType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Quic => "quic",
        }
    }
}
