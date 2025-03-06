use {
    super::transport::{Transport, TransportFactory},
    futures::StreamExt as _,
    phi_accrual_failure_detector::{Detector, SyncDetector},
    std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::IntervalStream,
    tokio_util::sync::DropGuard,
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt as _},
        metrics::{self, FutureExt as _, StringLabel},
    },
    wcn_rpc::PeerId,
};

struct Node {
    detector: SyncDetector,
    latency: Mutex<LatencyHistory>,
}

impl Node {
    fn latency(&self) -> f64 {
        // Safe unwrap, as it can't panic.
        self.latency.lock().unwrap().mean()
    }

    fn heartbeat(&self, latency: f64) {
        // Safe unwrap, as it can't panic.
        self.latency.lock().unwrap().update(latency);
        self.detector.heartbeat();
    }
}

struct NodeWrapper {
    node: Arc<Node>,
    _guard: DropGuard,
}

pub struct NodeStatus {
    pub is_monitoring: bool,
    pub is_available: bool,
    pub latency: f64,
    pub suspicion_score: f64,
}

#[derive(Default)]
pub struct ClusterMonitor {
    nodes: HashMap<PeerId, NodeWrapper>,
}

impl ClusterMonitor {
    pub fn insert<F>(&mut self, id: PeerId, factory: F)
    where
        F: TransportFactory + Send + Sync + 'static,
    {
        let node = Arc::new(Node {
            detector: SyncDetector::default(),
            latency: Mutex::new(LatencyHistory::new(5)),
        });

        let token = CancellationToken::new();

        ping_loop(factory, node.clone())
            .with_cancellation(token.clone())
            .with_metrics(metrics::future_metrics!("wcn_echo_client_ping_loop"))
            .spawn();

        self.nodes.insert(id, NodeWrapper {
            node,
            _guard: token.drop_guard(),
        });
    }

    pub fn remove(&mut self, id: &PeerId) {
        self.nodes.remove(id);
    }

    pub fn nodes(&self) -> impl Iterator<Item = &PeerId> {
        self.nodes.keys()
    }

    pub fn status(&self, id: &PeerId) -> Option<NodeStatus> {
        let node = self.nodes.get(id)?;
        let detector = &node.node.detector;

        Some(NodeStatus {
            is_monitoring: detector.is_monitoring(),
            is_available: detector.is_available(),
            latency: node.node.latency(),
            suspicion_score: detector.phi(),
        })
    }
}

async fn ping_loop<F>(factory: F, node: Arc<Node>)
where
    F: TransportFactory + Send + Sync + 'static,
{
    let addr_str = factory.address().to_string();

    loop {
        // Retry broken connections.
        if ping_loop_internal(&factory, &node).await.is_err() {
            metrics::counter!("wcn_pulse_monitor_connection_failure", StringLabel<"destination"> => &addr_str).increment(1);
        }

        // Added delay before retrying connection.
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn ping_loop_internal<F>(
    factory: &F,
    node: &Node,
) -> Result<(), <F::Transport as Transport>::Error>
where
    F: TransportFactory + Send + Sync + 'static,
{
    let transport = factory.create().await?;
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    IntervalStream::new(interval)
        .take_until(shutdown_rx.recv())
        .for_each_concurrent(None, |_| async {
            match transport.heartbeat().await {
                Ok(elapsed) => {
                    node.heartbeat(elapsed.as_secs_f64());
                }

                Err(_) => {
                    // Shutdown on the first error.
                    let _ = shutdown_tx.try_send(());
                }
            }
        })
        .await;

    Ok(())
}

// Simple ring buffer to calculate mean latency over an arbitrary window.
struct LatencyHistory {
    data: VecDeque<f64>,
    sum: f64,
    capacity: usize,
}

impl LatencyHistory {
    fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(capacity),
            sum: 0.0,
            capacity,
        }
    }

    fn update(&mut self, latency: f64) {
        if self.data.len() >= self.capacity {
            if let Some(latency) = self.data.pop_front() {
                self.sum -= latency;
            };
        }

        self.data.push_back(latency);
        self.sum += latency;
    }

    fn mean(&self) -> f64 {
        if self.data.is_empty() {
            0.0
        } else {
            self.sum / self.data.len() as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_history() {
        let mut hist = LatencyHistory::new(3);
        assert_eq!(hist.mean(), 0.0);
        hist.update(1.0);
        hist.update(2.0);
        hist.update(3.0);
        assert_eq!(hist.mean(), 2.0);
        hist.update(4.0);
        assert_eq!(hist.mean(), 3.0);
        hist.update(5.0);
        assert_eq!(hist.mean(), 4.0);
        hist.update(6.0);
        assert_eq!(hist.mean(), 5.0);
    }
}
