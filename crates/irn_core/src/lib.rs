use {
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, fmt::Debug, future::Future, net::SocketAddr, time::Duration},
    storage::Storage,
    wc::metrics::{self, otel},
};

pub use self::{
    config::Config,
    consensus::Consensus,
    network::{Multiaddr, Multihash, Network, RemoteNode},
};

pub mod config;
pub mod consensus;
pub mod exec;
pub mod logger;
pub mod network;
pub mod run;
pub mod signal;
pub mod storage;

type Node = irn::Node<Consensus, Network, Storage>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid metrics address: {0}")]
    InvalidMetricsAddress(#[from] std::net::AddrParseError),

    #[error("Metrics server error: {0}")]
    MetricsServer(#[from] hyper::Error),

    #[error("Failed to start API server: {0:?}")]
    ApiServer(#[from] api::Error),

    #[error("Failed to initialize networking: {0:?}")]
    Network(#[from] ::network::Error),

    #[error("Failed to initialize storage: {0:?}")]
    Storage(storage::Error),

    #[error("Failed to initialize Consensus: {0:?}")]
    Consensus(consensus::InitializationError),
}

#[derive(
    Clone, Copy, Debug, Default, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct TypeConfig;

fn serve_metrics(addr: &str, node: Node) -> Result<impl Future<Output = Result<(), Error>>, Error> {
    let addr: SocketAddr = addr.parse()?;

    let updater_handle = tokio::spawn(metrics_update_loop(node));

    let handler = move || async move {
        metrics::ServiceMetrics::export()
            .map_err(|err| tracing::warn!(?err, "failed to export prometheus metrics"))
            .unwrap_or_default()
    };

    let svc = axum::Router::new()
        .route("/metrics", axum::routing::get(handler))
        .into_make_service();

    Ok(async move {
        tracing::info!(?addr, "starting metrics server");

        let result = axum::Server::bind(&addr)
            .serve(svc)
            .await
            .map_err(Into::into);

        updater_handle.abort();

        result
    })
}

async fn metrics_update_loop(node: Node) {
    let sys_update_fut = async {
        use sysinfo::{CpuExt as _, DiskExt as _, SystemExt as _};

        let mut sys = sysinfo::System::new_all();

        loop {
            sys.refresh_cpu();

            for (n, cpu) in sys.cpus().iter().enumerate() {
                metrics::gauge!("irn_cpu_usage_percent_per_core_gauge", cpu.cpu_usage(), &[
                    otel::KeyValue::new("n_core", n as i64)
                ]);
            }

            sys.refresh_disks();

            for disk in sys.disks() {
                if disk.mount_point().to_str() == Some("/irn") {
                    metrics::gauge!("irn_disk_total_space", disk.total_space());
                    metrics::gauge!("irn_disk_available_space", disk.available_space());
                    break;
                }
            }

            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    };

    let mem_update_fut = async {
        loop {
            if let Err(err) = wc::alloc::stats::update_jemalloc_metrics() {
                tracing::warn!(?err, "failed to collect jemalloc stats");
            }

            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    };

    // We have a similar issue to https://github.com/facebook/rocksdb/issues/3889
    // PhysicalCoreID() consumes 5-10% CPU
    // TODO: Consider fixing this & re-enabling the stats
    let _rocks_update_fut = async {
        let db = node.storage().db();
        let mut metrics = RocksMetrics::default();

        loop {
            update_rocksdb_metrics(db, &mut metrics);

            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    };

    tokio::join!(sys_update_fut, mem_update_fut);
}

#[derive(Default)]
struct RocksMetrics {
    gauges: HashMap<String, otel::metrics::ObservableGauge<f64>>,
    counters: HashMap<String, otel::metrics::Counter<u64>>,
}

impl RocksMetrics {
    pub fn counter(&mut self, name: String) -> otel::metrics::Counter<u64> {
        self.counters
            .entry(name.clone())
            .or_insert_with(|| metrics::ServiceMetrics::meter().u64_counter(name).init())
            .clone()
    }

    pub fn gauge(&mut self, name: String) -> otel::metrics::ObservableGauge<f64> {
        self.gauges
            .entry(name.clone())
            .or_insert_with(|| {
                metrics::ServiceMetrics::meter()
                    .f64_observable_gauge(name)
                    .init()
            })
            .clone()
    }
}

fn update_rocksdb_metrics(db: &relay_rocks::RocksBackend, metrics: &mut RocksMetrics) {
    match db.memory_usage() {
        Ok(s) => {
            metrics::gauge!("irn_rocksdb_mem_table_total", s.mem_table_total as f64);
            metrics::gauge!(
                "irn_rocksdb_mem_table_unflushed",
                s.mem_table_unflushed as f64
            );
            metrics::gauge!(
                "irn_rocksdb_mem_table_readers_total",
                s.mem_table_readers_total as f64
            );
            metrics::gauge!("irn_rocksdb_cache_total", s.cache_total as f64);
        }

        Err(err) => tracing::warn!(?err, "failed to get rocksdb memory usage stats"),
    }

    let stats = match db.statistics() {
        Ok(Some(stats)) => stats,
        Ok(None) => {
            tracing::warn!("rocksdb statistics are disabled");
            return;
        }
        Err(err) => {
            tracing::warn!(?err, "failed to get rocksdb statistics");
            return;
        }
    };

    for (name, stat) in stats {
        let name = format!("irn_{}", name.replace('.', "_"));

        match stat {
            relay_rocks::db::Statistic::Ticker(count) => {
                metrics.counter(name).add(count, &[]);
            }

            relay_rocks::db::Statistic::Histogram(h) => {
                // The distribution is already calculated for us by RocksDB, so we use
                // `gauge`/`counter` here instead of `histogram`.

                metrics.counter(format!("{name}_count")).add(h.count, &[]);
                metrics.counter(format!("{name}_sum")).add(h.sum, &[]);

                let meter = metrics.gauge(name);

                meter.observe(h.p50, &[otel::KeyValue::new("p", "50")]);
                meter.observe(h.p95, &[otel::KeyValue::new("p", "95")]);
                meter.observe(h.p99, &[otel::KeyValue::new("p", "99")]);
                meter.observe(h.p100, &[otel::KeyValue::new("p", "100")]);
            }
        }
    }
}
