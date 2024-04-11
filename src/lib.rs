use {
    futures::{future::FusedFuture, FutureExt},
    irn::ShutdownReason,
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        fmt::Debug,
        future::Future,
        net::SocketAddr,
        pin::pin,
        time::Duration,
    },
    storage::Storage,
    wc::{
        future::StaticFutureExt,
        metrics::{self, otel},
    },
};

pub use self::{
    config::Config,
    consensus::Consensus,
    network::{Multiaddr, Multihash, Network, RemoteNode},
};

mod config;
pub mod consensus;
pub mod network;
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

pub async fn run(
    shutdown_fut: impl Future<Output = ShutdownReason>,
    cfg: &Config,
) -> Result<impl Future<Output = ()>, Error> {
    metrics::ServiceMetrics::init_with_name("irn_node");

    let storage = Storage::new(&cfg).map_err(Error::Storage)?;

    let network = Network::new(&cfg)?;

    let consensus = Consensus::new(&cfg, network.clone())
        .await
        .map_err(Error::Consensus)?;

    consensus.init(&cfg).await.map_err(Error::Consensus)?;

    let node_opts = irn::NodeOpts {
        replication_strategy: cfg.replication_strategy.clone(),
        replication_request_timeout: Duration::from_millis(cfg.replication_request_timeout),
        replication_concurrency_limit: cfg.request_concurrency_limit,
        replication_request_queue: cfg.request_limiter_queue,
        warmup_delay: Duration::from_millis(cfg.warmup_delay),
    };

    let node = irn::Node::new(cfg.id, node_opts, consensus, network, storage);

    Network::spawn_servers(&cfg, node.clone())?;

    let metrics_srv = serve_metrics(&cfg.metrics_addr, node.clone())?.spawn("metrics_server");

    let node_clone = node.clone();
    let node_fut = async move {
        match node.clone().run().await {
            Ok(shutdown_reason) => node.consensus().shutdown(shutdown_reason).await,
            Err(err) => tracing::warn!(?err, "Node::run"),
        };
    };

    Ok(async move {
        let mut shutdown_fut = pin!(shutdown_fut.fuse());
        let mut metrics_server_fut = pin!(metrics_srv.fuse());
        let mut node_fut = pin!(node_fut);

        loop {
            tokio::select! {
                biased;

                reason = &mut shutdown_fut, if !shutdown_fut.is_terminated() => {
                    if let Err(err) = node_clone.shutdown(reason) {
                        tracing::warn!("{err}");
                    }
                }

                _ = &mut metrics_server_fut, if !metrics_server_fut.is_terminated() => {
                    tracing::warn!("metrics server unexpectedly finished");
                }

                _ = &mut node_fut => {
                    break;
                }
            }
        }
    })
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

    let cx = otel::Context::new();

    for (name, stat) in stats {
        let name = format!("irn_{}", name.replace('.', "_"));

        match stat {
            relay_rocks::db::Statistic::Ticker(count) => {
                metrics.counter(name).add(&cx, count, &[]);
            }

            relay_rocks::db::Statistic::Histogram(h) => {
                // The distribution is already calculated for us by RocksDB, so we use
                // `gauge`/`counter` here instead of `histogram`.

                metrics
                    .counter(format!("{name}_count"))
                    .add(&cx, h.count, &[]);
                metrics.counter(format!("{name}_sum")).add(&cx, h.sum, &[]);

                let meter = metrics.gauge(name);

                meter.observe(&cx, h.p50, &[otel::KeyValue::new("p", "50")]);
                meter.observe(&cx, h.p95, &[otel::KeyValue::new("p", "95")]);
                meter.observe(&cx, h.p99, &[otel::KeyValue::new("p", "99")]);
                meter.observe(&cx, h.p100, &[otel::KeyValue::new("p", "100")]);
            }
        }
    }
}
