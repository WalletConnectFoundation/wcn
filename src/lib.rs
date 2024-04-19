use {
    ::network::{rpc, MeteredExt as _, NoHandshake, WithTimeoutsExt},
    derive_more::AsRef,
    futures::{future::FusedFuture, FutureExt},
    irn::{Running, ShutdownError, ShutdownReason},
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        fmt::Debug,
        future::Future,
        net::SocketAddr,
        pin::pin,
        sync::Arc,
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
mod consensus;
pub mod network;
pub mod storage;

#[cfg(feature = "test-cluster")]
pub mod test_cluster;

/// Handle to the external and internal api servers
#[derive(Clone)]
pub struct Servers {
    node: Node,
}

impl Servers {
    /// Returns a reference to the [`Node`] being served.
    pub fn node(&self) -> &Node {
        &self.node
    }
}

/// Local node being handled by this application.
#[derive(AsRef, Clone)]
pub struct Node {
    #[as_ref]
    inner: Arc<irn::Node<Consensus, Network, Storage>>,
    rpc_timeouts: rpc::Timeouts,

    pubsub: network::Pubsub,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Invalid address: {0}")]
    InvalidAddress(#[from] std::net::AddrParseError),

    #[error("Server error: {0}")]
    Server(#[from] hyper::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("Failed to start API server: {0:?}")]
    ApiServer(#[from] api::Error),

    #[error("Failed to start metrics server: {0:?}")]
    MetricsServer(#[from] MetricsError),

    #[error("Failed to initialize networking: {0:?}")]
    Network(#[from] ::network::Error),

    #[error(transparent)]
    NodeCreation(#[from] NodeCreationError),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeCreationError {
    #[error("Failed to initialize storage: {0:?}")]
    Storage(storage::Error),

    #[error("Failed to initialize Consensus: {0:?}")]
    Consensus(consensus::InitializationError),
}

impl Node {
    /// Creates a new [`Node`] and serves it via [`Servers`].
    pub async fn serve(
        cfg: Config,
    ) -> Result<Running<Servers, impl Future<Output = ()>>, ServeError> {
        metrics::ServiceMetrics::init_with_name("irn_node");

        let rpc_timeout = Duration::from_millis(cfg.network_request_timeout);
        let rpc_timeouts = rpc::Timeouts {
            unary: Some(rpc_timeout),
            streaming: None,
            oneshot: Some(rpc_timeout),
        };

        let client = ::network::Client::new(::network::ClientConfig {
            keypair: cfg.keypair.clone(),
            known_peers: cfg
                .known_peers
                .iter()
                .map(|(id, addr)| (id.id, addr.clone()))
                .collect(),
            handshake: NoHandshake,
            connection_timeout: Duration::from_millis(cfg.network_connection_timeout),
        })?
        .with_timeouts(rpc_timeouts)
        .metered();

        let network = Network {
            local_id: cfg.id,
            client,
        };

        let node_fut = Self::new(&cfg, network, rpc_timeouts).await?;
        let node = node_fut.handle.clone();

        let server_config = ::network::ServerConfig {
            addr: cfg.addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        let api_server_config = ::network::ServerConfig {
            addr: cfg.api_addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        ::network::run_server(server_config, NoHandshake, node.clone())?.spawn("rpc_server");
        ::api::server::run(api_server_config, node.clone())?.spawn("api_rpc_server");

        node.init(&cfg).await?;

        let metrics_srv = serve_metrics(&cfg.metrics_addr, node.clone())?.spawn("metrics_server");

        Ok(Running {
            handle: Servers { node },
            future: async move {
                let mut node_fut = pin!(node_fut);
                let mut metrics_server_fut = pin!(metrics_srv.fuse());

                loop {
                    tokio::select! {
                        biased;

                        _ = &mut metrics_server_fut, if !metrics_server_fut.is_terminated() => {
                            tracing::warn!("metrics server unexpectedly finished");
                        }

                        _ = &mut node_fut => {
                            break;
                        }
                    }
                }
            },
        })
    }

    /// Creates a new [`Running`] [`Node`] using the provided [`Config`] and
    /// [`Network`].
    async fn new(
        cfg: &Config,
        network: Network,
        rpc_timeouts: rpc::Timeouts,
    ) -> Result<Running<Self, impl Future<Output = ()>>, NodeCreationError> {
        let storage = Storage::new(cfg.rocksdb_dir.clone(), storage::Config {
            num_batch_threads: cfg.rocksdb_num_batch_threads,
            num_callback_threads: cfg.rocksdb_num_callback_threads,
        })
        .map_err(NodeCreationError::Storage)?;

        let consensus = Consensus::new(cfg, network.clone())
            .await
            .map_err(NodeCreationError::Consensus)?;

        let node_opts = irn::NodeOpts {
            id: cfg.id,
            replication_strategy: cfg.replication_strategy.clone(),
            replication_request_timeout: Duration::from_millis(cfg.replication_request_timeout),
            replication_concurrency_limit: cfg.request_concurrency_limit,
            replication_request_queue: cfg.request_limiter_queue,
            warmup_delay: Duration::from_millis(cfg.warmup_delay),
        };

        let node = irn::Node::new(node_opts, consensus, network, storage);

        Ok(Running {
            handle: Node {
                inner: Arc::new(node.clone()),
                rpc_timeouts,
                pubsub: network::Pubsub::new(),
            },
            future: async move {
                match node.clone().run().await {
                    Ok(shutdown_reason) => node.consensus().shutdown(shutdown_reason).await,
                    Err(err) => tracing::warn!(?err, "Node::run"),
                };
            },
        })
    }

    async fn init(&self, cfg: &Config) -> Result<(), NodeCreationError> {
        self.node()
            .consensus()
            .init(cfg)
            .await
            .map_err(NodeCreationError::Consensus)
    }
}

impl Node {
    /// Returns a reference to the inner [`irn::Node`].
    pub fn node(&self) -> &irn::Node<Consensus, Network, Storage> {
        &self.inner
    }

    fn consensus(&self) -> &Consensus {
        self.inner.consensus()
    }

    /// Initiates shut down process of this [`Node`].
    pub fn shutdown(&self, reason: ShutdownReason) {
        if let Err(err @ ShutdownError) = self.inner.shutdown(reason) {
            tracing::warn!("{err}");
        }
    }
}

#[derive(
    Clone, Copy, Debug, Default, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize,
)]
pub struct TypeConfig;

fn serve_metrics(
    addr: &str,
    node: Node,
) -> Result<impl Future<Output = Result<(), MetricsError>>, MetricsError> {
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
        let db = node.inner.storage().db();
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
