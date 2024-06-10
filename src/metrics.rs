use {
    crate::{config::Config, network::rpc, Error, Node},
    axum::extract::Path,
    std::{collections::HashMap, future::Future, net::SocketAddr, time::Duration},
    sysinfo::{NetworkExt, NetworksExt},
    tokio::sync::oneshot,
    wc::metrics::{self, otel},
};

fn update_loop(mut cancel: oneshot::Receiver<()>, node: Node, cfg: Config) {
    use sysinfo::{CpuExt as _, DiskExt as _, SystemExt as _};

    let mut sys = sysinfo::System::new_all();

    loop {
        match cancel.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        };

        sys.refresh_cpu();

        for (n, cpu) in sys.cpus().iter().enumerate() {
            metrics::gauge!("irn_cpu_usage_percent_per_core_gauge", cpu.cpu_usage(), &[
                otel::KeyValue::new("n_core", n as i64)
            ]);
        }

        sys.refresh_memory();

        metrics::gauge!("irn_total_memory", sys.total_memory());
        metrics::gauge!("irn_free_memory", sys.free_memory());
        metrics::gauge!("irn_available_memory", sys.available_memory());
        metrics::gauge!("irn_used_memory", sys.used_memory());

        sys.refresh_disks();

        // TODO: parent dir might not be the mount point
        // so this needs to be tested on different systems
        //
        let rocksdb_parent = cfg
            .rocksdb_dir
            .parent()
            .unwrap_or(&cfg.rocksdb_dir)
            .to_str();

        let raft_parent = cfg.raft_dir.parent().unwrap_or(&cfg.raft_dir).to_str();

        for disk in sys.disks() {
            if disk.mount_point().to_str() == rocksdb_parent
                || disk.mount_point().to_str() == raft_parent
            {
                metrics::gauge!("irn_disk_total_space", disk.total_space());
                metrics::gauge!("irn_disk_available_space", disk.available_space());
                break;
            }
        }

        sys.refresh_networks();

        for (name, net) in sys.networks().iter() {
            metrics::gauge!("irn_network_tx_bytes_total", net.total_transmitted(), &[
                otel::KeyValue::new("network", name.to_owned())
            ]);
            metrics::gauge!("irn_network_rx_bytes_total", net.total_received(), &[
                otel::KeyValue::new("network", name.to_owned())
            ]);
        }

        if let Err(err) = wc::alloc::stats::update_jemalloc_metrics() {
            tracing::warn!(?err, "failed to collect jemalloc stats");
        }

        // We have a similar issue to https://github.com/facebook/rocksdb/issues/3889
        // PhysicalCoreID() consumes 5-10% CPU
        // TODO: Consider fixing this & re-enabling the stats
        let db = node.storage().db();
        let mut metrics = RocksMetrics::default();
        let _ = move || update_rocksdb_metrics(db, &mut metrics);

        std::thread::sleep(Duration::from_secs(15));
    }
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

pub(crate) fn serve(
    cfg: Config,
    node: Node,
) -> Result<impl Future<Output = Result<(), Error>>, Error> {
    let addr: SocketAddr = cfg.metrics_addr.parse()?;

    let (tx, rx) = oneshot::channel();

    let node_ = node.clone();
    std::thread::spawn(move || update_loop(rx, node_, cfg));

    let svc = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { export_prometheus() }),
        )
        .route(
            "/metrics/:peer_id",
            axum::routing::get(move |Path(peer_id)| scrap_prometheus(node.clone(), peer_id)),
        )
        .into_make_service();

    Ok(async move {
        tracing::info!(?addr, "starting metrics server");

        let result = axum::Server::bind(&addr)
            .serve(svc)
            .await
            .map_err(Into::into);

        let _ = tx.send(());

        result
    })
}

pub fn export_prometheus() -> String {
    metrics::ServiceMetrics::export()
        .map_err(|err| tracing::warn!(?err, "failed to export prometheus metrics"))
        .unwrap_or_default()
}

async fn scrap_prometheus(node: Node, peer_id: libp2p::PeerId) -> String {
    if node.id().id == peer_id {
        return export_prometheus();
    }

    rpc::Send::<rpc::Metrics, _, _>::send(&node.network().client, peer_id, ())
        .await
        .map_err(|err| tracing::warn!(?err, %peer_id, "failed to scrap prometheus metrics"))
        .unwrap_or_default()
}
