use {
    crate::Error,
    metrics_exporter_prometheus::PrometheusHandle,
    std::{
        future::Future,
        net::SocketAddr,
        path::{Path, PathBuf},
        time::Duration,
    },
    sysinfo::{NetworkExt, NetworksExt},
    tap::{TapFallible, TapOptional},
    tokio::sync::oneshot,
    wcn_rocks::RocksBackend,
};

pub struct Config {
    pub rocksdb_dir: PathBuf,
    pub rocksdb: Option<RocksBackend>,
}

pub fn run_update_loop(cfg: Config) -> oneshot::Sender<()> {
    let (tx, rx) = oneshot::channel();
    tokio::task::spawn_blocking(move || update_loop(rx, cfg));
    tx
}

pub fn serve(
    addr: SocketAddr,
    prometheus: PrometheusHandle,
) -> Result<impl Future<Output = Result<(), Error>>, Error> {
    let (tx, rx) = oneshot::channel::<()>();

    tokio::task::spawn_blocking({
        let prometheus = prometheus.clone();
        move || prometheus_upkeep_loop(rx, prometheus)
    });

    let svc = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { prometheus.render() }),
        )
        .into_make_service();

    Ok(async move {
        let _tx = tx;

        tracing::info!(?addr, "starting metrics server");

        async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, svc).await
        }
        .await
        .map_err(Error::MetricsServer)
    })
}

fn prometheus_upkeep_loop(mut cancel: oneshot::Receiver<()>, prometheus: PrometheusHandle) {
    loop {
        if !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            return;
        }

        prometheus.run_upkeep();

        std::thread::sleep(Duration::from_secs(15));
    }
}

fn update_loop(mut cancel: oneshot::Receiver<()>, cfg: Config) {
    use sysinfo::{CpuExt as _, DiskExt as _, SystemExt as _};

    let mut sys = sysinfo::System::new_all();

    // Try to detect the closest mount point. Fallback to the parent directory.
    let storage_mount_point = find_storage_mount_point(cfg.rocksdb_dir.as_path())
        .tap_none(|| {
            tracing::warn!(
                path = ?cfg.rocksdb_dir,
                "failed to find mount point of rocksdb directory"
            )
        })
        .or_else(|| cfg.rocksdb_dir.parent())
        .unwrap_or(&cfg.rocksdb_dir);

    loop {
        match cancel.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        };

        if let Err(err) = wc::alloc::stats::update_jemalloc_metrics() {
            tracing::warn!(?err, "failed to get jemalloc allocation stats");
        }

        update_cgroup_stats();

        sys.refresh_cpu();

        for (n, cpu) in sys.cpus().iter().enumerate() {
            metrics::gauge!("wcn_cpu_usage_percent_per_core_gauge", "n_core" => n.to_string())
                .set(cpu.cpu_usage())
        }

        sys.refresh_memory();

        metrics::gauge!("wcn_total_memory").set(sys.total_memory() as f64);
        metrics::gauge!("wcn_free_memory").set(sys.free_memory() as f64);
        metrics::gauge!("wcn_available_memory").set(sys.available_memory() as f64);
        metrics::gauge!("wcn_used_memory").set(sys.used_memory() as f64);

        sys.refresh_disks();

        for disk in sys.disks() {
            if disk.mount_point() == storage_mount_point {
                metrics::gauge!("wcn_disk_total_space").set(disk.total_space() as f64);
                metrics::gauge!("wcn_disk_available_space").set(disk.available_space() as f64);
                break;
            }
        }

        sys.refresh_networks();

        for (name, net) in sys.networks().iter() {
            metrics::gauge!("wcn_network_tx_bytes_total", "network" => name.to_owned())
                .set(net.total_transmitted() as f64);
            metrics::gauge!("wcn_network_rx_bytes_total", "network" => name.to_owned())
                .set(net.total_received() as f64);
        }

        // We have a similar issue to https://github.com/facebook/rocksdb/issues/3889
        // PhysicalCoreID() consumes 5-10% CPU, so for now rocksdb metrics are behind a
        // flag.
        if let Some(db) = &cfg.rocksdb {
            update_rocksdb_metrics(db);
        }

        std::thread::sleep(Duration::from_secs(15));
    }
}

fn update_rocksdb_metrics(db: &RocksBackend) {
    match db.memory_usage() {
        Ok(s) => {
            metrics::gauge!("wcn_rocksdb_mem_table_total").set(s.mem_table_total as f64);
            metrics::gauge!("wcn_rocksdb_mem_table_unflushed").set(s.mem_table_unflushed as f64);
            metrics::gauge!("wcn_rocksdb_mem_table_readers_total",)
                .set(s.mem_table_readers_total as f64);
            metrics::gauge!("wcn_rocksdb_cache_total").set(s.cache_total as f64);
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
        let name = format!("wcn_{}", name.replace('.', "_"));

        match stat {
            wcn_rocks::db::Statistic::Ticker(count) => {
                metrics::counter!(name).increment(count);
            }

            wcn_rocks::db::Statistic::Histogram(h) => {
                // The distribution is already calculated for us by RocksDB, so we use
                // `gauge`/`counter` here instead of `histogram`.

                metrics::counter!(format!("{name}_count")).increment(h.count);
                metrics::counter!(format!("{name}_sum")).increment(h.sum);

                metrics::gauge!(name.clone(), "p" => "50").set(h.p50);
                metrics::gauge!(name.clone(), "p" => "95").set(h.p95);
                metrics::gauge!(name.clone(), "p" => "99").set(h.p99);
                metrics::gauge!(name, "p" => "100").set(h.p100);
            }
        }
    }
}

fn update_cgroup_stats() {
    // For details on the values see:
    //      https://www.kernel.org/doc/Documentation/cgroup-v2.txt
    const MEMORY_STAT_PATH: &str = "/sys/fs/cgroup/memory.stat";

    let Ok(data) = std::fs::read_to_string(MEMORY_STAT_PATH) else {
        return;
    };

    for line in data.lines() {
        let mut parts = line.split(' ');

        let (Some(stat), Some(val), None) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };

        let Ok(val) = val.parse::<f64>() else {
            continue;
        };

        metrics::gauge!("wcn_memory_stat", "stat" => stat.to_owned()).set(val);
    }
}

fn find_storage_mount_point(mut path: &Path) -> Option<&Path> {
    let mounts = proc_mounts::MountList::new()
        .tap_err(|err| tracing::warn!(?err, "failed to read list of mounted file systems"))
        .ok()?;

    loop {
        if mounts.get_mount_by_dest(path).is_some() {
            return Some(path);
        }

        if let Some(parent) = path.parent() {
            path = parent;
        } else {
            return None;
        }
    }
}
