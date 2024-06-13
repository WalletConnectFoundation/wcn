use {
    crate::{config::Config, network::rpc, Error, Node},
    axum::extract::Path,
    metrics_exporter_prometheus::PrometheusHandle,
    std::{future::Future, net::SocketAddr, time::Duration},
    sysinfo::{NetworkExt, NetworksExt},
    tokio::sync::oneshot,
};

fn update_loop(mut cancel: oneshot::Receiver<()>, node: Node, cfg: Config) {
    use sysinfo::{CpuExt as _, DiskExt as _, SystemExt as _};

    let mut sys = sysinfo::System::new_all();

    loop {
        match cancel.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        };

        tracing::info!("refreshing CPU");
        sys.refresh_cpu();

        for (n, cpu) in sys.cpus().iter().enumerate() {
            metrics::gauge!("irn_cpu_usage_percent_per_core_gauge", "n_core" => n.to_string())
                .set(cpu.cpu_usage())
        }

        tracing::info!("refreshing memory");
        sys.refresh_memory();

        metrics::gauge!("irn_total_memory").set(sys.total_memory() as f64);
        metrics::gauge!("irn_free_memory").set(sys.free_memory() as f64);
        metrics::gauge!("irn_available_memory").set(sys.available_memory() as f64);
        metrics::gauge!("irn_used_memory").set(sys.used_memory() as f64);

        tracing::info!("refreshing disks");
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
                metrics::gauge!("irn_disk_total_space").set(disk.total_space() as f64);
                metrics::gauge!("irn_disk_available_space").set(disk.available_space() as f64);
                break;
            }
        }

        tracing::info!("refreshing networks");
        sys.refresh_networks();

        for (name, net) in sys.networks().iter() {
            metrics::gauge!("irn_network_tx_bytes_total", "network" => name.to_owned())
                .set(net.total_transmitted() as f64);
            metrics::gauge!("irn_network_rx_bytes_total", "network" => name.to_owned())
                .set(net.total_received() as f64);
        }

        // We have a similar issue to https://github.com/facebook/rocksdb/issues/3889
        // PhysicalCoreID() consumes 5-10% CPU
        // TODO: Consider fixing this & re-enabling the stats
        let db = node.storage().db();
        let _ = move || update_rocksdb_metrics(db);

        std::thread::sleep(Duration::from_secs(15));
    }
}

fn update_rocksdb_metrics(db: &relay_rocks::RocksBackend) {
    match db.memory_usage() {
        Ok(s) => {
            metrics::gauge!("irn_rocksdb_mem_table_total").set(s.mem_table_total as f64);
            metrics::gauge!("irn_rocksdb_mem_table_unflushed").set(s.mem_table_unflushed as f64);
            metrics::gauge!("irn_rocksdb_mem_table_readers_total",)
                .set(s.mem_table_readers_total as f64);
            metrics::gauge!("irn_rocksdb_cache_total").set(s.cache_total as f64);
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
                metrics::counter!(name).increment(count);
            }

            relay_rocks::db::Statistic::Histogram(h) => {
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

pub(crate) fn serve(
    cfg: Config,
    node: Node,
    prometheus: PrometheusHandle,
) -> Result<impl Future<Output = Result<(), Error>>, Error> {
    let prometheus_ = prometheus.clone();

    let addr: SocketAddr = cfg.metrics_addr.parse()?;

    let (tx, rx) = oneshot::channel();

    let node_ = node.clone();
    tokio::task::spawn_blocking(move || update_loop(rx, node_, cfg));

    let svc = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { prometheus.render() }),
        )
        .route(
            "/metrics/:peer_id",
            axum::routing::get(move |Path(peer_id)| {
                scrap_prometheus(prometheus_.clone(), node.clone(), peer_id)
            }),
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

async fn scrap_prometheus(handle: PrometheusHandle, node: Node, peer_id: libp2p::PeerId) -> String {
    if node.id().id == peer_id {
        return handle.render();
    }

    rpc::Send::<rpc::Metrics, _, _>::send(&node.network().client, peer_id, ())
        .await
        .map_err(|err| tracing::warn!(?err, %peer_id, "failed to scrap prometheus metrics"))
        .unwrap_or_default()
}
