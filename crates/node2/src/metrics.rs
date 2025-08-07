use {
    crate::Config,
    std::{future::Future, io, net::SocketAddr, time::Duration},
    sysinfo::{NetworkExt, NetworksExt},
};

pub(super) async fn serve(cfg: &Config) -> io::Result<impl Future<Output = ()>> {
    let cfg_ = cfg.clone();
    tokio::task::spawn_blocking(move || update_loop(cfg_));

    let prometheus = cfg.prometheus_handle.clone();
    let svc = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { prometheus.render() }),
        )
        .into_make_service();

    let shutdown_signal = cfg.shutdown_signal.clone();
    let addr: SocketAddr = ([0, 0, 0, 0], cfg.metrics_server_port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    Ok(async {
        let _ = axum::serve(listener, svc)
            .with_graceful_shutdown(async move { shutdown_signal.wait().await })
            .await;
    })
}

fn update_loop(config: Config) {
    use sysinfo::{CpuExt as _, SystemExt as _};

    let mut sys = sysinfo::System::new_all();

    loop {
        if config.shutdown_signal.is_emitted() {
            return;
        }

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

        sys.refresh_networks();

        for (name, net) in sys.networks().iter() {
            metrics::gauge!("wcn_network_tx_bytes_total", "network" => name.to_owned())
                .set(net.total_transmitted() as f64);
            metrics::gauge!("wcn_network_rx_bytes_total", "network" => name.to_owned())
                .set(net.total_received() as f64);
        }

        config.prometheus_handle.run_upkeep();
        std::thread::sleep(Duration::from_secs(15));
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
