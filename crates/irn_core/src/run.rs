use {
    crate::{serve_metrics, Error},
    futures::{future::FusedFuture, FutureExt},
    irn::ShutdownReason,
    std::{future::Future, pin::pin, time::Duration},
    wc::{
        future::StaticFutureExt,
        metrics::{self},
    },
};

pub use crate::{
    config::Config,
    consensus::Consensus,
    network::{Multiaddr, Multihash, Network, RemoteNode},
    storage::Storage,
};

pub async fn run(
    shutdown_fut: impl Future<Output = ShutdownReason>,
    cfg: &Config,
) -> Result<impl Future<Output = ()>, Error> {
    metrics::ServiceMetrics::init_with_name("irn_node");

    let storage = Storage::new(cfg).map_err(Error::Storage)?;

    let network = Network::new(cfg)?;

    let consensus = Consensus::new(cfg, network.clone())
        .await
        .map_err(Error::Consensus)?;

    consensus.init(cfg).await.map_err(Error::Consensus)?;

    let node_opts = irn::NodeOpts {
        replication_strategy: cfg.replication_strategy.clone(),
        replication_request_timeout: Duration::from_millis(cfg.replication_request_timeout),
        replication_concurrency_limit: cfg.request_concurrency_limit,
        replication_request_queue: cfg.request_limiter_queue,
        warmup_delay: Duration::from_millis(cfg.warmup_delay),
    };

    let node = irn::Node::new(cfg.id, node_opts, consensus, network, storage);

    Network::spawn_servers(cfg, node.clone())?;

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
