use {
    crate::Error,
    futures::{FutureExt as _, SinkExt as _, StreamExt as _},
    governor::state::StreamRateLimitExt,
    std::{
        net::{IpAddr, SocketAddr},
        num::NonZeroU32,
        sync::Arc,
    },
    tap::TapFallible,
    tokio::{
        net::TcpStream,
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt as _},
        metrics::{self, FutureExt as _},
    },
};

#[derive(Clone, Debug)]
pub struct Config {
    pub address: IpAddr,
    pub max_connections: usize,

    /// Maximum requests per second.
    pub max_rate: NonZeroU32,
}

pub async fn spawn(config: Config) -> Result<(), Error> {
    let semaphore = Arc::new(Semaphore::new(config.max_connections));
    let address = SocketAddr::new(config.address, crate::SERVER_PORT);

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .tap_err(|err| tracing::warn!(?err, "failed to start echo server"))
        .map_err(Error::Listener)?;

    let token = CancellationToken::new();
    let _guard = token.clone().drop_guard();

    // TODO: Shutdown mechanism?
    loop {
        let stream = listener.accept().await.tap_err(|err| {
            tracing::warn!(?err, "incoming echo server connection failed");
        });

        let Ok((socket, _)) = stream else {
            metrics::counter!("wcn_echo_server_connection_failed").increment(1);
            continue;
        };

        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            metrics::counter!("wcn_echo_server_connection_dropped").increment(1);
            continue;
        };

        connection_handler(socket, config.max_rate, permit)
            .map(|res| {
                if let Err(err) = res {
                    tracing::warn!(?err, "echo stream error");
                    metrics::counter!("wcn_echo_server_stream_error").increment(1);
                }
            })
            .with_cancellation(token.child_token())
            .with_metrics(metrics::future_metrics!(
                "wcn_echo_server_connection_handler"
            ))
            .spawn();
    }
}

async fn connection_handler(
    stream: TcpStream,
    rate: NonZeroU32,
    _permit: OwnedSemaphorePermit,
) -> Result<(), Error> {
    let transport = super::create_transport(stream);
    let (mut tx, rx) = futures::StreamExt::split(transport);
    let limiter = governor::RateLimiter::direct(governor::Quota::per_second(rate));
    let mut rx = std::pin::pin!(rx.ratelimit_stream(&limiter));

    // Use this loop instead of `send_all()` to make sure the buffer is flushed
    // after each frame.
    while let Some(payload) = rx.next().await {
        let payload = payload.map_err(Error::Recv)?;
        tx.send(payload).await.map_err(Error::Send)?;
    }

    Ok(())
}
