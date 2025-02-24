use {
    crate::Error,
    futures::{FutureExt as _, SinkExt},
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tap::TapFallible,
    tokio::{
        net::TcpStream,
        sync::{OwnedSemaphorePermit, Semaphore},
    },
    tokio_stream::StreamExt as _,
    wc::{
        future::{CancellationToken, FutureExt, StaticFutureExt},
        metrics,
    },
};

#[derive(Clone, Debug)]
pub struct Config {
    pub address: SocketAddr,
    pub max_connections: usize,

    /// Maximum requests per second.
    pub max_rate: usize,
}

pub async fn spawn(config: Config) -> Result<(), Error> {
    let semaphore = Arc::new(Semaphore::new(config.max_connections));

    let listener = tokio::net::TcpListener::bind(config.address)
        .await
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
            .spawn();
    }
}

async fn connection_handler(
    stream: TcpStream,
    rate: usize,
    _permit: OwnedSemaphorePermit,
) -> Result<(), Error> {
    let transport = super::create_transport(stream);

    let (mut tx, rx) = futures::StreamExt::split(transport);
    let mut rx = std::pin::pin!(rx.throttle(Duration::from_millis(1000 / rate as u64)));

    while let Some(payload) = rx.next().await {
        let payload = payload.map_err(Error::Recv)?;
        tx.send(payload).await.map_err(Error::Send)?;
    }

    Ok(())
}
