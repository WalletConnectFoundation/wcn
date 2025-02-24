use {
    crate::{EchoPayload, Error},
    futures::{SinkExt, StreamExt},
    phi_accrual_failure_detector::{Detector as _, SyncDetector},
    std::{net::SocketAddr, time::Duration},
    time::OffsetDateTime,
    tokio::net::TcpSocket,
    tokio_util::sync::DropGuard,
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt as _},
        metrics::{self, FutureExt as _, StringLabel},
    },
};

pub struct Client {
    _guard: DropGuard,
}

impl Client {
    pub fn new(address: SocketAddr) -> Self {
        let token = CancellationToken::new();
        let _guard = token.clone().drop_guard();

        ping_loop(address)
            .with_cancellation(token)
            .with_metrics(metrics::future_metrics!("wcn_echo_client_ping_loop"))
            .spawn();

        Self { _guard }
    }
}

async fn ping_loop(addr: SocketAddr) -> Result<(), Error> {
    let detector = SyncDetector::default();

    loop {
        // Retry broken connections.
        if let Err(err) = ping_loop_internal(addr, &detector).await {
            tracing::warn!(?err, "ping loop ended with error");
        }

        // Added delay before retrying connection.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn ping_loop_internal(addr: SocketAddr, detector: &SyncDetector) -> Result<(), Error> {
    let stream = TcpSocket::new_v4()
        .map_err(Error::Connection)?
        .connect(addr)
        .await
        .map_err(Error::Connection)?;

    let (mut tx, mut rx) = super::create_transport(stream).split();

    let tx_loop = async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            tx.send(EchoPayload {
                seq_num: 0,
                timestamp: time::OffsetDateTime::now_utc(),
            })
            .await
            .map_err(Error::Send)?;
        }
    };

    let addr = addr.to_string();

    let rx_loop = async {
        while let Some(payload) = rx.next().await {
            let payload = payload.map_err(Error::Recv)?;
            let latency = (OffsetDateTime::now_utc() - payload.timestamp).as_seconds_f64();

            metrics::histogram!("wcn_echo_client_latency", StringLabel<"destination"> => &addr)
                .record(latency);
            detector.heartbeat();
        }

        Ok(())
    };

    let stats_loop = async {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            metrics::gauge!("wcn_echo_client_failure_suspicion", StringLabel<"destination"> => &addr)
                .set(detector.phi());
        }
    };

    tokio::select! {
        res = tx_loop => res,
        res = rx_loop => res,
        res = stats_loop => res,
    }
}
