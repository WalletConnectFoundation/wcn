use {
    crate::{EchoPayload, Error},
    futures::{SinkExt, StreamExt},
    phi_accrual_failure_detector::{Detector as _, SyncDetector},
    std::{
        net::{IpAddr, SocketAddr},
        time::Duration,
    },
    tokio::net::TcpSocket,
    tokio_util::sync::DropGuard,
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt as _},
        metrics::{self, FutureExt as _, StringLabel},
    },
};

#[allow(dead_code)]
pub struct Handle(DropGuard);

pub fn spawn(address: IpAddr) -> Handle {
    let token = CancellationToken::new();
    let guard = token.clone().drop_guard();
    let address = SocketAddr::new(address, crate::SERVER_PORT);

    ping_loop(address)
        .with_cancellation(token)
        .with_metrics(metrics::future_metrics!("wcn_echo_client_ping_loop"))
        .spawn();

    Handle(guard)
}

async fn ping_loop(addr: SocketAddr) {
    let detector = SyncDetector::default();
    let addr_str = addr.to_string();

    let conn_loop = async {
        loop {
            // Retry broken connections.
            if ping_loop_internal(addr, &detector).await.is_err() {
                metrics::counter!("wcn_echo_client_connection_failure", StringLabel<"destination"> => &addr_str).increment(1);
            }

            // Added delay before retrying connection.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    };

    let stats_loop = async {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            // Failure detector can't calculate suspicion level unless it's received at
            // least one heartbeat.
            if detector.is_monitoring() {
                metrics::gauge!("wcn_echo_client_failure_suspicion", StringLabel<"destination"> => &addr_str)
                    .set(detector.phi());
            }
        }
    };

    tokio::join!(conn_loop, stats_loop);
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
            tx.send(EchoPayload::new()).await.map_err(Error::Send)?;
        }
    };

    let addr = addr.to_string();

    let rx_loop = async {
        while let Some(payload) = rx.next().await {
            let payload = payload.map_err(Error::Recv)?;
            metrics::histogram!("wcn_echo_client_latency", StringLabel<"destination"> => &addr)
                .record(payload.elapsed().as_seconds_f64());
            detector.heartbeat();
        }

        Ok(())
    };

    tokio::select! {
        res = tx_loop => res,
        res = rx_loop => res,
    }
}
