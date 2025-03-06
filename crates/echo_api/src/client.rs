use {
    crate::{EchoPayload, Error},
    futures::{SinkExt, StreamExt},
    std::{collections::VecDeque, future::Future, net::SocketAddr, time::Duration},
    tap::TapFallible as _,
    tokio::{
        net::{TcpSocket, TcpStream},
        sync::{mpsc, oneshot, Mutex},
    },
    tokio_util::sync::DropGuard,
    wc::{
        future::{CancellationToken, FutureExt as _, StaticFutureExt as _},
        metrics::{self, FutureExt as _},
    },
};

const OUTSTANDING_HEARTBEAT_CAP: usize = 15;

#[derive(Debug, thiserror::Error)]
#[error("Client channel closed")]
struct ClientChannelClosed;

pub struct Client {
    tx: mpsc::Sender<oneshot::Sender<Result<Duration, Error>>>,
    _guard: DropGuard,
}

impl Client {
    pub async fn create(address: SocketAddr) -> Result<Self, Error> {
        let token = CancellationToken::new();
        let socket = TcpSocket::new_v4().map_err(Error::Connection)?;
        let _ = socket
            .set_nodelay(true)
            .tap_err(|err| tracing::warn!(?err, "failed to set TCP_NODELAY"));
        let stream = socket.connect(address).await.map_err(Error::Connection)?;
        let (tx, rx) = mpsc::channel(OUTSTANDING_HEARTBEAT_CAP);

        ping_loop(stream, rx)
            .with_cancellation(token.clone())
            .with_metrics(metrics::future_metrics!("wcn_echo_client_ping_loop"))
            .spawn();

        Ok(Self {
            tx,
            _guard: token.drop_guard(),
        })
    }

    pub fn heartbeat(&self) -> impl Future<Output = Result<Duration, Error>> + Send {
        let (tx, rx) = oneshot::channel();
        let res = self.tx.try_send(tx);

        async move {
            if res.is_err() {
                Err(Error::TooManyRequests)
            } else {
                rx.await
                    .map_err(|_| Error::Other(ClientChannelClosed.to_string()))?
            }
        }
    }
}

async fn ping_loop(
    stream: TcpStream,
    mut pulse_rx: mpsc::Receiver<oneshot::Sender<Result<Duration, Error>>>,
) {
    // TCP responses arrive in the same order, so just use a simple ring buffer.
    let responses = Mutex::new(VecDeque::with_capacity(OUTSTANDING_HEARTBEAT_CAP));
    let (mut tx, mut rx) = super::create_transport(stream).split();

    let tx_loop = async {
        while let Some(resp) = pulse_rx.recv().await {
            let mut responses = responses.lock().await;

            if responses.len() >= OUTSTANDING_HEARTBEAT_CAP {
                let _ = resp.send(Err(Error::TooManyRequests));
                break;
            }

            if let Err(err) = tx.send(EchoPayload::new()).await {
                let _ = resp.send(Err(Error::Send(err)));
                break;
            }

            responses.push_back(resp);
        }
    };

    let rx_loop = async {
        while let Some(payload) = rx.next().await {
            let Some(resp) = responses.lock().await.pop_front() else {
                break;
            };

            let result = payload
                .map(|payload| payload.elapsed().try_into().unwrap_or_default())
                .map_err(Error::Recv);

            let _ = resp.send(result);
        }
    };

    tokio::select! {
        _ = tx_loop => {},
        _ = rx_loop => {},
    }
}
