use {
    super::Error,
    crate::{
        client::{self, OutboundConnectionError, OutboundConnectionResult},
        tcp::connection_error_kind,
        ConnectionHeader,
    },
    derive_more::From,
    futures::{
        io::{ReadHalf, WriteHalf},
        AsyncReadExt,
    },
    libp2p::{identity::Keypair, Multiaddr},
    std::{
        future::{self, Future},
        io,
        pin::pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
        time::Duration,
    },
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::{TcpSocket, TcpStream},
        sync::{mpsc, oneshot},
    },
    tokio_rustls::{client::TlsStream, TlsConnector},
    tokio_util::compat::{
        Compat,
        FuturesAsyncReadCompatExt,
        FuturesAsyncWriteCompatExt,
        TokioAsyncReadCompatExt,
    },
    wc::future::FutureExt,
};

#[derive(Clone, Debug)]
pub struct Connector {
    tls_config: Arc<rustls::ClientConfig>,
    next_conn_id: Arc<AtomicUsize>,
}

impl Connector {
    pub fn new(keypair: Keypair) -> Result<Self, Error> {
        let tls_config = libp2p_tls::make_client_config(&keypair, None)
            .map_err(|err| Error::Tls(err.to_string()))?;

        Ok(Connector {
            tls_config: Arc::new(tls_config),
            next_conn_id: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl client::Connector for Connector {
    type Connection = OutboundConnection;

    fn connect(
        &self,
        multiaddr: &Multiaddr,
        header: ConnectionHeader,
    ) -> impl Future<Output = OutboundConnectionResult<Self::Connection>> {
        async move {
            let socket =
                TcpSocket::from_std_stream(std::net::TcpStream::from(super::new_socket()?));

            let addr = super::try_multiaddr_to_socketaddr(multiaddr)
                .ok_or(OutboundConnectionError::InvalidMultiaddr)?;

            let stream = socket.connect(addr).await?;

            let connector = TlsConnector::from(self.tls_config.clone());

            let mut stream = connector.connect(addr.ip().into(), stream).await?;

            header.write(&mut stream).await?;

            let cfg = yamux::Config::default();

            let conn = yamux::Connection::new(stream.compat(), cfg, yamux::Mode::Client);

            let (tx, rx) = mpsc::channel(1000);

            Ok(OutboundConnection {
                id: self.next_conn_id.fetch_add(1, Ordering::Relaxed),
                cmds: tx,
                task_handle: Arc::new(Some(tokio::spawn(OutboundConnection::drive(rx, conn)))),
            })
        }
    }
}

enum ConnectionCmd {
    EstablishStream(oneshot::Sender<yamux::Result<yamux::Stream>>),
}

type YamuxConnection = yamux::Connection<Compat<TlsStream<TcpStream>>>;

#[derive(Clone)]
pub struct OutboundConnection {
    id: usize,
    cmds: mpsc::Sender<ConnectionCmd>,
    task_handle: Arc<Option<tokio::task::JoinHandle<()>>>,
}

impl OutboundConnection {
    async fn drive(mut cmds: mpsc::Receiver<ConnectionCmd>, mut conn: YamuxConnection) {
        let mut pending_cmd = None;
        let mut keep_alive_fut = pin!(None);

        future::poll_fn::<(), _>(|cx| {
            if pending_cmd.is_none() {
                if let Poll::Ready(Some(cmd)) = cmds.poll_recv(cx) {
                    cx.waker().wake_by_ref();
                    pending_cmd = Some(cmd);
                }
            }

            if pending_cmd.is_some() {
                if let Poll::Ready(res) = conn.poll_new_outbound(cx) {
                    cx.waker().wake_by_ref();
                    if let Some(ConnectionCmd::EstablishStream(tx)) = pending_cmd.take() {
                        let _ = tx.send(res);
                    }
                }
            }

            // We use the first server stream as a keep alive transport.
            // There shouldn't be any other streams, but the `poll_next_inbound` needs to be
            // polled in order to drive the connection.
            match conn.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(stream))) => {
                    cx.waker().wake_by_ref();

                    if keep_alive_fut.is_none() {
                        keep_alive_fut.set(Some(Self::keep_alive(stream)));
                    }
                }
                Poll::Ready(Some(Err(_)) | None) => return Poll::Ready(()),
                Poll::Pending => {}
            };

            // If the keep alive future finishes - the connection is dead.
            if let Some(fut) = keep_alive_fut.as_mut().as_pin_mut() {
                if Future::poll(fut, cx).is_ready() {
                    return Poll::Ready(());
                }
            }

            Poll::Pending
        })
        .await
    }

    async fn keep_alive(stream: yamux::Stream) -> io::Result<()> {
        let mut stream = stream.compat_write();

        loop {
            async {
                let code = stream.read_u64().await?;
                stream.write_u64(code).await
            }
            .with_timeout(Duration::from_secs(2))
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, err))??;
        }
    }
}

impl Drop for OutboundConnection {
    fn drop(&mut self) {
        if let Some(handle) = Arc::get_mut(&mut self.task_handle).and_then(Option::take) {
            handle.abort();
        }
    }
}

impl client::OutboundConnection for OutboundConnection {
    type Read = Compat<ReadHalf<yamux::Stream>>;
    type Write = Compat<WriteHalf<yamux::Stream>>;

    fn id(&self) -> usize {
        self.id
    }

    fn establish_stream(
        &self,
    ) -> impl Future<Output = OutboundConnectionResult<(Self::Read, Self::Write)>> + Send {
        async {
            let (tx, rx) = oneshot::channel();

            let err = || OutboundConnectionError::other("connection_task_stopped");

            self.cmds
                .send(ConnectionCmd::EstablishStream(tx))
                .await
                .map_err(|_| err())?;

            let stream = rx.await.map_err(|_| err())??;

            let (rx, tx) = stream.split();

            Ok((rx.compat(), tx.compat_write()))
        }
    }
}

impl From<yamux::ConnectionError> for OutboundConnectionError {
    fn from(err: yamux::ConnectionError) -> Self {
        use yamux::ConnectionError as Err;

        let kind = match &err {
            Err::Io(err) => return Self::IO(err.kind()),
            other => connection_error_kind(other),
        };

        OutboundConnectionError::Other {
            kind,
            details: err.to_string(),
        }
    }
}
