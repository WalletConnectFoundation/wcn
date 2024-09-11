use {
    futures::{Future, Sink, StreamExt as _},
    pin_project::pin_project,
    serde::{Deserialize, Serialize},
    std::{convert::Infallible, io, pin::Pin, task},
    tokio_serde::Framed,
    tokio_serde_postcard::SymmetricalPostcard,
    tokio_stream::Stream,
    tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
};

/// Untyped bi-directional stream.
pub struct BiDirectionalStream {
    rx: RawRecvStream,
    tx: RawSendStream,
}

type RawSendStream = FramedWrite<quinn::SendStream, LengthDelimitedCodec>;
type RawRecvStream = FramedRead<quinn::RecvStream, LengthDelimitedCodec>;

impl BiDirectionalStream {
    pub fn new(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        Self {
            tx: FramedWrite::new(tx, LengthDelimitedCodec::new()),
            rx: FramedRead::new(rx, LengthDelimitedCodec::new()),
        }
    }

    pub fn upgrade<I, O>(self) -> (RecvStream<I>, SendStream<O>) {
        (
            RecvStream(Framed::new(self.rx, SymmetricalPostcard::default())),
            SendStream(Framed::new(self.tx, SymmetricalPostcard::default())),
        )
    }
}

/// [`Stream`] of outbound [`Message`]s.
#[pin_project]
pub struct SendStream<T>(#[pin] Framed<RawSendStream, T, T, SymmetricalPostcard<T>>);

impl<T: Serialize> Sink<T> for SendStream<T> {
    type Error = io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.project().0.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_close(cx)
    }
}

impl<T: Serialize + Unpin> SendStream<T> {
    /// Changes the type of this [`SendStream`].
    pub fn transmute<M>(self) -> SendStream<M> {
        SendStream(Framed::new(
            self.0.into_inner(),
            SymmetricalPostcard::default(),
        ))
    }

    /// Shut down the send stream gracefully.
    /// Completes when the peer has acknowledged all sent data.
    ///
    /// It's only required to call this if the [`RecvStream`] counterpart on the
    /// other side expects the [`Stream`] to be finished -- meaning to
    /// return `Poll::Ready(None)`.
    pub async fn finish(self) {
        let _ = self.0.into_inner().into_inner().finish().await;
    }
}

/// [`Stream`] of inbound [`Message`]s.
#[pin_project]
pub struct RecvStream<T>(#[pin] Framed<RawRecvStream, T, T, SymmetricalPostcard<T>>);

impl<T: for<'de> Deserialize<'de>> Stream for RecvStream<T> {
    type Item = io::Result<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<T> RecvStream<T>
where
    T: for<'de> Deserialize<'de> + Unpin,
{
    /// Tries to receive the next message from this [`RecvStream`].
    pub async fn recv_message(&mut self) -> TransportResult<T> {
        self.next()
            .await
            .ok_or_else(|| Error::StreamFinished)?
            .map_err(Into::into)
    }

    /// Changes the type of this [`RecvStream`].
    pub fn transmute<M>(self) -> RecvStream<M> {
        RecvStream(Framed::new(
            self.0.into_inner(),
            SymmetricalPostcard::default(),
        ))
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum Error {
    #[error("IO: {0:?}")]
    IO(io::ErrorKind),

    #[error("Stream unexpectedly finished")]
    StreamFinished,

    #[error("{_0}")]
    Other(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IO(err.kind())
    }
}

impl From<quinn::ConnectionError> for Error {
    fn from(err: quinn::ConnectionError) -> Self {
        Self::Other(format!("Connection: {err:?}"))
    }
}

pub type TransportResult<T> = Result<T, Error>;

/// Connection state before the [`Handshake`].
pub struct PendingConnection(pub(crate) quinn::Connection);

impl PendingConnection {
    /// Initiates the [`Handshake`].
    pub async fn initiate_handshake<Req, Resp>(
        &self,
    ) -> TransportResult<(RecvStream<Resp>, SendStream<Req>)> {
        let (tx, rx) = self.0.open_bi().await?;
        Ok(BiDirectionalStream::new(tx, rx).upgrade())
    }

    /// Accepts the [`Handshake`].
    pub async fn accept_handshake<Req, Resp>(
        &self,
    ) -> TransportResult<(RecvStream<Req>, SendStream<Resp>)> {
        let (tx, rx) = self.0.accept_bi().await?;
        Ok(BiDirectionalStream::new(tx, rx).upgrade())
    }
}

/// Application layer protocol specific handshake.
pub trait Handshake: Clone + Send + Sync + 'static {
    type Ok: Clone + Send + Sync + 'static;
    type Err: std::error::Error + Send;

    fn handle(
        &self,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send;
}

/// No-op [`Handshake`] implementation.
#[derive(Clone, Debug, Default)]
pub struct NoHandshake;

impl Handshake for NoHandshake {
    type Ok = ();
    type Err = Infallible;

    fn handle(
        &self,
        _: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async { Ok(()) }
    }
}
