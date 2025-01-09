use {
    crate::Message,
    futures::{stream::MapErr, Future, FutureExt as _, Sink, StreamExt as _, TryStreamExt},
    libp2p::PeerId,
    pin_project::pin_project,
    std::{
        convert::Infallible,
        io,
        pin::Pin,
        task::{self, ready},
    },
    tokio_serde::{formats::SymmetricalJson, Deserializer, Framed, Serializer},
    tokio_serde_postcard::SymmetricalPostcard,
    tokio_stream::Stream,
    tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
};

/// Serialization codec.
pub trait Codec: Send + Sync + 'static {
    type Serializer<T: Message>: Serializer<T, Error: Into<Error>>
        + Unpin
        + Default
        + Send
        + Sync
        + 'static;

    type Deserializer<T: Message>: Deserializer<T, Error: Into<Error>>
        + Unpin
        + Default
        + Send
        + Sync
        + 'static;
}

pub struct PostcardCodec;

impl Codec for PostcardCodec {
    type Serializer<T: Message> = SymmetricalPostcard<T>;
    type Deserializer<T: Message> = SymmetricalPostcard<T>;
}

pub struct JsonCodec;

impl Codec for JsonCodec {
    type Serializer<T: Message> = SymmetricalJson<T>;
    type Deserializer<T: Message> = SymmetricalJson<T>;
}

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

    pub fn upgrade<I: Message, O: Message, C: Codec>(self) -> (RecvStream<I, C>, SendStream<O, C>) {
        (
            RecvStream(Framed::new(
                self.rx.map_err(Into::into),
                C::Deserializer::default(),
            )),
            SendStream {
                inner: self.tx,
                codec: C::Serializer::default(),
            },
        )
    }
}

/// [`Stream`] of outbound [`Message`]s.
#[pin_project(project = SendStreamProj)]
pub struct SendStream<T: Message, C: Codec = PostcardCodec> {
    #[pin]
    inner: RawSendStream,
    #[pin]
    codec: C::Serializer<T>,
}

impl<T: Message> SendStream<T> {
    /// Waits until this [`SendStream`] is closed.
    pub async fn wait_closed(&mut self) {
        self.inner.get_mut().stopped().map(drop).await
    }
}

impl<T: Message, C: Codec> Sink<&T> for SendStream<T, C>
where
    C::Serializer<T>: Serializer<T>,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(mut self: Pin<&mut Self>, item: &T) -> Result<(), Self::Error> {
        let bytes = tokio_serde::Serializer::serialize(self.as_mut().project().codec, item)
            .map_err(Into::into)?;

        self.as_mut().project().inner.start_send(bytes)?;

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

impl<T: Message, C: Codec> Sink<T> for SendStream<T, C>
where
    C::Serializer<T>: Serializer<T>,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let bytes = tokio_serde::Serializer::serialize(self.as_mut().project().codec, &item)
            .map_err(Into::into)?;

        self.as_mut().project().inner.start_send(bytes)?;

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

/// [`Stream`] of inbound [`Message`]s.
#[pin_project]
pub struct RecvStream<T: Message, C: Codec = PostcardCodec>(
    #[allow(clippy::type_complexity)]
    #[pin]
    Framed<MapErr<RawRecvStream, fn(io::Error) -> Error>, T, T, C::Deserializer<T>>,
);

impl<T: Message, C: Codec> Stream for RecvStream<T, C>
where
    C::Deserializer<T>: Deserializer<T>,
{
    type Item = Result<T>;

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

impl<T: Message, C: Codec> RecvStream<T, C> {
    /// Tries to receive the next message from this [`RecvStream`].
    pub async fn recv_message(&mut self) -> Result<T> {
        self.next()
            .await
            .ok_or(Error::StreamFinished)?
            .map_err(Into::into)
    }

    /// Changes the type of this [`RecvStream`].
    pub fn transmute<M: Message>(self) -> RecvStream<M> {
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

    #[error("Codec: {_0}")]
    Codec(String),

    #[error("{_0}")]
    Other(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Codec(err.to_string())
    }
}

impl Error {
    pub fn as_str(&self) -> &'static str {
        use io::ErrorKind as IO;

        match self {
            Self::IO(IO::ConnectionRefused) => "connection_refused",
            Self::IO(IO::ConnectionReset) => "connection_reset",
            Self::IO(IO::ConnectionAborted) => "connection_aborted",
            Self::IO(IO::NotConnected) => "not_connected",
            Self::IO(IO::BrokenPipe) => "broken_pipe",
            Self::IO(IO::InvalidData) => "invalid_data",
            Self::IO(_) => "io",
            Self::StreamFinished => "stream_finished",
            Self::Codec(_) => "codec",
            Self::Other(_) => "other_transport",
        }
    }
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

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Connection state before the [`Handshake`].
pub struct PendingConnection(pub(crate) quinn::Connection);

impl PendingConnection {
    /// Initiates the [`Handshake`].
    pub async fn initiate_handshake<Req: Message, Resp: Message>(
        &self,
    ) -> Result<(RecvStream<Resp>, SendStream<Req>)> {
        let (tx, rx) = self.0.open_bi().await?;
        Ok(BiDirectionalStream::new(tx, rx).upgrade())
    }

    /// Accepts the [`Handshake`].
    pub async fn accept_handshake<Req: Message, Resp: Message>(
        &self,
    ) -> Result<(RecvStream<Req>, SendStream<Resp>)> {
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
        peer_id: PeerId,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send;
}

pub type HandshakeData<H> = <H as Handshake>::Ok;

/// No-op [`Handshake`] implementation.
#[derive(Clone, Debug, Default)]
pub struct NoHandshake;

impl Handshake for NoHandshake {
    type Ok = ();
    type Err = Infallible;

    fn handle(
        &self,
        _: PeerId,
        _: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async { Ok(()) }
    }
}

// Makes sure that an error serialized under different `Result` types has the
// same byte representation.
#[test]
fn test_result_serialization() {
    use tokio_serde::Serializer as _;

    type Result1 = crate::Result<()>;
    type Result2 = crate::Result<u8>;

    let err = crate::Error::new("test");

    let res1: Result1 = Err(err.clone());
    let res2: Result2 = Err(err);

    let bytes1 = Pin::new(&mut SymmetricalPostcard::<Result1>::new())
        .serialize(&res1)
        .unwrap();

    let bytes2 = Pin::new(&mut SymmetricalPostcard::<Result2>::new())
        .serialize(&res2)
        .unwrap();

    assert_eq!(bytes1, bytes2);
}
