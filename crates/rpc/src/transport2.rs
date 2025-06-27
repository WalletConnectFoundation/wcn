use {
    crate::MessageV2,
    bytes::{BufMut as _, Bytes, BytesMut},
    futures::{stream::MapErr, Sink, TryStreamExt},
    pin_project::pin_project,
    serde::{Deserialize, Serialize},
    std::{
        io,
        pin::Pin,
        task::{self, ready},
    },
    tokio_serde::Framed,
    tokio_stream::Stream,
    tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
};

/// Serialization codec.
pub trait Codec<M: MessageV2>:
    for<'a> Serializer<M::Borrowed<'a>> + Serializer<M> + Deserializer<M>
{
}

impl<M, C> Codec<M> for C
where
    M: MessageV2,
    C: for<'a> Serializer<M::Borrowed<'a>> + Serializer<M> + Deserializer<M>,
{
}

pub trait Serializer<T>:
    tokio_serde::Serializer<T, Error: Into<Error>> + Unpin + Default + Send + Sync + 'static
{
}

impl<T, S> Serializer<T> for S where
    S: tokio_serde::Serializer<T, Error: Into<Error>> + Unpin + Default + Send + Sync + 'static
{
}

pub trait Deserializer<T>:
    tokio_serde::Deserializer<T, Error: Into<Error>> + Unpin + Default + Send + Sync + 'static
{
}

impl<T, D> Deserializer<T> for D where
    D: tokio_serde::Deserializer<T, Error: Into<Error>> + Unpin + Default + Send + Sync + 'static
{
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PostcardCodec;

impl<T> tokio_serde::Deserializer<T> for PostcardCodec
where
    for<'a> T: Deserialize<'a>,
{
    type Error = io::Error;

    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<T, Self::Error> {
        postcard::from_bytes(src).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

impl<T> tokio_serde::Serializer<T> for PostcardCodec
where
    T: Serialize,
{
    type Error = io::Error;

    fn serialize(self: Pin<&mut Self>, data: &T) -> Result<Bytes, Self::Error> {
        postcard::experimental::serialized_size(data)
            .and_then(|size| postcard::to_io(data, BytesMut::with_capacity(size).writer()))
            .map(|writer| writer.into_inner().freeze())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

/// Untyped bi-directional stream.
pub struct BiDirectionalStream {
    pub(crate) rx: RawRecvStream,
    pub(crate) tx: RawSendStream,
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

    pub(crate) fn upgrade<I: MessageV2, C: Deserializer<I>>(
        self,
    ) -> (RecvStream<I, C>, SendStream<C>) {
        (
            RecvStream(Framed::new(self.rx.map_err(Into::into), C::default())),
            SendStream {
                inner: self.tx,
                codec: C::default(),
            },
        )
    }
}

/// [`Stream`] of outbound [Message][`MessageV2`]s.
#[pin_project(project = SendStreamProj)]
pub struct SendStream<C> {
    #[pin]
    inner: RawSendStream,
    #[pin]
    codec: C,
}

impl<T, C> Sink<&T> for SendStream<C>
where
    C: Serializer<T, Error: Into<Error>>,
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

/// [`Stream`] of inbound [Message][`MessageV2`]s.
#[pin_project]
pub struct RecvStream<T: MessageV2, C: Deserializer<T>>(
    #[allow(clippy::type_complexity)]
    #[pin]
    Framed<MapErr<RawRecvStream, fn(io::Error) -> Error>, T, T, C>,
);

impl<T: MessageV2, C: Deserializer<T>> Stream for RecvStream<T, C> {
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
