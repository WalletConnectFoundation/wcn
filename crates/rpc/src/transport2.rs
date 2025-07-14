use {
    crate::{BorrowedMessage, MessageV2},
    bytes::{BufMut as _, Bytes, BytesMut},
    serde::{Deserialize, Serialize},
    std::{error::Error as StdError, io, pin::Pin},
    tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec},
};

/// Serialization codec.
pub trait Codec<M: MessageV2>: Default + Serializer<M> + Deserializer<M> {}

impl<M: MessageV2, C> Codec<M> for C where C: Default + Serializer<M> + Deserializer<M> {}

pub trait Serializer<M: BorrowedMessage>:
    tokio_serde::Serializer<M, Error: StdError> + Unpin + Send + Sync + 'static
{
}

pub trait Deserializer<M: MessageV2>:
    tokio_serde::Deserializer<M, Error: StdError> + Unpin + Send + Sync + 'static
{
}

impl<M: BorrowedMessage, S> Serializer<M> for S where
    S: tokio_serde::Serializer<M, Error: StdError> + Unpin + Send + Sync + 'static
{
}

impl<M: MessageV2, S> Deserializer<M> for S where
    S: tokio_serde::Deserializer<M, Error: StdError> + Unpin + Send + Sync + 'static
{
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl<T> tokio_serde::Deserializer<T> for JsonCodec
where
    for<'a> T: Deserialize<'a>,
{
    type Error = serde_json::Error;

    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<T, Self::Error> {
        serde_json::from_slice(src)
    }
}

impl<T> tokio_serde::Serializer<T> for JsonCodec
where
    T: Serialize,
{
    type Error = serde_json::Error;

    fn serialize(self: Pin<&mut Self>, data: &T) -> Result<Bytes, Self::Error> {
        serde_json::to_vec(data).map(Into::into)
    }
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
pub(crate) struct BiDirectionalStream {
    pub rx: RecvStream,
    pub tx: SendStream,
}

pub(crate) type SendStream = FramedWrite<quinn::SendStream, LengthDelimitedCodec>;
pub(crate) type RecvStream = FramedRead<quinn::RecvStream, LengthDelimitedCodec>;

impl BiDirectionalStream {
    pub fn new(tx: quinn::SendStream, rx: quinn::RecvStream) -> Self {
        Self {
            tx: FramedWrite::new(tx, LengthDelimitedCodec::new()),
            rx: FramedRead::new(rx, LengthDelimitedCodec::new()),
        }
    }
}
