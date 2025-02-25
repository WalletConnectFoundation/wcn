use {
    serde::{Deserialize, Serialize},
    std::io,
    tokio::net::TcpStream,
    tokio_util::codec::LengthDelimitedCodec,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod server;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect: {0}")]
    Connection(io::Error),

    #[error("Failed to create listener: {0}")]
    Listener(io::Error),

    #[error("Failed to receive data: {0}")]
    Recv(io::Error),

    #[error("Failed to send data: {0}")]
    Send(io::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoPayload {
    #[serde(with = "time::serde::timestamp")]
    timestamp: time::OffsetDateTime,
}

impl EchoPayload {
    fn new() -> Self {
        Self {
            timestamp: time::OffsetDateTime::now_utc(),
        }
    }

    fn elapsed(&self) -> time::Duration {
        time::OffsetDateTime::now_utc() - self.timestamp
    }
}

type Transport<T> = tokio_serde::Framed<
    tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec>,
    T,
    T,
    tokio_serde_postcard::Postcard<T, T>,
>;

fn create_transport(stream: TcpStream) -> Transport<EchoPayload> {
    let transport = tokio_util::codec::Framed::new(stream, LengthDelimitedCodec::default());
    tokio_serde::Framed::new(transport, tokio_serde_postcard::Postcard::default())
}
