use std::{future::Future, net::SocketAddr, time::Duration};

pub trait TransportFactory {
    type Transport: Transport;

    fn address(&self) -> SocketAddr;

    fn create(
        &self,
    ) -> impl Future<Output = Result<Self::Transport, <Self::Transport as Transport>::Error>> + Send;
}

pub trait Transport: Send + Sync {
    type Error: std::error::Error;

    fn heartbeat(&self) -> impl Future<Output = Result<Duration, Self::Error>> + Send;
}

pub struct EchoApiTransport(echo_api::client::Client);

impl Transport for EchoApiTransport {
    type Error = echo_api::Error;

    fn heartbeat(&self) -> impl Future<Output = Result<Duration, Self::Error>> {
        self.0.heartbeat()
    }
}

pub struct EchoApiTransportFactory(pub SocketAddr);

impl TransportFactory for EchoApiTransportFactory {
    type Transport = EchoApiTransport;

    fn address(&self) -> SocketAddr {
        self.0
    }

    async fn create(&self) -> Result<EchoApiTransport, echo_api::Error> {
        echo_api::client::Client::create(self.0)
            .await
            .map(EchoApiTransport)
    }
}
