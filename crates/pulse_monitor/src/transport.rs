use {
    std::{future::Future, time::Duration},
    wcn_rpc::PeerAddr,
};

pub trait TransportFactory {
    type Transport: Transport;

    fn address(&self) -> PeerAddr;

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

pub(crate) struct EchoApiTransportFactory(pub PeerAddr);

impl TransportFactory for EchoApiTransportFactory {
    type Transport = EchoApiTransport;

    fn address(&self) -> PeerAddr {
        self.0.clone()
    }

    async fn create(&self) -> Result<EchoApiTransport, echo_api::Error> {
        // Echo server is currently hosted on the same address we're using for storage
        // API, but it's TCP instead of UDP.
        let socketaddr = self
            .0
            .quic_socketaddr()
            .map_err(|err| echo_api::Error::Other(err.to_string()))?;

        echo_api::client::Client::create(socketaddr)
            .await
            .map(EchoApiTransport)
    }
}

pub(crate) struct PulseApiTransport(pulse_api::Client);

impl Transport for PulseApiTransport {
    type Error = pulse_api::client::Error;

    fn heartbeat(&self) -> impl Future<Output = Result<Duration, Self::Error>> {
        self.0.heartbeat()
    }
}

pub(crate) struct PulseApiTransportFactory {
    pub addr: PeerAddr,
    pub client: pulse_api::Client,
}

impl TransportFactory for PulseApiTransportFactory {
    type Transport = PulseApiTransport;

    fn address(&self) -> PeerAddr {
        self.addr.clone()
    }

    async fn create(&self) -> Result<PulseApiTransport, pulse_api::client::Error> {
        Ok(PulseApiTransport(self.client.clone()))
    }
}
