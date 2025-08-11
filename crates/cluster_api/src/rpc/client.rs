pub use wcn_rpc::client2::Config;
use {
    super::*,
    futures::{Stream, StreamExt},
    wcn_rpc::client2::{Client, Connection, ConnectionHandler},
};

/// RPC [`Client`] of [`ClusterApi`].
pub type Cluster = Client<ClusterApi>;

/// Outbound [`Connection`] to [`ClusterApi`].
pub type ClusterConnection = Connection<ClusterApi>;

/// Creates a new [`ClusterApi`] RPC client.
pub fn new(config: Config) -> crate::Result<Cluster> {
    Client::new(config, ConnectionHandler).map_err(Into::into)
}

impl wcn_rpc::client2::Api for ClusterApi {
    type ConnectionParameters = ();
    type ConnectionHandler = ConnectionHandler;
}

impl crate::ClusterApi for Connection<ClusterApi> {
    async fn address(&self) -> crate::Result<Address> {
        Ok(GetAddress::send(self, &()).await??.into_inner())
    }

    async fn cluster_view(&self) -> crate::Result<ClusterView> {
        Ok(GetClusterView::send(self, &()).await??.into_inner())
    }

    async fn events(
        &self,
    ) -> crate::Result<impl Stream<Item = crate::Result<wcn_cluster::Event>> + Send + use<>> {
        let mut stream = self.send::<GetEventStream>()?.response_stream;

        stream.try_next_downcast::<Result<()>>().await??;

        Ok(stream
            .map_downcast::<Result<wcn_cluster::Event>>()
            .map(|res| res?.map_err(Into::into)))
    }
}

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(crate::ErrorKind::Transport)
            .with_message(format!("wcn_cluster_api::client::Error: {err}"))
    }
}
