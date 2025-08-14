pub use wcn_rpc::client2::Config;
use {
    super::*,
    futures::{Stream, StreamExt},
    wcn_rpc::client2::{Client, Connection, Outbound},
};

/// RPC [`Client`] of [`ClusterApi`].
pub type Cluster = Client<ClusterApi>;

/// Outbound [`Connection`] to [`ClusterApi`].
pub type ClusterConnection = Connection<ClusterApi>;

impl wcn_rpc::client2::Api for ClusterApi {
    type ConnectionParameters = ();
}

impl crate::ClusterApi for Connection<ClusterApi> {
    async fn address(&self) -> crate::Result<Address> {
        Ok(GetAddress::send_request(self, &()).await??.into_inner())
    }

    async fn cluster_view(&self) -> crate::Result<ClusterView> {
        Ok(GetClusterView::send_request(self, &()).await??.into_inner())
    }

    async fn events(
        &self,
    ) -> crate::Result<impl Stream<Item = crate::Result<wcn_cluster::Event>> + Send + use<>> {
        Ok(GetEventStream::send(self, |rpc: Outbound<_, _>| async {
            let mut stream = rpc.response_stream;

            if let Err(err) = stream.try_next_downcast::<Result<()>>().await? {
                return Ok(Err(err));
            }

            Ok(Ok(stream
                .map_downcast::<Result<wcn_cluster::Event>>()
                .map(|res| res?.map_err(Into::into))))
        })
        .await??)
    }
}

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(crate::ErrorKind::Transport)
            .with_message(format!("wcn_cluster_api::client::Error: {err}"))
    }
}
