pub use wcn_rpc::client2::Config;
use {
    super::*,
    futures::{Stream, StreamExt},
    wcn_rpc::{
        Request,
        Response,
        client2::{Client, Connection, ConnectionHandler, RpcHandler},
    },
};

pub type Cluster = Client<ClusterApi>;

/// Creates a new [`ClusterApi`] RPC client.
pub fn new(config: Config) -> wcn_rpc::client2::Result<Cluster> {
    Client::new(config, ConnectionHandler)
}

impl wcn_rpc::client2::Api for ClusterApi {
    type ConnectionParameters = ();
    type ConnectionHandler = ConnectionHandler;
    type RpcHandler = RpcHandler;
}

impl crate::ClusterApi for Connection<ClusterApi> {
    async fn address(&self) -> crate::Result<Address> {
        Ok(self
            .send::<GetAddress, Request<GetAddress>, Response<GetAddress>>(&())?
            .await??
            .into_inner())
    }

    async fn cluster_view(&self) -> crate::Result<ClusterView> {
        Ok(self
            .send::<GetClusterView, Request<GetClusterView>, Response<GetClusterView>>(&())?
            .await??
            .into_inner())
    }

    async fn events(
        &self,
    ) -> crate::Result<impl Stream<Item = crate::Result<wcn_cluster::Event>> + Send + 'static> {
        let (resp, stream) = self.send_streaming::<GetEventStream>(&())?.await?;

        resp?;

        Ok(stream.map(|res| {
            res.map_err(crate::Error::transport)?
                .map(MessageWrapper::into_inner)
                .map_err(crate::Error::transport)
        }))
    }
}

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(crate::ErrorKind::Transport)
            .with_message(format!("wcn_cluster_api::client::Error: {err}"))
    }
}
