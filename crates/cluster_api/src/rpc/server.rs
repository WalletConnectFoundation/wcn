use {
    crate::{
        ClusterApi,
        rpc::{GetAddress, GetClusterView, GetEventStream, Id as RpcId, MessageWrapper},
    },
    futures::{Stream, TryStreamExt as _},
    wcn_rpc::{
        Request,
        Response,
        StreamItem,
        server2::{Connection, HandleConnection, HandleRequest, HandleStreamingRequest, Result},
    },
};

pub fn new(cluster_api: impl ClusterApi) -> impl wcn_rpc::server2::Server {
    wcn_rpc::server2::new(ConnectionHandler {
        rpc_handler: RpcHandler { cluster_api },
    })
}

#[derive(Clone)]
struct ConnectionHandler<S: ClusterApi> {
    rpc_handler: RpcHandler<S>,
}

impl<S> HandleConnection for ConnectionHandler<S>
where
    S: ClusterApi,
{
    type Api = super::ClusterApi;

    async fn handle_connection(&self, conn: Connection<'_, Self::Api>) -> Result<()> {
        conn.handle(&self.rpc_handler, |rpc, handler| async move {
            match rpc.id() {
                RpcId::GetAddress => rpc.handle::<GetAddress>(&handler).await,
                RpcId::GetClusterView => rpc.handle::<GetClusterView>(&handler).await,
                RpcId::GetEventStream => rpc.handle::<GetEventStream>(&handler).await,
            }
        })
        .await
    }
}

#[derive(Clone)]
struct RpcHandler<S: ClusterApi> {
    cluster_api: S,
}

impl<S: ClusterApi> HandleRequest<GetAddress> for RpcHandler<S> {
    async fn handle_request(&self, _: Request<GetAddress>) -> Response<GetAddress> {
        self.cluster_api
            .address()
            .await
            .map(MessageWrapper)
            .map_err(Into::into)
    }
}

impl<S: ClusterApi> HandleRequest<GetClusterView> for RpcHandler<S> {
    async fn handle_request(&self, _: Request<GetClusterView>) -> Response<GetClusterView> {
        self.cluster_api
            .cluster_view()
            .await
            .map(MessageWrapper)
            .map_err(Into::into)
    }
}

impl<S: ClusterApi> HandleStreamingRequest<GetEventStream> for RpcHandler<S> {
    async fn handle_request(
        &self,
        _: Request<GetEventStream>,
    ) -> (
        Response<GetEventStream>,
        Option<impl Stream<Item = StreamItem<GetEventStream>> + Send>,
    ) {
        match self.cluster_api.events().await {
            Ok(stream) => {
                let stream = stream.map_ok(MessageWrapper).map_err(Into::into);

                (Ok(()), Some(stream))
            }

            Err(err) => (Err(super::Error::from(err)), None),
        }
    }
}
