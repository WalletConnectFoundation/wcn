use {
    super::Error,
    crate::rpc::{GetAddress, GetClusterView, GetEventStream, Id as RpcId, MessageWrapper},
    derive_where::derive_where,
    futures::{Stream, TryStreamExt as _},
    std::sync::Arc,
    wcn_cluster::smart_contract::{self, Read},
    wcn_rpc::{
        Request,
        Response,
        StreamItem,
        server2::{
            Connection,
            HandleConnection,
            HandleRequest,
            HandleStreamingRequest,
            Result,
            Server,
        },
    },
};

pub fn new(smart_contract: impl Read) -> impl Server {
    wcn_rpc::server2::new(ConnectionHandler {
        rpc_handler: Arc::new(RpcHandler { smart_contract }),
    })
}

#[derive_where(Clone)]
struct ConnectionHandler<S: Read> {
    rpc_handler: Arc<RpcHandler<S>>,
}

impl<S> HandleConnection for ConnectionHandler<S>
where
    S: Read,
{
    type Api = super::ClusterApi;

    async fn handle_connection(&self, conn: Connection<'_, Self::Api>) -> Result<()> {
        conn.handle(&self.rpc_handler, |rpc, handler| async move {
            let handler = handler.as_ref();

            match rpc.id() {
                RpcId::GetAddress => rpc.handle::<GetAddress>(handler).await,
                RpcId::GetClusterView => rpc.handle::<GetClusterView>(handler).await,
                RpcId::GetEventStream => rpc.handle::<GetEventStream>(handler).await,
            }
        })
        .await
    }
}

#[derive(Clone)]
struct RpcHandler<S: smart_contract::Read> {
    smart_contract: S,
}

impl<S: Read> HandleRequest<GetAddress> for RpcHandler<S> {
    async fn handle_request(&self, _: Request<GetAddress>) -> Response<GetAddress> {
        Ok(MessageWrapper(self.smart_contract.address()))
    }
}

impl<S: Read> HandleRequest<GetClusterView> for RpcHandler<S> {
    async fn handle_request(&self, _: Request<GetClusterView>) -> Response<GetClusterView> {
        self.smart_contract
            .cluster_view()
            .await
            .map(MessageWrapper)
            .map_err(Error::internal)
    }
}

impl<S: Read> HandleStreamingRequest<GetEventStream> for RpcHandler<S> {
    async fn handle_request(
        &self,
        _: Request<GetEventStream>,
    ) -> (
        Response<GetEventStream>,
        Option<impl Stream<Item = StreamItem<GetEventStream>> + Send>,
    ) {
        match self.smart_contract.events().await {
            Ok(stream) => {
                let stream = stream
                    .map_ok(MessageWrapper)
                    .map_err(crate::Error::internal)
                    .map_err(Into::into);

                (Ok(()), Some(stream))
            }

            Err(err) => (Err(Error::internal(err)), None),
        }
    }
}
