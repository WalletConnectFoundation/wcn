use {
    super::{Error, GetEventStreamItem},
    crate::rpc::{GetAddress, GetClusterView, GetEventStream, Id as RpcId, MessageWrapper},
    derive_where::derive_where,
    futures::{SinkExt as _, StreamExt as _},
    std::sync::Arc,
    tap::Pipe as _,
    wcn_cluster::smart_contract::{self, Read},
    wcn_rpc::{
        Request,
        Response,
        server2::{
            Connection,
            HandleConnection,
            HandleRequest,
            HandleRpc,
            Inbound,
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

impl<S: Read> HandleRpc<GetEventStream> for RpcHandler<S> {
    async fn handle_rpc(&self, rpc: &mut Inbound<GetEventStream>) -> Result<()> {
        let res = self.smart_contract.events().await;

        let (events, ack) = match res {
            Ok(events) => (Some(events), Ok(())),
            Err(err) => (None, Err(Error::internal(err))),
        };

        rpc.response_sink
            .send(&GetEventStreamItem::Ack(ack))
            .await?;

        if let Some(events) = events {
            events
                .map(|res| GetEventStreamItem::Event(res.map_err(Error::internal)))
                .pipe(|responses| rpc.response_sink.send_all(responses))
                .await?;
        }

        Ok(())
    }
}
