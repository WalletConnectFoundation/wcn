use {
    super::{ClusterApi, Error, GetEventStreamItem},
    crate::rpc::{GetAddress, GetClusterView, GetEventStream, Id as RpcId, MessageWrapper},
    futures::{FutureExt as _, SinkExt as _, StreamExt as _},
    futures_concurrency::future::Race as _,
    tap::Pipe as _,
    wcn_cluster::smart_contract,
    wcn_rpc::{
        Request,
        Response,
        server2::{Inbound, PendingConnection, Result},
    },
};

impl<S> wcn_rpc::server2::Api for ClusterApi<S>
where
    S: smart_contract::Read + Clone,
{
    type RpcHandler = Self;

    async fn handle_connection(conn: PendingConnection<'_, Self>) -> Result<()> {
        let rpc_handler = conn.api().clone();
        let conn = conn.accept(rpc_handler).await?;

        conn.handle(|rpc| async move {
            match rpc.id() {
                RpcId::GetAddress => rpc.handle_request::<GetAddress>(Self::get_address).await,
                RpcId::GetClusterView => {
                    rpc.handle_request::<GetClusterView>(Self::get_cluster_view)
                        .await
                }
                RpcId::GetEventStream => rpc.handle::<GetEventStream>(Self::get_event_stream).await,
            }
        })
        .await
    }
}

impl<S: smart_contract::Read> ClusterApi<S> {
    fn smart_contract(&self) -> &S {
        &self.state
    }

    async fn get_address(self, _: Request<GetAddress>) -> Response<GetAddress> {
        self.smart_contract()
            .address()
            .map(MessageWrapper)
            .map_err(Error::internal)
    }

    async fn get_cluster_view(self, _: Request<GetClusterView>) -> Response<GetClusterView> {
        self.smart_contract()
            .cluster_view()
            .await
            .map(MessageWrapper)
            .map_err(Error::internal)
    }

    async fn get_event_stream(self, mut rpc: Inbound<GetEventStream>) -> Result<()> {
        let res = self.smart_contract().events().await;

        let (events, ack) = match res {
            Ok(events) => (Some(events), Ok(())),
            Err(err) => (None, Err(Error::internal(err))),
        };

        rpc.response_sink
            .send(&GetEventStreamItem::Ack(ack))
            .await?;

        if let Some(events) = events {
            let fut = events
                .map(|res| GetEventStreamItem::Event(res.map_err(Error::internal)))
                .pipe(|responses| rpc.response_sink.send_all(responses));

            // Make sure that we won't block server shutdown.
            (fut, rpc.shutdown_signal.wait().map(Ok)).race().await?;
        }

        Ok(())
    }
}
