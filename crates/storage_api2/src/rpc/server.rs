use {
    super::*,
    crate::{rpc::Id as RpcId, StorageApi},
    wcn_rpc::{
        server2::{Connection, HandleConnection, HandleRequest, Result},
        Request,
        Response,
    },
};

/// Creates a new [`CoordinatorApi`] RPC server.
pub fn coordinator(storage_api: impl StorageApi + Clone) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Coordinator>(storage_api)
}

/// Creates a new [`ReplicaApi`] RPC server.
pub fn replica(storage_api: impl StorageApi + Clone) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Replica>(storage_api)
}

/// Creates a new [`DatabaseApi`] RPC server.
pub fn database(storage_api: impl StorageApi + Clone) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Database>(storage_api)
}

fn new<Kind>(storage_api: impl StorageApi + Clone) -> impl wcn_rpc::server2::Server
where
    Kind: Clone + Send + Sync + 'static,
    Api<Kind>: wcn_rpc::Api<RpcId = RpcId>,
{
    wcn_rpc::server2::new(ConnectionHandler {
        rpc_handler: RpcHandler { storage_api },
        _marker: PhantomData,
    })
}

#[derive(Clone)]
struct ConnectionHandler<S: StorageApi + Clone, Kind> {
    rpc_handler: RpcHandler<S>,
    _marker: PhantomData<Kind>,
}

impl<S, Kind> HandleConnection for ConnectionHandler<S, Kind>
where
    S: StorageApi + Clone,
    Kind: Clone + Send + Sync + 'static,
    Api<Kind>: wcn_rpc::Api<RpcId = RpcId>,
{
    type Api = super::Api<Kind>;

    async fn handle_connection(&self, conn: Connection<'_, Self::Api>) -> Result<()> {
        conn.handle(&self.rpc_handler, |rpc, handler| async move {
            match rpc.id() {
                RpcId::Get => rpc.handle::<Get>(&handler).await,
                RpcId::Set => rpc.handle::<Set>(&handler).await,
                RpcId::Del => rpc.handle::<Del>(&handler).await,
                RpcId::SetExp => rpc.handle::<SetExp>(&handler).await,
                RpcId::GetExp => rpc.handle::<GetExp>(&handler).await,
                RpcId::HGet => rpc.handle::<HGet>(&handler).await,
                RpcId::HSet => rpc.handle::<HSet>(&handler).await,
                RpcId::HDel => rpc.handle::<HDel>(&handler).await,
                RpcId::HSetExp => rpc.handle::<HSetExp>(&handler).await,
                RpcId::HGetExp => rpc.handle::<HGetExp>(&handler).await,
                RpcId::HCard => rpc.handle::<HCard>(&handler).await,
                RpcId::HScan => rpc.handle::<HScan>(&handler).await,
            }
        })
        .await
    }
}

#[derive(Clone)]
struct RpcHandler<S: StorageApi> {
    storage_api: S,
}

impl<S: StorageApi> HandleRequest<Get> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<Get>) -> Response<Get> {
        self.storage_api
            .get(&req)
            .await
            .map(|opt| opt.map(Record::into_static))
            .map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<Set> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<Set>) -> Response<Set> {
        self.storage_api.set(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<Del> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<Del>) -> Response<Del> {
        self.storage_api.del(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<GetExp> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<GetExp>) -> Response<GetExp> {
        self.storage_api.get_exp(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<SetExp> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<SetExp>) -> Response<SetExp> {
        self.storage_api.set_exp(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HGet> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HGet>) -> Response<HGet> {
        self.storage_api
            .hget(&req)
            .await
            .map(|opt| opt.map(Record::into_static))
            .map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HSet> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HSet>) -> Response<HSet> {
        self.storage_api.hset(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HDel> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HDel>) -> Response<HDel> {
        self.storage_api.hdel(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HGetExp> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HGetExp>) -> Response<HGetExp> {
        self.storage_api.hget_exp(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HSetExp> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HSetExp>) -> Response<HSetExp> {
        self.storage_api.hset_exp(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HCard> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HCard>) -> Response<HCard> {
        self.storage_api.hcard(&req).await.map_err(Into::into)
    }
}

impl<S: StorageApi> HandleRequest<HScan> for RpcHandler<S> {
    async fn handle_request(&self, req: Request<HScan>) -> Response<HScan> {
        self.storage_api
            .hscan(&req)
            .await
            .map(MapPage::into_static)
            .map_err(Into::into)
    }
}
