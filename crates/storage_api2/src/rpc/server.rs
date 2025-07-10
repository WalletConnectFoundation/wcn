use {
    super::*,
    crate::{
        self as storage_api,
        rpc::{self, Id as RpcId},
        Callback,
        StorageApi,
    },
    tap::TapFallible as _,
    wcn_rpc::{
        server2::{Connection, Error, HandleConnection, HandleUnary, Responder, Result},
        BorrowedMessage,
        MessageV2,
        Request,
        Serializer,
        UnaryRpc,
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
struct ConnectionHandler<S: StorageApi, Kind> {
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

impl<T, RPC: UnaryRpc<Response = rpc::Result<T>>> Callback for Responder<'_, RPC>
where
    T: MessageV2,
    for<'a, 'b> Result<&'a T, ErrorBorrowed<'b>>: BorrowedMessage<Owned = RPC::Response>,
    for<'a, 'b> RPC::Codec: Serializer<Result<&'a T, ErrorBorrowed<'b>>>,
    for<'a> &'a operation::Output:
        TryInto<&'a T, Error = derive_more::TryIntoError<&'a operation::Output>>,
{
    type Error = Error;

    async fn send_result(self, result: &storage_api::Result<operation::Output>) -> Result<()> {
        let result = match result {
            Ok(out) => out
                .try_into()
                .tap_err(|err| tracing::error!(?err, "Failed to downcast output"))
                .map_err(|_| ErrorBorrowed {
                    code: ErrorCode::Internal as u8,
                    message: None,
                }),
            Err(err) => Err(ErrorBorrowed {
                code: ErrorCode::from(err.kind) as u8,
                message: err.message.as_ref().map(|s| s.as_str()),
            }),
        };

        self.respond(&result).await
    }
}

impl<S: StorageApi> HandleUnary<Get> for RpcHandler<S> {
    async fn handle(&self, req: Request<Get>, resp: Responder<'_, Get>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<Set> for RpcHandler<S> {
    async fn handle(&self, req: Request<Set>, resp: Responder<'_, Set>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<Del> for RpcHandler<S> {
    async fn handle(&self, req: Request<Del>, resp: Responder<'_, Del>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<GetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<GetExp>, resp: Responder<'_, GetExp>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<SetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<SetExp>, resp: Responder<'_, SetExp>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HGet> for RpcHandler<S> {
    async fn handle(&self, req: Request<HGet>, resp: Responder<'_, HGet>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HSet> for RpcHandler<S> {
    async fn handle(&self, req: Request<HSet>, resp: Responder<'_, HSet>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HDel> for RpcHandler<S> {
    async fn handle(&self, req: Request<HDel>, resp: Responder<'_, HDel>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HGetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<HGetExp>, resp: Responder<'_, HGetExp>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HSetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<HSetExp>, resp: Responder<'_, HSetExp>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HCard> for RpcHandler<S> {
    async fn handle(&self, req: Request<HCard>, resp: Responder<'_, HCard>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}

impl<S: StorageApi> HandleUnary<HScan> for RpcHandler<S> {
    async fn handle(&self, req: Request<HScan>, resp: Responder<'_, HScan>) -> Result<()> {
        self.storage_api.execute_callback(req.into(), resp).await
    }
}
