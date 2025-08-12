use {
    super::*,
    crate::{
        self as storage_api,
        rpc::{self, Id as RpcId},
        ErrorKind,
        StorageApi,
    },
    futures::{SinkExt as _, StreamExt, TryStreamExt},
    std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    },
    tap::{Pipe as _, TapFallible as _},
    wc::metrics::{self, EnumLabel, StringLabel},
    wcn_rpc::{
        server2::{
            Connection,
            Error,
            HandleConnection,
            HandleRpc,
            HandleUnary,
            Inbound,
            PendingConnection,
            Responder,
            Result,
        },
        BorrowedMessage,
        MessageV2,
        PeerId,
        Request,
        Serializer,
        UnaryRpc,
    },
};

/// [`StorageApi`] config.
#[derive(Clone)]
pub struct ApiConfig {
    /// Inbound RPC timeout.
    pub rpc_timeout: Duration,
}

/// Creates a new [`CoordinatorApi`] RPC server.
pub fn coordinator(
    storage_api: impl storage_api::Factory<PeerId>,
) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Coordinator>(storage_api)
}

/// Creates a new [`ReplicaApi`] RPC server.
pub fn replica(storage_api: impl storage_api::Factory<PeerId>) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Replica>(storage_api)
}

/// Creates a new [`DatabaseApi`] RPC server.
pub fn database(storage_api: impl storage_api::Factory<PeerId>) -> impl wcn_rpc::server2::Server {
    new::<api_kind::Database>(storage_api)
}

fn new<Kind>(factory: impl storage_api::Factory<PeerId>) -> impl wcn_rpc::server2::Server
where
    Kind: Clone + Send + Sync + 'static,
    Api<Kind>: wcn_rpc::Api<RpcId = RpcId>,
{
    wcn_rpc::server2::new(ConnectionHandler {
        factory,
        _marker: PhantomData,
    })
}

#[derive(Clone)]
struct ConnectionHandler<F: storage_api::Factory<PeerId>, Kind> {
    factory: F,
    _marker: PhantomData<Kind>,
}

impl<F, Kind> HandleConnection for ConnectionHandler<F, Kind>
where
    F: storage_api::Factory<PeerId>,
    Kind: Clone + Send + Sync + 'static,
    Api<Kind>: wcn_rpc::Api<RpcId = RpcId>,
{
    type Api = super::Api<Kind>;

    async fn handle_connection(&self, conn: PendingConnection<'_, Self::Api>) -> Result<()> {
        let storage_api = match self.factory.new_storage_api(*conn.remote_peer_id()) {
            Ok(api) => api,
            Err(err) => return conn.reject(rpc::Error::from(err).code).await,
        };

        let conn = conn.accept().await?;

        let rpc_handler = RpcHandler {
            storage_api,
            remote_peer_addr: conn.remote_peer_addr().ip(),
            api_name: <Api<Kind> as wcn_rpc::Api>::NAME,
        };

        conn.handle(&rpc_handler, |rpc, handler| async move {
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
                RpcId::PullData => rpc.handle::<PullData>(&handler).await,
                RpcId::PushData => rpc.handle::<PushData>(&handler).await,
            }
        })
        .await
    }
}

#[derive(Clone)]
struct RpcHandler<S: StorageApi> {
    storage_api: S,
    remote_peer_addr: IpAddr,
    api_name: wcn_rpc::ApiName,
}

impl<S: StorageApi> RpcHandler<S> {
    fn new_callback<'a, RPC: UnaryRpc>(
        &self,
        operation_name: operation::Name,
        responder: Responder<'a, RPC>,
    ) -> Callback<'a, RPC> {
        Callback {
            api_name: self.api_name,
            remote_peer_addr: self.remote_peer_addr,
            operation_name,
            responder,
        }
    }
}

struct Callback<'a, RPC: UnaryRpc> {
    api_name: wcn_rpc::ApiName,
    remote_peer_addr: IpAddr,
    operation_name: operation::Name,
    responder: Responder<'a, RPC>,
}

impl<T, RPC: UnaryRpc<Response = rpc::Result<T>>> crate::Callback for Callback<'_, RPC>
where
    T: MessageV2,
    for<'a, 'b> Result<&'a T, ErrorBorrowed<'b>>: BorrowedMessage<Owned = RPC::Response>,
    for<'a, 'b> RPC::Codec: Serializer<Result<&'a T, ErrorBorrowed<'b>>>,
    for<'a> &'a operation::Output:
        TryInto<&'a T, Error = derive_more::TryIntoError<&'a operation::Output>>,
{
    type Error = Error;

    async fn send_result(self, result: &storage_api::Result<operation::Output>) -> Result<()> {
        if let Err(err) = &result {
            metrics::counter!("wcn_storage_api_inbound_rpc_errors",
                StringLabel<"remote_addr", IpAddr> => &self.remote_peer_addr,
                StringLabel<"api_kind", ApiName> => &self.api_name,
                EnumLabel<"operation_name", operation::Name> => self.operation_name,
                EnumLabel<"kind", ErrorKind> => err.kind
            )
            .increment(1);
        }

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
                message: err.message.as_deref(),
            }),
        };

        self.responder.respond(&result).await
    }
}

impl<S: StorageApi> HandleUnary<Get> for RpcHandler<S> {
    async fn handle(&self, req: Request<Get>, resp: Responder<'_, Get>) -> Result<()> {
        let cb = self.new_callback(operation::Name::Get, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<Set> for RpcHandler<S> {
    async fn handle(&self, req: Request<Set>, resp: Responder<'_, Set>) -> Result<()> {
        let cb = self.new_callback(operation::Name::Set, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<Del> for RpcHandler<S> {
    async fn handle(&self, req: Request<Del>, resp: Responder<'_, Del>) -> Result<()> {
        let cb = self.new_callback(operation::Name::Del, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<GetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<GetExp>, resp: Responder<'_, GetExp>) -> Result<()> {
        let cb = self.new_callback(operation::Name::GetExp, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<SetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<SetExp>, resp: Responder<'_, SetExp>) -> Result<()> {
        let cb = self.new_callback(operation::Name::SetExp, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HGet> for RpcHandler<S> {
    async fn handle(&self, req: Request<HGet>, resp: Responder<'_, HGet>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HGet, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HSet> for RpcHandler<S> {
    async fn handle(&self, req: Request<HSet>, resp: Responder<'_, HSet>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HSet, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HDel> for RpcHandler<S> {
    async fn handle(&self, req: Request<HDel>, resp: Responder<'_, HDel>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HDel, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HGetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<HGetExp>, resp: Responder<'_, HGetExp>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HGetExp, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HSetExp> for RpcHandler<S> {
    async fn handle(&self, req: Request<HSetExp>, resp: Responder<'_, HSetExp>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HSetExp, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HCard> for RpcHandler<S> {
    async fn handle(&self, req: Request<HCard>, resp: Responder<'_, HCard>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HCard, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleUnary<HScan> for RpcHandler<S> {
    async fn handle(&self, req: Request<HScan>, resp: Responder<'_, HScan>) -> Result<()> {
        let cb = self.new_callback(operation::Name::HScan, resp);
        self.storage_api.execute_callback(req.into(), cb).await
    }
}

impl<S: StorageApi> HandleRpc<PullData> for RpcHandler<S> {
    async fn handle_rpc(&self, rpc: &mut Inbound<PullData>) -> Result<()> {
        let req = rpc.request_stream.try_next().await?;

        let res = self
            .storage_api
            .read_data(req.keyrange, req.keyspace_version)
            .await;

        let mut data_stream = None;

        let ack = res.map(|data| data_stream = Some(data)).map_err(Into::into);
        rpc.response_sink.send(&PullDataResponse::Ack(ack)).await?;

        if let Some(data) = data_stream {
            data.map(|res| PullDataResponse::Item(res.map_err(Into::into)))
                .pipe(|responses| rpc.response_sink.send_all(responses))
                .await?;
        }

        Ok(())
    }
}

impl<S: StorageApi> HandleRpc<PushData> for RpcHandler<S> {
    async fn handle_rpc(&self, rpc: &mut Inbound<PushData>) -> Result<()> {
        let data_stream =
            (&mut rpc.request_stream).map_err(|err| crate::Error::internal().with_message(err));

        let res: super::Result<()> = self
            .storage_api
            .write_data(data_stream)
            .await
            .map_err(Into::into);

        rpc.response_sink.send(&res).await
    }
}
