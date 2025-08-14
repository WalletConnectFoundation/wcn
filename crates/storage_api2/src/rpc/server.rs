use {
    super::*,
    crate::{
        self as storage_api,
        rpc::{self, Id as RpcId},
        Callback,
        StorageApi,
    },
    futures::{SinkExt as _, StreamExt, TryStreamExt},
    tap::{Pipe as _, TapFallible as _},
    wcn_rpc::{
        server2::{Api, Error, Inbound, PendingConnection, Responder, Result},
        BorrowedMessage,
        MessageV2,
        PeerId,
        Request,
        Serializer,
    },
};

impl<Kind, S> Api for super::Api<Kind, S>
where
    Kind: Clone + Send + Sync + 'static,
    S: storage_api::Factory<PeerId>,
    super::Api<Kind, S>: wcn_rpc::Api<RpcId = RpcId>,
{
    type RpcHandler = RpcHandler<S::StorageApi>;

    async fn handle_connection(conn: PendingConnection<'_, Self>) -> Result<()> {
        let storage_api = match conn.api().state.new_storage_api(*conn.remote_peer_id()) {
            Ok(api) => api,
            Err(err) => return conn.reject(rpc::Error::from(err).code).await,
        };

        let rpc_handler = RpcHandler { storage_api };

        let conn = conn.accept(rpc_handler).await?;

        conn.handle(|rpc| async move {
            match rpc.id() {
                RpcId::Get => rpc.handle_unary::<Get>(RpcHandler::get).await,
                RpcId::Set => rpc.handle_unary::<Set>(RpcHandler::set).await,
                RpcId::Del => rpc.handle_unary::<Del>(RpcHandler::del).await,

                RpcId::GetExp => rpc.handle_unary::<GetExp>(RpcHandler::get_exp).await,
                RpcId::SetExp => rpc.handle_unary::<SetExp>(RpcHandler::set_exp).await,

                RpcId::HGet => rpc.handle_unary::<HGet>(RpcHandler::hget).await,
                RpcId::HSet => rpc.handle_unary::<HSet>(RpcHandler::hset).await,
                RpcId::HDel => rpc.handle_unary::<HDel>(RpcHandler::hdel).await,

                RpcId::HGetExp => rpc.handle_unary::<HGetExp>(RpcHandler::hget_exp).await,
                RpcId::HSetExp => rpc.handle_unary::<HSetExp>(RpcHandler::hset_exp).await,

                RpcId::HCard => rpc.handle_unary::<HCard>(RpcHandler::hcard).await,
                RpcId::HScan => rpc.handle_unary::<HScan>(RpcHandler::hscan).await,

                RpcId::ReadData => rpc.handle::<ReadData>(RpcHandler::read_data).await,
                RpcId::WriteData => rpc.handle::<WriteData>(RpcHandler::write_data).await,
            }
        })
        .await
    }
}

#[derive(Clone)]
pub struct RpcHandler<S: StorageApi> {
    storage_api: S,
}

impl<T, RPC: RpcV2<Response = rpc::Result<T>>> Callback for Responder<RPC>
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
                message: err.message.as_deref(),
            }),
        };

        self.respond(&result).await
    }
}

impl<S: StorageApi> RpcHandler<S> {
    async fn get(self, req: Request<Get>, responder: Responder<Get>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn set(self, req: Request<Set>, responder: Responder<Set>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn del(self, req: Request<Del>, responder: Responder<Del>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn get_exp(self, req: Request<GetExp>, responder: Responder<GetExp>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn set_exp(self, req: Request<SetExp>, responder: Responder<SetExp>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hget(self, req: Request<HGet>, responder: Responder<HGet>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hset(self, req: Request<HSet>, responder: Responder<HSet>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hdel(self, req: Request<HDel>, responder: Responder<HDel>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hget_exp(self, req: Request<HGetExp>, responder: Responder<HGetExp>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hset_exp(self, req: Request<HSetExp>, responder: Responder<HSetExp>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hcard(self, req: Request<HCard>, responder: Responder<HCard>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn hscan(self, req: Request<HScan>, responder: Responder<HScan>) -> Result<()> {
        self.storage_api
            .execute_callback(req.into(), responder)
            .await
    }

    async fn read_data(self, mut rpc: Inbound<ReadData>) -> Result<()> {
        let req = rpc.request_stream.try_next().await?;

        let res = self
            .storage_api
            .read_data(req.keyrange, req.keyspace_version)
            .await;

        let mut data_stream = None;

        let ack = res.map(|data| data_stream = Some(data)).map_err(Into::into);
        rpc.response_sink.send(&ReadDataResponse::Ack(ack)).await?;

        if let Some(data) = data_stream {
            data.map(|res| ReadDataResponse::Item(res.map_err(Into::into)))
                .pipe(|responses| rpc.response_sink.send_all(responses))
                .await?;
        }

        Ok(())
    }

    async fn write_data(self, mut rpc: Inbound<WriteData>) -> Result<()> {
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
