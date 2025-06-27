use {
    super::*,
    crate::{operation, MapPage, Operation, Record, Result, StorageApi},
    wcn_rpc::client2::{Connection, ConnectionHandler, RpcHandler},
};

impl<Kind> wcn_rpc::client2::Api for Api<Kind>
where
    Self: wcn_rpc::Api,
{
    type ConnectionParameters = ();
    type ConnectionHandler = ConnectionHandler;
    type RpcHandler = RpcHandler;
}

impl<Kind> StorageApi for Connection<Api<Kind>>
where
    Api<Kind>: wcn_rpc::client2::Api<
        ConnectionParameters = (),
        ConnectionHandler = ConnectionHandler,
        RpcHandler = RpcHandler,
    >,
{
    async fn get(&self, op: &operation::Get<'_>) -> Result<Option<Record<'_>>> {
        self.send::<Get>(&op)?.await?.map_err(Into::into)
    }

    async fn set(&self, op: &operation::Set<'_>) -> Result<()> {
        self.send::<Set>(&op)?.await?.map_err(Into::into)
    }

    async fn del(&self, op: &operation::Del<'_>) -> Result<()> {
        self.send::<Del>(&op)?.await?.map_err(Into::into)
    }

    async fn get_exp(&self, op: &operation::GetExp<'_>) -> Result<Option<RecordExpiration>> {
        self.send::<GetExp>(&op)?.await?.map_err(Into::into)
    }

    async fn set_exp(&self, op: &operation::SetExp<'_>) -> Result<()> {
        self.send::<SetExp>(&op)?.await?.map_err(Into::into)
    }

    async fn hget(&self, op: &operation::HGet<'_>) -> Result<Option<Record<'_>>> {
        self.send::<HGet>(&op)?.await?.map_err(Into::into)
    }

    async fn hset(&self, op: &operation::HSet<'_>) -> Result<()> {
        self.send::<HSet>(&op)?.await?.map_err(Into::into)
    }

    async fn hdel(&self, op: &operation::HDel<'_>) -> Result<()> {
        self.send::<HDel>(&op)?.await?.map_err(Into::into)
    }

    async fn hget_exp(&self, op: &operation::HGetExp<'_>) -> Result<Option<RecordExpiration>> {
        self.send::<HGetExp>(&op)?.await?.map_err(Into::into)
    }

    async fn hset_exp(&self, op: &operation::HSetExp<'_>) -> Result<()> {
        self.send::<HSetExp>(&op)?.await?.map_err(Into::into)
    }

    async fn hcard(&self, op: &operation::HCard<'_>) -> Result<u64> {
        self.send::<HCard>(&op)?.await?.map_err(Into::into)
    }

    async fn hscan(&self, op: &operation::HScan<'_>) -> Result<MapPage<'_>> {
        self.send::<HScan>(&op)?.await?.map_err(Into::into)
    }

    async fn execute<'a>(
        &'a self,
        operation: impl Into<crate::Operation<'a>> + Send + 'a,
    ) -> Result<operation::Output<'a>> {
        match operation.into() {
            Operation::Get(get) => self.get(get).await.map(Into::into),
            Operation::Set(set) => self.set(set).await.map(Into::into),
            Operation::Del(del) => self.del(del).await.map(Into::into),
            Operation::GetExp(get_exp) => self.get_exp(get_exp).await.map(Into::into),
            Operation::SetExp(set_exp) => self.set_exp(set_exp).await.map(Into::into),
            Operation::HGet(hget) => self.hget(hget).await.map(Into::into),
            Operation::HSet(hset) => self.hset(hset).await.map(Into::into),
            Operation::HDel(hdel) => self.hdel(hdel).await.map(Into::into),
            Operation::HGetExp(hget_exp) => self.hget_exp(hget_exp).await.map(Into::into),
            Operation::HSetExp(hset_exp) => self.hset_exp(hset_exp).await.map(Into::into),
            Operation::HCard(hcard) => self.hcard(hcard).await.map(Into::into),
            Operation::HScan(hscan) => self.hscan(hscan).await.map(Into::into),
        }
    }
}

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(
            crate::ErrorKind::Transport,
            Some(format!("wcn_rpc::client::Error: {err}")),
        )
    }
}
