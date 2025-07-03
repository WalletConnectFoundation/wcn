pub use wcn_rpc::client2::Config;
use {
    super::*,
    crate::{operation, MapPage, Operation, OperationRef, Record, Result, StorageApi},
    wcn_rpc::client2::{Client, Connection, ConnectionHandler, RpcHandler},
};

/// RPC [`Client`] of [`CoordinatorApi`].
pub type Coordinator = Client<CoordinatorApi>;

/// Outbound [`Connection`] to [`CoordinatorApi`].
pub type CoordinatorConnection = Connection<CoordinatorApi>;

/// RPC [`Client`] of [`ReplicaApi`].
pub type Replica = Client<ReplicaApi>;

/// Outbound [`Connection`] to [`ReplicaApi`].
pub type ReplicaConnection = Connection<ReplicaApi>;

/// RPC [`Client`] of [`DatabaseApi`].
pub type Database = Client<DatabaseApi>;

/// Outbound [`Connection`] to [`DatabaseApi`].
pub type DatabaseConnection = Connection<DatabaseApi>;

/// Creates a new [`Coordinator`] RPC client.
pub fn coordinator(config: Config) -> wcn_rpc::client2::Result<Coordinator> {
    Client::new(config, ConnectionHandler)
}

/// Creates a new [`ReplicaApi`] RPC client.
pub fn replica(config: Config) -> wcn_rpc::client2::Result<Replica> {
    Client::new(config, ConnectionHandler)
}

/// Creates a new [`DatabaseApi`] RPC client.
pub fn database(config: Config) -> wcn_rpc::client2::Result<Database> {
    Client::new(config, ConnectionHandler)
}

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
        self.send::<Get>(op)?.await?.map_err(Into::into)
    }

    async fn set(&self, op: &operation::Set<'_>) -> Result<()> {
        self.send::<Set>(op)?.await?.map_err(Into::into)
    }

    async fn del(&self, op: &operation::Del<'_>) -> Result<()> {
        self.send::<Del>(op)?.await?.map_err(Into::into)
    }

    async fn get_exp(&self, op: &operation::GetExp<'_>) -> Result<Option<RecordExpiration>> {
        self.send::<GetExp>(op)?.await?.map_err(Into::into)
    }

    async fn set_exp(&self, op: &operation::SetExp<'_>) -> Result<()> {
        self.send::<SetExp>(op)?.await?.map_err(Into::into)
    }

    async fn hget(&self, op: &operation::HGet<'_>) -> Result<Option<Record<'_>>> {
        self.send::<HGet>(op)?.await?.map_err(Into::into)
    }

    async fn hset(&self, op: &operation::HSet<'_>) -> Result<()> {
        self.send::<HSet>(op)?.await?.map_err(Into::into)
    }

    async fn hdel(&self, op: &operation::HDel<'_>) -> Result<()> {
        self.send::<HDel>(op)?.await?.map_err(Into::into)
    }

    async fn hget_exp(&self, op: &operation::HGetExp<'_>) -> Result<Option<RecordExpiration>> {
        self.send::<HGetExp>(op)?.await?.map_err(Into::into)
    }

    async fn hset_exp(&self, op: &operation::HSetExp<'_>) -> Result<()> {
        self.send::<HSetExp>(op)?.await?.map_err(Into::into)
    }

    async fn hcard(&self, op: &operation::HCard<'_>) -> Result<u64> {
        self.send::<HCard>(op)?.await?.map_err(Into::into)
    }

    async fn hscan(&self, op: &operation::HScan<'_>) -> Result<MapPage<'_>> {
        self.send::<HScan>(op)?.await?.map_err(Into::into)
    }

    async fn execute_ref<'a>(
        &'a self,
        operation: impl Into<crate::OperationRef<'a>> + Send + 'a,
    ) -> Result<operation::Output<'a>> {
        match operation.into() {
            OperationRef::Get(get) => self.get(get).await.map(Into::into),
            OperationRef::Set(set) => self.set(set).await.map(Into::into),
            OperationRef::Del(del) => self.del(del).await.map(Into::into),
            OperationRef::GetExp(get_exp) => self.get_exp(get_exp).await.map(Into::into),
            OperationRef::SetExp(set_exp) => self.set_exp(set_exp).await.map(Into::into),
            OperationRef::HGet(hget) => self.hget(hget).await.map(Into::into),
            OperationRef::HSet(hset) => self.hset(hset).await.map(Into::into),
            OperationRef::HDel(hdel) => self.hdel(hdel).await.map(Into::into),
            OperationRef::HGetExp(hget_exp) => self.hget_exp(hget_exp).await.map(Into::into),
            OperationRef::HSetExp(hset_exp) => self.hset_exp(hset_exp).await.map(Into::into),
            OperationRef::HCard(hcard) => self.hcard(hcard).await.map(Into::into),
            OperationRef::HScan(hscan) => self.hscan(hscan).await.map(Into::into),
        }
    }

    async fn execute<'a>(
        &'a self,
        operation: crate::Operation<'a>,
    ) -> Result<operation::Output<'a>> {
        Ok(match operation {
            Operation::Get(op) => self.send::<Get>(&op)?.await??.into(),
            Operation::Set(op) => self.send::<Set>(&op)?.await??.into(),
            Operation::Del(op) => self.send::<Del>(&op)?.await??.into(),
            Operation::GetExp(op) => self.send::<GetExp>(&op)?.await??.into(),
            Operation::SetExp(op) => self.send::<SetExp>(&op)?.await??.into(),
            Operation::HGet(op) => self.send::<HGet>(&op)?.await??.into(),
            Operation::HSet(op) => self.send::<HSet>(&op)?.await??.into(),
            Operation::HDel(op) => self.send::<HDel>(&op)?.await??.into(),
            Operation::HGetExp(op) => self.send::<HGetExp>(&op)?.await??.into(),
            Operation::HSetExp(op) => self.send::<HSetExp>(&op)?.await??.into(),
            Operation::HCard(op) => self.send::<HCard>(&op)?.await??.into(),
            Operation::HScan(op) => self.send::<HScan>(&op)?.await??.into(),
        })
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
