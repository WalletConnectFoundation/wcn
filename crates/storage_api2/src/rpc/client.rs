pub use wcn_rpc::client2::Config;
use {
    super::*,
    crate::{operation, Operation, Result, StorageApi},
    futures::TryFutureExt as _,
    wcn_rpc::client2::{Client, Connection, ConnectionHandler},
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
}

impl<Kind> StorageApi for Connection<Api<Kind>>
where
    Api<Kind>:
        wcn_rpc::client2::Api<ConnectionParameters = (), ConnectionHandler = ConnectionHandler>,
{
    async fn execute_ref(&self, operation: &Operation<'_>) -> Result<operation::Output> {
        use operation::{Borrowed, Owned};

        match operation {
            Operation::Owned(owned) => match owned {
                Owned::Get(op) => Get::send(self, op).map_ok(into).await,
                Owned::Set(op) => Set::send(self, op).map_ok(into).await,
                Owned::Del(op) => Del::send(self, op).map_ok(into).await,
                Owned::GetExp(op) => GetExp::send(self, op).map_ok(into).await,
                Owned::SetExp(op) => SetExp::send(self, op).map_ok(into).await,
                Owned::HGet(op) => HGet::send(self, op).map_ok(into).await,
                Owned::HSet(op) => HSet::send(self, op).map_ok(into).await,
                Owned::HDel(op) => HDel::send(self, op).map_ok(into).await,
                Owned::HGetExp(op) => HGetExp::send(self, op).map_ok(into).await,
                Owned::HSetExp(op) => HSetExp::send(self, op).map_ok(into).await,
                Owned::HCard(op) => HCard::send(self, op).map_ok(into).await,
                Owned::HScan(op) => HScan::send(self, op).map_ok(into).await,
            },
            Operation::Borrowed(borrowed) => match borrowed {
                Borrowed::Get(op) => Get::send(self, op).map_ok(into).await,
                Borrowed::Set(op) => Set::send(self, op).map_ok(into).await,
                Borrowed::Del(op) => Del::send(self, op).map_ok(into).await,
                Borrowed::GetExp(op) => GetExp::send(self, op).map_ok(into).await,
                Borrowed::SetExp(op) => SetExp::send(self, op).map_ok(into).await,
                Borrowed::HGet(op) => HGet::send(self, op).map_ok(into).await,
                Borrowed::HSet(op) => HSet::send(self, op).map_ok(into).await,
                Borrowed::HDel(op) => HDel::send(self, op).map_ok(into).await,
                Borrowed::HGetExp(op) => HGetExp::send(self, op).map_ok(into).await,
                Borrowed::HSetExp(op) => HSetExp::send(self, op).map_ok(into).await,
                Borrowed::HCard(op) => HCard::send(self, op).map_ok(into).await,
                Borrowed::HScan(op) => HScan::send(self, op).map_ok(into).await,
            },
        }?
    }

    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        self.execute_ref(&operation).await
    }
}

fn into<T>(result: super::Result<T>) -> Result<operation::Output>
where
    T: Into<operation::Output>,
{
    result.map(Into::into).map_err(Into::into)
}

impl From<wcn_rpc::client2::Error> for crate::Error {
    fn from(err: wcn_rpc::client2::Error) -> Self {
        Self::new(crate::ErrorKind::Transport)
            .with_message(format!("wcn_rpc::client::Error: {err}"))
    }
}
