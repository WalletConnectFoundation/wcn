pub use wcn_rpc::client::Config;
use {
    super::*,
    crate::{operation, rpc, KeyspaceVersion, Operation, Result, StorageApi},
    futures::{SinkExt, Stream, StreamExt as _, TryFutureExt as _},
    std::ops::RangeInclusive,
    wcn_rpc::client::{Client, Connection, Outbound},
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

impl<Kind, S> wcn_rpc::client::Api for Api<Kind, S>
where
    Self: wcn_rpc::Api,
{
    type ConnectionParameters = ();
}

impl<Kind> StorageApi for Connection<Api<Kind>>
where
    Api<Kind>: wcn_rpc::client::Api<ConnectionParameters = ()>,
{
    async fn execute_ref(&self, operation: &Operation<'_>) -> Result<operation::Output> {
        use operation::{Borrowed, Owned};

        match operation {
            Operation::Owned(owned) => match owned {
                Owned::Get(op) => Get::send_request(self, op).map_ok(into).await,
                Owned::Set(op) => Set::send_request(self, op).map_ok(into).await,
                Owned::Del(op) => Del::send_request(self, op).map_ok(into).await,
                Owned::GetExp(op) => GetExp::send_request(self, op).map_ok(into).await,
                Owned::SetExp(op) => SetExp::send_request(self, op).map_ok(into).await,
                Owned::HGet(op) => HGet::send_request(self, op).map_ok(into).await,
                Owned::HSet(op) => HSet::send_request(self, op).map_ok(into).await,
                Owned::HDel(op) => HDel::send_request(self, op).map_ok(into).await,
                Owned::HGetExp(op) => HGetExp::send_request(self, op).map_ok(into).await,
                Owned::HSetExp(op) => HSetExp::send_request(self, op).map_ok(into).await,
                Owned::HCard(op) => HCard::send_request(self, op).map_ok(into).await,
                Owned::HScan(op) => HScan::send_request(self, op).map_ok(into).await,
            },
            Operation::Borrowed(borrowed) => match borrowed {
                Borrowed::Get(op) => Get::send_request(self, op).map_ok(into).await,
                Borrowed::Set(op) => Set::send_request(self, op).map_ok(into).await,
                Borrowed::Del(op) => Del::send_request(self, op).map_ok(into).await,
                Borrowed::GetExp(op) => GetExp::send_request(self, op).map_ok(into).await,
                Borrowed::SetExp(op) => SetExp::send_request(self, op).map_ok(into).await,
                Borrowed::HGet(op) => HGet::send_request(self, op).map_ok(into).await,
                Borrowed::HSet(op) => HSet::send_request(self, op).map_ok(into).await,
                Borrowed::HDel(op) => HDel::send_request(self, op).map_ok(into).await,
                Borrowed::HGetExp(op) => HGetExp::send_request(self, op).map_ok(into).await,
                Borrowed::HSetExp(op) => HSetExp::send_request(self, op).map_ok(into).await,
                Borrowed::HCard(op) => HCard::send_request(self, op).map_ok(into).await,
                Borrowed::HScan(op) => HScan::send_request(self, op).map_ok(into).await,
            },
        }?
    }

    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        self.execute_ref(&operation).await
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: KeyspaceVersion,
    ) -> Result<impl Stream<Item = Result<DataItem>>> {
        Ok(ReadData::send(self, |mut rpc: Outbound<_, _>| async move {
            rpc.request_sink
                .send(&ReadDataRequest {
                    keyrange,
                    keyspace_version,
                })
                .await?;

            if let Err(err) = rpc
                .response_stream
                .try_next_downcast::<rpc::Result<()>>()
                .await?
            {
                return Ok(Err(err));
            };

            Ok(Ok(rpc
                .response_stream
                .map_downcast::<rpc::Result<DataItem>>()
                .map(|res| res?.map_err(Into::into))))
        })
        .await??)
    }

    async fn write_data(&self, stream: impl Stream<Item = Result<DataItem>> + Send) -> Result<()> {
        WriteData::send(self, |rpc: Outbound<_, _>| async move {
            let sink = SinkExt::<DataItem>::sink_map_err(rpc.request_sink, Into::into);
            Ok(stream.forward(sink).await)
        })
        .await?
    }
}

fn into<T>(result: super::Result<T>) -> Result<operation::Output>
where
    T: Into<operation::Output>,
{
    result.map(Into::into).map_err(Into::into)
}

impl From<wcn_rpc::client::Error> for crate::Error {
    fn from(err: wcn_rpc::client::Error) -> Self {
        Self::new(crate::ErrorKind::Transport)
            .with_message(format!("wcn_rpc::client::Error: {err}"))
    }
}
