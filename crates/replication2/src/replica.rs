use {
    cluster::Cluster,
    derive_where::derive_where,
    futures::Stream,
    std::ops::RangeInclusive,
    storage_api::{operation, DataItem, Error, Operation, Result, StorageApi},
};

#[derive_where(Clone)]
pub struct Replica<C: cluster::Config, DB: Clone> {
    _cluster: Cluster<C>,
    database: DB,
}

impl<C, DB> Replica<C, DB>
where
    C: cluster::Config,
    DB: StorageApi + Clone,
{
    /// Creates a new [`Replica`].
    pub fn new(cluster: Cluster<C>, database: DB) -> Self {
        Self {
            _cluster: cluster,
            database,
        }
    }
}

impl<C, DB> StorageApi for Replica<C, DB>
where
    C: cluster::Config,
    DB: StorageApi + Clone,
{
    async fn execute_ref(&self, operation: &Operation<'_>) -> Result<operation::Output> {
        // TODO: once we add signatures to write operations check them here

        self.database
            .execute_ref(operation)
            .await
            .map_err(|_| Error::new(storage_api::ErrorKind::Internal))
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> storage_api::Result<impl Stream<Item = storage_api::Result<DataItem>> + Send> {
        self.database.read_data(keyrange, keyspace_version).await
    }
}
