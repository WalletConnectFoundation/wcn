pub use storage_api as storage;
use {
    consistency::ReplicationResults,
    derive_more::derive::AsRef,
    domain::Cluster,
    futures::{channel::oneshot, stream::FuturesUnordered, FutureExt, StreamExt},
    irn_core::cluster,
    std::{collections::HashSet, future::Future, hash::BuildHasher, sync::Arc},
    storage_api::client::RemoteStorage,
    xxhash_rust::xxh3::Xxh3Builder,
};

mod consistency;
mod reconciliation;

/// IRN replication driver.
#[derive(Clone)]
pub struct Driver {
    client_api: client_api::Client,
    storage_api: storage_api::Client,
}

impl Driver {
    /// Gets a [`storage::Record`] by the provided [`storage::Key`].
    pub async fn get(&self, key: storage::Key) -> Result<Option<storage::Record>> {
        self.replicate(Get { key }).await
    }

    /// Sets the provided [`storage::Entry`].
    pub async fn set(&self, entry: storage::Entry) -> Result<()> {
        self.replicate(Set { entry }).await
    }

    /// Deletes a [`storage::Entry`] by the provided [`storage::Key`].
    pub async fn del(&self, key: storage::Key) -> Result<()> {
        let version = storage::EntryVersion::new();
        self.replicate(Del { key, version }).await
    }

    /// Gets a [`storage::EntryExpiration`] by the provided [`storage::Key`].
    pub async fn get_exp(&self, key: storage::Key) -> Result<Option<storage::EntryExpiration>> {
        self.replicate(GetExp { key }).await
    }

    /// Sets [`storage::EntryExpiration`] on the [`storage::Entry`] with the
    /// provided [`storage::Key`].
    pub async fn set_exp(
        &self,
        key: storage::Key,
        expiration: impl Into<storage::EntryExpiration>,
    ) -> Result<()> {
        self.replicate(SetExp {
            key,
            expiration: expiration.into(),
            version: storage::EntryVersion::new(),
        })
        .await
    }

    /// Gets a map [`storage::Record`] by the provided [`storage::Key`] and
    /// [`storage::Field`].
    pub async fn hget(
        &self,
        key: storage::Key,
        field: storage::Field,
    ) -> Result<Option<storage::Record>> {
        self.replicate(HGet { key, field }).await
    }

    /// Sets the provided [`storage::MapEntry`].
    pub async fn hset(&self, entry: storage::MapEntry) -> Result<()> {
        self.replicate(HSet { entry }).await
    }

    /// Deletes a [`storage::MapEntry`] by the provided [`storage::Key`] and
    /// [`storage::Field`].
    pub async fn hdel(&self, key: storage::Key, field: storage::Field) -> Result<()> {
        self.replicate(HDel {
            key,
            field,
            version: storage::EntryVersion::new(),
        })
        .await
    }

    /// Gets a [`storage::EntryExpiration`] by the provided [`storage::Key`] and
    /// [`Field`].
    pub async fn hget_exp(
        &self,
        key: storage::Key,
        field: storage::Field,
    ) -> Result<Option<storage::EntryExpiration>> {
        self.replicate(HGetExp { key, field }).await
    }

    /// Sets [`storage::Expiration`] on the [`storage::MapEntry`] with the
    /// provided [`storage::Key`] and [`storage::Field`].
    pub async fn hset_exp(
        &self,
        key: storage::Key,
        field: storage::Field,
        expiration: impl Into<storage::EntryExpiration>,
    ) -> Result<()> {
        self.replicate(HSetExp {
            key,
            field,
            expiration: expiration.into(),
            version: storage::EntryVersion::new(),
        })
        .await
    }

    /// Returns cardinality of the map with the provided [`storage::Key`].
    pub async fn hcard(&self, key: storage::Key) -> Result<u64> {
        self.replicate(HCard { key }).await
    }

    /// Returns a [`storage::MapPage`] by iterating over the [`storage::Field`]s
    /// of the map with the provided [`storage::Key`].
    pub async fn hscan(
        &self,
        key: storage::Key,
        count: u32,
        cursor: Option<storage::Field>,
    ) -> Result<storage::MapPage> {
        self.replicate(HScan { key, count, cursor }).await
    }

    /// Returns [`Value`]s of the map with the provided [`Key`].
    pub async fn hvals(self, key: storage::Key) -> Result<Vec<storage::Value>> {
        // `1000` is generous, relay has ~100 limit for these small maps
        self.hscan(key, 1000, None)
            .await
            .map(|page| page.records.into_iter().map(|rec| rec.field).collect())
    }

    /// Publishes the provided message to the specified channel.
    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        todo!()
        // Publish::send(&self.rpc_client, &self.server_addr, PublishRequest {
        //     channel,
        //     message,
        // })
        // .await
        // .map_err(Into::into)
    }

    /// Subscribes to the [`storage::SubscriptionEvent`]s of the provided
    /// `channel`s, and handles them using the provided `event_handler`.
    pub async fn subscribe<F: Future<Output = ()> + Send + Sync>(
        &self,
        channels: HashSet<Vec<u8>>,
        event_handler: impl Fn(storage::SubscriptionEvent) -> F + Send + Sync,
    ) -> Result<()> {
        todo!()
        // Subscribe::send(
        //     &self.rpc_client,
        //     &self.server_addr,
        //     |mut tx, mut rx| async move {
        //         tx.send(SubscribeRequest { channels }).await?;

        //         loop {
        //             let resp = match rx.recv_message().await {
        //                 Ok(rpc_res) => rpc_res?,
        //                 Err(transport::Error::StreamFinished) => return
        // Ok(()),                 Err(err) => return Err(err.into()),
        //             };

        //             event_handler(SubscriptionEvent {
        //                 channel: resp.channel,
        //                 message: resp.message,
        //             })
        //             .await
        //         }
        //     },
        // )
        // .await
        // .map_err(Into::into)
    }

    async fn replicate<Op: StorageOperation>(&self, operation: Op) -> Result<Op::Output> {
        let (tx, rx) = oneshot::channel();

        tokio::spawn(replication_task(self.clone(), operation, tx));

        rx.await.map_err(|_| Error::TaskCancelled)?
    }

    fn cluster(&self) -> Arc<Cluster> {
        todo!()
    }
}

async fn replication_task<Op: StorageOperation>(
    driver: Driver,
    operation: Op,
    result_channel: oneshot::Sender<Result<Op::Output>>,
) {
    static HASHER: Xxh3Builder = Xxh3Builder::new();

    let key_hash = HASHER.hash_one(operation.as_ref().as_bytes());

    let cluster = driver.cluster();

    let replica_set = match cluster.replica_set(key_hash, Op::IS_WRITE) {
        Ok(set) => set,
        Err(err) => return drop(result_channel.send(Err(Error::Cluster(err)))),
    };

    let mut result_stream: FuturesUnordered<_> = replica_set
        .nodes
        .map(|node| {
            operation
                .execute(driver.storage_api.remote_storage(&node.addr))
                .map(|res| (&node.addr, res))
        })
        .collect();

    let mut quorum = consistency::MajorityQuorum::new(replica_set.required_count);

    let mut result_channel = Some(result_channel);
    while let Some((addr, result)) = result_stream.next().await {
        quorum.push(addr.clone(), result);

        if let Some(res) = quorum.is_reached() {
            if let Some(channel) = result_channel.take() {
                let _ = channel.send(res.clone().map_err(Error::Client));
            }
        }
    }

    if let Some(Ok(value)) = quorum.is_reached() {
        let stream: FuturesUnordered<_> = quorum
            .minority_replicas()
            .map(|addr| {
                operation
                    .repair(driver.storage_api.remote_storage(addr), value)
                    .map(|res| { // TODO: meter
                    })
            })
            .collect();

        return drop(stream.collect::<Vec<()>>().await);
    }

    let res = Op::reconcile(quorum.into_results(), replica_set.required_count)
        .ok_or(Error::InconsistentResults);

    if let Some(channel) = result_channel.take() {
        let _ = channel.send(res);
    }
}

trait StorageOperation: AsRef<storage::Key> + Send + Sync + 'static {
    type Output: Clone + Eq + Send;

    const IS_WRITE: bool;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> + Send;

    fn repair(
        &self,
        _storage: RemoteStorage<'_>,
        _output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<()>> + Send {
        async { Ok(()) }
    }

    fn reconcile(
        _results: ReplicationResults<Self::Output>,
        _required_replicas: usize,
    ) -> Option<Self::Output> {
        None
    }
}

#[derive(AsRef)]
struct Get {
    #[as_ref]
    key: storage::Key,
}

impl StorageOperation for Get {
    type Output = Option<storage::Record>;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.get(self.key.clone())
    }

    fn repair(
        &self,
        storage: RemoteStorage<'_>,
        output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<()>> {
        let entry = output.as_ref().map(|rec| storage::Entry {
            key: self.key.clone(),
            value: rec.value.clone(),
            expiration: rec.expiration.clone(),
            version: rec.version.clone(),
        });

        async move {
            if let Some(entry) = entry {
                storage.set(entry).await
            } else {
                Ok(())
            }
        }
    }
}

struct Set {
    entry: storage::Entry,
}

impl AsRef<storage::Key> for Set {
    fn as_ref(&self) -> &storage::Key {
        &self.entry.key
    }
}

impl StorageOperation for Set {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.set(self.entry.clone())
    }
}

#[derive(AsRef)]
struct Del {
    #[as_ref]
    key: storage::Key,
    version: storage::EntryVersion,
}

impl StorageOperation for Del {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.del(self.key.clone(), self.version)
    }
}

#[derive(AsRef)]
struct GetExp {
    #[as_ref]
    key: storage::Key,
}

impl StorageOperation for GetExp {
    type Output = Option<storage::EntryExpiration>;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.get_exp(self.key.clone())
    }
}

#[derive(AsRef)]
struct SetExp {
    #[as_ref]
    key: storage::Key,
    expiration: storage::EntryExpiration,
    version: storage::EntryVersion,
}

impl StorageOperation for SetExp {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.set_exp(self.key.clone(), self.expiration, self.version)
    }
}

#[derive(AsRef)]
struct HGet {
    #[as_ref]
    key: storage::Key,
    field: storage::Field,
}

impl StorageOperation for HGet {
    type Output = Option<storage::Record>;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hget(self.key.clone(), self.field.clone())
    }

    fn repair(
        &self,
        storage: RemoteStorage<'_>,
        output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<()>> {
        let entry = output.as_ref().map(|rec| storage::MapEntry {
            key: self.key.clone(),
            field: self.field.clone(),
            value: rec.value.clone(),
            expiration: rec.expiration.clone(),
            version: rec.version.clone(),
        });

        async move {
            if let Some(entry) = entry {
                storage.hset(entry).await
            } else {
                Ok(())
            }
        }
    }
}

struct HSet {
    entry: storage::MapEntry,
}

impl AsRef<storage::Key> for HSet {
    fn as_ref(&self) -> &storage::Key {
        &self.entry.key
    }
}

impl StorageOperation for HSet {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hset(self.entry.clone())
    }
}

#[derive(AsRef)]
struct HDel {
    #[as_ref]
    key: storage::Key,
    field: storage::Field,
    version: storage::EntryVersion,
}

impl StorageOperation for HDel {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hdel(self.key.clone(), self.field.clone(), self.version)
    }
}

#[derive(AsRef)]
struct HGetExp {
    #[as_ref]
    key: storage::Key,
    field: storage::Field,
}

impl StorageOperation for HGetExp {
    type Output = Option<storage::EntryExpiration>;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hget_exp(self.key.clone(), self.field.clone())
    }
}

#[derive(AsRef)]
struct HSetExp {
    #[as_ref]
    key: storage::Key,
    field: storage::Field,
    expiration: storage::EntryExpiration,
    version: storage::EntryVersion,
}

impl StorageOperation for HSetExp {
    type Output = ();

    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hset_exp(
            self.key.clone(),
            self.field.clone(),
            self.expiration,
            self.version,
        )
    }
}

#[derive(AsRef)]
struct HCard {
    #[as_ref]
    key: storage::Key,
}

impl StorageOperation for HCard {
    type Output = u64;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hcard(self.key.clone())
    }

    fn reconcile(
        results: ReplicationResults<Self::Output>,
        required_replicas: usize,
    ) -> Option<Self::Output> {
        reconciliation::reconcile_map_cardinality(results, required_replicas)
    }
}

#[derive(AsRef)]
struct HScan {
    #[as_ref]
    key: storage::Key,
    count: u32,
    cursor: Option<storage::Field>,
}

impl StorageOperation for HScan {
    type Output = storage::MapPage;

    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hscan(self.key.clone(), self.count, self.cursor.clone())
    }

    fn reconcile(
        results: ReplicationResults<Self::Output>,
        required_replicas: usize,
    ) -> Option<Self::Output> {
        reconciliation::reconcile_map_page(results, required_replicas)
    }
}

/// [`Driver`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error of a [`Driver`] operation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("Cluster error: {_0:?}")]
    Cluster(cluster::Error),

    #[error("Task cancelled")]
    TaskCancelled,

    #[error("Inconsistent results")]
    InconsistentResults,

    #[error("Client: {_0}")]
    Client(storage_api::client::Error),
}
