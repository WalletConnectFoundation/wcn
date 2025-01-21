pub use {
    client_api::SubscriptionEvent,
    storage_api::{self as storage, auth, identity, Multiaddr, PeerId},
};
use {
    consistency::ReplicationResults,
    derive_more::derive::AsRef,
    domain::{Cluster, HASHER},
    futures::{channel::oneshot, stream::FuturesUnordered, FutureExt, Stream, StreamExt},
    rpc::{quic, tcp, Connector},
    std::{collections::HashSet, future::Future, hash::BuildHasher, sync::Arc, time::Duration},
    storage_api::client::RemoteStorage,
    tap::{Pipe, TapFallible as _},
    wc::metrics::{
        self,
        enum_ordinalize::Ordinalize,
        future_metrics,
        EnumLabel,
        FutureExt as _,
        StringLabel,
    },
    wcn_core::cluster,
};

mod consistency;
mod reconciliation;

/// WCN replication driver.
#[derive(Clone)]
pub struct Driver {
    client_api: client_api::Client<quic::Connector>,
    storage_api: storage_api::Client<tcp::Connector>,
}

/// Replication config.
pub struct Config {
    /// [`Keypair`] to be used for RPC clients.
    pub keypair: identity::Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of an API operation.
    pub operation_timeout: Duration,

    /// [`Multiaddr`]s of the nodes to connect to.
    pub nodes: HashSet<Multiaddr>,

    /// A list of storage namespaces to be used.
    pub namespaces: Vec<auth::Auth>,
}

impl Config {
    /// Creates a new [`Config`] with the provided list of nodes to connect to
    /// and other options defaults.
    pub fn new(nodes: HashSet<Multiaddr>) -> Self {
        Self {
            keypair: identity::Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            nodes,
            namespaces: Vec::new(),
        }
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: identity::Keypair) -> Self {
        self.keypair = keypair;
        self
    }

    /// Overwrites [`Config::connection_timeout`].
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Overwrites [`Config::operation_timeout`].
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Overwrites [`Config::namespaces`].
    pub fn with_namespaces(mut self, namespaces: impl Into<Vec<auth::Auth>>) -> Self {
        self.namespaces = namespaces.into();
        self
    }
}

/// Error of [`Driver::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);

impl Driver {
    /// Creates a new replication [`Driver`].
    pub async fn new(cfg: Config) -> Result<Self, CreationError> {
        let quic_connector = quic::Connector::new(cfg.keypair.clone())
            .map_err(|err| CreationError(err.to_string()))?;

        let tcp_connector =
            tcp::Connector::new(cfg.keypair).map_err(|err| CreationError(err.to_string()))?;

        let client_api_cfg = client_api::client::Config::new(cfg.nodes)
            .with_connection_timeout(cfg.connection_timeout)
            .with_operation_timeout(cfg.operation_timeout)
            .with_namespaces(cfg.namespaces);

        let client_api = client_api::Client::new(quic_connector, client_api_cfg)
            .await
            .map_err(|err| CreationError(err.to_string()))?;

        let storage_api_cfg = storage_api::client::Config::new(client_api.auth_token())
            .with_connection_timeout(cfg.connection_timeout)
            .with_operation_timeout(cfg.operation_timeout);

        let storage_api = storage_api::Client::new(tcp_connector, storage_api_cfg);

        Ok(Self {
            client_api,
            storage_api,
        })
    }

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
    pub async fn hvals(&self, key: storage::Key) -> Result<Vec<storage::Value>> {
        // `1000` is generous, relay has ~100 limit for these small maps
        self.hscan(key, 1000, None)
            .await
            .map(|page| page.records.into_iter().map(|rec| rec.value).collect())
    }

    /// Publishes the provided message to the specified channel.
    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        self.client_api
            .publish(channel, message)
            .with_metrics(future_metrics!("wcn_replication_driver_publish"))
            .await
            .tap_err(|_| metrics::counter!("wcn_replication_driver_publish_errors").increment(1))
            .map_err(Error::ClientApi)
    }

    /// Subscribes to the [`storage::SubscriptionEvent`]s of the provided
    /// `channel`s, and handles them using the provided `event_handler`.
    pub async fn subscribe(
        &self,
        channels: HashSet<Vec<u8>>,
    ) -> Result<impl Stream<Item = Result<SubscriptionEvent>>> {
        let stream = self
            .client_api
            .subscribe(channels)
            .with_metrics(future_metrics!("wcn_replication_driver_subscribe"))
            .await
            .tap_err(|_| metrics::counter!("wcn_replication_driver_subscribe_errors").increment(1))
            .map_err(Error::ClientApi)?
            .map(|res| res.map_err(Error::ClientApi));

        Ok(stream)
    }

    async fn replicate<Op: StorageOperation>(&self, operation: Op) -> Result<Op::Output> {
        async move {
            ReplicationTask::spawn(self, operation)
                .await
                .map_err(|_| Error::TaskCancelled)?
        }
        .with_metrics(future_metrics!(
            "wcn_replication_driver_operation",
            EnumLabel<"name", OperationName> => Op::NAME
        ))
        .await
        .tap_err(|err| {
            metrics::counter!("wcn_replication_driver_operation_errors",
                EnumLabel<"operation", OperationName> => Op::NAME,
                StringLabel<"error"> => err.as_str()
            )
            .increment(1)
        })
    }

    fn cluster(&self) -> Arc<Cluster> {
        self.client_api.cluster().load_full()
    }
}

type Quorum<T> = consistency::MajorityQuorum<T>;

struct ReplicationTask<Op: StorageOperation> {
    driver: Driver,

    operation: Op,
    key_hash: u64,

    result_channel: Option<oneshot::Sender<Result<Op::Output>>>,
}

impl<Op: StorageOperation> ReplicationTask<Op> {
    fn spawn(driver: &Driver, operation: Op) -> oneshot::Receiver<Result<Op::Output>> {
        let key_hash = HASHER.hash_one(operation.as_ref().as_bytes());

        let (tx, rx) = oneshot::channel();

        Self {
            driver: driver.clone(),
            operation,
            key_hash,
            result_channel: Some(tx),
        }
        .run()
        .with_metrics(future_metrics!("wcn_replication_driver_task",
            EnumLabel<"operation", OperationName> => Op::NAME
        ))
        .pipe(tokio::spawn);

        rx
    }

    async fn run(mut self) {
        let mut attempt = 0;

        // Retry once if we've got a `KeyspaceVersionMismatch` error.
        let quorum = loop {
            attempt += 1;

            match self.execute_operation().await {
                Ok(quorum) => break quorum,
                Err(err) if err.is_keyspace_version_mismatch() && attempt < 2 => {
                    // TODO: reconsider this value / make configurable once tested on Mainnet
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                Err(err) => {
                    if let Some(channel) = self.result_channel.take() {
                        let _ = channel.send(Err(err));
                    }
                    return;
                }
            }
        };

        if let Some(Ok(value)) = quorum.is_reached() {
            self.repair(&quorum, value).await;
        }

        if let Some(channel) = self.result_channel.take() {
            let _ = channel.send(self.reconcile(quorum));
        }
    }

    async fn execute_operation(&mut self) -> Result<consistency::MajorityQuorum<Op::Output>> {
        let cluster = self.driver.cluster();

        let replica_set = match cluster.replica_set(self.key_hash, Op::IS_WRITE) {
            Ok(set) => set,
            Err(err) => return Err(Error::Cluster(err)),
        };

        let mut result_stream: FuturesUnordered<_> = replica_set
            .nodes
            .map(|node| {
                self.operation
                    .execute(self.driver.storage_api.remote_storage(&node.addr))
                    .map(|res| (&node.addr, res))
            })
            .collect();

        let mut quorum = consistency::MajorityQuorum::new(replica_set.required_count);

        while let Some((addr, result)) = result_stream.next().await {
            quorum.push(addr.clone(), result);

            let Some(result) = quorum.is_reached() else {
                continue;
            };

            match result {
                Ok(value) => {
                    if let Some(channel) = self.result_channel.take() {
                        let _ = channel.send(Ok(value.clone()));
                    }
                }
                Err(err) => return Err(Error::StorageApi(err.clone())),
            };
        }

        Ok(quorum)
    }

    async fn repair(&self, quorum: &Quorum<Op::Output>, value: &Op::Output) {
        let stream: FuturesUnordered<_> = quorum
            .minority_replicas()
            .map(|addr| {
                self.operation
                    .repair(self.driver.storage_api.remote_storage(addr), value)
                    .map(|res| match res {
                        Ok(true) => {
                            tracing::debug!("repaired");
                            metrics::counter!("wcn_replication_driver_read_repairs",
                                EnumLabel<"operation_name", OperationName> => Op::NAME
                            )
                            .increment(1);
                        }
                        Ok(false) => {}
                        Err(err) => {
                            tracing::debug!(?err, "repair failed");
                            metrics::counter!("wcn_replication_driver_read_repair_errors",
                                EnumLabel<"operation_name", OperationName> => Op::NAME
                            )
                            .increment(1);
                        }
                    })
            })
            .collect();

        stream.collect::<Vec<()>>().await;
    }

    fn reconcile(&self, quorum: Quorum<Op::Output>) -> Result<Op::Output> {
        let required_replicas = quorum.threshold();
        match Op::reconcile(quorum.into_results(), required_replicas) {
            Some(Ok(value)) => {
                metrics::counter!("wcn_replication_driver_reconciliations",
                    EnumLabel<"operation_name", OperationName> => Op::NAME
                )
                .increment(1);
                Ok(value)
            }
            Some(Err(_)) => {
                metrics::counter!("wcn_replication_driver_reconciliation_errors",
                    EnumLabel<"operation_name", OperationName> => Op::NAME
                )
                .increment(1);
                Err(Error::InconsistentResults)
            }
            None => Err(Error::InconsistentResults),
        }
    }
}

#[derive(Clone, Copy, Ordinalize)]
enum OperationName {
    Get,
    Set,
    Del,
    GetExp,
    SetExp,
    HGet,
    HSet,
    HDel,
    HGetExp,
    HSetExp,
    HCard,
    HScan,
}

impl metrics::Enum for OperationName {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Del => "del",
            Self::GetExp => "get_exp",
            Self::SetExp => "set_exp",
            Self::HGet => "hget",
            Self::HSet => "hset",
            Self::HDel => "hdel",
            Self::HGetExp => "hget_exp",
            Self::HSetExp => "hset_exp",
            Self::HCard => "hcard",
            Self::HScan => "hscan",
        }
    }
}

trait StorageOperation: AsRef<storage::Key> + Send + Sync + 'static {
    type Output: Clone + Eq + Send + Sync;

    const NAME: OperationName;
    const IS_WRITE: bool;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> + Send;

    fn repair(
        &self,
        _storage: RemoteStorage<'_, impl Connector>,
        _output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<bool>> + Send {
        async { Ok(false) }
    }

    fn reconcile(
        _results: ReplicationResults<Self::Output>,
        _required_replicas: usize,
    ) -> Option<reconciliation::Result<Self::Output>> {
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

    const NAME: OperationName = OperationName::Get;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.get(self.key.clone())
    }

    fn repair(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
        output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<bool>> {
        let entry = output.as_ref().map(|rec| storage::Entry {
            key: self.key.clone(),
            value: rec.value.clone(),
            expiration: rec.expiration,
            version: rec.version,
        });

        async move {
            if let Some(entry) = entry {
                storage.set(entry).await.map(|()| true)
            } else {
                Ok(false)
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

    const NAME: OperationName = OperationName::Set;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::Del;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::GetExp;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::SetExp;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::HGet;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hget(self.key.clone(), self.field.clone())
    }

    fn repair(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
        output: &Self::Output,
    ) -> impl Future<Output = storage_api::client::Result<bool>> {
        let entry = output.as_ref().map(|rec| storage::MapEntry {
            key: self.key.clone(),
            field: self.field.clone(),
            value: rec.value.clone(),
            expiration: rec.expiration,
            version: rec.version,
        });

        async move {
            if let Some(entry) = entry {
                storage.hset(entry).await.map(|()| true)
            } else {
                Ok(false)
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

    const NAME: OperationName = OperationName::HSet;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::HDel;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::HGetExp;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::HSetExp;
    const IS_WRITE: bool = true;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
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

    const NAME: OperationName = OperationName::HCard;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hcard(self.key.clone())
    }

    fn reconcile(
        results: ReplicationResults<Self::Output>,
        required_replicas: usize,
    ) -> Option<reconciliation::Result<Self::Output>> {
        Some(reconciliation::reconcile_map_cardinality(
            results,
            required_replicas,
        ))
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

    const NAME: OperationName = OperationName::HScan;
    const IS_WRITE: bool = false;

    fn execute(
        &self,
        storage: RemoteStorage<'_, impl Connector>,
    ) -> impl Future<Output = storage_api::client::Result<Self::Output>> {
        storage.hscan(self.key.clone(), self.count, self.cursor.clone())
    }

    fn reconcile(
        results: ReplicationResults<Self::Output>,
        required_replicas: usize,
    ) -> Option<reconciliation::Result<Self::Output>> {
        Some(reconciliation::reconcile_map_page(
            results,
            required_replicas,
        ))
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

    #[error("Storage API: {_0}")]
    StorageApi(storage_api::client::Error),

    #[error("Client API: {_0}")]
    ClientApi(client_api::client::Error),
}

impl Error {
    fn is_keyspace_version_mismatch(&self) -> bool {
        matches!(
            self,
            Self::StorageApi(storage_api::client::Error::KeyspaceVersionMismatch)
        )
    }

    fn as_str(&self) -> &'static str {
        use {
            client_api::client::Error as ClientError,
            cluster::Error as ClusterError,
            storage::client::Error as StorageError,
        };

        match self {
            Self::Cluster(err) => match err {
                ClusterError::NotBootstrapped => "cluster_not_bootstrapped",
                ClusterError::NodeAlreadyExists => "cluster_node_already_exists",
                ClusterError::NodeAlreadyStarted => "cluster_node_already_started",
                ClusterError::UnknownNode => "cluster_unknown_node",
                ClusterError::TooManyNodes => "cluster_too_many_nodes",
                ClusterError::TooFewNodes => "cluster_too_few_nodes",
                ClusterError::NotNormal => "cluster_not_normal",
                ClusterError::NoMigration => "cluster_no_migration",
                ClusterError::KeyspaceVersionMismatch => "cluster_keyspace_version_mismatch",
                ClusterError::InvalidNode(_) => "cluster_invalid_node",
                ClusterError::Bug(_) => "cluster_bug",
            },
            Self::TaskCancelled => "task_cancelled",
            Self::InconsistentResults => "inconsistent_results",
            Self::StorageApi(err) => match err {
                StorageError::Transport(_) => "storage_transport",
                StorageError::Timeout => "storage_timeout",
                StorageError::Throttled => "storage_throttled",
                StorageError::Unauthorized => "storage_unauthorized",
                StorageError::KeyspaceVersionMismatch => "storage_keyspace_version_mismatch",
                StorageError::Other(_) => "storage_other",
            },
            Self::ClientApi(err) => match err {
                ClientError::Api(_) => "client_api",
                ClientError::Transport(_) => "client_transport",
                ClientError::Unauthorized => "client_unauthorized",
                ClientError::Timeout => "client_timeout",
                ClientError::Serialization => "client_serialization",
                ClientError::TokenTtl => "client_token_ttl",
                ClientError::TokenUpdate(_) => "client_token_update",
                ClientError::ClusterUpdate(_) => "client_cluster_update",
                ClientError::Cluster(_) => "client_cluster",
                ClientError::NodeNotAvailable => "client_node_not_available",
                ClientError::Other(_) => "client_other",
            },
        }
    }
}
