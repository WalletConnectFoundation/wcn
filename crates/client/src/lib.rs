use {
    arc_swap::ArcSwap,
    std::{
        net::SocketAddrV4,
        sync::Arc,
        time::{Duration, Instant},
    },
    tap::TapFallible as _,
    tokio::sync::oneshot,
    wc::{
        future::FutureExt,
        metrics::{
            self,
            EnumLabel,
            FutureExt as _,
            StringLabel,
            enum_ordinalize::Ordinalize,
            future_metrics,
        },
    },
    wcn_cluster::{node_operator::Id as NodeOperatorId, smart_contract},
    wcn_cluster_api::Error as ClusterError,
    wcn_storage_api2::{
        Error as CoordinatorError,
        MapEntryBorrowed,
        Namespace,
        Record,
        RecordBorrowed,
        RecordExpiration,
        RecordVersion,
        StorageApi,
        operation as op,
        rpc::client::CoordinatorConnection,
    },
};
pub use {
    wcn_cluster::EncryptionKey,
    wcn_rpc::{PeerId, identity::Keypair},
    wcn_storage_api2::{ErrorKind as CoordinatorErrorKind, MapPage},
};

mod cluster;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No available nodes")]
    NoAvailableNodes,

    #[error("Retries exhausted")]
    RetriesExhausted,

    #[error("Failed to initialize cluster: {0}")]
    ClusterCreation(#[from] wcn_cluster::CreationError),

    #[error("RPC client error: {0}")]
    Rpc(#[from] wcn_rpc::client2::Error),

    #[error("Cluster API error: {0}")]
    ClusterApi(#[from] ClusterError),

    #[error("Coordinator API error: {0}")]
    CoordinatorApi(#[from] CoordinatorError),

    #[error("Failed to read smart contract: {0}")]
    SmartContract(#[from] smart_contract::ReadError),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn internal(err: impl ToString) -> Self {
        Self::Internal(err.to_string())
    }
}

pub struct Config {
    /// [`Keypair`] to be used for RPC clients.
    pub keypair: Keypair,

    /// Symmetrical encryption key used for accessing the on-chain data.
    pub cluster_encryption_key: EncryptionKey,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a storage API operation.
    pub operation_timeout: Duration,

    /// Reconnection interval of the RPC client.
    pub reconnect_interval: Duration,

    /// Concurrency limit for the storage operations.
    pub max_concurrent_rpcs: u32,

    /// [`PeerAddr`]s of the bootstrap nodes.
    pub nodes: Vec<PeerAddr>,

    /// Additional metrics tag being used for all [`Driver`] metrics.
    pub metrics_tag: &'static str,
}

#[derive(Debug, Clone)]
pub struct PeerAddr {
    pub id: PeerId,
    pub addr: SocketAddrV4,
}

pub struct Client {
    cluster: cluster::Cluster,
    connection_timeout: Duration,
    metrics_tag: &'static str,
    _shutdown_tx: oneshot::Sender<()>,
}

impl Client {
    pub async fn new(config: Config) -> Result<Self, Error> {
        let cluster_api =
            wcn_cluster_api::rpc::ClusterApi::new().with_rpc_timeout(Duration::from_secs(5));

        let cluster_api_client_cfg = wcn_rpc::client2::Config {
            keypair: config.keypair.clone(),
            connection_timeout: config.connection_timeout,
            reconnect_interval: config.reconnect_interval,
            max_concurrent_rpcs: 50,
            priority: wcn_rpc::transport::Priority::High,
        };

        let cluster_api_client =
            wcn_rpc::client2::Client::new(cluster_api_client_cfg, cluster_api)?;

        let coordinator_api =
            wcn_storage_api2::rpc::CoordinatorApi::new().with_rpc_timeout(Duration::from_secs(2));

        let coordinator_api_client_cfg = wcn_rpc::client2::Config {
            keypair: config.keypair,
            connection_timeout: config.connection_timeout,
            reconnect_interval: config.reconnect_interval,
            max_concurrent_rpcs: config.max_concurrent_rpcs,
            priority: wcn_rpc::transport::Priority::High,
        };

        let coordinator_api_client =
            wcn_rpc::client2::Client::new(coordinator_api_client_cfg, coordinator_api)?;

        // Initialize the client using one or more bootstrap nodes:
        // - fetch the current version of the cluster view;
        // - using the cluster view, initialize [`wcn_cluster::Cluster`];
        // - from a properly initialized [`wcn_cluster::Cluster`] obtain a
        //   [`wcn_cluster::View`];
        // - use the view to create a different version of [`cluster::SmartContract`],
        //   which would always use an up-to-date version of the cluster view for the
        //   cluster API;
        // - spawn a task to monitor cluster updates and send an up-to-date version of
        //   the cluster view to the smart contract.
        let initial_cluster_view =
            cluster::fetch_cluster_view(&cluster_api_client, &config.nodes).await?;

        let cluster_cfg = cluster::Config {
            encryption_key: config.cluster_encryption_key,
            cluster_api: cluster_api_client,
            coordinator_api: coordinator_api_client,
        };

        let bootstrap_sc = cluster::SmartContract::Static(initial_cluster_view);
        let bootstrap_cluster = cluster::Cluster::new(cluster_cfg.clone(), bootstrap_sc).await?;
        let cluster_view = Arc::new(ArcSwap::new(bootstrap_cluster.view()));
        let dynamic_sc = cluster::SmartContract::Dynamic(cluster_view.clone());
        let cluster = cluster::Cluster::new(cluster_cfg, dynamic_sc).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(cluster::update_task(
            shutdown_rx,
            cluster.clone(),
            cluster_view,
        ));

        Ok(Self {
            cluster,
            connection_timeout: config.connection_timeout,
            metrics_tag: config.metrics_tag,
            _shutdown_tx: shutdown_tx,
        })
    }

    pub fn with_observer<O>(self, observer: O) -> WithObserver<O>
    where
        O: RequestObserver,
    {
        WithObserver {
            core: self,
            observer,
        }
    }
}

pub trait RequestExecutor {
    fn using_next_node<F, R>(&self, cb: F) -> Result<R, Error>
    where
        F: Fn(&cluster::Node) -> R;

    fn execute_request<T>(
        &self,
        client_conn: &CoordinatorConnection,
        op: &op::Operation<'_>,
    ) -> impl Future<Output = Result<T, CoordinatorError>> + Send + Sync
    where
        op::Output: op::DowncastOutput<T>;

    fn request<T>(
        &self,
        op: &op::Operation<'_>,
    ) -> impl Future<Output = Result<T, CoordinatorError>> + Send + Sync
    where
        op::Output: op::DowncastOutput<T>;
}

pub trait ClientBuilder: RequestExecutor + Sized {
    fn with_retries(self, max_attempts: usize) -> WithRetries<Self> {
        WithRetries {
            core: self,
            max_attempts,
        }
    }

    fn build(self) -> ReplicaClient<Self> {
        ReplicaClient {
            inner: Arc::new(self),
        }
    }
}

impl<T> ClientBuilder for T where T: RequestExecutor + Sized {}

pub struct RequestMetadata {
    pub operator_id: NodeOperatorId,
    pub node_id: PeerId,
    pub operation: OperationName,
    pub duration: Duration,
    pub result: Result<(), CoordinatorErrorKind>,
}

pub trait RequestObserver {
    fn observe(&self, metadata: RequestMetadata);
}

impl RequestExecutor for Client {
    fn using_next_node<F, R>(&self, cb: F) -> Result<R, Error>
    where
        F: Fn(&cluster::Node) -> R,
    {
        // Constraints:
        // - Each next request should go to a different operator.
        // - Find an available (i.e. connected) node of the next operator, filtering out
        //   broken connections. The expectation is that each operator should have at
        //   least one available node at all times.

        self.cluster.using_view(|view| {
            view.node_operators()
                .next()
                .find_next_node(|node| (!node.coordinator_conn.is_closed()).then(|| cb(node)))
                .ok_or_else(|| Error::NoAvailableNodes)
        })
    }

    async fn execute_request<T>(
        &self,
        client_conn: &CoordinatorConnection,
        op: &op::Operation<'_>,
    ) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        let op_name = op_name(op);

        let is_connected = client_conn
            .wait_open()
            .with_timeout(self.connection_timeout)
            .await
            .is_ok();

        if !is_connected {
            return Err(CoordinatorError::new(CoordinatorErrorKind::Timeout));
        }

        client_conn
            .execute_ref(op)
            .with_metrics(future_metrics!(
                "wcn_client_operation",
                EnumLabel<"name", OperationName> => op_name,
                StringLabel<"tag", &'static str> => &self.metrics_tag
            ))
            .await
            .tap_err(|err| {
                metrics::counter!("wcn_client_operation_errors",
                    EnumLabel<"operation", OperationName> => op_name,
                    EnumLabel<"error", CoordinatorErrorKind> => err.kind(),
                    StringLabel<"tag", &'static str> => &self.metrics_tag
                )
                .increment(1)
            })?
            .try_into()
            .map_err(|err| CoordinatorError::internal().with_message(err))
    }

    async fn request<T>(&self, op: &op::Operation<'_>) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        let conn = self
            .using_next_node(|node| node.coordinator_conn.clone())
            .map_err(coordinator_transport_err)?;

        self.execute_request(&conn, op).await
    }
}

pub struct WithRetries<C = Client> {
    core: C,
    max_attempts: usize,
}

impl<C> RequestExecutor for WithRetries<C>
where
    C: RequestExecutor + Send + Sync,
{
    fn using_next_node<F, R>(&self, cb: F) -> Result<R, Error>
    where
        F: Fn(&cluster::Node) -> R,
    {
        self.core.using_next_node(cb)
    }

    async fn execute_request<T>(
        &self,
        client_conn: &CoordinatorConnection,
        op: &op::Operation<'_>,
    ) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        self.core.execute_request(client_conn, op).await
    }

    async fn request<T>(&self, op: &op::Operation<'_>) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        let mut attempt = 0;

        while attempt < self.max_attempts {
            match self.core.request(op).await {
                Ok(data) => return Ok(data),

                Err(err) => match err.kind() {
                    CoordinatorErrorKind::Timeout | CoordinatorErrorKind::Transport => attempt += 1,

                    _ => return Err(err),
                },
            }
        }

        Err(CoordinatorError::internal().with_message(Error::RetriesExhausted))
    }
}

pub struct WithObserver<O> {
    core: Client,
    observer: O,
}

impl<O> RequestExecutor for WithObserver<O>
where
    O: RequestObserver + Send + Sync,
{
    fn using_next_node<F, R>(&self, cb: F) -> Result<R, Error>
    where
        F: Fn(&cluster::Node) -> R,
    {
        self.core.using_next_node(cb)
    }

    async fn execute_request<T>(
        &self,
        client_conn: &CoordinatorConnection,
        op: &op::Operation<'_>,
    ) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        self.core.execute_request(client_conn, op).await
    }

    async fn request<T>(&self, op: &op::Operation<'_>) -> Result<T, CoordinatorError>
    where
        op::Output: op::DowncastOutput<T>,
    {
        let op_name = op_name(op);
        let start_time = Instant::now();

        let (conn, mut metadata) = self
            .using_next_node(|node| {
                (node.coordinator_conn.clone(), RequestMetadata {
                    operator_id: node.operator_id,
                    node_id: node.node.peer_id,
                    operation: op_name,
                    duration: Duration::ZERO,
                    result: Ok(()),
                })
            })
            .map_err(coordinator_transport_err)?;

        let result = self.execute_request(&conn, op).await;

        metadata.duration = start_time.elapsed();
        metadata.result = result.as_ref().map(|_| ()).map_err(|err| err.kind());

        self.observer.observe(metadata);

        result
    }
}

#[derive(Clone)]
pub struct ReplicaClient<C> {
    inner: Arc<C>,
}

impl<C> ReplicaClient<C>
where
    C: RequestExecutor + Send + Sync,
{
    pub async fn get(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, Error> {
        Ok(self
            .request::<Option<Record>>(op::GetBorrowed {
                namespace,
                key: key.as_ref(),
                keyspace_version: None,
            })
            .await?
            .map(|rec| rec.value))
    }

    pub async fn set(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl: Duration,
    ) -> Result<(), Error> {
        self.request(op::SetBorrowed {
            namespace,
            key: key.as_ref(),
            record: RecordBorrowed {
                value: value.as_ref(),
                expiration: ttl.into(),
                version: RecordVersion::now(),
            },
            keyspace_version: None,
        })
        .await
    }

    pub async fn del(&self, namespace: Namespace, key: impl AsRef<[u8]>) -> Result<(), Error> {
        self.request(op::DelBorrowed {
            namespace,
            key: key.as_ref(),
            version: RecordVersion::now(),
            keyspace_version: None,
        })
        .await
    }

    pub async fn get_exp(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Duration>, Error> {
        Ok(self
            .request::<Option<RecordExpiration>>(op::GetExpBorrowed {
                namespace,
                key: key.as_ref(),
                keyspace_version: None,
            })
            .await?
            .map(Into::into))
    }

    pub async fn set_exp(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        ttl: Duration,
    ) -> Result<(), Error> {
        self.request(op::SetExpBorrowed {
            namespace,
            key: key.as_ref(),
            expiration: ttl.into(),
            version: RecordVersion::now(),
            keyspace_version: None,
        })
        .await
    }

    pub async fn hget(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        field: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, Error> {
        Ok(self
            .request::<Option<Record>>(op::HGetBorrowed {
                namespace,
                key: key.as_ref(),
                field: field.as_ref(),
                keyspace_version: None,
            })
            .await?
            .map(|rec| rec.value))
    }

    pub async fn hset(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        field: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl: Duration,
    ) -> Result<(), Error> {
        self.request(op::HSetBorrowed {
            namespace,
            key: key.as_ref(),
            entry: MapEntryBorrowed {
                field: field.as_ref(),
                record: RecordBorrowed {
                    value: value.as_ref(),
                    expiration: ttl.into(),
                    version: RecordVersion::now(),
                },
            },
            keyspace_version: None,
        })
        .await
    }

    pub async fn hdel(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        field: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        self.request(op::HDelBorrowed {
            namespace,
            key: key.as_ref(),
            field: field.as_ref(),
            version: RecordVersion::now(),
            keyspace_version: None,
        })
        .await
    }

    pub async fn hget_exp(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        field: impl AsRef<[u8]>,
    ) -> Result<Option<Duration>, Error> {
        Ok(self
            .request::<Option<RecordExpiration>>(op::HGetExpBorrowed {
                namespace,
                key: key.as_ref(),
                field: field.as_ref(),
                keyspace_version: None,
            })
            .await?
            .map(Into::into))
    }

    pub async fn hset_exp(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        field: impl AsRef<[u8]>,
        ttl: Duration,
    ) -> Result<(), Error> {
        self.request(op::HSetExpBorrowed {
            namespace,
            key: key.as_ref(),
            field: field.as_ref(),
            expiration: ttl.into(),
            version: RecordVersion::now(),
            keyspace_version: None,
        })
        .await
    }

    pub async fn hcard(&self, namespace: Namespace, key: impl AsRef<[u8]>) -> Result<u64, Error> {
        self.request(op::HCardBorrowed {
            namespace,
            key: key.as_ref(),
            keyspace_version: None,
        })
        .await
    }

    pub async fn hscan(
        &self,
        namespace: Namespace,
        key: impl AsRef<[u8]>,
        count: u32,
        cursor: Option<impl AsRef<[u8]>>,
    ) -> Result<MapPage, Error> {
        self.request::<MapPage>(op::HScanBorrowed {
            namespace,
            key: key.as_ref(),
            count,
            cursor: cursor.as_ref().map(|cursor| cursor.as_ref()),
            keyspace_version: None,
        })
        .await
    }

    async fn request<T>(&self, op: impl Into<op::Borrowed<'_>>) -> Result<T, Error>
    where
        op::Output: op::DowncastOutput<T>,
    {
        self.inner
            .request(&op::Operation::Borrowed(op.into()))
            .await
            .map_err(Into::into)
    }
}

#[inline]
fn coordinator_transport_err(err: impl ToString) -> CoordinatorError {
    CoordinatorError::new(CoordinatorErrorKind::Transport).with_message(err)
}

#[derive(Clone, Copy, Ordinalize)]
pub enum OperationName {
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

fn op_name(op: &op::Operation<'_>) -> OperationName {
    match op {
        op::Operation::Owned(op) => match op {
            op::Owned::Get(_) => OperationName::Get,
            op::Owned::Set(_) => OperationName::Set,
            op::Owned::Del(_) => OperationName::Del,
            op::Owned::GetExp(_) => OperationName::GetExp,
            op::Owned::SetExp(_) => OperationName::SetExp,
            op::Owned::HGet(_) => OperationName::HGet,
            op::Owned::HSet(_) => OperationName::HSet,
            op::Owned::HDel(_) => OperationName::HDel,
            op::Owned::HGetExp(_) => OperationName::HGetExp,
            op::Owned::HSetExp(_) => OperationName::HSetExp,
            op::Owned::HCard(_) => OperationName::HCard,
            op::Owned::HScan(_) => OperationName::HScan,
        },

        op::Operation::Borrowed(op) => match op {
            op::Borrowed::Get(_) => OperationName::Get,
            op::Borrowed::Set(_) => OperationName::Set,
            op::Borrowed::Del(_) => OperationName::Del,
            op::Borrowed::GetExp(_) => OperationName::GetExp,
            op::Borrowed::SetExp(_) => OperationName::SetExp,
            op::Borrowed::HGet(_) => OperationName::HGet,
            op::Borrowed::HSet(_) => OperationName::HSet,
            op::Borrowed::HDel(_) => OperationName::HDel,
            op::Borrowed::HGetExp(_) => OperationName::HGetExp,
            op::Borrowed::HSetExp(_) => OperationName::HSetExp,
            op::Borrowed::HCard(_) => OperationName::HCard,
            op::Borrowed::HScan(_) => OperationName::HScan,
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(dead_code)]
    struct Observer;

    impl RequestObserver for Observer {
        fn observe(&self, _metadata: RequestMetadata) {}
    }

    async fn _create_observable_client(
        config: Config,
    ) -> ReplicaClient<WithRetries<WithObserver<Observer>>> {
        Client::new(config)
            .await
            .unwrap()
            .with_observer(Observer)
            .with_retries(3)
            .build()
    }

    async fn _create_retryable_client(config: Config) -> ReplicaClient<WithRetries> {
        Client::new(config).await.unwrap().with_retries(3).build()
    }
}
