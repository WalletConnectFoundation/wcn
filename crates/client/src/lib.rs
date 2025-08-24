use {
    arc_swap::ArcSwap,
    derive_where::derive_where,
    encryption::Encrypt as _,
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
    wcn_storage_api::{
        MapEntryBorrowed,
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
    encryption::{Error as EncryptionError, Key as EncryptionKey},
    libp2p_identity::{Keypair, PeerId},
    smart_contract::ReadError as SmartContractError,
    wcn_cluster::{CreationError as ClusterCreationError, EncryptionKey as ClusterKey},
    wcn_cluster_api::Error as ClusterError,
    wcn_rpc::client::Error as RpcError,
    wcn_storage_api::{
        Error as CoordinatorError,
        ErrorKind as CoordinatorErrorKind,
        MapPage,
        Namespace,
    },
};

mod cluster;
mod encryption;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No available nodes")]
    NoAvailableNodes,

    #[error("Retries exhausted")]
    RetriesExhausted,

    #[error("Failed to initialize cluster: {0}")]
    ClusterCreation(#[from] ClusterCreationError),

    #[error("RPC client error: {0}")]
    Rpc(#[from] RpcError),

    #[error("Cluster API error: {0}")]
    ClusterApi(#[from] ClusterError),

    #[error("Coordinator API error: {0}")]
    CoordinatorApi(#[from] CoordinatorError),

    #[error("Failed to read smart contract: {0}")]
    SmartContract(#[from] SmartContractError),

    #[error("Encryption failed: {0}")]
    Encryption(#[from] EncryptionError),

    #[error("Invalid response type")]
    InvalidResponseType,

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
    pub cluster_key: ClusterKey,

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

        let cluster_api_client_cfg = wcn_rpc::client::Config {
            keypair: config.keypair.clone(),
            connection_timeout: config.connection_timeout,
            reconnect_interval: config.reconnect_interval,
            max_concurrent_rpcs: 50,
            priority: wcn_rpc::transport::Priority::High,
        };

        let cluster_api_client = wcn_rpc::client::Client::new(cluster_api_client_cfg, cluster_api)?;

        let coordinator_api =
            wcn_storage_api::rpc::CoordinatorApi::new().with_rpc_timeout(Duration::from_secs(2));

        let coordinator_api_client_cfg = wcn_rpc::client::Config {
            keypair: config.keypair,
            connection_timeout: config.connection_timeout,
            reconnect_interval: config.reconnect_interval,
            max_concurrent_rpcs: config.max_concurrent_rpcs,
            priority: wcn_rpc::transport::Priority::High,
        };

        let coordinator_api_client =
            wcn_rpc::client::Client::new(coordinator_api_client_cfg, coordinator_api)?;

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
            encryption_key: config.cluster_key,
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

    fn using_next_node<F, R>(&self, cb: F) -> R
    where
        F: Fn(&cluster::Node) -> R,
    {
        // Constraints:
        // - Each next request should go to a different operator.
        // - Find an available (i.e. connected) node of the next operator, filtering out
        //   broken connections. The expectation is that each operator should have at
        //   least one available node at all times.

        self.cluster.using_view(|view| {
            let operators = view.node_operators();

            // Iterate over all of the operators to find one with a connected node.
            let result = operators.find_next_operator(|operator| {
                operator
                    .find_next_node(|node| (!node.coordinator_conn.is_closed()).then(|| cb(node)))
            });

            if let Some(result) = result {
                // We've found a connected node.
                result
            } else {
                // If the above failed, return the next node in hopes that the connection will
                // be established during the request.
                cb(operators.next().next_node())
            }
        })
    }

    async fn execute_request(
        &self,
        client_conn: &CoordinatorConnection,
        op: &op::Operation<'_>,
    ) -> Result<op::Output, Error> {
        let op_name = op_name(op);

        let is_connected = client_conn
            .wait_open()
            .with_timeout(self.connection_timeout)
            .await
            .is_ok();

        if !is_connected {
            // Getting to this point means we've tried every operator to find a connected
            // node and failed. Then we tried to open connection to the next node and also
            // failed.
            return Err(Error::NoAvailableNodes);
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
            })
            .map_err(Into::into)
    }
}

pub trait RequestExecutor {
    fn request(
        &self,
        op: op::Operation<'_>,
    ) -> impl Future<Output = Result<op::Output, Error>> + Send + Sync;
}

pub trait ClientBuilder: RequestExecutor + Sized {
    fn with_retries(self, max_attempts: usize) -> WithRetries<Self> {
        WithRetries {
            core: self,
            max_attempts,
        }
    }

    fn with_encryption(self, key: EncryptionKey) -> WithEncryption<Self> {
        WithEncryption { core: self, key }
    }

    fn build(self) -> ReplicaClient<Self> {
        ReplicaClient {
            inner: Arc::new(self),
        }
    }
}

impl<T> ClientBuilder for T where T: RequestExecutor + Sized {}

#[derive(Debug, Clone)]
pub struct RequestMetadata {
    pub operator_id: NodeOperatorId,
    pub node_id: PeerId,
    pub operation: OperationName,
    pub duration: Duration,
}

pub trait RequestObserver {
    fn observe(&self, metadata: RequestMetadata, result: &Result<op::Output, Error>);
}

impl RequestExecutor for Client {
    async fn request(&self, op: op::Operation<'_>) -> Result<op::Output, Error> {
        let conn = self.using_next_node(|node| node.coordinator_conn.clone());
        self.execute_request(&conn, &op).await
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
    async fn request(&self, op: op::Operation<'_>) -> Result<op::Output, Error> {
        let mut attempt = 0;

        while attempt < self.max_attempts {
            match self.core.request(op.clone()).await {
                Ok(data) => return Ok(data),

                Err(err) => match err {
                    Error::CoordinatorApi(err)
                        if err.kind() == CoordinatorErrorKind::Timeout
                            || err.kind() == CoordinatorErrorKind::Transport =>
                    {
                        attempt += 1
                    }

                    err => return Err(err),
                },
            }
        }

        Err(Error::RetriesExhausted)
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
    async fn request(&self, op: op::Operation<'_>) -> Result<op::Output, Error> {
        let op_name = op_name(&op);
        let start_time = Instant::now();

        let (conn, mut metadata) = self.core.using_next_node(|node| {
            (node.coordinator_conn.clone(), RequestMetadata {
                operator_id: node.operator_id,
                node_id: node.node.peer_id,
                operation: op_name,
                duration: Duration::ZERO,
            })
        });

        let result = self.core.execute_request(&conn, &op).await;
        metadata.duration = start_time.elapsed();
        self.observer.observe(metadata, &result);

        result
    }
}

pub struct WithEncryption<C = Client> {
    core: C,
    key: EncryptionKey,
}

impl<C> RequestExecutor for WithEncryption<C>
where
    C: RequestExecutor + Send + Sync,
{
    async fn request(&self, op: op::Operation<'_>) -> Result<op::Output, Error> {
        let op = op.encrypt(&self.key)?;
        let mut output = self.core.request(op).await?;
        encryption::decrypt_output(&mut output, &self.key)?;
        Ok(output)
    }
}

#[derive_where(Clone)]
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
    ) -> Result<Option<Record>, Error> {
        self.request::<Option<Record>>(op::GetBorrowed {
            namespace,
            key: key.as_ref(),
            keyspace_version: None,
        })
        .await
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
    ) -> Result<Option<Record>, Error> {
        self.request::<Option<Record>>(op::HGetBorrowed {
            namespace,
            key: key.as_ref(),
            field: field.as_ref(),
            keyspace_version: None,
        })
        .await
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
            .request(op::Operation::Borrowed(op.into()))
            .await?
            .try_into()
            .map_err(|_| Error::InvalidResponseType)
    }
}

#[derive(Debug, Clone, Copy, Ordinalize)]
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
        fn observe(&self, _metadata: RequestMetadata, _result: &Result<op::Output, Error>) {}
    }

    async fn _create_observable_client(
        config: Config,
    ) -> ReplicaClient<WithEncryption<WithRetries<WithObserver<Observer>>>> {
        Client::new(config)
            .await
            .unwrap()
            .with_observer(Observer)
            .with_retries(3)
            .with_encryption(EncryptionKey::new(b"12345").unwrap())
            .build()
    }

    async fn _create_retryable_client(config: Config) -> ReplicaClient<WithRetries> {
        Client::new(config).await.unwrap().with_retries(3).build()
    }

    async fn _create_encrypted_client(config: Config) -> ReplicaClient<WithEncryption> {
        Client::new(config)
            .await
            .unwrap()
            .with_encryption(EncryptionKey::new(b"12345").unwrap())
            .build()
    }
}
