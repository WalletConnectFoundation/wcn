use {
    super::*,
    crate::{operation, MapPage, MapRecord, Operation, Record, StorageApi},
    futures::SinkExt,
    std::{collections::HashSet, result::Result as StdResult, time::Duration},
    wcn_rpc::{
        client::middleware::{MeteredExt, Timeouts, WithTimeouts, WithTimeoutsExt as _},
        identity::Keypair,
        middleware::Metered,
        transport::{self, NoHandshake},
        PeerAddr,
    },
};

/// Storage API RPC client.
#[derive(Clone)]
pub struct Client {
    inner: Inner,
}

type Inner = Metered<WithTimeouts<wcn_rpc::quic::Client>>;

/// [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Name of the RPC server the [`Client`] is supposed to connect to.
    pub server_name: rpc::ServerName,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] RPC call.
    pub rpc_timeout: Duration,

    /// Additional label to be used for all metrics of the [`Server`].
    pub metrics_tag: &'static str,
}

impl Config {
    pub fn new(server_name: rpc::ServerName) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            server_name,
            connection_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(10),
            metrics_tag: "default",
        }
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = keypair;
        self
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
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
        self.rpc_timeout = timeout;
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Overwrites [`Config::metrics_tag`].
    pub fn with_metrics_tag(mut self, tag: &'static str) -> Self {
        self.metrics_tag = tag;
        self
    }
}

impl Client {
    /// Creates a new [`Client`].
    pub fn new(config: Config) -> StdResult<Self, CreationError> {
        let rpc_client_config = wcn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
            server_name: config.server_name,
            priority: transport::Priority::High,
        };

        let timeouts = Timeouts::new().with_default(config.rpc_timeout);

        let rpc_client = wcn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .with_timeouts(timeouts)
            .metered_with_tag(config.metrics_tag);

        Ok(Self { inner: rpc_client })
    }

    pub fn remote_storage<'a>(&'a self, server_addr: &'a PeerAddr) -> RemoteStorage<'a> {
        RemoteStorage {
            client: self,
            server_addr,
            expected_keyspace_version: None,
        }
    }
}

/// Handle to a remote Storage API (Server).
#[derive(Clone, Copy)]
pub struct RemoteStorage<'a> {
    client: &'a Client,
    server_addr: &'a PeerAddr,
    expected_keyspace_version: Option<u64>,
}

impl RemoteStorage<'_> {
    fn rpc_client(&self) -> &Inner {
        &self.client.inner
    }

    fn context(&self, namespace: &Namespace) -> Context {
        Context {
            namespace_node_operator_id: namespace.node_operator_id,
            namespace_id: namespace.id,
            keyspace_version: self.expected_keyspace_version,
        }
    }

    /// Specifies the expected version of the keyspace of the [`RemoteStorage`].
    pub fn expecting_keyspace_version(mut self, version: u64) -> Self {
        self.expected_keyspace_version = Some(version);
        self
    }
}

impl<'a> StorageApi for RemoteStorage<'a> {
    type Error = Error;

    async fn get(&self, get: operation::Get) -> Result<Option<Record>> {
        Get::send(self.rpc_client(), self.server_addr, &GetRequest {
            context: self.context(&get.namespace),
            key: get.key.into(),
        })
        .await
        .map(|opt| {
            opt.map(|resp| Record {
                value: resp.value.into(),
                expiration: resp.expiration.into(),
                version: resp.version.into(),
            })
        })
        .map_err(Into::into)
    }

    async fn set(&self, set: operation::Set) -> Result<()> {
        Set::send(self.rpc_client(), self.server_addr, &SetRequest {
            context: self.context(&set.namespace),
            key: set.entry.key.into(),
            value: set.entry.value.into(),
            expiration: set.entry.expiration.into(),
            version: set.entry.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn set_exp(&self, set_exp: operation::SetExp) -> Result<()> {
        SetExp::send(self.rpc_client(), self.server_addr, &SetExpRequest {
            context: self.context(&set_exp.namespace),
            key: set_exp.key.into(),
            expiration: set_exp.expiration.into(),
            version: set_exp.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn del(&self, del: operation::Del) -> Result<()> {
        Del::send(self.rpc_client(), self.server_addr, &DelRequest {
            context: self.context(&del.namespace),
            key: del.key.into(),
            version: del.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn get_exp(&self, get_exp: operation::GetExp) -> Result<Option<EntryExpiration>> {
        GetExp::send(self.rpc_client(), self.server_addr, &GetExpRequest {
            context: self.context(&get_exp.namespace),
            key: get_exp.key.into(),
        })
        .await
        .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
        .map_err(Into::into)
    }

    async fn hget(&self, hget: operation::HGet) -> Result<Option<Record>> {
        HGet::send(self.rpc_client(), self.server_addr, &HGetRequest {
            context: self.context(&hget.namespace),
            key: hget.key.into(),
            field: hget.field.into(),
        })
        .await
        .map(|opt| {
            opt.map(|resp| Record {
                value: resp.value.into(),
                expiration: resp.expiration.into(),
                version: resp.version.into(),
            })
        })
        .map_err(Into::into)
    }

    async fn hset(&self, hset: operation::HSet) -> Result<()> {
        HSet::send(self.rpc_client(), self.server_addr, &HSetRequest {
            context: self.context(&hset.namespace),
            key: hset.entry.key.into(),
            field: hset.entry.field.into(),
            value: hset.entry.value.into(),
            expiration: hset.entry.expiration.into(),
            version: hset.entry.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn hdel(&self, hdel: operation::HDel) -> Result<()> {
        HDel::send(self.rpc_client(), self.server_addr, &HDelRequest {
            context: self.context(&hdel.namespace),
            key: hdel.key.into(),
            field: hdel.field.into(),
            version: hdel.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn hget_exp(&self, hget_exp: operation::HGetExp) -> Result<Option<EntryExpiration>> {
        HGetExp::send(self.rpc_client(), self.server_addr, &HGetExpRequest {
            context: self.context(&hget_exp.namespace),
            key: hget_exp.key.into(),
            field: hget_exp.field.into(),
        })
        .await
        .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
        .map_err(Into::into)
    }

    async fn hset_exp(&self, hset_exp: operation::HSetExp) -> Result<()> {
        HSetExp::send(self.rpc_client(), self.server_addr, &HSetExpRequest {
            context: self.context(&hset_exp.namespace),
            key: hset_exp.key.into(),
            field: hset_exp.field.into(),
            expiration: hset_exp.expiration.into(),
            version: hset_exp.version.into(),
        })
        .await
        .map_err(Into::into)
    }

    async fn hcard(&self, hcard: operation::HCard) -> Result<u64> {
        HCard::send(self.rpc_client(), self.server_addr, &HCardRequest {
            context: self.context(&hcard.namespace),
            key: hcard.key.into(),
        })
        .await
        .map(|resp| resp.cardinality)
        .map_err(Into::into)
    }

    async fn hscan(&self, hscan: operation::HScan) -> Result<MapPage> {
        let count = hscan.count;

        let resp = HScan::send(self.rpc_client(), self.server_addr, &HScanRequest {
            context: self.context(&hscan.namespace),
            key: hscan.key.into(),
            count: hscan.count,
            cursor: hscan.cursor.map(Into::into),
        })
        .await
        .map_err(Error::from)?;

        Ok(MapPage {
            has_next: resp.records.len() >= count as usize,
            records: resp
                .records
                .into_iter()
                .map(|record| MapRecord {
                    field: record.field.into(),
                    value: record.value.into(),
                    expiration: EntryExpiration::from(record.expiration),
                    version: EntryVersion::from(record.version),
                })
                .collect(),
        })
    }

    async fn execute(
        &self,
        operation: impl Into<crate::Operation> + Send,
    ) -> Result<operation::Output> {
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

/// Error of [`Client::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);

/// Error of a [`Client`] operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum Error {
    /// Transport errort.
    #[error("Transport: {_0}")]
    Transport(String),

    /// Operation timed out.
    #[error("Timeout")]
    Timeout,

    /// Server is throttling.
    #[error("Throttled")]
    Throttled,

    /// Client is not authorized to perform the operation.
    #[error("Unauthorized")]
    Unauthorized,

    /// Keyspace versions of client and server don't match.
    #[error("Keyspace version mismatch")]
    KeyspaceVersionMismatch,

    /// Other client/server error.
    #[error("{_0}")]
    Other(String),
}

impl From<wcn_rpc::client::Error> for Error {
    fn from(err: wcn_rpc::client::Error) -> Self {
        let rpc_err = match err {
            wcn_rpc::client::Error::Transport(err) => return Self::Transport(err.to_string()),
            wcn_rpc::client::Error::Rpc { error, .. } => error,
        };

        match rpc_err.code.as_ref() {
            wcn_rpc::error_code::TIMEOUT => Self::Timeout,
            error_code::KEYSPACE_VERSION_MISMATCH => Self::KeyspaceVersionMismatch,
            error_code::UNAUTHORIZED => Self::Unauthorized,
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<operation::WrongOutput> for Error {
    fn from(err: operation::WrongOutput) -> Self {
        Self::Other(err.to_string())
    }
}
