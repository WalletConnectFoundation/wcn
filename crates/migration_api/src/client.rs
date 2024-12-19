use {
    super::*,
    futures::{SinkExt as _, Stream, StreamExt as _},
    irn_rpc::{
        client::middleware::{MeteredExt, WithTimeoutsExt},
        identity::Keypair,
        middleware::{Metered, Timeouts, WithTimeouts},
        transport::{self, NoHandshake},
    },
    std::{collections::HashSet, io, result::Result as StdResult, time::Duration},
};

/// Migration API client.
#[derive(Clone)]
pub struct Client {
    rpc: RpcClient,
}

type RpcClient = Metered<WithTimeouts<irn_rpc::quic::Client>>;

/// [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] operation.
    pub operation_timeout: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
        }
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
        self.operation_timeout = timeout;
        self
    }
}

impl Client {
    /// Creates a new [`Client`].
    pub fn new(config: Config) -> StdResult<Self, CreationError> {
        let rpc_client_config = irn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
            server_name: crate::RPC_SERVER_NAME,
            priority: transport::Priority::Low,
        };

        let timeouts = Timeouts::new().with_default(config.operation_timeout);

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .with_timeouts(timeouts)
            .metered();

        Ok(Self { rpc: rpc_client })
    }

    /// Pulls data from a remote peer.
    pub async fn pull_data(
        &self,
        from: &Multiaddr,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> Result<impl Stream<Item = Result<ExportItem>>> {
        let range = &keyrange;
        PullData::send(&self.rpc, from, &move |mut tx, rx| async move {
            tx.send(PullDataRequest {
                keyrange: range.clone(),
                keyspace_version,
            })
            .await?;

            Ok(rx.map(|item| match item {
                Ok(Ok(item)) => Ok(item),
                Err(err) => Err(err.into()),
                Ok(Err(err)) => Err(irn_rpc::client::Error::from(err).into()),
            }))
        })
        .await
        .map_err(Into::into)
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

    /// Client is not a cluster member.
    #[error("Not cluster member")]
    NotClusterMember,

    /// Keyspace versions of client and server don't match.
    #[error("Keyspace version mismatch")]
    KeyspaceVersionMismatch,

    /// Storage export operation error.
    #[error("Storage export failed: {_0}")]
    StorageExport(String),

    /// Other client/server error.
    #[error("{_0}")]
    Other(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Transport(format!("IO: {err}"))
    }
}

impl From<irn_rpc::client::Error> for Error {
    fn from(err: irn_rpc::client::Error) -> Self {
        let rpc_err = match err {
            irn_rpc::client::Error::Transport(err) => return Self::Transport(err.to_string()),
            irn_rpc::client::Error::Rpc { error, .. } => error,
        };

        match rpc_err.code.as_ref() {
            irn_rpc::error_code::TIMEOUT => Self::Timeout,
            crate::error_code::NOT_CLUSTER_MEMBER => Self::NotClusterMember,
            crate::error_code::KEYSPACE_VERSION_MISMATCH => Self::KeyspaceVersionMismatch,
            crate::error_code::STORAGE_EXPORT_FAILED => Self::StorageExport(
                rpc_err
                    .description
                    .map(|desc| desc.to_string())
                    .unwrap_or_default(),
            ),
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
