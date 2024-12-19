use {
    super::*,
    irn_rpc::{
        client::middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
        identity::Keypair,
        transport::{self, NoHandshake},
    },
    std::{collections::HashSet, convert::Infallible, result::Result as StdResult, time::Duration},
};

/// Admin API client.
#[derive(Clone)]
pub struct Client {
    rpc_client: RpcClient,
    server_addr: Multiaddr,
}

type RpcClient = WithTimeouts<irn_rpc::quic::Client>;

/// [`Client`] config.
#[derive(Clone)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] operation.
    pub operation_timeout: Duration,

    /// [`Multiaddr`] of the Admin API server.
    pub server_addr: Multiaddr,
}

impl Config {
    pub fn new(server_addr: Multiaddr) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            server_addr,
        }
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = keypair;
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
            priority: transport::Priority::High,
        };

        let timeouts = Timeouts::new()
            .with_default(config.operation_timeout)
            .with::<{ GetMemoryProfile::ID }>(
                MEMORY_PROFILE_MAX_DURATION + config.operation_timeout,
            );

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .with_timeouts(timeouts);

        Ok(Self {
            rpc_client,
            server_addr: config.server_addr,
        })
    }

    pub fn set_server_addr(&mut self, addr: Multiaddr) {
        self.server_addr = addr;
    }

    /// Gets [`ClusterView`].
    pub async fn get_cluster_view(&self) -> Result<ClusterView> {
        GetClusterView::send(&self.rpc_client, &self.server_addr, &())
            .await
            .map_err(Into::into)
    }

    /// Gets [`NodeStatus`].
    pub async fn get_node_status(&self) -> Result<NodeStatus, GetNodeStatusError> {
        GetNodeStatus::send(&self.rpc_client, &self.server_addr, &())
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)
    }

    /// Decommissions a node.
    ///
    /// If `force` is true the node will be decommissioned even if it's not in
    /// the `Normal` state.
    pub async fn decommission_node(
        &self,
        id: PeerId,
        force: bool,
    ) -> Result<(), DecommissionNodeError> {
        let req = DecommissionNodeRequest { id, force };

        DecommissionNode::send(&self.rpc_client, &self.server_addr, &req)
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)
    }

    /// Runs the memory profiler for a specified duration and returns the
    /// compressed profile data.
    pub async fn memory_profile(
        &self,
        duration: Duration,
    ) -> Result<MemoryProfile, MemoryProfileError> {
        let req = MemoryProfileRequest { duration };

        GetMemoryProfile::send(&self.rpc_client, &self.server_addr, &req)
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)
    }
}

/// Error of [`Client::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);

/// Error of a [`Client`] operation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error<A = Infallible> {
    /// API error.
    #[error("API: {0:?}")]
    Api(A),

    /// Transport error.
    #[error("Transport: {0}")]
    Transport(String),

    /// Client is not authorized to perform the operation.
    #[error("Client is not authorized to perform the operation")]
    Unauthorized,

    /// Operation timed out.
    #[error("Operation timed out")]
    Timeout,

    /// Other error.
    #[error("Other: {0}")]
    Other(String),
}

impl<A> From<irn_rpc::client::Error> for Error<A> {
    fn from(err: irn_rpc::client::Error) -> Self {
        let rpc_err = match err {
            irn_rpc::client::Error::Transport(err) => return Self::Transport(err.to_string()),
            irn_rpc::client::Error::Rpc { error, .. } => error,
        };

        match rpc_err.code.as_ref() {
            irn_rpc::error_code::TIMEOUT => Self::Timeout,
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, A = Infallible> = std::result::Result<T, Error<A>>;
