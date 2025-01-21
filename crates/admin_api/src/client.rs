use {
    super::*,
    std::{collections::HashSet, convert::Infallible, time::Duration},
    wcn_rpc::{
        client::{
            middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
            Connector,
        },
        transport::NoHandshake,
    },
};

/// Admin API client.
#[derive(Clone)]
pub struct Client<T: Connector> {
    rpc_client: RpcClient<T>,
    server_addr: Multiaddr,
}

type RpcClient<T> = WithTimeouts<wcn_rpc::ClientImpl<T>>;

/// [`Client`] config.
#[derive(Clone)]
pub struct Config {
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
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            server_addr,
        }
    }
}

impl<T: Connector> Client<T> {
    /// Creates a new [`Client`].
    pub fn new(transport: T, config: Config) -> Self {
        let rpc_client_config = wcn_rpc::client::Config {
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
            server_name: crate::RPC_SERVER_NAME,
        };

        let timeouts = Timeouts::new()
            .with_default(config.operation_timeout)
            .with::<{ GetMemoryProfile::ID }>(
                MEMORY_PROFILE_MAX_DURATION + config.operation_timeout,
            );

        let rpc_client = wcn_rpc::client::new(transport, rpc_client_config).with_timeouts(timeouts);

        Self {
            rpc_client,
            server_addr: config.server_addr,
        }
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

impl<A> From<wcn_rpc::client::Error> for Error<A> {
    fn from(err: wcn_rpc::client::Error) -> Self {
        let rpc_err = match err {
            wcn_rpc::client::Error::Connection(err) => return Self::Transport(err.to_string()),
            wcn_rpc::client::Error::Rpc { error, .. } => error,
            err => return Self::Other(err.to_string()),
        };

        match rpc_err.code.as_ref() {
            wcn_rpc::error_code::TIMEOUT => Self::Timeout,
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, A = Infallible> = std::result::Result<T, Error<A>>;
