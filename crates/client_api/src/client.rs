use {
    super::*,
    irn_rpc::{
        client::middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
        identity::Keypair,
        transport::NoHandshake,
    },
    std::{collections::HashSet, convert::Infallible, result::Result as StdResult, time::Duration},
};

/// API client.
#[derive(Clone)]
pub struct Client {
    rpc_client: RpcClient,
    server_addr: Multiaddr,
    namespaces: Vec<ns_auth::Auth>,
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

    /// [`Multiaddr`] of the API server.
    pub server_addr: Multiaddr,

    pub namespaces: Vec<ns_auth::Auth>,
}

impl Config {
    pub fn new(server_addr: Multiaddr) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            server_addr,
            namespaces: Default::default(),
        }
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = keypair;
        self
    }

    pub fn with_namespaces(mut self, namespaces: Vec<ns_auth::Auth>) -> Self {
        self.namespaces = namespaces;
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
        };

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .with_timeouts(Timeouts::new().with_default(config.operation_timeout));

        Ok(Self {
            rpc_client,
            server_addr: config.server_addr,
            namespaces: config.namespaces,
        })
    }

    pub fn set_server_addr(&mut self, addr: Multiaddr) {
        self.server_addr = addr;
    }

    pub async fn create_auth_token(&self) -> Result<auth::Token, auth::Error> {
        let nonce = CreateAuthNonce::send(&self.rpc_client, &self.server_addr, ())
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)?;

        let namespaces = self
            .namespaces
            .iter()
            .map(|auth| auth::NamespaceAuth {
                namespace: auth.public_key(),
                signature: auth.sign(nonce.as_ref()),
            })
            .collect();

        let req = auth::TokenConfig {
            api: auth::Api::Storage,
            duration: None,
            namespaces,
        };

        CreateAuthToken::send(&self.rpc_client, &self.server_addr, req)
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
        use irn_rpc::client::middleware::error_code;

        let rpc_err = match err {
            irn_rpc::client::Error::Transport(err) => return Self::Transport(err.to_string()),
            irn_rpc::client::Error::Rpc(err) => err,
        };

        match rpc_err.code.as_ref() {
            error_code::TIMEOUT => Self::Timeout,
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, A = Infallible> = std::result::Result<T, Error<A>>;
