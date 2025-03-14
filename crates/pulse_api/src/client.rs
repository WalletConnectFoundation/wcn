use {
    super::*,
    std::{
        collections::HashSet,
        result::Result as StdResult,
        time::{self, Duration},
    },
    wcn_rpc::{
        client::middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
        identity::Keypair,
        transport::{self, NoHandshake},
        PeerAddr,
    },
};

/// Pulse API client.
#[derive(Clone)]
pub struct Client {
    rpc_client: WithTimeouts<wcn_rpc::quic::Client>,
    server_addr: PeerAddr,
}

impl Client {
    /// Creates a new [`Client`].
    pub fn new(server_addr: PeerAddr) -> StdResult<Self, CreationError> {
        let rpc_client_config = wcn_rpc::client::Config {
            keypair: Keypair::generate_ed25519(),
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: Duration::from_secs(10),
            server_name: crate::RPC_SERVER_NAME,
            priority: transport::Priority::High,
        };

        let timeouts = Timeouts::new().with_default(Duration::from_secs(60));

        let rpc_client = wcn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .with_timeouts(timeouts);

        Ok(Self {
            rpc_client,
            server_addr,
        })
    }

    /// Sends a heartbeat request and returns the [`Duration`] it took to
    /// receive the response.
    pub async fn heartbeat(&self) -> Result<Duration> {
        let start_time = time::Instant::now();
        Heartbeat::send(&self.rpc_client, &self.server_addr, &()).await?;
        Ok(start_time.elapsed())
    }
}

/// Error of [`Client::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);

/// Error of a [`Client`] operation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// Transport error.
    #[error("Transport: {0}")]
    Transport(String),

    /// Operation timed out.
    #[error("Operation timed out")]
    Timeout,

    /// Other error.
    #[error("Other: {0}")]
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
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T> = std::result::Result<T, Error>;
