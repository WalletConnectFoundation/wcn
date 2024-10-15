use {
    super::*,
    arc_swap::ArcSwap,
    futures_util::Stream,
    irn_rpc::{
        client::middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
        identity::Keypair,
        transport::NoHandshake,
    },
    std::{collections::HashSet, convert::Infallible, sync::Arc, time::Duration},
};

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

/// API client.
#[derive(Clone)]
pub struct Client {
    rpc_client: RpcClient,
    server_addr: Multiaddr,
    namespaces: Arc<Vec<ns_auth::Auth>>,
    cluster: Arc<ArcSwap<domain::Cluster>>,
}

impl Client {
    // TODO: Support multiple endpoints.
    /// Creates a new [`Client`].
    pub async fn new(config: Config) -> Result<Self, super::Error> {
        let rpc_client_config = irn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
        };

        let timeouts = Timeouts::new()
            .with_default(config.operation_timeout)
            .with::<{ KeyspaceUpdates::ID }>(None);

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| Error::Other(err.to_string()))?
            .with_timeouts(timeouts);

        let client = Self {
            rpc_client,
            server_addr: config.server_addr,
            namespaces: Arc::new(config.namespaces),
            cluster: Arc::new(ArcSwap::from_pointee(domain::Cluster::new())),
        };

        client.update_keyspace().await?;

        Ok(client)
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

    pub fn cluster(&self) -> Arc<domain::Cluster> {
        self.cluster.load_full()
    }

    pub async fn keyspace_updates(
        &self,
    ) -> impl Stream<Item = Arc<domain::Cluster>> + Send + 'static {
        let this = self.clone();

        async_stream::stream! {
            loop {
                let subscribe =
                    KeyspaceUpdates::send(&this.rpc_client, &this.server_addr, |_, rx| async move {
                        Ok(rx)
                    });

                let mut rx = match subscribe.await {
                    Ok(rx) => rx,

                    Err(err) => {
                        tracing::error!(?err, "failed to subscribe to any peer");
                        // TODO: Metrics.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                loop {
                    match rx.recv_message().await {
                        Ok(update) => {
                            let _ = this.apply_keyspace_update(update).await.map_err(|err| {
                                tracing::warn!(?err, "failed to apply keyspace update");
                            });

                            yield this.cluster();
                        }

                        Err(err) => {
                            tracing::warn!(?err, "failed to receive keyspace update frame, resubscribing...");
                            // TODO: Metrics.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn update_keyspace(&self) -> Result<(), super::Error> {
        let update = GetKeyspace::send(&self.rpc_client, &self.server_addr, ())
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)?;

        self.apply_keyspace_update(update).await?;

        Ok(())
    }

    async fn apply_keyspace_update(&self, update: KeyspaceUpdate) -> Result<(), super::Error> {
        let cluster = tokio::task::spawn_blocking(move || {
            let snapshot = postcard::from_bytes(&update.0).map_err(|_| Error::Serialization)?;
            let cluster = domain::Cluster::from_snapshot(snapshot).map_err(Error::Cluster)?;
            Ok::<_, Error<super::Error>>(cluster)
        })
        .await
        .map_err(|err| Error::Other(err.to_string()))??;

        self.cluster.store(Arc::new(cluster));

        Ok(())
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

    #[error("Serialization failed")]
    Serialization,

    #[error("Cluster view error: {0}")]
    Cluster(#[from] irn_core::cluster::Error),

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
