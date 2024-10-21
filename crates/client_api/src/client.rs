use {
    super::*,
    arc_swap::ArcSwap,
    futures_util::SinkExt as _,
    irn_rpc::{
        client::{
            middleware::{Timeouts, WithTimeouts, WithTimeoutsExt as _},
            AnyPeer,
        },
        identity::Keypair,
        transport::{self, NoHandshake},
        Multiaddr,
    },
    std::{collections::HashSet, convert::Infallible, future::Future, sync::Arc, time::Duration},
    tokio::sync::oneshot,
};

const DEFAULT_AUTH_TOKEN_TTL: Duration = Duration::from_secs(30 * 60);
const MIN_AUTH_TOKEN_TTL: Duration = Duration::from_secs(5 * 60);

/// [`Client`] config.
#[derive(Clone)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] operation.
    pub operation_timeout: Duration,

    /// [`Multiaddr`] of the API servers.
    pub nodes: HashSet<Multiaddr>,

    pub auth_purpose: token::Purpose,

    pub auth_token_ttl: Duration,

    pub namespaces: Vec<auth::Auth>,
}

impl Config {
    pub fn new(nodes: impl Into<HashSet<Multiaddr>>) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            nodes: nodes.into(),
            auth_purpose: token::Purpose::Storage,
            auth_token_ttl: DEFAULT_AUTH_TOKEN_TTL,
            namespaces: Default::default(),
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

    pub fn with_namespaces(mut self, namespaces: impl Into<Vec<auth::Auth>>) -> Self {
        self.namespaces = namespaces.into();
        self
    }

    pub fn with_auth_ttl(mut self, ttl: Duration) -> Self {
        self.auth_token_ttl = ttl;
        self
    }
}

struct Inner {
    rpc_client: WithTimeouts<irn_rpc::quic::Client>,
    namespaces: Vec<auth::Auth>,
    auth_ttl: Duration,
    auth_token: Arc<ArcSwap<token::Token>>,
    cluster: Arc<ArcSwap<domain::Cluster>>,
    nodes: Vec<Multiaddr>,
}

impl Inner {
    async fn refresh_auth_token(&self) -> Result<(), token::Error> {
        let address = rand::seq::SliceRandom::choose(&self.nodes[..], &mut rand::thread_rng())
            .ok_or(Error::NodeNotAvailable)?;

        let nonce = CreateAuthNonce::send(&self.rpc_client, address, &())
            .await
            .map_err(Error::from)?;

        let namespaces = self
            .namespaces
            .iter()
            .map(|auth| token::NamespaceAuth {
                namespace: auth.public_key(),
                signature: auth.sign(nonce.as_ref()),
            })
            .collect();

        let req = token::Config {
            purpose: token::Purpose::Storage,
            duration: Some(self.auth_ttl),
            namespaces,
        };

        let token = CreateAuthToken::send(&self.rpc_client, address, &req)
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)?;

        self.auth_token.store(Arc::new(token));

        Ok(())
    }

    async fn update_cluster(&self) -> Result<(), super::Error> {
        let update = GetCluster::send(&self.rpc_client, &AnyPeer, &())
            .await
            .map_err(Error::from)?
            .map_err(Error::Api)?;

        self.apply_cluster_update(update).await?;

        Ok(())
    }

    async fn apply_cluster_update(&self, update: ClusterUpdate) -> Result<(), super::Error> {
        let cluster = tokio::task::spawn_blocking(move || {
            let snapshot = postcard::from_bytes(&update.0).map_err(|_| Error::Serialization)?;
            let cluster =
                Arc::new(domain::Cluster::from_snapshot(snapshot).map_err(Error::Cluster)?);
            Ok::<_, Error<super::Error>>(cluster)
        })
        .await
        .map_err(|err| Error::Other(err.to_string()))??;

        self.cluster.store(cluster);

        Ok(())
    }
}

async fn updater(inner: Arc<Inner>, shutdown_rx: oneshot::Receiver<()>) {
    tokio::select! {
        _ = cluster_update(&inner) => {},
        _ = auth_token_update(&inner) => {},
        _ = shutdown_rx => {}
    }
}

async fn cluster_update(inner: &Inner) {
    loop {
        let stream =
            ClusterUpdates::send(&inner.rpc_client, &AnyPeer, &|_, rx| async move { Ok(rx) }).await;

        let mut rx = match stream {
            Ok(rx) => rx,

            Err(err) => {
                tracing::error!(?err, "failed to subscribe to any peer");

                wc::metrics::counter!("irn_client_api_cluster_connection_failed").increment(1);

                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        loop {
            match rx.recv_message().await {
                Ok(Ok(update)) => {
                    if let Err(err) = inner.apply_cluster_update(update).await {
                        tracing::warn!(?err, "failed to apply cluster update");
                    }

                    wc::metrics::counter!("irn_client_api_cluster_updates").increment(1);
                }

                res @ (Ok(Err(_)) | Err(_)) => {
                    tracing::warn!(
                        ?res,
                        "failed to receive cluster update frame, resubscribing..."
                    );

                    wc::metrics::counter!("irn_client_api_cluster_updates_failed").increment(1);

                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }
}

async fn auth_token_update(inner: &Inner) {
    // Subtract 2 minutes from the token duration to refresh it before it expires.
    let normal_delay = inner
        .auth_ttl
        .checked_sub(Duration::from_secs(2 * 60))
        .unwrap_or(Duration::from_secs(30));

    let mut next_delay = normal_delay;

    // Delay the initial refresh, since we assume the token has just been created in
    // the constructor.
    loop {
        tokio::time::sleep(next_delay).await;

        if let Err(err) = inner.refresh_auth_token().await {
            tracing::warn!(?err, "failed to refresh auth token");

            wc::metrics::counter!("irn_client_api_token_updates_failed").increment(1);

            next_delay = Duration::from_secs(1);
        } else {
            wc::metrics::counter!("irn_client_api_token_updates").increment(1);

            next_delay = normal_delay;
        }
    }
}

/// API client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
    _shutdown_tx: Arc<oneshot::Sender<()>>,
}

impl Client {
    /// Creates a new [`Client`].
    pub async fn new(config: Config) -> Result<Self> {
        if config.auth_token_ttl < MIN_AUTH_TOKEN_TTL {
            return Err(Error::TokenTtl);
        }

        let nodes = config.nodes.iter().cloned().collect();

        let rpc_client_config = irn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: config.nodes,
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
        };

        let timeouts = Timeouts::new()
            .with_default(config.operation_timeout)
            .with::<{ Subscribe::ID }>(None)
            .with::<{ ClusterUpdates::ID }>(None);

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| Error::Other(err.to_string()))?
            .with_timeouts(timeouts);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let inner = Arc::new(Inner {
            rpc_client,
            namespaces: config.namespaces,
            auth_ttl: config.auth_token_ttl,
            auth_token: Arc::new(ArcSwap::from_pointee(token::Token::default())),
            cluster: Arc::new(ArcSwap::from_pointee(domain::Cluster::new())),
            nodes,
        });

        // Preload the client with initial auth token and cluster state.
        let (token_res, cluster_res) =
            tokio::join!(inner.refresh_auth_token(), inner.update_cluster());

        token_res.map_err(|err| Error::TokenUpdate(err.to_string()))?;
        cluster_res.map_err(|err| Error::ClusterUpdate(err.to_string()))?;

        // Run a task that will periodically refresh auth token and cluster state.
        tokio::spawn(updater(inner.clone(), shutdown_rx));

        Ok(Self {
            inner,
            _shutdown_tx: Arc::new(shutdown_tx),
        })
    }

    /// Publishes the provided message to the specified channel.
    pub async fn publish(self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        Publish::send(&self.inner.rpc_client, &AnyPeer, &PublishRequest {
            channel,
            message,
        })
        .await
        .map_err(Into::into)
    }

    /// Subscribes to the [`SubscriptionEvent`]s of the provided `channel`s, and
    /// handles them using the provided `event_handler`.
    pub async fn subscribe<F: Future<Output = ()> + Send + Sync>(
        self,
        channels: HashSet<Vec<u8>>,
        event_handler: impl Fn(SubscriptionEvent) -> F + Send + Sync,
    ) -> Result<()> {
        let channels = &channels;
        let event_handler = &event_handler;

        Subscribe::send(
            &self.inner.rpc_client,
            &AnyPeer,
            &|mut tx, mut rx| async move {
                tx.send(SubscribeRequest {
                    channels: channels.clone(),
                })
                .await?;

                loop {
                    let resp = match rx.recv_message().await {
                        Ok(rpc_res) => rpc_res?,
                        Err(transport::Error::StreamFinished) => return Ok(()),
                        Err(err) => return Err(err.into()),
                    };

                    event_handler(SubscriptionEvent {
                        channel: resp.channel,
                        message: resp.message,
                    })
                    .await
                }
            },
        )
        .await
        .map_err(Into::into)
    }

    pub fn cluster(&self) -> Arc<ArcSwap<domain::Cluster>> {
        self.inner.cluster.clone()
    }

    pub fn peek_cluster(&self) -> arc_swap::Guard<Arc<domain::Cluster>> {
        self.inner.cluster.load()
    }

    pub fn auth_token(&self) -> Arc<ArcSwap<token::Token>> {
        self.inner.auth_token.clone()
    }

    pub fn peek_auth_token(&self) -> arc_swap::Guard<Arc<token::Token>> {
        self.inner.auth_token.load()
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

    #[error("Invalid auth token TTL")]
    TokenTtl,

    #[error("Failed to update auth token: {0}")]
    TokenUpdate(String),

    #[error("Failed to update cluster: {0}")]
    ClusterUpdate(String),

    #[error("Cluster view error: {0}")]
    Cluster(#[from] domain::cluster::Error),

    #[error("Node not available")]
    NodeNotAvailable,

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
