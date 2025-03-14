use {
    super::*,
    futures_util::{SinkExt, Stream, StreamExt},
    std::{
        collections::HashSet,
        future::Future,
        pin::pin,
        sync::{Arc, Mutex},
        time::Duration,
    },
    wcn_rpc::{
        identity::{ed25519::Keypair as Ed25519Keypair, Keypair},
        middleware::Timeouts,
        server::{
            middleware::{Auth, MeteredExt as _, WithAuthExt as _, WithTimeoutsExt as _},
            ClientConnectionInfo,
        },
        transport::{
            self,
            BiDirectionalStream,
            NoHandshake,
            PostcardCodec,
            RecvStream,
            SendStream,
        },
        Multiaddr,
        PeerId,
    },
};

/// [`Server`] config.
pub struct Config {
    /// [`Multiaddr`] of the server.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Timeout of a [`Server`] operation.
    pub operation_timeout: Duration,

    /// A list of clients authorized to use the API.
    pub authorized_clients: Option<HashSet<PeerId>>,

    /// Network ID to use for auth tokens.
    pub network_id: String,

    pub cluster_view: domain::ClusterView,

    pub max_concurrent_connections: u32,

    pub max_concurrent_streams: u32,
}

/// API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Publishes the provided message to the specified channel.
    fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> impl Future<Output = ()> + Send;

    /// Subscribes to the [`SubscriptionEvent`]s of the provided `channel`s.
    fn subscribe(
        &self,
        channels: HashSet<Vec<u8>>,
    ) -> impl Future<Output = Result<impl Stream<Item = SubscriptionEvent> + Send + 'static>> + Send;

    /// Runs this [`Server`] using the provided [`Config`].
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()>, ServeError> {
        let timeouts = Timeouts::new()
            .with_default(cfg.operation_timeout)
            .with::<{ Subscribe::ID }>(None)
            .with::<{ ClusterUpdates::ID }>(None);

        let rpc_server_config = wcn_rpc::server::Config {
            name: crate::RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        let quic_server_config = wcn_rpc::quic::server::Config {
            name: const { crate::RPC_SERVER_NAME.as_str() },
            addr: cfg.addr,
            keypair: cfg.keypair.clone(),
            max_connections: cfg.max_concurrent_connections,
            max_connections_per_ip: 100,
            max_connection_rate_per_ip: 100,
            max_streams: cfg.max_concurrent_streams,
            priority: transport::Priority::High,
        };

        let inner = Arc::new(Inner {
            keypair: cfg
                .keypair
                .clone()
                .try_into_ed25519()
                .map_err(|_| ServeError::Key)?,
            network_id: cfg.network_id,
            cluster_view: cfg.cluster_view,
            api_server: self,
            config: rpc_server_config,
        });

        // Allow auth to be disabled for testing purposes.
        let auth = if let Some(authorized_clients) = cfg.authorized_clients {
            Auth::new(authorized_clients)
        } else {
            tracing::warn!("client API auth is disabled");

            Auth::disabled()
        };

        let rpc_server = RpcServer { inner }
            .with_auth(auth)
            .with_timeouts(timeouts)
            .metered();

        wcn_rpc::quic::server::run(rpc_server, quic_server_config).map_err(ServeError::Quic)
    }
}

struct Inner<S> {
    keypair: Ed25519Keypair,
    network_id: String,
    cluster_view: domain::ClusterView,
    api_server: S,
    config: rpc::server::Config,
}

#[derive(Clone)]
struct RpcServer<S> {
    inner: Arc<Inner<S>>,
}

impl<S: Server> RpcServer<S> {
    fn create_auth_nonce(&self, conn_info: &ClientConnectionInfo<Self>) -> auth::Nonce {
        let nonce = auth::Nonce::generate();
        // Safe unwrap, as this lock can't be poisoned.
        let mut lock = conn_info.storage.ns_nonce.lock().unwrap();
        *lock = Some(nonce);
        nonce
    }

    fn take_auth_nonce(&self, conn_info: &ClientConnectionInfo<Self>) -> Option<auth::Nonce> {
        // Safe unwrap, as this lock can't be poisoned.
        let mut lock = conn_info.storage.ns_nonce.lock().unwrap();
        lock.take()
    }

    fn create_auth_token(
        &self,
        conn_info: &ClientConnectionInfo<Self>,
        req: token::Config,
    ) -> Result<token::Token, token::Error> {
        let mut claims = token::Claims {
            aud: self.inner.network_id.clone(),
            iss: self.inner.keypair.public().into(),
            sub: conn_info.peer_id,
            api: req.purpose,
            iat: token::create_timestamp(None),
            exp: req.duration.map(|dur| token::create_timestamp(Some(dur))),
            nsp: Default::default(),
        };

        if !req.namespaces.is_empty() {
            if let Some(nonce) = self.take_auth_nonce(conn_info) {
                claims.nsp = req
                    .namespaces
                    .into_iter()
                    .map(|auth| {
                        auth.namespace
                            .verify(nonce.as_ref(), &auth.signature)
                            .map(|_| auth.namespace.into())
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| token::Error::NamespaceSignature)?;
            } else {
                return Err(token::Error::NamespaceSignature);
            }
        }

        claims.encode(&self.inner.keypair)
    }

    fn cluster_snapshot(&self) -> Result<ClusterUpdate, super::Error> {
        let cluster = self.inner.cluster_view.cluster();

        serde_json::to_vec(&cluster.snapshot())
            .map(ClusterUpdate)
            .map_err(|_| super::Error::Serialization)
    }

    async fn handle_cluster_updates(
        &self,
        mut tx: SendStream<wcn_rpc::Result<ClusterUpdate>>,
    ) -> Result<(), wcn_rpc::server::Error> {
        let mut updates = std::pin::pin!(self.inner.cluster_view.updates());

        loop {
            tokio::select! {
                _ = tx.wait_closed() => return Ok(()),
                update = updates.next() => match update {
                    Some(_) => {
                        let snapshot = self
                            .cluster_snapshot()
                            .map_err(|err| wcn_rpc::transport::Error::Other(err.to_string()))?;

                        tx.send(Ok(snapshot))
                            .await?;
                    }
                    None => return Ok(()),
                }
            };
        }
    }

    async fn publish(&self, req: PublishRequest) {
        self.inner
            .api_server
            .publish(req.channel, req.message)
            .await;
    }

    async fn subscribe(
        &self,
        mut rx: RecvStream<SubscribeRequest>,
        mut tx: SendStream<wcn_rpc::Result<SubscribeResponse>>,
    ) -> wcn_rpc::server::Result<()> {
        let req = rx.recv_message().await?;

        let events = match self.inner.api_server.subscribe(req.channels).await {
            Ok(events) => events,
            Err(err) => return Ok(tx.send(Err(err.into_rpc_error())).await?),
        };

        let mut events = pin!(events);

        while let Some(evt) = events.next().await {
            tx.send(Ok(SubscribeResponse {
                channel: evt.channel,
                message: evt.message,
            }))
            .await?;
        }

        Ok(())
    }
}

#[derive(Default)]
struct Storage {
    ns_nonce: Mutex<Option<auth::Nonce>>,
}

impl<S> rpc::Server for RpcServer<S>
where
    S: Server,
{
    type Handshake = NoHandshake;
    type ConnectionData = Arc<Storage>;
    type Codec = PostcardCodec;

    fn config(&self) -> &wcn_rpc::server::Config<Self::Handshake> {
        &self.inner.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = match id {
                CreateAuthNonce::ID => {
                    CreateAuthNonce::handle(stream, |_| async {
                        Ok(self.create_auth_nonce(conn_info))
                    })
                    .await
                }

                CreateAuthToken::ID => {
                    CreateAuthToken::handle(stream, |req| async {
                        Ok(self.create_auth_token(conn_info, req))
                    })
                    .await
                }

                GetCluster::ID => {
                    GetCluster::handle(stream, |_| async { Ok(self.cluster_snapshot()) }).await
                }

                ClusterUpdates::ID => {
                    ClusterUpdates::handle(stream, |_, tx| self.handle_cluster_updates(tx)).await
                }

                Publish::ID => Publish::handle(stream, |req| self.publish(req)).await,

                Subscribe::ID => Subscribe::handle(stream, |rx, tx| self.subscribe(rx, tx)).await,

                id => return tracing::warn!("Unexpected RPC: {}", rpc::Name::new(id)),
            }
            .map_err(|err| {
                tracing::debug!(name = %rpc::Name::new(id), ?err, "Failed to handle RPC");
            });
        }
    }
}

/// Error of [`Server::serve`]
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("{0:?}")]
    Quic(wcn_rpc::quic::Error),

    #[error("Invalid server keypair")]
    Key,
}

/// Error of a [`Server`] operation.
#[derive(Clone, Debug)]
pub struct Error(String);

impl Error {
    fn into_rpc_error(self) -> wcn_rpc::Error {
        wcn_rpc::Error {
            code: "internal".into(),
            description: Some(self.0.into()),
        }
    }
}

/// [`Server`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
