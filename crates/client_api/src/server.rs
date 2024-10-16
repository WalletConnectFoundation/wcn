use {
    super::*,
    futures_util::{SinkExt, StreamExt},
    irn_rpc::{
        identity::{ed25519::Keypair as Ed25519Keypair, Keypair},
        middleware::Timeouts,
        server::{
            middleware::{Auth, MeteredExt as _, WithAuthExt as _, WithTimeoutsExt as _},
            ConnectionInfo,
        },
        transport::{BiDirectionalStream, NoHandshake, SendStream},
    },
    std::{collections::HashSet, future::Future, sync::Mutex, time::Duration},
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
}

/// API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Runs this [`Server`] using the provided [`Config`].
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()>, Error> {
        let timeouts = Timeouts::new()
            .with_default(cfg.operation_timeout)
            .with::<{ ClusterUpdates::ID }>(None);

        let rpc_server = Adapter {
            keypair: cfg
                .keypair
                .clone()
                .try_into_ed25519()
                .map_err(|_| Error::Key)?,
            network_id: cfg.network_id,
            ns_nonce: Default::default(),
            server: self,
            cluster_view: cfg.cluster_view,
        }
        .with_auth(Auth {
            // TODO: Decide what to do with auth.
            authorized_clients: cfg.authorized_clients.unwrap_or_default(),
        })
        .with_timeouts(timeouts)
        .metered();

        let rpc_server_config = irn_rpc::server::Config {
            name: "client_api",
            addr: cfg.addr,
            keypair: cfg.keypair,
            // TODO: Make these configurable or find good defaults.
            max_concurrent_connections: 500,
            max_concurrent_rpcs: 100,
        };

        irn_rpc::quic::server::run(rpc_server, rpc_server_config, NoHandshake).map_err(Error::Quic)
    }
}

struct Adapter<S> {
    keypair: Ed25519Keypair,
    network_id: String,
    ns_nonce: Mutex<Option<auth::Nonce>>,
    server: S,
    cluster_view: domain::ClusterView,
}

// Adapter is cloned per connection, so it's important to have a unique
// `ns_nonce` for each adapter.
impl<S> Clone for Adapter<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            keypair: self.keypair.clone(),
            network_id: self.network_id.clone(),
            ns_nonce: Default::default(),
            server: self.server.clone(),
            cluster_view: self.cluster_view.clone(),
        }
    }
}

impl<S> Adapter<S> {
    fn create_auth_nonce(&self) -> auth::Nonce {
        let nonce = auth::Nonce::generate();
        // Safe unwrap, as this lock can't be poisoned.
        let mut lock = self.ns_nonce.lock().unwrap();
        *lock = Some(nonce);
        nonce
    }

    fn take_auth_nonce(&self) -> Option<auth::Nonce> {
        // Safe unwrap, as this lock can't be poisoned.
        let mut lock = self.ns_nonce.lock().unwrap();
        lock.take()
    }

    fn create_auth_token(
        &self,
        peer_id: PeerId,
        req: token::Config,
    ) -> Result<token::Token, token::Error> {
        let mut claims = token::TokenClaims {
            aud: self.network_id.clone(),
            iss: self.keypair.public().into(),
            sub: peer_id,
            api: req.api,
            iat: token::create_timestamp(None),
            exp: req.duration.map(|dur| token::create_timestamp(Some(dur))),
            nsp: Default::default(),
        };

        if !req.namespaces.is_empty() {
            if let Some(nonce) = self.take_auth_nonce() {
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

        claims.encode(&self.keypair)
    }

    fn cluster_snapshot(&self) -> Result<ClusterUpdate, super::Error> {
        let cluster = self.cluster_view.cluster();

        postcard::to_allocvec(&cluster.snapshot())
            .map(ClusterUpdate)
            .map_err(|_| super::Error::Serialization)
    }

    async fn handle_cluster_updates(
        &self,
        mut tx: SendStream<ClusterUpdate>,
    ) -> Result<(), irn_rpc::server::Error> {
        let mut updates = std::pin::pin!(self.cluster_view.updates());

        while updates.next().await.is_some() {
            let snapshot = self
                .cluster_snapshot()
                .map_err(|err| irn_rpc::transport::Error::Other(err.to_string()))?;

            tx.send(snapshot)
                .await
                .map_err(|err| irn_rpc::transport::Error::IO(err.kind()))?;
        }

        Ok(())
    }
}

impl<S> rpc::Server for Adapter<S>
where
    S: Server,
{
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        async move {
            let _ = match id {
                CreateAuthNonce::ID => {
                    CreateAuthNonce::handle(stream, |_| async { self.create_auth_nonce() }).await
                }

                CreateAuthToken::ID => {
                    CreateAuthToken::handle(stream, |req| async {
                        self.create_auth_token(conn_info.peer_id, req)
                    })
                    .await
                }

                GetCluster::ID => {
                    GetCluster::handle(stream, |_| async { self.cluster_snapshot() }).await
                }

                ClusterUpdates::ID => {
                    ClusterUpdates::handle(stream, |_, rx| self.handle_cluster_updates(rx)).await
                }

                id => return tracing::warn!("Unexpected RPC: {}", rpc::Name::new(id)),
            }
            .map_err(|err| {
                tracing::debug!(name = %rpc::Name::new(id), ?err, "Failed to handle RPC");
            });
        }
    }
}

impl<S> rpc::server::Marker for Adapter<S> {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0:?}")]
    Quic(irn_rpc::quic::Error),

    #[error("Invalid server keypair")]
    Key,

    #[error("Serialization failed")]
    Serialization,
}

#[cfg(test)]
mod tests {
    use {super::*, irn_core::Cluster, token::Config};

    #[test]
    #[allow(clippy::redundant_clone)]
    fn nonce() {
        let cluster_view = Cluster::new().into_viewable().view();
        let adapter1 = Adapter {
            keypair: Ed25519Keypair::generate(),
            network_id: "test_network".to_owned(),
            ns_nonce: Default::default(),
            server: (),
            cluster_view,
        };

        let nonce1 = adapter1.create_auth_nonce();
        let adapter2 = adapter1.clone();
        let nonce1_verify = adapter1.take_auth_nonce();
        let nonce2_verify = adapter2.take_auth_nonce();

        // Nonce should be unique per adapter.
        assert_eq!(nonce1_verify, Some(nonce1));
        assert!(nonce1_verify.is_some());
        assert!(nonce2_verify.is_none());
    }

    #[test]
    fn auth_token() {
        let cluster_view = Cluster::new().into_viewable().view();
        let adapter_keypair = Ed25519Keypair::generate();
        let adapter_peer_id = Keypair::from(adapter_keypair.clone()).public().to_peer_id();

        let adapter = Adapter {
            keypair: adapter_keypair,
            network_id: "test_network".to_owned(),
            ns_nonce: Default::default(),
            server: (),
            cluster_view,
        };

        let client_peer_id = Keypair::from(Ed25519Keypair::generate())
            .public()
            .to_peer_id();

        let token = adapter
            .create_auth_token(client_peer_id, Config {
                api: token::Api::Storage,
                duration: None,
                namespaces: Vec::new(),
            })
            .unwrap();

        let claims = token.decode().unwrap();

        assert_eq!(claims.network_id(), "test_network");
        assert_eq!(claims.issuer_peer_id(), adapter_peer_id);
        assert_eq!(claims.client_peer_id(), client_peer_id);
        assert_eq!(claims.api(), token::Api::Storage);
    }
}
