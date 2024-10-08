use {
    super::*,
    irn_rpc::{
        identity::{ed25519::Keypair as Ed25519Keypair, Keypair},
        middleware::Timeouts,
        server::{
            middleware::{Auth, MeteredExt as _, WithAuthExt as _, WithTimeoutsExt as _},
            ConnectionInfo,
        },
        transport::{BiDirectionalStream, NoHandshake},
    },
    std::{collections::HashSet, future::Future, time::Duration},
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
    pub authorized_clients: HashSet<PeerId>,

    /// Network ID to use for auth tokens.
    pub network_id: String,
}

/// API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Runs this [`Server`] using the provided [`Config`].
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()>, Error> {
        let rpc_server = Adapter {
            keypair: cfg
                .keypair
                .clone()
                .try_into_ed25519()
                .map_err(|_| Error::Key)?,
            network_id: cfg.network_id,
            _server: self,
        }
        .with_auth(Auth {
            authorized_clients: cfg.authorized_clients,
        })
        .with_timeouts(Timeouts::new().with_default(cfg.operation_timeout))
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

#[derive(Clone)]
struct Adapter<S> {
    keypair: Ed25519Keypair,
    network_id: String,
    _server: S,
}

impl<S> Adapter<S>
where
    S: Server,
{
    fn create_auth_token(
        &self,
        conn_info: &ConnectionInfo,
        req: auth::TokenConfig,
    ) -> Result<auth::Token, auth::Error> {
        auth::TokenClaims {
            aud: self.network_id.clone(),
            iss: self.keypair.public().into(),
            sub: conn_info.peer_id,
            api: req.namespace,
            iat: create_timestamp(None),
            exp: req.duration.map(|dur| create_timestamp(Some(dur))),
        }
        .encode(&self.keypair)
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
                CreateAuthToken::ID => {
                    CreateAuthToken::handle(stream, |req| async {
                        self.create_auth_token(conn_info, req)
                    })
                    .await
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
    #[error("")]
    Quic(irn_rpc::quic::Error),

    #[error("Invalid server keypair")]
    Key,
}

fn create_timestamp(offset: Option<Duration>) -> i64 {
    let now = chrono::Utc::now().timestamp();

    if let Some(offset) = offset {
        now + offset.as_secs().try_into().unwrap_or(0)
    } else {
        now
    }
}
