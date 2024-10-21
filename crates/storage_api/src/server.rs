use {
    super::*,
    futures::SinkExt as _,
    irn_rpc::{
        identity::Keypair,
        middleware::Timeouts,
        quic,
        server::{
            middleware::{MeteredExt as _, WithTimeoutsExt as _},
            ConnectionInfo,
        },
        transport::{self, BiDirectionalStream, PendingConnection},
    },
    std::{collections::HashSet, future::Future, sync::Arc, time::Duration},
};

/// Storage namespace.
pub type Namespace = Vec<u8>;

/// Storage API [`Server`] config.
pub struct Config {
    /// [`Multiaddr`] of the server.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Timeout of a [`Server`] operation.
    pub operation_timeout: Duration,

    /// Maximum allowed amount of concurrent connections.
    pub max_concurrent_connections: u32,

    /// Maximum allowed amount of concurrent operations.
    pub max_concurrent_ops: u32,
}

/// Storage API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Returns the current keyspace version of this [`Server`].
    fn keyspace_version(&self) -> u64;

    /// Gets a [`Record`] by the provided [`Key`].
    fn get(&self, key: Key) -> impl Future<Output = Result<Option<Record>>> + Send;

    /// Sets the provided [`Entry`] only if the version of the existing
    /// [`Entry`] is < than the new one.
    fn set(&self, entry: Entry) -> impl Future<Output = Result<()>> + Send;

    /// Deletes an [`Entry`] by the provided [`Key`] only if the version of the
    /// [`Entry`] is < than the provided `version`.
    fn del(&self, key: Key, version: EntryVersion) -> impl Future<Output = Result<()>> + Send;

    /// Gets an [`EntryExpiration`] by the provided [`Key`].
    fn get_exp(&self, key: Key) -> impl Future<Output = Result<Option<EntryExpiration>>> + Send;

    /// Sets [`Expiration`] on the [`Entry`] with the provided [`Key`] only if
    /// the version of the [`Entry`] is < than the provided `version`.
    fn set_exp(
        &self,
        key: Key,
        expiration: impl Into<EntryExpiration>,
        version: EntryVersion,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
    fn hget(&self, key: Key, field: Field) -> impl Future<Output = Result<Option<Record>>> + Send;

    /// Sets the provided [`MapEntry`] only if the version of the existing
    /// [`MapEntry`] is < than the new one.
    fn hset(&self, entry: MapEntry) -> impl Future<Output = Result<()>> + Send;

    /// Deletes a [`MapEntry`] by the provided [`Key`] only if the version of
    /// the [`MapEntry`] is < than the provided `version`.
    fn hdel(
        &self,
        key: Key,
        field: Field,
        version: EntryVersion,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Gets an [`EntryExpiration`] by the provided [`Key`] and [`Field`].
    fn hget_exp(
        &self,
        key: Key,
        field: Field,
    ) -> impl Future<Output = Result<Option<EntryExpiration>>> + Send;

    /// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
    /// [`Field`] only if the version of the [`MapEntry`] is < than the
    /// provided `version`.
    fn hset_exp(
        &self,
        key: Key,
        field: Field,
        expiration: impl Into<EntryExpiration>,
        version: EntryVersion,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Returns cardinality of the map with the provided [`Key`].
    fn hcard(&self, key: Key) -> impl Future<Output = Result<u64>> + Send;

    /// Returns [`Field`]s of the map with the provided [`Key`].
    fn hfields(&self, key: Key) -> impl Future<Output = Result<Vec<Field>>> + Send;

    /// Returns [`Value`]s of the map with the provided [`Key`].
    fn hvals(&self, key: Key) -> impl Future<Output = Result<Vec<Value>>> + Send;

    /// Returns a [`MapPage`] by iterating over the [`Field`]s of the map with
    /// the provided [`Key`].
    fn hscan(
        &self,
        key: Key,
        count: u32,
        cursor: Option<Field>,
    ) -> impl Future<Output = Result<Vec<MapRecord>>> + Send;

    /// Runs this [`Server`] using the provided [`Config`] and
    /// [`Authenticator`].
    fn serve(
        self,
        cfg: Config,
        authenticator: impl Authenticator,
    ) -> Result<impl Future<Output = ()>, quic::Error> {
        let timeouts = Timeouts::new().with_default(cfg.operation_timeout);

        let rpc_server = RpcServer { server: self }.with_timeouts(timeouts).metered();

        let rpc_server_config = irn_rpc::server::Config {
            name: "storage_api",
            addr: cfg.addr,
            keypair: cfg.keypair,
            max_concurrent_connections: cfg.max_concurrent_connections,
            max_concurrent_rpcs: cfg.max_concurrent_ops,
        };

        irn_rpc::quic::server::run(rpc_server, rpc_server_config, Handshake { authenticator })
    }
}

struct RpcHandler<'a, S> {
    api_server: &'a S,
    conn_info: &'a ConnectionInfo<HandshakeData>,
}

impl<'a, S: Server> RpcHandler<'a, S> {
    fn prepare_key(&self, key: ExtendedKey) -> irn_rpc::Result<Key> {
        if let Some(keyspace_version) = key.keyspace_version {
            if keyspace_version != self.api_server.keyspace_version() {
                return Err(irn_rpc::Error::new(error_code::KEYSPACE_VERSION_MISMATCH));
            }
        }

        let key = Key::from_raw_bytes(key.inner)
            .ok_or_else(|| irn_rpc::Error::new(error_code::INVALID_KEY))?;

        if let Some(namespace) = key.namespace() {
            if !self.conn_info.handshake_data.namespaces.contains(namespace) {
                return Err(irn_rpc::Error::new(error_code::UNAUTHORIZED));
            }
        }

        Ok(key)
    }

    async fn get(&self, req: GetRequest) -> irn_rpc::Result<Option<GetResponse>> {
        let record = self
            .api_server
            .get(self.prepare_key(req.key)?)
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(record.map(|rec| GetResponse {
            value: rec.value,
            expiration: rec.expiration.timestamp(),
            version: rec.version.timestamp(),
        }))
    }

    async fn set(&self, req: SetRequest) -> irn_rpc::Result<()> {
        let entry = Entry {
            key: self.prepare_key(req.key)?,
            value: req.value,
            expiration: EntryExpiration::from(req.expiration),
            version: EntryVersion::from(req.version),
        };

        self.api_server
            .set(entry)
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn del(&self, req: DelRequest) -> irn_rpc::Result<()> {
        self.api_server
            .del(self.prepare_key(req.key)?, EntryVersion::from(req.version))
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn get_exp(&self, req: GetExpRequest) -> irn_rpc::Result<Option<GetExpResponse>> {
        let expiration = self
            .api_server
            .get_exp(self.prepare_key(req.key)?)
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(expiration.map(|exp| GetExpResponse {
            expiration: exp.timestamp(),
        }))
    }

    async fn set_exp(&self, req: SetExpRequest) -> irn_rpc::Result<()> {
        self.api_server
            .set_exp(
                self.prepare_key(req.key)?,
                EntryExpiration::from(req.expiration),
                EntryVersion::from(req.version),
            )
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hget(&self, req: HGetRequest) -> irn_rpc::Result<Option<HGetResponse>> {
        let record = self
            .api_server
            .hget(self.prepare_key(req.key)?, req.field)
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(record.map(|rec| HGetResponse {
            value: rec.value,
            expiration: rec.expiration.timestamp(),
            version: rec.version.timestamp(),
        }))
    }

    async fn hset(&self, req: HSetRequest) -> irn_rpc::Result<()> {
        let entry = MapEntry {
            key: self.prepare_key(req.key)?,
            field: req.field,
            value: req.value,
            expiration: EntryExpiration::from(req.expiration),
            version: EntryVersion::from(req.version),
        };

        self.api_server
            .hset(entry)
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hdel(&self, req: HDelRequest) -> irn_rpc::Result<()> {
        self.api_server
            .hdel(
                self.prepare_key(req.key)?,
                req.field,
                EntryVersion::from(req.version),
            )
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hget_exp(&self, req: HGetExpRequest) -> irn_rpc::Result<Option<HGetExpResponse>> {
        let expiration = self
            .api_server
            .hget_exp(self.prepare_key(req.key)?, req.field)
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(expiration.map(|exp| HGetExpResponse {
            expiration: exp.timestamp(),
        }))
    }

    async fn hset_exp(&self, req: HSetExpRequest) -> irn_rpc::Result<()> {
        self.api_server
            .hset_exp(
                self.prepare_key(req.key)?,
                req.field,
                EntryExpiration::from(req.expiration),
                EntryVersion::from(req.version),
            )
            .await
            .map_err(Error::into_rpc_error)
    }

    async fn hcard(&self, req: HCardRequest) -> irn_rpc::Result<HCardResponse> {
        self.api_server
            .hcard(self.prepare_key(req.key)?)
            .await
            .map(|cardinality| HCardResponse { cardinality })
            .map_err(Error::into_rpc_error)
    }

    async fn hscan(&self, req: HScanRequest) -> irn_rpc::Result<HScanResponse> {
        let records = self
            .api_server
            .hscan(self.prepare_key(req.key)?, req.count, req.cursor)
            .await
            .map_err(Error::into_rpc_error)?;

        Ok(HScanResponse {
            records: records
                .into_iter()
                .map(|rec| HScanResponseRecord {
                    field: rec.field,
                    value: rec.value,
                    expiration: rec.expiration.timestamp(),
                    version: rec.version.timestamp(),
                })
                .collect(),
        })
    }
}

#[derive(Clone, Debug)]
struct RpcServer<S> {
    server: S,
}

impl<S, V> rpc::Server<Handshake<V>> for RpcServer<S>
where
    S: Server,
    V: Authenticator,
{
    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ConnectionInfo<HandshakeData>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let handler = RpcHandler {
                api_server: &self.server,
                conn_info,
            };

            let _ = match id {
                Get::ID => Get::handle(stream, |req| handler.get(req)).await,
                Set::ID => Set::handle(stream, |req| handler.set(req)).await,
                Del::ID => Del::handle(stream, |req| handler.del(req)).await,
                GetExp::ID => GetExp::handle(stream, |req| handler.get_exp(req)).await,
                SetExp::ID => SetExp::handle(stream, |req| handler.set_exp(req)).await,

                HGet::ID => HGet::handle(stream, |req| handler.hget(req)).await,
                HSet::ID => HSet::handle(stream, |req| handler.hset(req)).await,
                HDel::ID => HDel::handle(stream, |req| handler.hdel(req)).await,
                HGetExp::ID => HGetExp::handle(stream, |req| handler.hget_exp(req)).await,
                HSetExp::ID => HSetExp::handle(stream, |req| handler.hset_exp(req)).await,
                HCard::ID => HCard::handle(stream, |req| handler.hcard(req)).await,
                HScan::ID => HScan::handle(stream, |req| handler.hscan(req)).await,

                id => return tracing::warn!("Unexpected RPC: {}", rpc::Name::new(id)),
            }
            .map_err(
                |err| tracing::debug!(name = %rpc::Name::new(id), ?err, "Failed to handle RPC"),
            );
        }
    }
}

impl<S> rpc::server::Marker for RpcServer<S> {}

/// Error of a [`Server`] operation.
#[derive(Clone, Debug)]
pub struct Error(String);

impl Error {
    fn into_rpc_error(self) -> irn_rpc::Error {
        irn_rpc::Error {
            code: "internal".into(),
            description: Some(self.0.into()),
        }
    }
}

/// [`Server`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Server part of the [`network::Handshake`].
#[derive(Clone)]
pub struct Handshake<V> {
    authenticator: V,
}

#[derive(Clone, Debug)]
pub struct HandshakeData {
    pub namespaces: Arc<HashSet<Namespace>>,
}

impl<A: Authenticator> transport::Handshake for Handshake<A> {
    type Ok = HandshakeData;
    type Err = HandshakeError;

    fn handle(
        &self,
        peer_id: PeerId,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .accept_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let req = rx.recv_message().await?;

            let err_resp = match self
                .authenticator
                .validate_access_token(&req.access_token, peer_id)
            {
                Ok(data) => {
                    tx.send(Ok(())).await?;
                    return Ok(HandshakeData {
                        namespaces: Arc::new(
                            data.namespaces()
                                .into_iter()
                                .map(|ns| ns.as_bytes().to_vec())
                                .collect(),
                        ),
                    });
                }
                Err(err) => HandshakeErrorResponse::InvalidToken(err),
            };

            tx.send(Err(err_resp.clone())).await?;
            Err(err_resp.into())
        }
    }
}

/// Inbound connection authenticator.
pub trait Authenticator: Clone + Send + Sync + 'static {
    /// Indicates whether the specified peer is an authorized access token
    /// issuer.
    fn is_authorized_token_issuer(&self, peer_id: PeerId) -> bool;

    /// Network id of the local Storage API server.
    fn network_id(&self) -> &str;

    /// Validates the provided access token.
    fn validate_access_token(
        &self,
        token: &auth::Token,
        client_peer_id: PeerId,
    ) -> Result<auth::token::Claims, String> {
        let claims = token.decode().map_err(|err| err.to_string())?;

        if claims.is_expired() {
            return Err("Token expired".to_string());
        }

        match claims.purpose() {
            auth::token::Purpose::Storage => {}
        };

        if self.network_id() != claims.network_id() {
            return Err("Wrong network".to_string());
        }

        if !self.is_authorized_token_issuer(claims.issuer_peer_id()) {
            return Err("Unauthorized token issuer".to_string());
        }

        if claims.client_peer_id() != client_peer_id {
            return Err("Wrong PeerId".to_string());
        }

        Ok(claims)
    }
}
