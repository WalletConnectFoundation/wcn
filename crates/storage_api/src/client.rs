use {
    super::*,
    arc_swap::ArcSwap,
    futures::SinkExt,
    irn_rpc::{
        client::middleware::{
            self,
            MeteredExt,
            Timeouts,
            WithRetries,
            WithRetriesExt,
            WithTimeouts,
            WithTimeoutsExt as _,
        },
        identity::Keypair,
        middleware::Metered,
        transport::{self, PendingConnection},
    },
    std::{
        collections::HashSet,
        future::Future,
        result::Result as StdResult,
        sync::Arc,
        time::Duration,
    },
};

/// Storage API client.
#[derive(Clone)]
pub struct Client {
    rpc: RpcClient,
}

type RpcClient =
    WithTimeouts<WithRetries<Metered<irn_rpc::quic::Client<Handshake>>, RetryStrategy>>;

/// Storage API access token.
pub type AccessToken = Arc<ArcSwap<auth::token::Token>>;

/// [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] operation.
    pub operation_timeout: Duration,

    /// Storage API access token.
    pub access_token: AccessToken,
}

impl Config {
    pub fn new(access_token: AccessToken) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            access_token,
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
        let handshake = Handshake {
            access_token: config.access_token,
        };

        let rpc_client_config = irn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: HashSet::new(),
            handshake,
            connection_timeout: config.connection_timeout,
        };

        let timeouts = Timeouts::new()
            .with_default(config.operation_timeout)
            .with::<{ Subscribe::ID }>(None);

        let rpc_client = irn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .metered()
            .with_retries(RetryStrategy)
            .with_timeouts(timeouts);

        Ok(Self { rpc: rpc_client })
    }

    pub fn remote_storage<'a>(&'a self, server_addr: &'a Multiaddr) -> RemoteStorage<'_> {
        RemoteStorage {
            client: self,
            server_addr,
            expected_keyspace_version: None,
        }
    }
}

#[derive(Clone, Debug)]
struct RetryStrategy;

impl middleware::RetryStrategy for RetryStrategy {
    fn requires_retry(
        &self,
        rpc_id: irn_rpc::Id,
        error: &irn_rpc::client::Error,
        attempt: usize,
    ) -> Option<Duration> {
        use crate::error_code;

        // These RPCs are non-retryable
        if let Publish::ID | Subscribe::ID = rpc_id {
            return None;
        }

        let rpc_error = match error {
            irn_rpc::client::Error::Transport(_) => return Some(Duration::from_millis(50)),
            irn_rpc::client::Error::Rpc { error, .. } => error,
        };

        Some(match rpc_error.code.as_ref() {
            // These errors are non-retryable
            irn_rpc::error_code::THROTTLED
            | error_code::INVALID_KEY
            | error_code::KEYSPACE_VERSION_MISMATCH
            | error_code::UNAUTHORIZED => return None,

            // On the first attempt retry immediately.
            _ if attempt == 1 => Duration::ZERO,

            _ => Duration::from_millis(100),
        })
    }
}

/// Handle to a remote Storage API (Server).
pub struct RemoteStorage<'a> {
    client: &'a Client,
    server_addr: &'a Multiaddr,
    expected_keyspace_version: Option<u64>,
}

impl<'a> RemoteStorage<'a> {
    fn extended_key(&self, key: Key) -> ExtendedKey {
        ExtendedKey {
            inner: key.0,
            keyspace_version: self.expected_keyspace_version,
        }
    }

    fn rpc_client(&self) -> &RpcClient {
        &self.client.rpc
    }

    /// Specifies the expected version of the keyspace of the [`RemoteStorage`].
    pub fn expecting_keyspace_version(mut self, version: u64) -> Self {
        self.expected_keyspace_version = Some(version);
        self
    }

    /// Gets a [`Record`] by the provided [`Key`].
    pub async fn get(self, key: Key) -> Result<Option<Record>> {
        Get::send(self.rpc_client(), self.server_addr, &GetRequest {
            key: self.extended_key(key),
        })
        .await
        .map(|opt| opt.map(|resp| Record::new(resp.value, resp.expiration, resp.version)))
        .map_err(Into::into)
    }

    /// Sets the provided [`Entry`] only if the version of the existing
    /// [`Entry`] is < than the new one.
    pub async fn set(self, entry: Entry) -> Result<()> {
        Set::send(self.rpc_client(), self.server_addr, &SetRequest {
            key: self.extended_key(entry.key),
            value: entry.value,
            expiration: entry.expiration.timestamp(),
            version: entry.version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Deletes an [`Entry`] by the provided [`Key`] only if the version of the
    /// [`Entry`] is < than the provided `version`.
    pub async fn del(self, key: Key, version: EntryVersion) -> Result<()> {
        Del::send(self.rpc_client(), self.server_addr, &DelRequest {
            key: self.extended_key(key),
            version: version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Gets an [`EntryExpiration`] by the provided [`Key`].
    pub async fn get_exp(self, key: Key) -> Result<Option<EntryExpiration>> {
        GetExp::send(self.rpc_client(), self.server_addr, &GetExpRequest {
            key: self.extended_key(key),
        })
        .await
        .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
        .map_err(Into::into)
    }

    /// Sets [`Expiration`] on the [`Entry`] with the provided [`Key`] only if
    /// the version of the [`Entry`] is < than the provided `version`.
    pub async fn set_exp(
        self,
        key: Key,
        expiration: impl Into<EntryExpiration>,
        version: EntryVersion,
    ) -> Result<()> {
        SetExp::send(self.rpc_client(), self.server_addr, &SetExpRequest {
            key: self.extended_key(key),
            expiration: expiration.into().timestamp(),
            version: version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
    pub async fn hget(self, key: Key, field: Field) -> Result<Option<Record>> {
        HGet::send(self.rpc_client(), self.server_addr, &HGetRequest {
            key: self.extended_key(key),
            field,
        })
        .await
        .map(|opt| opt.map(|resp| Record::new(resp.value, resp.expiration, resp.version)))
        .map_err(Into::into)
    }

    /// Sets the provided [`MapEntry`] only if the version of the existing
    /// [`MapEntry`] is < than the new one.
    pub async fn hset(self, entry: MapEntry) -> Result<()> {
        HSet::send(self.rpc_client(), self.server_addr, &HSetRequest {
            key: self.extended_key(entry.key),
            field: entry.field,
            value: entry.value,
            expiration: entry.expiration.timestamp(),
            version: entry.version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Deletes a [`MapEntry`] by the provided [`Key`] only if the version of
    /// the [`MapEntry`] is < than the provided `version`.
    pub async fn hdel(self, key: Key, field: Field, version: EntryVersion) -> Result<()> {
        HDel::send(self.rpc_client(), self.server_addr, &HDelRequest {
            key: self.extended_key(key),
            field,
            version: version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Gets an [`EntryExpiration`] by the provided [`Key`] and [`Field`].
    pub async fn hget_exp(self, key: Key, field: Field) -> Result<Option<EntryExpiration>> {
        HGetExp::send(self.rpc_client(), self.server_addr, &HGetExpRequest {
            key: self.extended_key(key),
            field,
        })
        .await
        .map(|opt| opt.map(|resp| EntryExpiration::from(resp.expiration)))
        .map_err(Into::into)
    }

    /// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
    /// [`Field`] only if the version of the [`MapEntry`] is < than the
    /// provided `version`.
    pub async fn hset_exp(
        self,
        key: Key,
        field: Field,
        expiration: impl Into<EntryExpiration>,
        version: EntryVersion,
    ) -> Result<()> {
        HSetExp::send(self.rpc_client(), self.server_addr, &HSetExpRequest {
            key: self.extended_key(key),
            field,
            expiration: expiration.into().timestamp(),
            version: version.timestamp(),
        })
        .await
        .map_err(Into::into)
    }

    /// Returns cardinality of the map with the provided [`Key`].
    pub async fn hcard(self, key: Key) -> Result<u64> {
        HCard::send(self.rpc_client(), self.server_addr, &HCardRequest {
            key: self.extended_key(key),
        })
        .await
        .map(|resp| resp.cardinality)
        .map_err(Into::into)
    }

    /// Returns a [`MapPage`] by iterating over the [`Field`]s of the map with
    /// the provided [`Key`].
    pub async fn hscan(self, key: Key, count: u32, cursor: Option<Field>) -> Result<MapPage> {
        let resp = HScan::send(self.rpc_client(), self.server_addr, &HScanRequest {
            key: self.extended_key(key),
            count,
            cursor,
        })
        .await
        .map_err(|err| Error::from(err))?;

        Ok(MapPage {
            has_next: resp.records.len() >= count as usize,
            records: resp
                .records
                .into_iter()
                .map(|record| MapRecord {
                    field: record.field,
                    value: record.value,
                    expiration: EntryExpiration::from(record.expiration),
                    version: EntryVersion::from(record.version),
                })
                .collect(),
        })
    }

    /// Publishes the provided message to the specified channel.
    pub async fn publish(self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        Publish::send(self.rpc_client(), self.server_addr, &PublishRequest {
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
            self.rpc_client(),
            self.server_addr,
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
}

/// Error of [`Client::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);

/// Error of a [`Client`] operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum Error {
    /// Transport errort.
    #[error("Transport: {_0}")]
    Transport(String),

    /// Operation timed out.
    #[error("Timeout")]
    Timeout,

    /// Server is throttling.
    #[error("Throttled")]
    Throttled,

    /// Client is not authorized to perform the operation.
    #[error("Unauthorized")]
    Unauthorized,

    /// Keyspace versions of client and server don't match.
    #[error("Keyspace version mismatch")]
    KeyspaceVersionMismatch,

    /// Other client/server error.
    #[error("{_0}")]
    Other(String),
}

impl From<irn_rpc::client::Error> for Error {
    fn from(err: irn_rpc::client::Error) -> Self {
        let rpc_err = match err {
            irn_rpc::client::Error::Transport(err) => return Self::Transport(err.to_string()),
            irn_rpc::client::Error::Rpc { error, .. } => error,
        };

        match rpc_err.code.as_ref() {
            irn_rpc::error_code::TIMEOUT => Self::Timeout,
            crate::error_code::KEYSPACE_VERSION_MISMATCH => Self::KeyspaceVersionMismatch,
            crate::error_code::UNAUTHORIZED => Self::Unauthorized,
            _ => Self::Other(format!("{rpc_err:?}")),
        }
    }
}

/// [`Client`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Client part of the [`network::Handshake`].
#[derive(Clone)]
struct Handshake {
    access_token: AccessToken,
}

impl transport::Handshake for Handshake {
    type Ok = ();
    type Err = HandshakeError;

    fn handle(
        &self,
        _peer_id: PeerId,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .initiate_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let req = HandshakeRequest {
                access_token: self.access_token.load().as_ref().to_owned(),
            };

            tx.send(req)
                .await
                .map_err(|err| HandshakeError::Transport(err.into()))?;

            rx.recv_message().await?.map_err(Into::into)
        }
    }
}
