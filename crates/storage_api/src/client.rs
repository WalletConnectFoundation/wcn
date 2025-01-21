use {
    super::*,
    arc_swap::ArcSwap,
    futures::{SinkExt, TryFutureExt as _},
    std::{collections::HashSet, future::Future, sync::Arc, time::Duration},
    wcn_rpc::{
        client::{
            middleware::{
                self,
                MeteredExt,
                Timeouts,
                WithRetries,
                WithRetriesExt,
                WithTimeouts,
                WithTimeoutsExt as _,
            },
            Connector,
            OutboundConnection,
            OutboundConnectionError,
            OutboundConnectionResult,
        },
        middleware::Metered,
        transport::{BiDirectionalStream, PostcardCodec},
    },
};

/// Storage API client.
#[derive(Clone)]
pub struct Client<C: Connector> {
    rpc: RpcClient<C>,
}

type RpcClient<C> =
    WithRetries<Metered<WithTimeouts<wcn_rpc::ClientImpl<C, Handshake>>>, RetryStrategy>;

/// Storage API access token.
pub type AccessToken = Arc<ArcSwap<auth::token::Token>>;

/// [`Client`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,

    /// Timeout of a [`Client`] operation.
    pub operation_timeout: Duration,

    /// Storage API access token.
    pub access_token: AccessToken,

    /// Maximum number of attempts to try before failing an operation.
    pub max_attempts: usize,
}

impl Config {
    pub fn new(access_token: AccessToken) -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            access_token,
            max_attempts: 3,
        }
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

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

impl<C: Connector> Client<C> {
    /// Creates a new [`Client`].
    pub fn new(connector: C, config: Config) -> Self {
        let handshake = Handshake {
            access_token: config.access_token,
        };

        let rpc_client_config = wcn_rpc::client::Config {
            known_peers: HashSet::new(),
            handshake,
            connection_timeout: config.connection_timeout,
            server_name: crate::RPC_SERVER_NAME,
        };

        let timeouts = Timeouts::new().with_default(config.operation_timeout);

        let rpc_client = wcn_rpc::client::new(connector, rpc_client_config)
            .with_timeouts(timeouts)
            .metered()
            .with_retries(RetryStrategy::new(config.max_attempts));

        Self { rpc: rpc_client }
    }

    pub fn remote_storage<'a>(&'a self, server_addr: &'a Multiaddr) -> RemoteStorage<'a, C> {
        RemoteStorage {
            client: self,
            server_addr,
            expected_keyspace_version: None,
        }
    }
}

#[derive(Clone, Debug)]
struct RetryStrategy {
    max_attempts: usize,
}

impl RetryStrategy {
    fn new(max_attempts: usize) -> Self {
        Self { max_attempts }
    }
}

impl middleware::RetryStrategy for RetryStrategy {
    fn requires_retry(
        &self,
        _rpc_id: wcn_rpc::Id,
        error: &wcn_rpc::client::Error,
        attempt: usize,
    ) -> Option<Duration> {
        use {crate::error_code, wcn_rpc::client::Error as Err};

        if attempt >= self.max_attempts {
            return None;
        }

        let rpc_error = match error {
            Err::NoAvailablePeers | Err::Lock | Err::Rng => return None,
            Err::Connection(_) => return Some(Duration::from_millis(50)),
            Err::Rpc { error, .. } => error,
        };

        Some(match rpc_error.code.as_ref() {
            // These errors are non-retryable
            wcn_rpc::error_code::THROTTLED
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
#[derive(Clone, Copy)]
pub struct RemoteStorage<'a, C: Connector> {
    client: &'a Client<C>,
    server_addr: &'a Multiaddr,
    expected_keyspace_version: Option<u64>,
}

impl<C: Connector> RemoteStorage<'_, C> {
    fn extended_key(&self, key: Key) -> ExtendedKey {
        ExtendedKey {
            inner: key.0,
            keyspace_version: self.expected_keyspace_version,
        }
    }

    fn rpc_client(&self) -> &RpcClient<C> {
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
        .map_err(Error::from)?;

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
}

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

impl From<wcn_rpc::client::Error> for Error {
    fn from(err: wcn_rpc::client::Error) -> Self {
        use wcn_rpc::client::Error as Err;

        let rpc_err = match err {
            wcn_rpc::client::Error::Connection(err) => return Self::Transport(err.to_string()),
            wcn_rpc::client::Error::Rpc { error, .. } => error,
            Err::NoAvailablePeers | Err::Lock | Err::Rng => return Self::Other(err.to_string()),
        };

        match rpc_err.code.as_ref() {
            wcn_rpc::error_code::TIMEOUT => Self::Timeout,
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

impl wcn_rpc::client::Handshake for Handshake {
    type Data = ();

    fn handle(
        &self,
        conn: &impl OutboundConnection,
    ) -> impl Future<Output = OutboundConnectionResult<Self::Data>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .establish_stream()
                .map_ok(|(rx, tx)| BiDirectionalStream::new(rx, tx))
                .await?
                .upgrade::<HandshakeResponse, HandshakeRequest, PostcardCodec>();

            let req = HandshakeRequest {
                access_token: self.access_token.load().as_ref().to_owned(),
            };

            tx.send(req).await?;

            rx.recv_message()
                .await?
                .map_err(|err| OutboundConnectionError::Other {
                    kind: "handshake",
                    details: format!("{err:?}"),
                })
        }
    }
}
