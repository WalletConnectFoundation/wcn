#![allow(clippy::manual_async_fn)]

pub use {
    auth,
    wcn_rpc::{identity, Multiaddr, PeerAddr, PeerId},
};
use {
    futures::{FutureExt as _, TryFutureExt as _},
    std::{borrow::Cow, future::Future, str::FromStr, time::Duration},
    time::OffsetDateTime as DateTime,
};

pub mod operation;
pub use operation::Operation;

#[cfg(any(feature = "rpc_client", feature = "rpc_server"))]
mod rpc;

/// Namespace within a WCN cluster.
///
/// Namespaces are isolated and every [`StorageApi`] [`Operation`] gets executed
/// on a specific [`Namespace`].
#[derive(Clone, Copy, Debug)]
pub struct Namespace {
    /// ID of the node operator to which this namespace belongs.
    ///
    /// Currentry an Ethereum address.
    node_operator_id: [u8; 20],

    /// ID of this [`Namespace`] within the node operator scope.
    id: u8,
}

/// Version of the keyspace of a WCN cluster.
///
/// Keyspace changes after data rebalancing within a WCN Cluster.
/// For data consistency reasons WCN Coordinators and WCN Replicas need to
/// operate using same [`KeyspaceVersion`]. So for Coordinator->Replica
/// [`StorageApi`] calls [`Operation`]s need to include the expected
/// [`KeyspaceVersion`].
///
/// For Client->Coordinator and Replica->Database calls the [`KeyspaceVersion`]
/// validation is not required.
pub type KeyspaceVersion = u64;

/// WCN Storage API.
///
/// Lingua franka of the WCN network:
/// - Clients use it to execute storage operations on the network via sending
///   them to Replication Coordinators (WCN nodes hosting coordinator RPC
///   servers).
/// - Replication Coordinators use it to replicate storage operations across the
///   network via sending them to Replicas (WCN nodes hosting replica RPC
///   servers).
/// - Replicas use it to finally execute the operations on their local WCN
///   Database instances.
pub trait StorageApi: Clone + Send + Sync + 'static {
    /// Executes the provided [`operation::Get`].
    fn get<'a>(
        &'a self,
        get: operation::Get<'a>,
    ) -> impl Future<Output = Result<Option<Record>>> + Send + 'a {
        self.execute(get).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Set`].
    fn set<'a>(&'a self, set: operation::Set<'a>) -> impl Future<Output = Result<()>> + Send {
        self.execute(set).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Del`].
    fn del<'a>(&'a self, del: operation::Del<'a>) -> impl Future<Output = Result<()>> + Send {
        self.execute(del).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::GetExp`].
    fn get_exp<'a>(
        &'a self,
        get_exp: operation::GetExp<'a>,
    ) -> impl Future<Output = Result<Option<EntryExpiration>>> + Send {
        self.execute(get_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::SetExp`].
    fn set_exp<'a>(
        &'a self,
        set_exp: operation::SetExp<'a>,
    ) -> impl Future<Output = Result<()>> + Send {
        self.execute(set_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGet`].
    fn hget<'a>(
        &'a self,
        hget: operation::HGet<'a>,
    ) -> impl Future<Output = Result<Option<Record>>> + Send {
        self.execute(hget).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSet`].
    fn hset<'a>(&'a self, hset: operation::HSet<'a>) -> impl Future<Output = Result<()>> + Send {
        self.execute(hset).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HDel`].
    fn hdel<'a>(&'a self, hdel: operation::HDel<'a>) -> impl Future<Output = Result<()>> + Send {
        self.execute(hdel).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGetExp`].
    fn hget_exp<'a>(
        &'a self,
        hget_exp: operation::HGetExp<'a>,
    ) -> impl Future<Output = Result<Option<EntryExpiration>>> + Send {
        self.execute(hget_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSetExp`].
    fn hset_exp<'a>(
        &'a self,
        hset_exp: operation::HSetExp<'a>,
    ) -> impl Future<Output = Result<()>> + Send {
        self.execute(hset_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HCard`].
    fn hcard<'a>(
        &'a self,
        hcard: operation::HCard<'a>,
    ) -> impl Future<Output = Result<u64>> + Send {
        self.execute(hcard).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HScan`].
    fn hscan<'a>(
        &'a self,
        hscan: operation::HScan<'a>,
    ) -> impl Future<Output = Result<MapPage>> + Send {
        self.execute(hscan).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`StorageApi`] [`Operation`].
    fn execute<'a>(
        &'a self,
        operation: impl Into<Operation<'a>> + Send,
    ) -> impl Future<Output = Result<operation::Output>> + Send;
}

/// Raw bytes.
pub type Bytes<'a> = Cow<'a, [u8]>;

/// Key in a KV storage.
#[derive(Clone, Debug)]
pub struct Key<'a>(pub Bytes<'a>);

/// Value in a KV storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value<'a>(pub Bytes<'a>);

/// Subkey of a [`MapEntry`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field<'a>(pub Bytes<'a>);

/// Basic KV storage entry.
#[derive(Clone, Debug)]
pub struct Entry<'a> {
    /// [`Key`] of this [`Entry`].
    pub key: Key<'a>,

    /// [`Value`] of this [`Entry`].
    pub value: Value<'a>,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

/// Map entry in which each [`Value`] is associated with both [`Key`] and subkey
/// ([`Field`]).
#[derive(Clone, Debug)]
pub struct MapEntry<'a> {
    /// [`Key`] of this [`Entry`].
    pub key: Key<'a>,

    /// [`Field`] of this [`Entry`].
    pub field: Field<'a>,

    /// [`Value`] of this [`Entry`].
    pub value: Value<'a>,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

/// [`Entry`]/[`MapEntry`] without the associated [`Key`]/[`Field`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Value of this [`Record`].
    pub value: Value<'static>,

    /// Expiration time of the associated [`Entry`]/[`MapEntry`].
    pub expiration: EntryExpiration,

    /// Version of the associated [`Entry`]/[`MapEntry`].
    pub version: EntryVersion,
}

/// [`MapEntry`] without the associated [`Key`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapRecord {
    /// Field of this [`MapRecord`].
    pub field: Field<'static>,

    /// Value of this [`MapRecord`].
    pub value: Value<'static>,

    /// Expiration time of the associated [`MapEntry`].
    pub expiration: EntryExpiration,

    /// Version of the associated [`MapEntry`].
    pub version: EntryVersion,
}

/// [`Entry`]/[`MapEntry`] expiration time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EntryExpiration {
    unix_timestamp_secs: u64,
}

impl From<Duration> for EntryExpiration {
    fn from(dur: Duration) -> Self {
        Self {
            unix_timestamp_secs: (DateTime::now_utc() + dur).unix_timestamp() as u64,
        }
    }
}

impl From<DateTime> for EntryExpiration {
    fn from(dt: DateTime) -> Self {
        Self {
            unix_timestamp_secs: dt.unix_timestamp() as u64,
        }
    }
}

impl EntryExpiration {
    pub fn from_unix_timestamp_secs(timestamp: u64) -> Self {
        Self {
            unix_timestamp_secs: timestamp,
        }
    }

    pub fn unix_timestamp_secs(&self) -> u64 {
        self.unix_timestamp_secs
    }

    pub fn to_duration(&self) -> Duration {
        let expiry = DateTime::from_unix_timestamp(self.unix_timestamp_secs as i64)
            .unwrap_or(DateTime::UNIX_EPOCH);

        (expiry - DateTime::now_utc())
            .try_into()
            .unwrap_or_default()
    }
}

/// [`Entry`]/[`MapEntry`] version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EntryVersion {
    unix_timestamp_micros: u64,
}

impl EntryVersion {
    #[allow(clippy::new_without_default)]
    pub fn new() -> EntryVersion {
        Self {
            unix_timestamp_micros: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        }
    }

    pub fn from_unix_timestamp_micros(timestamp: u64) -> Self {
        Self {
            unix_timestamp_micros: timestamp,
        }
    }

    pub fn unix_timestamp_micros(&self) -> u64 {
        self.unix_timestamp_micros
    }
}

/// Page of [`MapRecord`]s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapPage {
    /// [`MapRecords`] of this [`Page`].
    pub records: Vec<MapRecord>,

    /// Indicator of whether there's a next [`Page`] or not.
    pub has_next: bool,
}

impl MapPage {
    /// Returns cursor pointing to the next [`Page`] if there is one.
    pub fn next_page_cursor(&self) -> Option<&Field> {
        self.has_next
            .then(|| self.records.last().map(|entry| &entry.field))
            .flatten()
    }
}

impl FromStr for Namespace {
    type Err = InvalidNamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use InvalidNamespaceError as Error;

        let mut parts = s.split('/');

        let (operator_id, id) = (|| Some((parts.next()?, parts.next()?)))()
            .ok_or(Error("Not enough components".into()))?;

        if parts.next().is_some() {
            return Err(Error("Too many components".into()));
        }

        Ok(Self {
            node_operator_id: const_hex::decode_to_array(operator_id)
                .map_err(|err| Error(format!("Invalid node operator id: {err}").into()))?,
            id: id
                .parse()
                .map_err(|err| Error(format!("Invalid id: {err}").into()))?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid namespace: {_0}")]
pub struct InvalidNamespaceError(Cow<'static, str>);

/// Result of [`StorageApi`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error of [`StorageApi`].
#[derive(Debug, thiserror::Error)]
#[error("{kind:?}({details:?})")]
pub struct Error {
    kind: ErrorKind,
    details: Option<String>,
}

impl Error {
    /// Returns [`ErrorKind`] of this [`Error`].
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

/// [`Error`] kind.
#[derive(Clone, Copy, Debug)]
pub enum ErrorKind {
    /// Client is not authorized to perfrom an [`Operation`].
    Unauthorized,

    /// [`KeyspaceVersion`] mismatch.
    KeyspaceVersionMismatch,

    /// [`Operation`] timeout.
    Timeout,

    /// Internal error.
    Internal,

    /// Transport error.
    Transport,

    // NOTE: This is effectively a bug, it's here just for completeness.
    /// [`operation::WrongOutputError`].
    WrongOperationOutput,

    /// Unable to determine [`ErrorKind`] of an [`Error`].
    Unknown,
}

impl Error {
    /// Creates a new [`Error`].
    fn new(kind: ErrorKind, details: Option<String>) -> Self {
        Self { kind, details }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            details: None,
        }
    }
}

#[cfg(test)]
#[test]
fn test_namespace_from_str() {
    todo!()
}
