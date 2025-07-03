#![allow(clippy::manual_async_fn)]

pub use {
    auth,
    wcn_rpc::{identity, Multiaddr, PeerAddr, PeerId},
};
use {
    futures::FutureExt as _,
    serde::{Deserialize, Serialize},
    std::{
        borrow::Cow,
        future::Future,
        str::FromStr,
        sync::{atomic, atomic::AtomicUsize},
        time::Duration,
    },
    time::OffsetDateTime as DateTime,
};

pub mod operation;
pub use operation::{Operation, OperationRef};

#[cfg(any(feature = "rpc_client", feature = "rpc_server"))]
pub mod rpc;

/// Namespace within a WCN cluster.
///
/// Namespaces are isolated and every [`StorageApi`] [`Operation`] gets executed
/// on a specific [`Namespace`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
/// operate using the same [`KeyspaceVersion`]. So for Coordinator->Replica
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
pub trait StorageApi: Send + Sync + 'static {
    /// Executes the provided [`operation::Get`].
    fn get<'a>(
        &'a self,
        get: &'a operation::Get<'a>,
    ) -> impl Future<Output = Result<Option<Record<'a>>>> + Send + 'a {
        self.execute_ref(OperationRef::Get(get))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Set`].
    fn set<'a>(
        &'a self,
        set: &'a operation::Set<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::Set(set))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Del`].
    fn del<'a>(
        &'a self,
        del: &'a operation::Del<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::Del(del))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::GetExp`].
    fn get_exp<'a>(
        &'a self,
        get_exp: &'a operation::GetExp<'a>,
    ) -> impl Future<Output = Result<Option<RecordExpiration>>> + Send + 'a {
        self.execute_ref(OperationRef::GetExp(get_exp))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::SetExp`].
    fn set_exp<'a>(
        &'a self,
        set_exp: &'a operation::SetExp<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::SetExp(set_exp))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGet`].
    fn hget<'a>(
        &'a self,
        hget: &'a operation::HGet<'a>,
    ) -> impl Future<Output = Result<Option<Record<'a>>>> + Send + 'a {
        self.execute_ref(OperationRef::HGet(hget))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSet`].
    fn hset<'a>(
        &'a self,
        hset: &'a operation::HSet<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::HSet(hset))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HDel`].
    fn hdel<'a>(
        &'a self,
        hdel: &'a operation::HDel<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::HDel(hdel))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGetExp`].
    fn hget_exp<'a>(
        &'a self,
        hget_exp: &'a operation::HGetExp<'a>,
    ) -> impl Future<Output = Result<Option<RecordExpiration>>> + Send + 'a {
        self.execute_ref(OperationRef::HGetExp(hget_exp))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSetExp`].
    fn hset_exp<'a>(
        &'a self,
        hset_exp: &'a operation::HSetExp<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.execute_ref(OperationRef::HSetExp(hset_exp))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HCard`].
    fn hcard<'a>(
        &'a self,
        hcard: &'a operation::HCard<'a>,
    ) -> impl Future<Output = Result<u64>> + Send + 'a {
        self.execute_ref(OperationRef::HCard(hcard))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HScan`].
    fn hscan<'a>(
        &'a self,
        hscan: &'a operation::HScan<'a>,
    ) -> impl Future<Output = Result<MapPage<'a>>> + Send + 'a {
        self.execute_ref(OperationRef::HScan(hscan))
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`StorageApi`] [`OperationRef`].
    fn execute_ref<'a>(
        &'a self,
        operation: OperationRef<'a>,
    ) -> impl Future<Output = Result<operation::Output<'a>>> + Send + 'a {
        self.execute(operation.to_owned())
    }

    /// Executes the provided [`StorageApi`] [`Operation`].
    fn execute<'a>(
        &'a self,
        operation: Operation<'a>,
    ) -> impl Future<Output = Result<operation::Output<'a>>> + Send + 'a;
}

/// Raw bytes.
pub type Bytes<'a> = Cow<'a, [u8]>;

/// Raw [`Bytes`] value with metadata related to this value.
///
/// [`Record`]s are being deleted from WCN Database after specified
/// [`Record::expiration`] time.
///
/// A [`Record`] can not be overwritten by another [`Record`] with a lesser
/// [version][`Record::version`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record<'a> {
    /// Value of this [`Record`].
    pub value: Bytes<'a>,

    /// Expiration time of [`Record`].
    pub expiration: RecordExpiration,

    /// Version of this [`Record`].
    pub version: RecordVersion,
}

impl Record<'_> {
    /// Converts `Self` into 'static.
    pub fn into_static(self) -> Record<'static> {
        Record {
            value: Cow::Owned(self.value.into_owned()),
            expiration: self.expiration,
            version: self.version,
        }
    }
}

/// Entry within a Map.
///
/// Maps are a separate data type of WCN Database, similar to Redis Hashes.
/// They differ from regular KV pairs by having a subkey (AKA
/// [field][MapEntry::field]).
///
/// Each Map key contains an ordered set of entries (ascending order by
/// [`MapEntry::field`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapEntry<'a> {
    /// Subkey of this [`MapEntry`].
    pub field: Bytes<'a>,

    /// [`Record`] of this [`MapEntry`].
    pub record: Record<'a>,
}

impl MapEntry<'_> {
    /// Converts `Self` into 'static.
    pub fn into_static(self) -> MapEntry<'static> {
        MapEntry {
            field: Cow::Owned(self.field.into_owned()),
            record: self.record.into_static(),
        }
    }
}

/// Expiration time of a [`Record`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RecordExpiration {
    unix_timestamp_secs: u64,
}

impl From<Duration> for RecordExpiration {
    fn from(dur: Duration) -> Self {
        Self {
            unix_timestamp_secs: (DateTime::now_utc() + dur).unix_timestamp() as u64,
        }
    }
}

impl From<DateTime> for RecordExpiration {
    fn from(dt: DateTime) -> Self {
        Self {
            unix_timestamp_secs: dt.unix_timestamp() as u64,
        }
    }
}

impl From<RecordExpiration> for Duration {
    fn from(exp: RecordExpiration) -> Self {
        let expiry = DateTime::from_unix_timestamp(exp.unix_timestamp_secs as i64)
            .unwrap_or(DateTime::UNIX_EPOCH);

        (expiry - DateTime::now_utc())
            .try_into()
            .unwrap_or_default()
    }
}

/// Version of a [`Record`].
///
/// [`RecordVersion`] is a local client-side generated timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RecordVersion {
    /// UNIX timestamp (microseconds) representation of this [`RecordVersion`].
    pub unix_timestamp_micros: u64,
}

impl RecordVersion {
    /// Generates a new [`RecordVersion`] using the current timestamp.
    pub fn now() -> RecordVersion {
        Self {
            unix_timestamp_micros: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        }
    }
}

/// Page of [map entries][MapEntry].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapPage<'a> {
    /// [`MapRecord`]s of this [`Page`].
    pub entries: Vec<MapEntry<'a>>,

    /// Indicator of whether there's a next [`Page`] or not.
    pub has_next: bool,
}

impl<'a> MapPage<'a> {
    /// Returns cursor pointing to the next [`Page`] if there is one.
    pub fn next_page_cursor(&self) -> Option<&Bytes<'a>> {
        self.has_next
            .then(|| self.entries.last().map(|entry| &entry.field))
            .flatten()
    }

    /// Converts `Self` into 'static.
    pub fn into_static(self) -> MapPage<'static> {
        MapPage {
            entries: self
                .entries
                .into_iter()
                .map(MapEntry::into_static)
                .collect(),
            has_next: self.has_next,
        }
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

/// Error of parsing [`Namespace`] from a string.
#[derive(Debug, thiserror::Error)]
#[error("Invalid namespace: {_0}")]
pub struct InvalidNamespaceError(Cow<'static, str>);

/// [`StorageApi`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// [`StorageApi`] error.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// [`StorageApi`] Load balancer.
///
/// Load balances execution of [`Operation`]s across a list of [`StorageApi`]
/// implementors using a round-robin strategy.
pub struct LoadBalancer<S> {
    /// List of [`StorageApi`]s of this [`LoadBalancer`].
    ///
    /// Can be safely modified at any point.
    pub apis: Vec<S>,

    counter: AtomicUsize,
}

impl<S: StorageApi> LoadBalancer<S> {
    /// Creates a new [`LoadBalancer`].
    pub fn new(connections: impl IntoIterator<Item = S>) -> Self {
        Self {
            apis: connections.into_iter().collect(),
            // overflows and starts from `0`
            counter: usize::MAX.into(),
        }
    }

    fn next_api(&self) -> Result<&S> {
        if self.apis.len() == 0 {
            return Err(Error::new(
                ErrorKind::Internal,
                Some("LoadBalancer::apis is empty".to_string()),
            ));
        }

        let n = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        Ok(&self.apis[n % self.apis.len()])
    }
}

impl<S: StorageApi> StorageApi for LoadBalancer<S> {
    async fn execute_ref<'a>(
        &'a self,
        operation: OperationRef<'a>,
    ) -> Result<operation::Output<'a>> {
        self.next_api()?.execute_ref(operation).await
    }

    async fn execute<'a>(
        &'a self,
        operation: crate::Operation<'a>,
    ) -> Result<operation::Output<'a>> {
        self.next_api()?.execute(operation).await
    }
}

#[cfg(test)]
#[test]
fn test_namespace_from_str() {
    fn ns(s: &str) -> Result<Namespace, InvalidNamespaceError> {
        s.parse()
    }

    assert!(ns("0x14Cb1e6fb683A83455cA283e10f4959740A49ed7/0").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/0").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/255").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/256").is_err());
    assert!(ns("4Cb1e6fb683A83455cA283e10f4959740A49ed7/1").is_err());
}
