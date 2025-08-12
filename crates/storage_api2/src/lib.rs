#![allow(clippy::manual_async_fn)]

use {
    futures::{stream, Stream},
    serde::{Deserialize, Serialize},
    std::{array, borrow::Cow, future::Future, ops::RangeInclusive, str::FromStr, time::Duration},
    strum::IntoStaticStr,
    time::OffsetDateTime as DateTime,
    wc::metrics::{self, enum_ordinalize::Ordinalize},
    wcn_rpc::Message,
};

pub mod operation;
pub use operation::Operation;

pub mod rpc;

#[cfg(feature = "testing")]
pub mod testing;

/// Namespace within a WCN cluster.
///
/// Namespaces are isolated and every [`StorageApi`] [`Operation`] gets executed
/// on a specific [`Namespace`].
///
/// Currently it's represented by the node operator's Ethereum address, with an
/// extra byte appended to support multiple namespaces per operator.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Message)]
pub struct Namespace([u8; 21]);

impl Namespace {
    /// Returns the inner byte representation of this [`Namespace`].
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns ID of the node operator this [`Namespace`] belongs to.
    pub fn node_operator_id(&self) -> [u8; 20] {
        array::from_fn(|idx| self.0[idx])
    }

    /// Returns index of this [`Namespace`] within the scope of the node
    /// operator.
    pub fn idx(&self) -> u8 {
        // NOTE(unwrap): it's a non-empty array
        *self.0.last().unwrap()
    }
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
    /// Executes the provided [`StorageApi`] [`Operation`] using a reference.
    fn execute_ref(
        &self,
        operation: &Operation<'_>,
    ) -> impl Future<Output = Result<operation::Output>> + Send {
        async { self.execute(operation.clone()).await }
    }

    /// Executes the provided [`StorageApi`] [`Operation`] using a [`Callback`].
    fn execute_callback<C: Callback>(
        &self,
        operation: Operation<'_>,
        callback: C,
    ) -> impl Future<Output = Result<(), C::Error>> + Send {
        async move {
            let result = self.execute(operation).await;
            callback.send_result(&result).await
        }
    }

    /// Executes the provided [`StorageApi`] [`Operation`].
    fn execute(
        &self,
        operation: Operation<'_>,
    ) -> impl Future<Output = Result<operation::Output>> + Send {
        async move { self.execute_ref(&operation).await }
    }

    /// Reads all data stored in a WCN Database within the specified
    /// `keyrange`.
    ///
    /// Intended to be implemented for WCN Databases and WCN Replicas, but
    /// not WCN Coordinators.
    ///
    /// Same [`KeyspaceVersion`] validation applies as for the regular
    /// [`Operation`]s.
    fn read_data(
        &self,
        _keyrange: RangeInclusive<u64>,
        _keyspace_version: KeyspaceVersion,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<DataItem>> + Send>> + Send {
        async { Err::<stream::Empty<_>, _>(Error::unauthorized()) }
    }

    /// Writes all data from the provided [`Stream`] into WCN Database.
    ///
    /// Should only be implemented for WCN Databases.
    fn write_data(
        &self,
        _stream: impl Stream<Item = Result<DataItem>> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Err(Error::unauthorized()) }
    }
}

/// [`StorageApi`] factory.
pub trait Factory<Args>: Clone + Send + Sync + 'static {
    /// [`StorageApi`] types produced by this factory.
    type StorageApi: StorageApi + Clone;

    /// Creates a new [`StorageApi`].
    fn new_storage_api(&self, args: Args) -> Result<Self::StorageApi>;
}

impl<Args, S> Factory<Args> for S
where
    S: StorageApi + Clone,
{
    type StorageApi = Self;

    fn new_storage_api(&self, _args: Args) -> Result<Self::StorageApi> {
        Ok(self.clone())
    }
}

/// [`StorageApi`] callback for returning borrowed [`operation::Output`]s.
pub trait Callback: Send {
    type Error: Send + 'static;

    fn send_result(
        self,
        result: &Result<operation::Output>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// [`Stream`] item of [`StorageApi::pull_data`] and [`StorageApi::push_data`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Message)]
pub enum DataItem {
    /// [`DataFrame`].
    Frame(DataFrame),

    /// Indicates that data transfer has been successfully completed and
    /// contains the number of processed [`DataFrame`]s.
    Done(u64),
}

/// Raw chunk of data taken from a WCN Database.
///
/// Intended to be used for data migration purposes only.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Message)]
pub struct DataFrame {
    /// [`DataType`] of the key-value pair this [`DataFrame`] represents.
    pub data_type: DataType,

    /// Raw key bytes as they are being stored in the WCN Database.
    pub key: Vec<u8>,

    /// Raw value bytes as they are being stored in the WCN Database.
    pub value: Vec<u8>,
}

/// [`DataFrame`] data type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Message)]
pub enum DataType {
    /// Regular key-value pair.
    Kv,

    /// Key-value pair, in which the value contains a sub-map.
    Map,
}

/// Raw bytes value with metadata related to this value.
///
/// [`Record`]s are being deleted from WCN Database after specified
/// [`Record::expiration`] time.
///
/// A [`Record`] can not be overwritten by another [`Record`] with a lesser
/// [version][`Record::version`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Message)]
pub struct RecordBorrowed<'a> {
    /// Value of this [`Record`].
    pub value: &'a [u8],

    /// Expiration time of [`Record`].
    pub expiration: RecordExpiration,

    /// Version of this [`Record`].
    pub version: RecordVersion,
}

impl<'a> RecordBorrowed<'a> {
    pub fn new(value: &'a [u8], expiration: impl Into<RecordExpiration>) -> Self {
        Self {
            value,
            expiration: expiration.into(),
            version: RecordVersion::now(),
        }
    }
}

impl Record {
    pub fn borrow(&self) -> RecordBorrowed<'_> {
        RecordBorrowed {
            value: &self.value,
            expiration: self.expiration,
            version: self.version,
        }
    }
}

impl RecordBorrowed<'_> {
    pub fn into_owned(&self) -> Record {
        wcn_rpc::BorrowedMessage::into_owned(self)
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Message)]
pub struct MapEntryBorrowed<'a> {
    pub field: &'a [u8],
    pub record: RecordBorrowed<'a>,
}

/// Expiration time of a [`Record`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Message)]
pub struct RecordExpiration {
    unix_timestamp_secs: u64,
}

impl RecordExpiration {
    pub fn from_unix_timestamp_secs(timestamp: u64) -> Self {
        Self {
            unix_timestamp_secs: timestamp,
        }
    }

    pub fn to_unix_timestamp_secs(&self) -> u64 {
        self.unix_timestamp_secs
    }
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Message)]
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

    pub fn from_unix_timestamp_micros(timestamp: u64) -> Self {
        Self {
            unix_timestamp_micros: timestamp,
        }
    }

    pub fn to_unix_timestamp_micros(&self) -> u64 {
        self.unix_timestamp_micros
    }
}

/// Page of [map entries][MapEntry].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Message)]
pub struct MapPage {
    /// [`MapRecord`]s of this [`Page`].
    pub entries: Vec<MapEntry>,

    /// Indicator of whether there's a next [`Page`] or not.
    pub has_next: bool,
}

impl MapPage {
    /// Returns cursor pointing to the next [`Page`] if there is one.
    pub fn next_page_cursor(&self) -> Option<&[u8]> {
        self.has_next
            .then(|| self.entries.last().map(|entry| entry.field.as_slice()))
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

        let mut bytes = [0u8; 21];

        const_hex::decode_to_slice(operator_id, &mut bytes[..20])
            .map_err(|err| Error(format!("Invalid node operator id: {err}").into()))?;

        bytes[20] = id
            .parse()
            .map_err(|err| Error(format!("Invalid id: {err}").into()))?;

        Ok(Self(bytes))
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
#[error("{kind:?}({message:?})")]
pub struct Error {
    kind: ErrorKind,
    message: Option<String>,
}

/// [`Error`] kind.
#[derive(Clone, Copy, Debug, IntoStaticStr, PartialEq, Eq, Ordinalize)]
pub enum ErrorKind {
    /// Client is not authorized to perform an [`Operation`].
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
    pub const fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }

    /// Creates a new [`Error`] with [`ErrorKind::Unauthorized`].
    pub const fn unauthorized() -> Self {
        Self::new(ErrorKind::Unauthorized)
    }

    /// Creates a new [`Error`] with [`ErrorKind::Timeout`].
    pub const fn timeout() -> Self {
        Self::new(ErrorKind::Timeout)
    }

    /// Creates a new [`Error`] with [`ErrorKind::KeyspaceVersionMismatch`].
    pub const fn keyspace_version_mismatch() -> Self {
        Self::new(ErrorKind::KeyspaceVersionMismatch)
    }

    /// Creates a new [`Error`] with [`ErrorKind::Internal`].
    pub const fn internal() -> Self {
        Self::new(ErrorKind::Internal)
    }

    pub fn with_message(mut self, message: impl ToString) -> Self {
        self.message = Some(message.to_string());
        self
    }

    /// Returns [`ErrorKind`] of this [`Error`].
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }
}

impl metrics::Enum for ErrorKind {
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
#[test]
fn namespace_from_str() {
    fn ns(s: &str) -> Result<Namespace, InvalidNamespaceError> {
        s.parse()
    }

    assert!(ns("0x14Cb1e6fb683A83455cA283e10f4959740A49ed7/0").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/0").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/255").is_ok());
    assert!(ns("14Cb1e6fb683A83455cA283e10f4959740A49ed7/256").is_err());
    assert!(ns("4Cb1e6fb683A83455cA283e10f4959740A49ed7/1").is_err());
}
