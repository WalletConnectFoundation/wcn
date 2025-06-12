#![allow(clippy::manual_async_fn)]

pub use {
    auth,
    wcn_rpc::{identity, Multiaddr, PeerAddr, PeerId},
};
use {
    derive_more::AsRef,
    futures::{FutureExt as _, TryFutureExt as _},
    std::{future::Future, marker::PhantomData, str::FromStr, time::Duration},
    time::OffsetDateTime as DateTime,
};

pub mod operation;
pub use operation::Operation;

#[cfg(any(feature = "rpc_client", feature = "rpc_server"))]
pub mod rpc;

/// Namespace within WCN network.
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
pub trait StorageApi {
    type Error: From<operation::WrongOutput>;

    /// Executes the provided [`operation::Get`].
    fn get(
        &self,
        get: operation::Get,
    ) -> impl Future<Output = Result<Option<Record>, Self::Error>> + Send {
        self.execute(get).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Set`].
    fn set(&self, set: operation::Set) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(set).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::Del`].
    fn del(&self, del: operation::Del) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(del).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::GetExp`].
    fn get_exp(
        &self,
        get_exp: operation::GetExp,
    ) -> impl Future<Output = Result<Option<EntryExpiration>, Self::Error>> + Send {
        self.execute(get_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::SetExp`].
    fn set_exp(
        &self,
        set_exp: operation::SetExp,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(set_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGet`].
    fn hget(
        &self,
        hget: operation::HGet,
    ) -> impl Future<Output = Result<Option<Record>, Self::Error>> + Send {
        self.execute(hget).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSet`].
    fn hset(&self, hset: operation::HSet) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(hset).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HDel`].
    fn hdel(&self, hdel: operation::HDel) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(hdel).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HGetExp`].
    fn hget_exp(
        &self,
        hget_exp: operation::HGetExp,
    ) -> impl Future<Output = Result<Option<EntryExpiration>, Self::Error>> + Send {
        self.execute(hget_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HSetExp`].
    fn hset_exp(
        &self,
        hset_exp: operation::HSetExp,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.execute(hset_exp)
            .map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HCard`].
    fn hcard(
        &self,
        hcard: operation::HCard,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        self.execute(hcard).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`operation::HScan`].
    fn hscan(
        &self,
        hscan: operation::HScan,
    ) -> impl Future<Output = Result<MapPage, Self::Error>> + Send {
        self.execute(hscan).map(operation::Output::downcast_result)
    }

    /// Executes the provided [`StorageApi`] [`Operation`].
    fn execute(
        &self,
        operation: impl Into<Operation> + Send,
    ) -> impl Future<Output = Result<operation::Output, Self::Error>> + Send;

    /// Makes this [`StorageApi`] [`Namespaced`].
    fn namespaced(self, namespace: impl Into<Namespace>) -> Namespaced<Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace: namespace.into(),
            storage_api: self,
            _marker: PhantomData,
        }
    }

    /// Makes this [`StorageApi`] [`Namespaced`] using references instead of
    /// owned values.
    fn namespaced_ref<N: AsRef<Namespace>>(&self, namespace: N) -> Namespaced<Self, N, &Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace,
            storage_api: self,
            _marker: PhantomData,
        }
    }
}

/// A convienience wrapper around a [`StorageApi`] for client usage.
///
/// Each function on this wrapper has the same arguments as the respective
/// [`Operation`] constructor with [`Namespace`] being automatically specified.
#[derive(Clone, Debug)]
pub struct Namespaced<S, N = Namespace, T = Owned<S>> {
    namespace: N,
    storage_api: T,
    _marker: PhantomData<S>,
}

/// Owned [`StorageApi`].
///
/// Required to make [`Namespaced`] generic over ownership.
pub struct Owned<S>(S);

impl<S, N, T> Namespaced<S, N, T>
where
    N: AsRef<Namespace>,
    T: AsRef<S>,
    S: StorageApi,
{
    /// Returns reference to the underlying [`Namespace`].
    pub fn namespace(&self) -> &Namespace {
        self.namespace.as_ref()
    }

    /// Returns reference to the underlying [`StorageApi`].
    pub fn storage_api(&self) -> &S {
        self.storage_api.as_ref()
    }

    /// Gets a [`Record`] by the provided [`Key`].
    pub async fn get(&self, key: Key<impl Into<Bytes>>) -> Result<Option<Record>, S::Error> {
        self.storage_api()
            .get(operation::Get::new(*self.namespace(), key))
            .await
    }

    /// Sets a new [`Entry`].
    async fn set(
        &self,
        key: Key<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Result<(), S::Error> {
        let namespace = *self.namespace();
        self.storage_api()
            .set(operation::Set::new(namespace, key, value, expiration))
            .await
    }

    /// Deletes an [`Entry`] by the provided [`Key`].
    async fn del(&self, key: Key<impl Into<Bytes>>) -> Result<(), S::Error> {
        self.storage_api()
            .del(operation::Del::new(*self.namespace(), key))
            .await
    }

    /// Gets an [`EntryExpiration`] by the provided [`Key`].
    async fn get_exp(
        &self,
        key: Key<impl Into<Bytes>>,
    ) -> Result<Option<EntryExpiration>, S::Error> {
        self.storage_api()
            .get_exp(operation::GetExp::new(*self.namespace(), key))
            .await
    }

    /// Sets [`EntryExpiration`] on the [`Entry`] with the provided [`Key`].
    async fn set_exp(
        &self,
        key: Key<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Result<(), S::Error> {
        self.storage_api()
            .set_exp(operation::SetExp::new(*self.namespace(), key, expiration))
            .await
    }

    /// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
    async fn hget(
        &self,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Result<Option<Record>, S::Error> {
        self.storage_api()
            .hget(operation::HGet::new(*self.namespace(), key, field))
            .await
    }

    /// Sets a new [`MapEntry`].
    async fn hset(
        &self,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Result<(), S::Error> {
        let namespace = *self.namespace();
        self.storage_api()
            .hset(operation::HSet::new(
                namespace, key, value, field, expiration,
            ))
            .await
    }

    /// Deletes a [`MapEntry`] by the provided [`Key`] and [`Field`].
    async fn hdel(
        &self,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Result<(), S::Error> {
        self.storage_api()
            .hdel(operation::HDel::new(*self.namespace(), key, field))
            .await
    }

    /// Gets a [`EntryExpiration`] by the provided [`Key`] and [`Field`].
    async fn hget_exp(
        &self,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Result<Option<EntryExpiration>, S::Error> {
        self.storage_api()
            .hget_exp(operation::HGetExp::new(*self.namespace(), key, field))
            .await
    }

    /// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
    /// [`Field`].
    async fn hset_exp(
        &self,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Result<(), S::Error> {
        let namespace = *self.namespace();
        self.storage_api()
            .hset_exp(operation::HSetExp::new(namespace, key, field, expiration))
            .await
    }

    /// Returns cardinality of the map with the provided [`Key`].
    async fn hcard(&self, key: Key<impl Into<Bytes>>) -> Result<u64, S::Error> {
        self.storage_api()
            .hcard(operation::HCard::new(*self.namespace(), key))
            .await
    }

    /// Returns a [`MapPage`] by iterating over the [`Field`]s  of the map with
    /// the provided [`Key`].
    async fn hscan(
        &self,
        key: Key<impl Into<Bytes>>,
        count: impl Into<u32>,
        cursor: Option<Field<impl Into<Bytes>>>,
    ) -> Result<MapPage, S::Error> {
        self.storage_api()
            .hscan(operation::HScan::new(*self.namespace(), key, count, cursor))
            .await
    }

    /// Returns [`Value`]s of the map with the provided [`Key`].
    async fn hvals(&self, key: Key<impl Into<Bytes>>) -> Result<Vec<Value>, S::Error> {
        // `1000` is generous, relay has ~100 limit for these small maps
        self.hscan(key, 1000u32, None::<Field>)
            .map_ok(|page| page.records.into_iter().map(|rec| rec.value).collect())
            .await
    }
}

/// Raw bytes.
pub type Bytes = Vec<u8>;

/// Key in a KV storage.
#[derive(Clone, Debug)]
pub struct Key<T = Bytes>(pub T);

impl<T> Key<T> {
    /// Converts `Self` into `Key<U>`.
    pub fn into<U>(self) -> Key<U>
    where
        T: Into<U>,
    {
        Self(self.into())
    }
}

/// Value in a KV storage.
#[derive(Clone, Debug)]
pub struct Value<T = Bytes>(pub T);

impl<T> Value<T> {
    /// Converts `Self` into `Value<U>`.
    pub fn into<U>(self) -> Value<U>
    where
        T: Into<U>,
    {
        Self(self.into())
    }
}

/// Subkey of a [`MapEntry`].
#[derive(Clone, Debug)]
pub struct Field<T = Bytes>(pub T);

impl<T> Field<T> {
    /// Converts `Self` into `Field<U>`.
    pub fn into<U>(self) -> Value<U>
    where
        T: Into<U>,
    {
        Self(self.into())
    }
}

/// Basic KV storage entry.
#[derive(Clone, Debug)]
pub struct Entry {
    /// [`Key`] of this [`Entry`].
    pub key: Key,

    /// [`Value`] of this [`Entry`].
    pub value: Value,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

impl Entry {
    /// Creates a new [`Entry`].
    pub fn new(
        key: Key<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// Map entry in which each [`Value`] is associated with both [`Key`] and subkey
/// ([`Field`]).
#[derive(Clone, Debug)]
pub struct MapEntry {
    /// [`Key`] of this [`Entry`].
    pub key: Key,

    /// [`Field`] of this [`Entry`].
    pub field: Field,

    /// [`Value`] of this [`Entry`].
    pub value: Value,

    /// Expiration time of this [`Entry`].
    pub expiration: EntryExpiration,

    /// Version of this [`Entry`].
    pub version: EntryVersion,
}

impl MapEntry {
    /// Creates a new [`MapEntry`].
    pub fn new(
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// [`Entry`]/[`MapEntry`] without the associated [`Key`]/[`Field`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Value of this [`Record`].
    pub value: Value,

    /// Expiration time of the associated [`Entry`]/[`MapEntry`].
    pub expiration: EntryExpiration,

    /// Version of the associated [`Entry`]/[`MapEntry`].
    pub version: EntryVersion,
}

impl Record {
    /// Creates a new [`Record`].
    pub fn new(
        value: impl Into<Value>,
        expiration: impl Into<EntryExpiration>,
        version: impl Into<EntryVersion>,
    ) -> Self {
        Self {
            value: value.into(),
            expiration: expiration.into(),
            version: version.into(),
        }
    }
}

/// [`MapEntry`] without the associated [`Key`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MapRecord {
    /// Field of this [`MapRecord`].
    pub field: Field,

    /// Value of this [`MapRecord`].
    pub value: Value,

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

impl<S> AsRef<S> for Owned<S> {
    fn as_ref(&self) -> &S {
        &self.0
    }
}

impl AsRef<Namespace> for Namespace {
    fn as_ref(&self) -> &Namespace {
        self
    }
}

impl FromStr for Namespace {
    type Err = InvalidNamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('/');
        let addr = parts.next()?;

        // const_hex::decode_to_array(input)
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid namespace: {_0}")]
pub struct InvalidNamespaceError(String);
