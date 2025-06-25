//! Storage API operations.

use {
    crate::{
        Entry,
        EntryExpiration,
        EntryVersion,
        Field,
        Key,
        KeyspaceVersion,
        MapEntry,
        MapPage,
        MapRecord,
        Namespace,
        Record,
    },
    derive_more::derive::{From, TryInto},
    std::any::type_name,
    strum::{EnumDiscriminants, IntoDiscriminant},
    wc::metrics::{self, enum_ordinalize::Ordinalize},
};

/// Sum type of all Storage API operations.
#[derive(Clone, Debug, From, EnumDiscriminants)]
#[strum_discriminants(name(Name))]
#[strum_discriminants(derive(Ordinalize))]
pub enum Operation<'a> {
    Get(Get<'a>),
    Set(Set<'a>),
    Del(Del<'a>),
    GetExp(GetExp<'a>),
    SetExp(SetExp<'a>),

    HGet(HGet<'a>),
    HSet(HSet<'a>),
    HDel(HDel<'a>),
    HGetExp(HGetExp<'a>),
    HSetExp(HSetExp<'a>),
    HCard(HCard<'a>),
    HScan(HScan<'a>),
}

impl metrics::Enum for Name {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Del => "del",
            Self::GetExp => "get_exp",
            Self::SetExp => "set_exp",
            Self::HGet => "hget",
            Self::HSet => "hset",
            Self::HDel => "hdel",
            Self::HGetExp => "hget_exp",
            Self::HSetExp => "hset_exp",
            Self::HCard => "hcard",
            Self::HScan => "hscan",
        }
    }
}

impl<'a> Operation<'a> {
    /// Returns [`Name`] of this [`Operation`].
    pub fn name(&self) -> Name {
        self.discriminant()
    }

    /// Returns key of this [`Operation`].
    pub fn key(&self) -> &Key<'a> {
        match self {
            Self::Get(get) => &get.key,
            Self::Set(set) => &set.entry.key,
            Self::Del(del) => &del.key,
            Self::GetExp(get_exp) => &get_exp.key,
            Self::SetExp(set_exp) => &set_exp.key,
            Self::HGet(hget) => &hget.key,
            Self::HSet(hset) => &hset.entry.key,
            Self::HDel(hdel) => &hdel.key,
            Self::HGetExp(hget_exp) => &hget_exp.key,
            Self::HSetExp(hset_exp) => &hset_exp.key,
            Self::HCard(hcard) => &hcard.key,
            Self::HScan(hscan) => &hscan.key,
        }
    }
}

/// Gets a [`Record`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct Get<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`Entry`].
#[derive(Clone, Debug)]
pub struct Set<'a> {
    pub namespace: Namespace,
    pub entry: Entry<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes an [`Entry`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct Del<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub version: EntryVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets an [`EntryExpiration`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct GetExp<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`EntryExpiration`] on the [`Entry`] with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct SetExp<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGet<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub field: Field<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug)]
pub struct HSet<'a> {
    pub namespace: Namespace,
    pub entry: MapEntry<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`MapEntry`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HDel<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub field: Field<'a>,
    pub version: EntryVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`EntryExpiration`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGetExp<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub field: Field<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
/// [`Field`].
#[derive(Clone, Debug)]
pub struct HSetExp<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub field: Field<'a>,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns cardinality of the map with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HCard<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns a [`MapPage`] by iterating over the [`Field`]s of the map with
/// the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HScan<'a> {
    pub namespace: Namespace,
    pub key: Key<'a>,
    pub count: u32,
    pub cursor: Option<Field<'a>>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// [`Operation`] output.
#[derive(Clone, Debug, From, PartialEq, Eq, EnumDiscriminants, TryInto)]
#[strum_discriminants(name(OutputName))]
#[strum_discriminants(derive(strum::Display))]
pub enum Output {
    Record(Option<Record>),
    Expiration(Option<EntryExpiration>),
    MapRecord(Option<MapRecord>),
    MapPage(MapPage),
    Cardinality(u64),
    None,
}

impl From<()> for Output {
    fn from(_: ()) -> Self {
        Self::None
    }
}

impl Output {
    /// Tries to downcast an [`Output`] within a [`Result`] into a concrete
    /// output type.
    pub fn downcast_result<T, E>(operation_result: Result<Self, E>) -> Result<T, E>
    where
        Self: TryInto<T, Error = derive_more::TryIntoError<Self>>,
        WrongOutputError: Into<E>,
    {
        operation_result?
            .try_into()
            .map_err(|err| WrongOutputError {
                expected: type_name::<T>(),
                got: err.input.discriminant(),
            })
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Wrong operation output (expected: {expected}, got: {got})")]
pub struct WrongOutputError {
    expected: &'static str,
    got: OutputName,
}

impl From<WrongOutputError> for crate::Error {
    fn from(err: WrongOutputError) -> Self {
        Self::new(
            crate::ErrorKind::WrongOperationOutput,
            Some(format!("{err}")),
        )
    }
}
