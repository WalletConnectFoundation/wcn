//! Storage API operations.

use {
    crate::{
        Bytes,
        Entry,
        EntryExpiration,
        EntryVersion,
        Field,
        Key,
        MapEntry,
        MapPage,
        MapRecord,
        Namespace,
        Record,
        Value,
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
pub enum Operation {
    Get(Get),
    Set(Set),
    Del(Del),
    GetExp(GetExp),
    SetExp(SetExp),

    HGet(HGet),
    HSet(HSet),
    HDel(HDel),
    HGetExp(HGetExp),
    HSetExp(HSetExp),
    HCard(HCard),
    HScan(HScan),
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

impl Operation {
    /// Returns [`Name`] of this [`Operation`].
    pub fn name(&self) -> Name {
        self.discriminant()
    }

    /// Returns [`Key`] of this [`Operation`].
    pub fn key(&self) -> &Key {
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
pub struct Get {
    pub namespace: Namespace,
    pub key: Key,
}

impl Get {
    /// Creates a new [`Get`] operation.
    pub fn new(namespace: impl Into<Namespace>, key: Key<impl Into<Bytes>>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

/// Sets a new [`Entry`].
#[derive(Clone, Debug)]
pub struct Set {
    pub namespace: Namespace,
    pub entry: Entry,
}

impl Set {
    /// Creates a new [`Set`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            entry: Entry::new(key, value, expiration),
        }
    }
}

/// Deletes an [`Entry`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct Del {
    pub namespace: Namespace,
    pub key: Key,
    pub version: EntryVersion,
}

impl Del {
    /// Creates a new [`Del`] operation.
    pub fn new(namespace: impl Into<Namespace>, key: Key<impl Into<Bytes>>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            version: EntryVersion::new(),
        }
    }
}

/// Gets an [`EntryExpiration`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct GetExp {
    pub namespace: Namespace,
    pub key: Key,
}

impl GetExp {
    /// Creates a new [`GetExp`] operation.
    pub fn new(namespace: impl Into<Namespace>, key: Key<impl Into<Bytes>>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

/// Sets [`EntryExpiration`] on the [`Entry`] with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct SetExp {
    pub namespace: Namespace,
    pub key: Key,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
}

impl SetExp {
    /// Creates a new [`SetExp`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGet {
    pub namespace: Namespace,
    pub key: Key,
    pub field: Field,
}

impl HGet {
    /// Creates a new [`HGet`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            field: field.convert(),
        }
    }
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug)]
pub struct HSet {
    pub namespace: Namespace,
    pub entry: MapEntry,
}

impl HSet {
    /// Creates a new [`HSet`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        value: Value<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            entry: MapEntry::new(key, field, value, expiration),
        }
    }
}

/// Deletes a [`MapEntry`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HDel {
    pub namespace: Namespace,
    pub key: Key,
    pub field: Field,
    pub version: EntryVersion,
}

impl HDel {
    /// Creates a new [`HDel`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            field: field.convert(),
            version: EntryVersion::new(),
        }
    }
}

/// Gets a [`EntryExpiration`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGetExp {
    pub namespace: Namespace,
    pub key: Key,
    pub field: Field,
}

impl HGetExp {
    /// Creates a new [`HGetExp`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            field: field.convert(),
        }
    }
}

/// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
/// [`Field`].
#[derive(Clone, Debug)]
pub struct HSetExp {
    pub namespace: Namespace,
    pub key: Key,
    pub field: Field,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
}

impl HSetExp {
    /// Creates a new [`HSetExp`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        field: Field<impl Into<Bytes>>,
        expiration: impl Into<EntryExpiration>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            field: field.convert(),
            expiration: expiration.into(),
            version: EntryVersion::new(),
        }
    }
}

/// Returns cardinality of the map with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HCard {
    pub namespace: Namespace,
    pub key: Key,
}

impl HCard {
    /// Creates a new [`HCard`] operation.
    pub fn new(namespace: impl Into<Namespace>, key: Key<impl Into<Bytes>>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

/// Returns a [`MapPage`] by iterating over the [`Field`]s of the map with
/// the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HScan {
    pub namespace: Namespace,
    pub key: Key,
    pub count: u32,
    pub cursor: Option<Field>,
}

impl HScan {
    /// Creates a new [`HCard`] operation.
    pub fn new(
        namespace: impl Into<Namespace>,
        key: Key<impl Into<Bytes>>,
        count: impl Into<u32>,
        cursor: Option<Field<impl Into<Bytes>>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            count: count.into(),
            cursor: cursor.map(Field::convert),
        }
    }
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
        WrongOutput: Into<E>,
    {
        operation_result?
            .try_into()
            .map_err(|err| WrongOutput {
                expected: type_name::<T>(),
                got: err.input.discriminant(),
            })
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Wrong operation output (expected: {expected}, got: {got})")]
pub struct WrongOutput {
    expected: &'static str,
    got: OutputName,
}
