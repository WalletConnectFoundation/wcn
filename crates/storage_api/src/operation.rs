use {
    crate::{
        Entry,
        EntryExpiration,
        EntryVersion,
        Field,
        Key,
        MapEntry,
        MapPage,
        MapRecord,
        Record,
    },
    derive_more::derive::{From, TryInto},
    std::any::type_name,
    strum::{EnumDiscriminants, IntoDiscriminant},
    wc::metrics::{self, enum_ordinalize::Ordinalize},
};

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
    pub key: Key,
}

/// Sets a new [`Entry`].
#[derive(Clone, Debug)]
pub struct Set {
    pub entry: Entry,
}

/// Deletes an [`Entry`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct Del {
    pub key: Key,
    pub version: EntryVersion,
}

/// Gets an [`EntryExpiration`] by the provided [`Key`].
#[derive(Clone, Debug)]
pub struct GetExp {
    pub key: Key,
}

/// Sets [`EntryExpiration`] on the [`Entry`] with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct SetExp {
    pub key: Key,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
}

/// Gets a map [`Record`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGet {
    pub key: Key,
    pub field: Field,
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug)]
pub struct HSet {
    pub entry: MapEntry,
}

/// Deletes a [`MapEntry`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HDel {
    pub key: Key,
    pub field: Field,
    pub version: EntryVersion,
}

/// Gets a [`EntryExpiration`] by the provided [`Key`] and [`Field`].
#[derive(Clone, Debug)]
pub struct HGetExp {
    pub key: Key,
    pub field: Field,
}

/// Sets [`Expiration`] on the [`MapEntry`] with the provided [`Key`] and
/// [`Field`].
#[derive(Clone, Debug)]
pub struct HSetExp {
    pub key: Key,
    pub field: Field,
    pub expiration: EntryExpiration,
    pub version: EntryVersion,
}

/// Returns cardinality of the map with the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HCard {
    pub key: Key,
}

/// Returns a [`MapPage`] by iterating over the [`Field`]s  of the map with
/// the provided [`Key`].
#[derive(Clone, Debug)]
pub struct HScan {
    pub key: Key,
    pub count: u32,
    pub cursor: Option<Field>,
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
