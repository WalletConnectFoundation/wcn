//! Storage API operations.

use {
    crate::{
        Bytes,
        Error,
        ErrorKind,
        KeyspaceVersion,
        MapEntry,
        MapPage,
        Namespace,
        Record,
        RecordExpiration,
        RecordVersion,
        Result,
    },
    derive_more::derive::{From, TryInto},
    serde::{Deserialize, Serialize},
    strum::{EnumDiscriminants, IntoDiscriminant},
    tap::TapFallible as _,
    wc::metrics::{self, enum_ordinalize::Ordinalize},
};

/// Sum type of all Storage API operations.
#[derive(Clone, Debug, From, PartialEq, Eq)]
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

impl<'a> Operation<'a> {
    /// Converts &self to [`OperationRef`].
    ///
    /// Reference to reference conversion, does not re-allocate.
    pub fn to_ref(&'a self) -> OperationRef<'a> {
        match self {
            Self::Get(op) => OperationRef::Get(op),
            Self::Set(op) => OperationRef::Set(op),
            Self::Del(op) => OperationRef::Del(op),
            Self::GetExp(op) => OperationRef::GetExp(op),
            Self::SetExp(op) => OperationRef::SetExp(op),
            Self::HGet(op) => OperationRef::HGet(op),
            Self::HSet(op) => OperationRef::HSet(op),
            Self::HDel(op) => OperationRef::HDel(op),
            Self::HGetExp(op) => OperationRef::HGetExp(op),
            Self::HSetExp(op) => OperationRef::HSetExp(op),
            Self::HCard(op) => OperationRef::HCard(op),
            Self::HScan(op) => OperationRef::HScan(op),
        }
    }
}

/// Sum type of references to all Storage API operations.
#[derive(Clone, Debug, From, EnumDiscriminants, PartialEq, Eq)]
#[strum_discriminants(name(Name))]
#[strum_discriminants(derive(Ordinalize))]
pub enum OperationRef<'a> {
    Get(&'a Get<'a>),
    Set(&'a Set<'a>),
    Del(&'a Del<'a>),
    GetExp(&'a GetExp<'a>),
    SetExp(&'a SetExp<'a>),

    HGet(&'a HGet<'a>),
    HSet(&'a HSet<'a>),
    HDel(&'a HDel<'a>),
    HGetExp(&'a HGetExp<'a>),
    HSetExp(&'a HSetExp<'a>),
    HCard(&'a HCard<'a>),
    HScan(&'a HScan<'a>),
}

impl<'a> OperationRef<'a> {
    /// Converts `self` into owned [`Operation`].
    ///
    /// Re-allocates the underying heap-allocated data.
    pub fn to_owned(self) -> Operation<'a> {
        match self {
            Self::Get(op) => Operation::Get(op.clone()),
            Self::Set(op) => Operation::Set(op.clone()),
            Self::Del(op) => Operation::Del(op.clone()),
            Self::GetExp(op) => Operation::GetExp(op.clone()),
            Self::SetExp(op) => Operation::SetExp(op.clone()),
            Self::HGet(op) => Operation::HGet(op.clone()),
            Self::HSet(op) => Operation::HSet(op.clone()),
            Self::HDel(op) => Operation::HDel(op.clone()),
            Self::HGetExp(op) => Operation::HGetExp(op.clone()),
            Self::HSetExp(op) => Operation::HSetExp(op.clone()),
            Self::HCard(op) => Operation::HCard(op.clone()),
            Self::HScan(op) => Operation::HScan(op.clone()),
        }
    }
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

impl<'a> OperationRef<'a> {
    /// Returns [`Name`] of this [`Operation`].
    pub fn name(&self) -> Name {
        self.discriminant()
    }

    /// Returns key of this [`Operation`].
    pub fn key(&self) -> &Bytes<'a> {
        match self {
            Self::Get(get) => &get.key,
            Self::Set(set) => &set.key,
            Self::Del(del) => &del.key,
            Self::GetExp(get_exp) => &get_exp.key,
            Self::SetExp(set_exp) => &set_exp.key,
            Self::HGet(hget) => &hget.key,
            Self::HSet(hset) => &hset.key,
            Self::HDel(hdel) => &hdel.key,
            Self::HGetExp(hget_exp) => &hget_exp.key,
            Self::HSetExp(hset_exp) => &hset_exp.key,
            Self::HCard(hcard) => &hcard.key,
            Self::HScan(hscan) => &hscan.key,
        }
    }
}

/// Gets a [`Record`] by the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Get<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`Record`] under the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Set<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub record: Record<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`Record`] by the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Del<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`Record`] with the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a Map [`Record`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HGet<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HSet<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub entry: MapEntry<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`MapEntry`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HDel<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HGetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`MapEntry`] with the provided key and
/// field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HSetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns cardinality of the Map with the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HCard<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns a [`MapPage`] by iterating over the fields of the Map with
/// the provided key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HScan<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub count: u32,
    pub cursor: Option<Bytes<'a>>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// [`Operation`] output.
#[derive(Clone, Debug, From, PartialEq, Eq, EnumDiscriminants, TryInto)]
#[strum_discriminants(name(OutputName))]
#[strum_discriminants(derive(strum::Display))]
pub enum Output<'a> {
    Record(Option<Record<'a>>),
    Expiration(Option<RecordExpiration>),
    MapPage(MapPage<'a>),
    Cardinality(u64),
    None,
}

impl From<()> for Output<'_> {
    fn from(_: ()) -> Self {
        Self::None
    }
}

impl Output<'_> {
    /// Tries to downcast an [`Output`] within a [`Result`] into a concrete
    /// output type.
    pub fn downcast_result<T>(operation_result: Result<Self>) -> Result<T>
    where
        Self: TryInto<T, Error = derive_more::TryIntoError<Self>>,
    {
        operation_result?
            .try_into()
            .tap_err(|err| tracing::error!(?err, "Failed to downcast output"))
            .map_err(|err| Error::new(ErrorKind::Internal).with_message(err))
    }

    /// Converts `Self` into 'static.
    pub fn into_static(self) -> Output<'static> {
        match self {
            Self::Record(opt) => Output::Record(opt.map(Record::into_static)),
            Self::Expiration(record_expiration) => Output::Expiration(record_expiration),
            Self::MapPage(map_page) => Output::MapPage(map_page.into_static()),
            Self::Cardinality(card) => Output::Cardinality(card),
            Self::None => Output::None,
        }
    }
}
