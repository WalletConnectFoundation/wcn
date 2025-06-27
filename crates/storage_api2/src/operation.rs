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
#[derive(Clone, Debug, From, EnumDiscriminants)]
#[strum_discriminants(name(Name))]
#[strum_discriminants(derive(Ordinalize))]
pub enum Operation<'a> {
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Get<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`Record`] under the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Set<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub record: Record<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`Record`] by the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Del<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`Record`] with the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a Map [`Record`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HGet<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HSet<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub entry: MapEntry<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`MapEntry`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HDel<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key and field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HGetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`MapEntry`] with the provided key and
/// field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HSetExp<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub field: Bytes<'a>,
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns cardinality of the Map with the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HCard<'a> {
    pub namespace: Namespace,
    pub key: Bytes<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns a [`MapPage`] by iterating over the fields of the Map with
/// the provided key.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl<'a> From<()> for Output<'a> {
    fn from(_: ()) -> Self {
        Self::None
    }
}

impl<'a> Output<'a> {
    /// Tries to downcast an [`Output`] within a [`Result`] into a concrete
    /// output type.
    pub fn downcast_result<T>(operation_result: Result<Self>) -> Result<T>
    where
        Self: TryInto<T, Error = derive_more::TryIntoError<Self>>,
    {
        operation_result?
            .try_into()
            .tap_err(|err| tracing::error!(?err, "Failed to downcast output"))
            .map_err(|err| Error::new(ErrorKind::Internal, Some(err.to_string())))
    }
}
