//! Storage API operations.

use {
    crate::{
        Error,
        ErrorKind,
        KeyspaceVersion,
        MapEntryBorrowed,
        MapPage,
        Namespace,
        Record,
        RecordExpiration,
        RecordVersion,
        Result,
    },
    derive_more::derive::{From, TryInto},
    serde::Serialize,
    tap::TapFallible as _,
    wcn_rpc::{BorrowedMessage, Message},
};

/// Sum type of all Storage API operations.
#[derive(Clone, Debug, From, PartialEq, Eq)]
pub enum Operation<'a> {
    #[from(forward)]
    Owned(Owned),
    Borrowed(Borrowed<'a>),
}

impl Operation<'_> {
    /// Converts this [`Operation`] into [`Owned`].
    ///
    /// Re-allocates if it's [`Borrowed`].
    pub fn into_owned(self) -> Owned {
        match self {
            Self::Owned(owned) => owned,
            Self::Borrowed(borrowed) => borrowed.into_owned(),
        }
    }
}

#[derive(Clone, Debug, From, PartialEq, Eq)]
pub enum Owned {
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

#[derive(Clone, Debug, From, PartialEq, Eq)]
pub enum Borrowed<'a> {
    Get(GetBorrowed<'a>),
    Set(SetBorrowed<'a>),
    Del(DelBorrowed<'a>),
    GetExp(GetExpBorrowed<'a>),
    SetExp(SetExpBorrowed<'a>),

    HGet(HGetBorrowed<'a>),
    HSet(HSetBorrowed<'a>),
    HDel(HDelBorrowed<'a>),
    HGetExp(HGetExpBorrowed<'a>),
    HSetExp(HSetExpBorrowed<'a>),
    HCard(HCardBorrowed<'a>),
    HScan(HScanBorrowed<'a>),
}

impl Borrowed<'_> {
    /// Converts this [`Borrowed`] [`Operation`] into [`Owned`] by
    /// re-allocating.
    pub fn into_owned(self) -> Owned {
        match self {
            Self::Get(op) => Owned::Get(op.into_owned()),
            Self::Set(op) => Owned::Set(op.into_owned()),
            Self::Del(op) => Owned::Del(op.into_owned()),
            Self::GetExp(op) => Owned::GetExp(op.into_owned()),
            Self::SetExp(op) => Owned::SetExp(op.into_owned()),
            Self::HGet(op) => Owned::HGet(op.into_owned()),
            Self::HSet(op) => Owned::HSet(op.into_owned()),
            Self::HDel(op) => Owned::HDel(op.into_owned()),
            Self::HGetExp(op) => Owned::HGetExp(op.into_owned()),
            Self::HSetExp(op) => Owned::HSetExp(op.into_owned()),
            Self::HCard(op) => Owned::HCard(op.into_owned()),
            Self::HScan(op) => Owned::HScan(op.into_owned()),
        }
    }
}

/// Gets a [`Record`] by the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct GetBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`Record`] under the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct SetBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub record: Record,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`Record`] by the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct DelBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct GetExpBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`Record`] with the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct SetExpBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a Map [`Record`] by the provided key and field.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HGetBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets a new [`MapEntry`].
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HSetBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub entry: MapEntryBorrowed<'a>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Deletes a [`MapEntry`] by the provided key and field.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HDelBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Gets a [`RecordExpiration`] by the provided key and field.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HGetExpBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Sets [`RecordExpiration`] on the [`MapEntry`] with the provided key and
/// field.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HSetExpBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub expiration: RecordExpiration,
    pub version: RecordVersion,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns cardinality of the Map with the provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HCardBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// Returns a [`MapPage`] by iterating over the fields of the Map with the
/// provided key.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Message)]
pub struct HScanBorrowed<'a> {
    pub namespace: Namespace,
    pub key: &'a [u8],
    pub count: u32,
    pub cursor: Option<&'a [u8]>,
    pub keyspace_version: Option<KeyspaceVersion>,
}

/// [`Operation`] output.
#[derive(Clone, Debug, From, PartialEq, Eq, TryInto)]
#[try_into(owned, ref)]
pub enum Output {
    Record(Option<Record>),
    Expiration(Option<RecordExpiration>),
    MapPage(MapPage),
    Cardinality(u64),
    None(()),
}

impl Output {
    /// Constructs [`Output::None`]
    pub fn none() -> Output {
        Self::None(())
    }
}

impl Output {
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
}
