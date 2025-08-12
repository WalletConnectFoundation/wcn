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
        RecordBorrowed,
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

    #[from]
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

    /// Returns [`Namespace`] of this [`Operation`].
    pub fn namespace(&self) -> &Namespace {
        match self {
            Self::Owned(owned) => owned.namespace(),
            Self::Borrowed(borrowed) => borrowed.namespace(),
        }
    }

    /// Returns the key of a KV pair of this [`Operation`].
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Owned(owned) => owned.key(),
            Self::Borrowed(borrowed) => borrowed.key(),
        }
    }

    /// Indicates whether this [`Operation`] is a write.
    pub fn is_write(&self) -> bool {
        match self {
            Self::Owned(owned) => owned.is_write(),
            Self::Borrowed(borrowed) => borrowed.is_write(),
        }
    }

    /// Returns [`KeyspaceVersion`] of this [`Operation`]
    pub fn keyspace_version(&self) -> Option<KeyspaceVersion> {
        match self {
            Operation::Owned(owned) => owned.keyspace_version(),
            Operation::Borrowed(borrowed) => borrowed.keyspace_version(),
        }
    }

    /// Sets [`KeyspaceVersion`] of this [`Operation`].
    pub fn set_keyspace_version(&mut self, version: KeyspaceVersion) {
        match self {
            Self::Owned(owned) => owned.set_keyspace_version(version),
            Self::Borrowed(borrowed) => borrowed.set_keyspace_version(version),
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

impl Owned {
    /// Returns [`Namespace`] of this [`Owned`] [`Operation`].
    pub fn namespace(&self) -> &Namespace {
        match self {
            Self::Get(op) => &op.namespace,
            Self::Set(op) => &op.namespace,
            Self::Del(op) => &op.namespace,
            Self::GetExp(op) => &op.namespace,
            Self::SetExp(op) => &op.namespace,
            Self::HGet(op) => &op.namespace,
            Self::HSet(op) => &op.namespace,
            Self::HDel(op) => &op.namespace,
            Self::HGetExp(op) => &op.namespace,
            Self::HSetExp(op) => &op.namespace,
            Self::HCard(op) => &op.namespace,
            Self::HScan(op) => &op.namespace,
        }
    }

    /// Returns the key of a KV pair of this [`Owned`] [`Operation`].
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get(op) => &op.key,
            Self::Set(op) => &op.key,
            Self::Del(op) => &op.key,
            Self::GetExp(op) => &op.key,
            Self::SetExp(op) => &op.key,
            Self::HGet(op) => &op.key,
            Self::HSet(op) => &op.key,
            Self::HDel(op) => &op.key,
            Self::HGetExp(op) => &op.key,
            Self::HSetExp(op) => &op.key,
            Self::HCard(op) => &op.key,
            Self::HScan(op) => &op.key,
        }
    }

    /// Indicates whether this [`Owned`] [`Operation`] is a write.
    pub fn is_write(&self) -> bool {
        match self {
            Self::Set(_)
            | Self::Del(_)
            | Self::SetExp(_)
            | Self::HSet(_)
            | Self::HDel(_)
            | Self::HSetExp(_) => true,
            Self::Get(_)
            | Self::GetExp(_)
            | Self::HGet(_)
            | Self::HGetExp(_)
            | Self::HCard(_)
            | Self::HScan(_) => false,
        }
    }

    /// Returns [`KeyspaceVersion`] of this [`Owned`] [`Operation`]
    pub fn keyspace_version(&self) -> Option<KeyspaceVersion> {
        match self {
            Self::Get(get) => get.keyspace_version,
            Self::Set(set) => set.keyspace_version,
            Self::Del(del) => del.keyspace_version,
            Self::GetExp(get_exp) => get_exp.keyspace_version,
            Self::SetExp(set_exp) => set_exp.keyspace_version,
            Self::HGet(hget) => hget.keyspace_version,
            Self::HSet(hset) => hset.keyspace_version,
            Self::HDel(hdel) => hdel.keyspace_version,
            Self::HGetExp(hget_exp) => hget_exp.keyspace_version,
            Self::HSetExp(hset_exp) => hset_exp.keyspace_version,
            Self::HCard(hcard) => hcard.keyspace_version,
            Self::HScan(hscan) => hscan.keyspace_version,
        }
    }

    /// Sets [`KeyspaceVersion`] of this [`Owned`] [`Operation`].
    pub fn set_keyspace_version(&mut self, version: KeyspaceVersion) {
        *match self {
            Self::Get(get) => &mut get.keyspace_version,
            Self::Set(set) => &mut set.keyspace_version,
            Self::Del(del) => &mut del.keyspace_version,
            Self::GetExp(get_exp) => &mut get_exp.keyspace_version,
            Self::SetExp(set_exp) => &mut set_exp.keyspace_version,
            Self::HGet(hget) => &mut hget.keyspace_version,
            Self::HSet(hset) => &mut hset.keyspace_version,
            Self::HDel(hdel) => &mut hdel.keyspace_version,
            Self::HGetExp(hget_exp) => &mut hget_exp.keyspace_version,
            Self::HSetExp(hset_exp) => &mut hset_exp.keyspace_version,
            Self::HCard(hcard) => &mut hcard.keyspace_version,
            Self::HScan(hscan) => &mut hscan.keyspace_version,
        } = Some(version);
    }
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
    /// Returns [`Namespace`] of this [`Borrowed`] [`Operation`].
    pub fn namespace(&self) -> &Namespace {
        match self {
            Self::Get(op) => &op.namespace,
            Self::Set(op) => &op.namespace,
            Self::Del(op) => &op.namespace,
            Self::GetExp(op) => &op.namespace,
            Self::SetExp(op) => &op.namespace,
            Self::HGet(op) => &op.namespace,
            Self::HSet(op) => &op.namespace,
            Self::HDel(op) => &op.namespace,
            Self::HGetExp(op) => &op.namespace,
            Self::HSetExp(op) => &op.namespace,
            Self::HCard(op) => &op.namespace,
            Self::HScan(op) => &op.namespace,
        }
    }

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

    /// Returns the key of a KV pair of this [`Borrowed`] [`Operation`].
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get(op) => op.key,
            Self::Set(op) => op.key,
            Self::Del(op) => op.key,
            Self::GetExp(op) => op.key,
            Self::SetExp(op) => op.key,
            Self::HGet(op) => op.key,
            Self::HSet(op) => op.key,
            Self::HDel(op) => op.key,
            Self::HGetExp(op) => op.key,
            Self::HSetExp(op) => op.key,
            Self::HCard(op) => op.key,
            Self::HScan(op) => op.key,
        }
    }

    /// Indicates whether this [`Borrowed`] [`Operation`] is a write.
    pub fn is_write(&self) -> bool {
        match self {
            Self::Set(_)
            | Self::Del(_)
            | Self::SetExp(_)
            | Self::HSet(_)
            | Self::HDel(_)
            | Self::HSetExp(_) => true,
            Self::Get(_)
            | Self::GetExp(_)
            | Self::HGet(_)
            | Self::HGetExp(_)
            | Self::HCard(_)
            | Self::HScan(_) => false,
        }
    }

    /// Returns [`KeyspaceVersion`] of this [`Borrowed`] [`Operation`]
    pub fn keyspace_version(&self) -> Option<KeyspaceVersion> {
        match self {
            Self::Get(get) => get.keyspace_version,
            Self::Set(set) => set.keyspace_version,
            Self::Del(del) => del.keyspace_version,
            Self::GetExp(get_exp) => get_exp.keyspace_version,
            Self::SetExp(set_exp) => set_exp.keyspace_version,
            Self::HGet(hget) => hget.keyspace_version,
            Self::HSet(hset) => hset.keyspace_version,
            Self::HDel(hdel) => hdel.keyspace_version,
            Self::HGetExp(hget_exp) => hget_exp.keyspace_version,
            Self::HSetExp(hset_exp) => hset_exp.keyspace_version,
            Self::HCard(hcard) => hcard.keyspace_version,
            Self::HScan(hscan) => hscan.keyspace_version,
        }
    }

    /// Sets [`KeyspaceVersion`] of this [`Borrowed`] [`Operation`].
    pub fn set_keyspace_version(&mut self, version: KeyspaceVersion) {
        *match self {
            Borrowed::Get(get) => &mut get.keyspace_version,
            Borrowed::Set(set) => &mut set.keyspace_version,
            Borrowed::Del(del) => &mut del.keyspace_version,
            Borrowed::GetExp(get_exp) => &mut get_exp.keyspace_version,
            Borrowed::SetExp(set_exp) => &mut set_exp.keyspace_version,
            Borrowed::HGet(hget) => &mut hget.keyspace_version,
            Borrowed::HSet(hset) => &mut hset.keyspace_version,
            Borrowed::HDel(hdel) => &mut hdel.keyspace_version,
            Borrowed::HGetExp(hget_exp) => &mut hget_exp.keyspace_version,
            Borrowed::HSetExp(hset_exp) => &mut hset_exp.keyspace_version,
            Borrowed::HCard(hcard) => &mut hcard.keyspace_version,
            Borrowed::HScan(hscan) => &mut hscan.keyspace_version,
        } = Some(version);
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
    pub record: RecordBorrowed<'a>,
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
