//! Payload wrapping container for storing extra information alongside incoming
//! payload.

use {
    super::compaction::Merge,
    crate::util::timestamp_secs,
    derive_more::From,
    serde::{Deserialize, Serialize},
};

/// Unix timestamp in seconds.
pub type UnixTimestampSecs = u64;

/// Unix timestamp in microseconds.
pub type UnixTimestampMicros = u64;

/// Custom wrapper to avoid using `Option<()>` which causes serialization
/// issues.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Payload<T> {
    #[default]
    None,
    Some(T),
}

impl<T> Payload<T> {
    fn as_option(&self) -> Option<&T> {
        match self {
            Self::None => None,
            Self::Some(data) => Some(data),
        }
    }
}

impl<T> From<Payload<T>> for Option<T> {
    fn from(value: Payload<T>) -> Self {
        match value {
            Payload::None => None,
            Payload::Some(data) => Some(data),
        }
    }
}

impl<T> From<Option<T>> for Payload<T> {
    fn from(value: Option<T>) -> Self {
        value.map(Self::Some).unwrap_or(Self::None)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MergeOp<T> {
    kind: MergeOpKind<T>,
    timestamp: UnixTimestampMicros,
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeSet<T> {
    value: T,
    expiration: UnixTimestampSecs,
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeSetVal<T> {
    value: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeSetExp {
    expiration: UnixTimestampSecs,
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeDel {
    expiration: UnixTimestampSecs,
}

#[derive(Debug, From, Serialize, Deserialize)]
enum MergeOpKind<T> {
    Set(MergeSet<T>),
    SetVal(MergeSetVal<T>),
    SetExp(MergeSetExp),
    Del(MergeDel),
}

// Max TTL currently required by the business logic, with extra time leeway.
const MAX_TTL_SECS: u64 = 30 * 86400 + 120;

impl<T> MergeOp<T> {
    fn new(kind: impl Into<MergeOpKind<T>>, timestamp: UnixTimestampMicros) -> Self {
        use MergeOpKind as Kind;

        const TIME_LEEWAY_MICROS: u64 = 120 * 1000 * 1000;
        let now = crate::util::timestamp_micros();

        if timestamp > now + TIME_LEEWAY_MICROS {
            tracing::warn!(now, timestamp, "invalid merge op timestamp");
        }

        let kind = kind.into();
        let expires = match &kind {
            Kind::Set(set) => Some(set.expiration),
            Kind::SetExp(set_exp) => Some(set_exp.expiration),
            Kind::Del(del) => Some(del.expiration),
            Kind::SetVal(_) => None,
        };

        if let Some(expiration) = expires {
            let now = crate::util::timestamp_secs();

            if expiration > now + MAX_TTL_SECS {
                tracing::warn!(now, expiration, "invalid merge op ttl");
            }
        }

        Self { kind, timestamp }
    }

    pub fn set(value: T, expiration: UnixTimestampSecs, timestamp: UnixTimestampMicros) -> Self {
        Self::new(MergeSet { value, expiration }, timestamp)
    }

    pub fn set_val(value: T, timestamp: UnixTimestampMicros) -> Self {
        Self::new(MergeSetVal { value }, timestamp)
    }

    pub fn set_exp(expiration: UnixTimestampSecs, timestamp: UnixTimestampMicros) -> Self {
        Self::new(MergeSetExp { expiration }, timestamp)
    }

    pub fn del(timestamp: UnixTimestampMicros) -> Self {
        // For how long the mark of the deleted record should stay in the database.
        // We need it to properly merge concurrent updates/deletes during data
        // migrations.
        const LINGER_SECS: u64 = 24 * 60 * 60; // a day

        Self::new(
            MergeDel {
                expiration: timestamp_secs() + LINGER_SECS,
            },
            timestamp,
        )
    }
}

impl<T> Merge<Self> for MergeOp<T> {
    fn merge(mut self, input: Self) -> Self {
        use MergeOpKind as Kind;

        if input.timestamp <= self.timestamp {
            // Ignore updates with timestamps preceding this one.
            tracing::debug!(
                this_timestamp = self.timestamp,
                other_timestamp = input.timestamp,
                "ignoring merge operand. invalid timestamp"
            );
            return self;
        }

        self.kind = match (self.kind, input.kind) {
            (_, set @ Kind::Set(_)) => set,
            (_, del @ Kind::Del(_)) => del,

            (Kind::SetVal(_), set_val @ Kind::SetVal(_)) => set_val,
            (Kind::SetExp(_), set_exp @ Kind::SetExp(_)) => set_exp,

            (Kind::Set(set), Kind::SetVal(set_val)) => MergeSet {
                value: set_val.value,
                expiration: set.expiration,
            }
            .into(),
            (Kind::Set(set), Kind::SetExp(set_exp)) => MergeSet {
                value: set.value,
                expiration: set_exp.expiration,
            }
            .into(),

            (Kind::SetVal(set_val), Kind::SetExp(set_exp)) => MergeSet {
                value: set_val.value,
                expiration: set_exp.expiration,
            }
            .into(),
            (Kind::SetExp(set_exp), Kind::SetVal(set_val)) => MergeSet {
                value: set_val.value,
                expiration: set_exp.expiration,
            }
            .into(),

            (del @ Kind::Del(_), Kind::SetVal(_) | Kind::SetExp(_)) => del,
        };

        self.timestamp = input.timestamp;
        self
    }
}

/// To augment data with additional information (metadata), the incoming data is
/// stored inside of a typed `DataContext<T>` wrapper. Payload is stored as is,
/// and is serialized along with the wrapper, on persistence.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct DataContext<T> {
    /// Wrapped data. It can be `None` if the context is initialized during
    /// merging without an existing value. Contexts without the payload should
    /// be treated as empty keys, and return an 'entry not found' error most for
    /// most operations involving reading.
    payload: Payload<T>,
    /// Unix-timestamp (in seconds) with expiration time.
    expires: UnixTimestampSecs,
    /// Unix-timestamp (in microseconds) of last modified time.
    updated: Option<UnixTimestampMicros>,
    /// Unix-timestamp (in microseconds) of entry creation time.
    created: UnixTimestampMicros,
}

impl<T> DataContext<T> {
    /// Returns a reference to contained data.
    pub fn payload(&self) -> Option<&T> {
        self.payload.as_option()
    }

    /// Converts from `DataContext<T>` to `T`. For expired payload it will
    /// always return `None`, even if the context object contains a payload.
    pub fn into_payload(self) -> Option<T> {
        if self.expired() {
            None
        } else {
            self.payload.into()
        }
    }

    /// Returns expiration timestamp (in seconds), if it is set for the object.
    pub fn expiration_timestamp(&self) -> UnixTimestampSecs {
        self.expires
    }

    /// Returns `true` if expiration time is set and reached, so the object is
    /// considered expired.
    pub fn expired(&self) -> bool {
        self.expires <= crate::util::timestamp_secs()
    }

    /// Returns modification timestamp (in milliseconds), if it is set for the
    /// object.
    pub fn modification_timestamp(&self) -> Option<UnixTimestampMicros> {
        self.updated
    }

    /// Returns entry creation timestamp (in milliseconds).
    pub fn creation_timestamp(&self) -> UnixTimestampMicros {
        self.created
    }
}

/// Merges changes from the specified merge operation.
impl<T> Merge<MergeOp<T>> for DataContext<T> {
    fn merge(mut self, op: MergeOp<T>) -> Self {
        use MergeOpKind as Kind;

        let updated_timestamp = self.updated.unwrap_or(self.created);

        if op.timestamp <= updated_timestamp {
            // Ignore updates with timestamps preceding this one.
            tracing::debug!(
                this_timestamp = updated_timestamp,
                other_timestamp = op.timestamp,
                "ignoring data context merge. invalid timestamp"
            );
            return self;
        }

        match op.kind {
            Kind::Set(set) => {
                self.payload = Payload::Some(set.value);
                self.expires = set.expiration;
            }
            Kind::SetVal(set_val) => self.payload = Payload::Some(set_val.value),
            Kind::SetExp(set_exp) => self.expires = set_exp.expiration,
            Kind::Del(del) => {
                self.payload = Payload::None;
                self.expires = del.expiration;
            }
        };

        self.updated = Some(op.timestamp);
        self
    }
}
