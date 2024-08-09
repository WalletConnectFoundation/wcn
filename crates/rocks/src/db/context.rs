//! Payload wrapping container for storing extra information alongside incoming
//! payload.

use {
    super::compaction::Merge,
    serde::{Deserialize, Serialize},
};

/// Unix timestamp in seconds.
pub type UnixTimestampSecs = u64;

/// Unix timestamp in microseconds.
pub type UnixTimestampMicros = u64;

/// Custom wrapper for timestamp updates to avoid using double option as the
/// serialized field, since serde would serialize `Some(None)` as `None`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TimestampUpdate {
    Set(UnixTimestampSecs),
    Extend(UnixTimestampSecs),
    Unset,
}

impl Merge<Self> for TimestampUpdate {
    fn merge(&mut self, input: Self) {
        let input = match input {
            Self::Extend(extend_to) => match *self {
                // Extend if we have a specific value set.
                Self::Set(set_to) => {
                    if extend_to > set_to {
                        Self::Set(extend_to)
                    } else {
                        *self
                    }
                }

                // Straightforward extension. Select the max value.
                Self::Extend(extend_from) => Self::Extend(extend_from.max(extend_to)),

                // If the value's been unset, it'll remain unset.
                Self::Unset => *self,
            },

            // `Set` and `Unset` variants overwrite the value unconditionally.
            _ => input,
        };

        *self = input;
    }
}

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
    expires: Option<UnixTimestampSecs>,
    timestamp: UnixTimestampMicros,
}

#[derive(Debug, Default, Serialize, Deserialize)]
enum MergeOpKind<T> {
    Set(T),
    SetExp,
    #[default]
    Del,
}

// Max TTL currently required by the business logic, with extra time leeway.
const MAX_TTL_SECS: u64 = 30 * 86400 + 120;

impl<T> MergeOp<T> {
    fn new(
        kind: MergeOpKind<T>,
        expires: Option<UnixTimestampSecs>,
        timestamp: UnixTimestampMicros,
    ) -> Self {
        const TIME_LEEWAY_MICROS: u64 = 120 * 1000 * 1000;
        let now = crate::util::timestamp_micros();

        if timestamp > now + TIME_LEEWAY_MICROS {
            tracing::warn!(now, timestamp, "invalid merge op timestamp");
        }

        if let Some(expiration) = expires {
            let now = crate::util::timestamp_secs();

            if expiration > now + MAX_TTL_SECS {
                tracing::warn!(now, expiration, "invalid merge op ttl");
            }
        }

        Self {
            kind,
            expires,
            timestamp,
        }
    }

    pub fn set(
        value: T,
        expiration: Option<UnixTimestampSecs>,
        timestamp: UnixTimestampMicros,
    ) -> Self {
        Self::new(MergeOpKind::Set(value), expiration, timestamp)
    }

    pub fn set_exp(expiration: Option<UnixTimestampSecs>, timestamp: UnixTimestampMicros) -> Self {
        Self::new(MergeOpKind::SetExp, expiration, timestamp)
    }

    pub fn del(timestamp: UnixTimestampMicros) -> Self {
        // For how long the mark of the deleted record should stay in the database.
        // We need it to properly merge concurrent updates/deletes during data
        // migrations.
        const LINGER_SECS: UnixTimestampSecs = 24 * 60 * 60; // a day

        Self::new(MergeOpKind::Del, Some(LINGER_SECS), timestamp)
    }
}

impl<T> Merge<Self> for MergeOp<T> {
    fn merge(&mut self, input: Self) {
        use MergeOpKind as Kind;

        if input.timestamp <= self.timestamp {
            // Ignore updates with timestamps preceding this one.
            tracing::debug!(
                this_timestamp = self.timestamp,
                other_timestamp = input.timestamp,
                "ignoring merge operand. invalid timestamp"
            );
            return;
        }

        self.kind = match (std::mem::take(&mut self.kind), input.kind) {
            (_, set @ Kind::Set(_)) => set,
            (_, del @ Kind::Del) => del,
            (set @ Kind::Set(_), Kind::SetExp) => set,
            (set_exp @ Kind::SetExp, Kind::SetExp) => set_exp,
            (Kind::Del, Kind::SetExp) => {
                tracing::warn!("It shouldn't be possible to set_exp for a deleted record");
                return;
            }
        };

        self.expires = input.expires;
        self.timestamp = input.timestamp;
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
    expires: Option<UnixTimestampSecs>,
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
    pub fn expiration_timestamp(&self) -> Option<UnixTimestampSecs> {
        self.expires
    }

    /// Returns `true` if expiration time is set and reached, so the object is
    /// considered expired.
    pub fn expired(&self) -> bool {
        self.expires.map_or(false, |expiry_time| {
            expiry_time <= crate::util::timestamp_secs()
        })
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
    fn merge(&mut self, op: MergeOp<T>) {
        use MergeOpKind as Kind;

        let updated_timestamp = self.updated.unwrap_or(self.created);

        if op.timestamp <= updated_timestamp {
            // Ignore updates with timestamps preceding this one.
            tracing::debug!(
                this_timestamp = updated_timestamp,
                other_timestamp = op.timestamp,
                "ignoring data context merge. invalid timestamp"
            );
            return;
        }

        match op.kind {
            Kind::Set(value) => self.payload = Payload::Some(value),
            Kind::SetExp => {}
            Kind::Del => self.payload = Payload::None,
        };

        self.expires = op.expires;
        self.updated = Some(op.timestamp);
    }
}
