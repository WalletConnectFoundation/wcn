pub use {
    columns::{InternalMapColumn, InternalStringColumn, MapColumn, StringColumn},
    keys::GenericKey,
};
use {
    serde::{Deserialize, Serialize},
    std::fmt,
};

pub mod columns;
pub mod keys;
pub mod test_types;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ColumnFamilyName {
    GenericString,
    GenericMap,
    InternalGenericString,
    InternalGenericMap,
    InternalHintedOps,
}

impl ColumnFamilyName {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GenericString => "generic_string",
            Self::GenericMap => "generic_map",
            Self::InternalGenericString => "internal_generic_string",
            Self::InternalGenericMap => "internal_generic_map",
            Self::InternalHintedOps => "internal_hinted_ops",
        }
    }
}

impl fmt::Display for ColumnFamilyName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<ColumnFamilyName> for String {
    fn from(name: ColumnFamilyName) -> Self {
        name.to_string()
    }
}
