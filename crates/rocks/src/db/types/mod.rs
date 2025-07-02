use {
    crate::{db::context::UnixTimestampMicros, UnixTimestampSecs},
    serde::{Deserialize, Serialize},
};
pub use {map::MapStorage, string::StringStorage};

pub mod common;
pub mod map;
pub mod string;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record<V = Vec<u8>> {
    pub value: V,
    pub expiration: UnixTimestampSecs,
    pub version: UnixTimestampMicros,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MapRecord<F = Vec<u8>, V = Vec<u8>> {
    pub field: F,
    pub value: V,
    pub expiration: UnixTimestampSecs,
    pub version: UnixTimestampMicros,
}

/// Defines `(field, value)` pair.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Default)]
pub struct Pair<F, V> {
    pub field: F,
    pub value: V,
}

impl<F, V> Pair<F, V> {
    pub fn new(field: F, value: V) -> Self {
        Self { field, value }
    }
}
