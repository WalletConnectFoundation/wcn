use crate::db::{
    cf,
    compaction,
    context::{DataContext, MergeOp},
    schema::{ColumnFamilyName, GenericKey},
};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StringColumn;

/// Main column for string data.
impl cf::Column for StringColumn {
    const NAME: ColumnFamilyName = ColumnFamilyName::GenericString;

    const PREFIX_SEEK_ENABLED: bool = false;
    type CompactionFilterFactory = compaction::ExpiredDataCompactionFilterFactory<Self>;

    type KeyType = GenericKey;
    type SubKeyType = ();
    type ValueType = Vec<u8>;
    type MergeOperator =
        compaction::AsymmetricMerge<Self, MergeOp<Self::ValueType>, DataContext<Self::ValueType>>;
}

/// Internal column for string data.
///
/// Internal columns are used to store system data that should not be exposed to
/// the user.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InternalStringColumn;

impl cf::Column for InternalStringColumn {
    const NAME: ColumnFamilyName = ColumnFamilyName::InternalGenericString;

    const PREFIX_SEEK_ENABLED: bool = false;
    type CompactionFilterFactory = compaction::NoopCompactionFilterFactory;

    type KeyType = GenericKey;
    type SubKeyType = ();
    type ValueType = Vec<u8>;
    type MergeOperator =
        compaction::AsymmetricMerge<Self, MergeOp<Self::ValueType>, DataContext<Self::ValueType>>;
}

/// Main column for map data type.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct MapColumn;

impl cf::Column for MapColumn {
    const NAME: ColumnFamilyName = ColumnFamilyName::GenericMap;

    const PREFIX_SEEK_ENABLED: bool = true;
    type CompactionFilterFactory = compaction::ExpiredDataCompactionFilterFactory<Self>;

    type KeyType = GenericKey;
    type SubKeyType = Vec<u8>;
    type ValueType = Vec<u8>;
    type MergeOperator =
        compaction::AsymmetricMerge<Self, MergeOp<Self::ValueType>, DataContext<Self::ValueType>>;
}

/// Internal column for map data type.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InternalMapColumn;

impl cf::Column for InternalMapColumn {
    const NAME: ColumnFamilyName = ColumnFamilyName::InternalGenericMap;

    const PREFIX_SEEK_ENABLED: bool = true;
    type CompactionFilterFactory = compaction::NoopCompactionFilterFactory;

    type KeyType = GenericKey;
    type SubKeyType = Vec<u8>;
    type ValueType = Vec<u8>;
    type MergeOperator =
        compaction::AsymmetricMerge<Self, MergeOp<Self::ValueType>, DataContext<Self::ValueType>>;
}
