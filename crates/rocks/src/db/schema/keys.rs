use {
    crate::{
        util::serde::{deserialize_from_bytes, serialize_to_bytes, FromBytes, ToBytes},
        StorageError,
        StorageResult,
    },
    byteorder::{BigEndian, ReadBytesExt, WriteBytesExt},
    serde::{Deserialize, Serialize},
    std::io::{Cursor, Write},
};

/// Length of the `u64` prefix used as the keyring position.
pub const KEYRING_POSITION_PREFIX_LENGTH: usize = 8;

/// Primary key for the column family.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct GenericKey {
    position: u64,
    data: Vec<u8>,
}

impl ToBytes for GenericKey {
    fn to_bytes(&self) -> StorageResult<Vec<u8>> {
        // Key position + data length + data.
        let capacity = 8 + 4 + self.data.len();

        let mut bytes = Vec::with_capacity(capacity);

        let mut cursor = Cursor::new(&mut bytes);

        cursor
            .write_u64::<BigEndian>(self.position)
            .map_err(|_| StorageError::Serialize)?;

        cursor
            .write_u32::<BigEndian>(self.data.len() as u32)
            .map_err(|_| StorageError::Serialize)?;

        cursor
            .write(&self.data)
            .map_err(|_| StorageError::Serialize)?;

        Ok(bytes)
    }
}

impl FromBytes for GenericKey {
    fn from_bytes(bytes: &[u8]) -> StorageResult<Self> {
        let mut cursor = Cursor::new(bytes);

        let position = cursor
            .read_u64::<BigEndian>()
            .map_err(|_| StorageError::Deserialize)?;

        let data_len = cursor
            .read_u32::<BigEndian>()
            .map_err(|_| StorageError::Deserialize)? as usize;

        let pos = cursor.position() as usize;
        let slice = &bytes[pos..];

        if slice.len() != data_len {
            Err(StorageError::Deserialize)
        } else {
            let data = Vec::from(slice);

            Ok(Self { position, data })
        }
    }
}

impl Serialize for GenericKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_to_bytes(self, serializer)
    }
}

impl<'de> Deserialize<'de> for GenericKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_from_bytes::<Self, D>(deserializer)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("Invalid key length: {0}")]
    Length(usize),

    #[error("Failed to read keyring position")]
    ReadKeyringPosition,

    #[error("Failed to read buffer length")]
    ReadBufferLength,

    #[error("Invalid buffer length. Expected: {expected} Actual: {actual}")]
    InvalidBuffer { expected: usize, actual: usize },
}

impl GenericKey {
    /// Creates a new key with a keyspace position determined by the supplied
    /// partitioner.
    pub fn new(position: u64, data: Vec<u8>) -> Self {
        Self { position, data }
    }

    /// Creates a new key with a keyspace position provided by the caller.
    pub fn with_position(position: u64, data: Vec<u8>) -> Self {
        Self { position, data }
    }

    /// Data associated with this key.
    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn calculate_size(data: &[u8]) -> Result<usize, KeyError> {
        // We expect valid length to be at least `key_position + buffer_length +
        // buffer`, where `buffer` should be at least 1 byte.
        const MIN_LEN: usize = KEYRING_POSITION_PREFIX_LENGTH + 4 + 1;

        let actual_len = data.len();

        if actual_len < MIN_LEN {
            return Err(KeyError::Length(actual_len));
        }

        let mut cursor = Cursor::new(data);

        // Key position.
        cursor
            .read_u64::<BigEndian>()
            .map_err(|_| KeyError::ReadKeyringPosition)?;

        // Buffer length.
        let buffer_len = cursor
            .read_u32::<BigEndian>()
            .map_err(|_| KeyError::ReadBufferLength)? as usize;

        let curr_pos = cursor.position() as usize;
        let expected_len = curr_pos + buffer_len;

        // It's possible that the actual length is greater than the expected length,
        // since the data slice we're given here may also contain serialized field data
        // used with `map` type.
        if actual_len >= expected_len {
            Ok(expected_len)
        } else {
            Err(KeyError::InvalidBuffer {
                expected: expected_len,
                actual: actual_len,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::util::serde::{deserialize, serialize},
    };

    #[test]
    fn key_parsing() {
        let key_data = vec![1, 2, 3, 4, 5];

        // Generic set storage key.
        let key = GenericKey::new(1, key_data);
        let key_bytes = key.to_bytes().unwrap();
        let new_key = GenericKey::from_bytes(&key_bytes).unwrap();
        assert_eq!(new_key, key);

        let key_to_bytes = key.to_bytes().unwrap();
        let key_serialized = serialize(&key).unwrap();

        // Make sure that serde uses on `ToBytes` implementation. The skipped first byte
        // is a prefix added by the serializer. The rest of the bytes are supposed to be
        // the exact `ToBytes` data.
        assert_eq!(key_to_bytes, key_serialized[1..]);

        // Make sure that serde uses on `FromBytes` implementation.
        assert_eq!(
            GenericKey::from_bytes(&key_to_bytes).unwrap(),
            deserialize(&key_serialized).unwrap()
        );
    }
}
