use {
    crate::{Data, StorageError, StorageResult},
    serde::{de::DeserializeOwned, Serialize},
};

pub fn serialize<T>(data: &T) -> StorageResult<Data>
where
    T: Serialize,
{
    rmp_serde::to_vec(data).map_err(|_| StorageError::Serialize)
}

pub fn deserialize<T>(data: &[u8]) -> StorageResult<T>
where
    T: DeserializeOwned,
{
    rmp_serde::from_slice(data).map_err(|_| StorageError::Deserialize)
}

pub mod flags_as_bits {
    use {
        bitflags::Flags,
        serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer},
    };

    pub fn serialize<T, S>(data: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Flags,
        T::Bits: Serialize,
        S: Serializer,
    {
        data.bits().serialize(serializer)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: Flags,
        T::Bits: DeserializeOwned,
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        T::Bits::deserialize(deserializer)
            .map(T::from_bits)
            .map_err(D::Error::custom)?
            .ok_or_else(|| D::Error::custom("invalid flag bits"))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn flags_as_bits() {
        use serde::{Deserialize, Serialize};

        bitflags::bitflags! {
            #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
            struct TestFlags: u8 {
                const One  = 0b00000001;
                const Two  = 0b00000010;
                const Four = 0b00000100;
            }
        }

        #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
        struct Data {
            #[serde(with = "super::flags_as_bits")]
            flags: TestFlags,
        }

        let data = Data {
            flags: TestFlags::One | TestFlags::Four,
        };

        let serialized = serde_json::to_string(&data).unwrap();
        assert_eq!(serialized, r#"{"flags":5}"#);

        let deserialized: Data = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, data);

        assert!(serde_json::from_str::<Data>(r#"{"flags":8}"#).is_err());
    }
}
