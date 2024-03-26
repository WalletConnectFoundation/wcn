use {
    crate::Error,
    relay_storage::keys::{FromBytes, ToBytes},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    tap::TapFallible,
};

pub fn serialize<T: Serialize>(data: &T) -> Result<Vec<u8>, Error> {
    postcard::experimental::serialized_size(data)
        .and_then(|size| postcard::to_io(data, std::io::Cursor::new(Vec::with_capacity(size))))
        .map(|writer| writer.into_inner())
        .tap_err(|err| tracing::debug!(?err, "serialization error"))
        .map_err(|_| Error::Serialize)
}

pub fn deserialize<T: DeserializeOwned>(data: &[u8]) -> Result<T, Error> {
    postcard::from_bytes(data)
        .tap_err(|err| tracing::debug!(?err, "deserialization error"))
        .map_err(|_| Error::Deserialize)
}

/// Serialize any type that implements `ToBytes` using given serializer.
pub fn serialize_to_bytes<T: ToBytes, S: serde::Serializer>(
    data: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let bytes = data.to_bytes().map_err(serde::ser::Error::custom)?;
    serializer.serialize_bytes(&bytes)
}

/// Deserialize any type that implements `FromBytes` using given deserializer.
pub fn deserialize_from_bytes<'de, T: FromBytes, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    let bytes = <&[u8]>::deserialize(deserializer)?;
    T::from_bytes(bytes).map_err(serde::de::Error::custom)
}
