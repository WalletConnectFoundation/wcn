use {
    derive_more::AsRef,
    ring::{
        aead,
        hkdf,
        signature::{self, KeyPair as _},
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::sync::Arc,
};

pub const PUBLIC_KEY_LEN: usize = signature::ED25519_PUBLIC_KEY_LEN;

const INFO_CLIENT_KEY: &[u8] = b"client_auth_key";
const INFO_NAMESPACE_KEY: &[u8] = b"namespace_auth_key";
const INFO_ENCRYPTION_KEY: &[u8] = b"encryption_key";
const KEY_SALT: &[u8] = b"irn_api_client";
const KEY_LEN_SIG_SEED: usize = 32;
const KEY_LEN_ENCRYPTION: usize = 32;
const SIG_LEN_ED25519: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid key length")]
    KeyLength,

    #[error("Failed to serialize data")]
    Serialize,

    #[error("Failed to deserialize data")]
    Deserialize,

    #[error("Signature validation failed")]
    Signature,

    #[error("Invalid secret")]
    Secret,

    #[error("Invalid namespace")]
    Namespace,
}

impl From<ring::error::Unspecified> for Error {
    fn from(_: ring::error::Unspecified) -> Self {
        Self::KeyLength
    }
}

#[derive(Clone)]
pub struct Auth {
    sig_keypair: Arc<signature::Ed25519KeyPair>,
    encryption_key: aead::LessSafeKey,
}

impl Auth {
    pub fn from_secret(secret: &[u8], namespace: &[u8]) -> Result<Self, Error> {
        if secret.is_empty() {
            return Err(Error::Secret);
        }

        if namespace.is_empty() {
            return Err(Error::Namespace);
        }

        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, KEY_SALT).extract(secret);

        let mut sig_key_seed = [0; KEY_LEN_SIG_SEED];
        prk.expand(&[INFO_NAMESPACE_KEY, namespace], hkdf::HKDF_SHA256)?
            .fill(&mut sig_key_seed)?;

        let mut encryption_key = [0; KEY_LEN_ENCRYPTION];
        prk.expand(&[INFO_ENCRYPTION_KEY, namespace], &aead::CHACHA20_POLY1305)?
            .fill(&mut encryption_key)?;

        let sig_keypair = signature::Ed25519KeyPair::from_seed_unchecked(&sig_key_seed)
            .map(Arc::new)
            .map_err(|_| Error::KeyLength)?;

        let encryption_key = aead::LessSafeKey::new(aead::UnboundKey::new(
            &aead::CHACHA20_POLY1305,
            &encryption_key,
        )?);

        Ok(Self {
            sig_keypair,
            encryption_key,
        })
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        // Safe to unwrap, because we know the slice length is correct.
        Signature::try_from(self.sig_keypair.sign(data).as_ref()).unwrap()
    }

    pub fn public_key(&self) -> PublicKey {
        // Safe to unwrap, because we know the slice length is correct.
        PublicKey::try_from(self.sig_keypair.public_key().as_ref()).unwrap()
    }

    pub fn serialize<T>(&self, value: &T) -> Result<Vec<u8>, Error>
    where
        T: Serialize,
    {
        // Generate a unique nonce.
        let nonce = Nonce::generate().into_inner();

        let data_len =
            postcard::experimental::serialized_size(value).map_err(|_| Error::Serialize)?;

        // Compute the total data length for `nonce + serialized data + tag`.
        let mut data = Vec::with_capacity(aead::NONCE_LEN + data_len + aead::MAX_TAG_LEN);

        // Write nonce bytes.
        data.extend_from_slice(&nonce);

        // Write serialized data.
        let mut data = postcard::to_io(value, data).map_err(|_| Error::Serialize)?;

        // Encrypt the data.
        let tag = self.encryption_key.seal_in_place_separate_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::empty(),
            &mut data[aead::NONCE_LEN..],
        )?;

        data.extend_from_slice(tag.as_ref());

        Ok(data)
    }

    #[inline]
    pub fn deserialize<T>(&self, data: &[u8]) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        self.deserialize_in_place(data.into())
    }

    pub fn deserialize_in_place<T>(&self, mut data: Vec<u8>) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        if data.len() < aead::NONCE_LEN + aead::MAX_TAG_LEN + 1 {
            return Err(Error::Deserialize);
        }

        self.encryption_key.open_in_place(
            aead::Nonce::try_assume_unique_for_key(&data[..aead::NONCE_LEN])?,
            aead::Aad::empty(),
            &mut data[aead::NONCE_LEN..],
        )?;

        postcard::from_bytes(&data[aead::NONCE_LEN..]).map_err(|_| Error::Deserialize)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRef)]
#[serde(transparent)]
pub struct Nonce([u8; aead::NONCE_LEN]);

impl Nonce {
    pub fn generate() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_micros();

        // N.B. `u64` can fit 584554 average Gregorian years in microseconds.
        let timestamp = (timestamp as u64).to_be_bytes();
        let padding = rand::random::<u32>().to_be_bytes();

        let mut data = [0; aead::NONCE_LEN];
        data[..timestamp.len()].copy_from_slice(&timestamp);
        data[timestamp.len()..].copy_from_slice(&padding);

        Self(data)
    }

    #[inline]
    pub fn into_inner(self) -> [u8; aead::NONCE_LEN] {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize, AsRef, PartialEq, Eq)]
#[serde(transparent)]
pub struct PublicKey([u8; signature::ED25519_PUBLIC_KEY_LEN]);

impl PublicKey {
    pub fn verify(&self, data: &[u8], sig: &Signature) -> Result<(), Error> {
        signature::UnparsedPublicKey::new(&signature::ED25519, &self.0)
            .verify(data, sig.as_ref())
            .map_err(|_| Error::Signature)
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_| Error::KeyLength)?))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRef)]
#[serde(transparent)]
pub struct Signature(#[serde(with = "serde_big_array::BigArray")] [u8; SIG_LEN_ED25519]);

impl TryFrom<&[u8]> for Signature {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_| Error::Signature)?))
    }
}

pub fn client_key_from_secret(secret: &[u8]) -> Result<ed25519_dalek::SigningKey, Error> {
    if secret.is_empty() {
        return Err(Error::Secret);
    }

    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, KEY_SALT).extract(secret);

    let mut sig_key_seed = [0; KEY_LEN_SIG_SEED];
    prk.expand(&[INFO_CLIENT_KEY], hkdf::HKDF_SHA256)?
        .fill(&mut sig_key_seed)?;

    Ok(ed25519_dalek::SigningKey::from_bytes(&sig_key_seed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption() {
        // Same secret, different namespaces.
        {
            let auth1 = Auth::from_secret(b"secret", b"namespace1").unwrap();
            let auth2 = Auth::from_secret(b"secret", b"namespace2").unwrap();

            let data = vec![1u8, 2, 3, 4, 5];

            let encrypted = auth1.serialize(&data).unwrap();
            let decrypted = auth1
                .deserialize_in_place::<Vec<u8>>(encrypted.clone())
                .unwrap();

            assert_eq!(data, decrypted);

            let decrypted = auth2.deserialize_in_place::<Vec<u8>>(encrypted);

            assert!(decrypted.is_err());
        }

        // Same namespace, different secrets.
        {
            let auth1 = Auth::from_secret(b"secret1", b"namespace").unwrap();
            let auth2 = Auth::from_secret(b"secret2", b"namespace").unwrap();

            let data = vec![1u8, 2, 3, 4, 5];

            let encrypted = auth1.serialize(&data).unwrap();
            let decrypted = auth1
                .deserialize_in_place::<Vec<u8>>(encrypted.clone())
                .unwrap();

            assert_eq!(data, decrypted);

            let decrypted = auth2.deserialize_in_place::<Vec<u8>>(encrypted);

            assert!(decrypted.is_err());
        }
    }

    #[test]
    fn signature() {
        // Same secret, different namespaces.
        {
            let auth1 = Auth::from_secret(b"secret", b"namespace1").unwrap();
            let auth2 = Auth::from_secret(b"secret", b"namespace2").unwrap();

            let data = vec![1u8, 2, 3, 4, 5];
            let sig1 = auth1.sign(&data);
            let sig2 = auth2.sign(&data);

            assert!(sig1.as_ref() != sig2.as_ref());
            assert!(auth1.public_key().verify(&data, &sig1).is_ok());
            assert!(auth2.public_key().verify(&data, &sig1).is_err());
        }

        // Same namespace, different secrets.
        {
            let auth1 = Auth::from_secret(b"secret1", b"namespace").unwrap();
            let auth2 = Auth::from_secret(b"secret2", b"namespace").unwrap();

            let data = vec![1u8, 2, 3, 4, 5];
            let sig1 = auth1.sign(&data);
            let sig2 = auth2.sign(&data);

            assert!(sig1.as_ref() != sig2.as_ref());
            assert!(auth1.public_key().verify(&data, &sig1).is_ok());
            assert!(auth2.public_key().verify(&data, &sig1).is_err());
        }
    }
}
