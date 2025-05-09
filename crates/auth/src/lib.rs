pub use token::Token;
use {
    derive_more::AsRef,
    ring::{
        aead,
        hkdf,
        signature::{self, KeyPair as _},
    },
    serde::{Deserialize, Serialize},
    std::sync::Arc,
    wcn_rpc::{
        identity::{self, ed25519},
        PeerId,
    },
};

pub mod token;

pub const PUBLIC_KEY_LEN: usize = ring::signature::ED25519_PUBLIC_KEY_LEN;

const INFO_CLIENT_KEY: &[u8] = b"client_auth_key";
const INFO_NAMESPACE_KEY: &[u8] = b"namespace_auth_key";
const INFO_ENCRYPTION_KEY: &[u8] = b"encryption_key";
const KEY_SALT: &[u8] = b"irn_api_client";
const KEY_LEN_SIG_SEED: usize = 32;
const KEY_LEN_ENCRYPTION: usize = 32;
const SIG_LEN_ED25519: usize = 64;

#[derive(Debug, PartialEq, Eq, thiserror::Error, Clone)]
pub enum Error {
    #[error("Invalid key length")]
    KeyLength,

    #[error("Invalid key encoding")]
    KeyEncoding,

    #[error("Invalid data")]
    Data,

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

    pub fn seal(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        if data.is_empty() {
            return Ok(data);
        }

        // Generate a unique nonce.
        let nonce = Nonce::generate().into_inner();

        // Compute the total data length for `nonce + serialized data + tag`.
        let mut out = Vec::with_capacity(aead::NONCE_LEN + data.len() + aead::MAX_TAG_LEN);

        // Write nonce bytes.
        out.extend_from_slice(&nonce);

        // Write data.
        out.extend_from_slice(&data);

        // Encrypt the data.
        let tag = self.encryption_key.seal_in_place_separate_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::empty(),
            &mut out[aead::NONCE_LEN..],
        )?;

        // Write tag.
        out.extend_from_slice(tag.as_ref());

        Ok(out)
    }

    pub fn open_in_place<'in_out>(&self, data: &'in_out mut [u8]) -> Result<&'in_out [u8], Error> {
        if data.is_empty() {
            Ok(data)
        } else if data.len() < aead::NONCE_LEN + aead::MAX_TAG_LEN + 1 {
            Err(Error::Data)
        } else {
            Ok(self.encryption_key.open_in_place(
                aead::Nonce::try_assume_unique_for_key(&data[..aead::NONCE_LEN])?,
                aead::Aad::empty(),
                &mut data[aead::NONCE_LEN..],
            )?)
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRef, PartialEq, Eq)]
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

    pub fn as_bytes(&self) -> &[u8; signature::ED25519_PUBLIC_KEY_LEN] {
        &self.0
    }
}

impl From<[u8; signature::ED25519_PUBLIC_KEY_LEN]> for PublicKey {
    fn from(value: [u8; signature::ED25519_PUBLIC_KEY_LEN]) -> Self {
        Self(value)
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

/// Creates an ed25519 private key from the provided secret.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Raw,
    Base64,
    Hex,
}

/// Decodes an ed25519 private key bytes into [`ed25519_dalek::SigningKey`].
pub fn client_key_from_bytes(
    bytes: &[u8],
    encoding: Encoding,
) -> Result<ed25519_dalek::SigningKey, Error> {
    let decoded = match encoding {
        Encoding::Raw => bytes.try_into().map_err(|_| Error::KeyLength)?,

        Encoding::Base64 => data_encoding::BASE64
            .decode(bytes)
            .map_err(|_| Error::KeyEncoding)?
            .try_into()
            .map_err(|_| Error::KeyLength)?,

        Encoding::Hex => data_encoding::HEXLOWER_PERMISSIVE
            .decode(bytes)
            .map_err(|_| Error::KeyEncoding)?
            .try_into()
            .map_err(|_| Error::KeyLength)?,
    };

    Ok(ed25519_dalek::SigningKey::from_bytes(&decoded))
}

/// Converts an ed25519 public key into [`network::PeerId`].
pub fn peer_id(public_key: &ed25519_dalek::VerifyingKey) -> PeerId {
    // Safe, as the key is guaranteed to be valid.
    let public_key = ed25519::PublicKey::try_from_bytes(public_key.as_bytes()).unwrap();

    identity::PublicKey::from(public_key).to_peer_id()
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
            let encrypted = auth1.seal(data.clone()).unwrap();

            let mut encrypted1 = encrypted.clone();
            let decrypted = auth1.open_in_place(&mut encrypted1).unwrap();

            assert_eq!(data, decrypted);

            let mut encrypted2 = encrypted;
            let decrypted = auth2.open_in_place(&mut encrypted2);

            assert!(decrypted.is_err());
        }

        // Same namespace, different secrets.
        {
            let auth1 = Auth::from_secret(b"secret1", b"namespace").unwrap();
            let auth2 = Auth::from_secret(b"secret2", b"namespace").unwrap();

            let data = vec![1u8, 2, 3, 4, 5];
            let encrypted = auth1.seal(data.clone()).unwrap();

            let mut encrypted1 = encrypted.clone();
            let decrypted = auth1.open_in_place(&mut encrypted1).unwrap();

            assert_eq!(data, decrypted);

            let mut encrypted2 = encrypted;
            let decrypted = auth2.open_in_place(&mut encrypted2);

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
