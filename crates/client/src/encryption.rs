use ring::{aead, hkdf};

const INFO_ENCRYPTION_KEY: &[u8] = b"encryption_key";
const KEY_SALT: &[u8] = b"wcn_client";
const KEY_LEN_ENCRYPTION: usize = 32;

#[derive(Debug, PartialEq, Eq, thiserror::Error, Clone)]
pub enum Error {
    #[error("Invalid key length")]
    KeyLength,

    #[error("Invalid data")]
    Data,

    #[error("Invalid secret")]
    Secret,
}

impl From<ring::error::Unspecified> for Error {
    fn from(_: ring::error::Unspecified) -> Self {
        Self::KeyLength
    }
}

#[derive(Clone)]
pub struct Encryption {
    key: aead::LessSafeKey,
}

impl Encryption {
    pub fn new(secret: &[u8]) -> Result<Self, Error> {
        if secret.is_empty() {
            return Err(Error::Secret);
        }

        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, KEY_SALT).extract(secret);

        let mut encryption_key = [0; KEY_LEN_ENCRYPTION];
        prk.expand(&[INFO_ENCRYPTION_KEY], &aead::CHACHA20_POLY1305)?
            .fill(&mut encryption_key)?;

        let key = aead::LessSafeKey::new(aead::UnboundKey::new(
            &aead::CHACHA20_POLY1305,
            &encryption_key,
        )?);

        Ok(Self { key })
    }

    pub fn seal(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Generate a unique nonce.
        let nonce = generate_nonce();

        // Compute the total data length for `nonce + serialized data + tag`.
        let mut out = Vec::with_capacity(aead::NONCE_LEN + data.len() + aead::MAX_TAG_LEN);

        // Write nonce bytes.
        out.extend_from_slice(&nonce);

        // Write data.
        out.extend_from_slice(&data);

        // Encrypt the data.
        let tag = self.key.seal_in_place_separate_tag(
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
            Ok(self.key.open_in_place(
                aead::Nonce::try_assume_unique_for_key(&data[..aead::NONCE_LEN])?,
                aead::Aad::empty(),
                &mut data[aead::NONCE_LEN..],
            )?)
        }
    }
}

fn generate_nonce() -> [u8; aead::NONCE_LEN] {
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
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption() {
        let auth1 = Encryption::new(b"secret1").unwrap();
        let auth2 = Encryption::new(b"secret2").unwrap();

        let data = vec![1u8, 2, 3, 4, 5];
        let encrypted = auth1.seal(&data).unwrap();

        let mut encrypted1 = encrypted.clone();
        let decrypted = auth1.open_in_place(&mut encrypted1).unwrap();

        assert_eq!(data, decrypted);

        let mut encrypted2 = encrypted;
        let decrypted = auth2.open_in_place(&mut encrypted2);

        assert!(decrypted.is_err());
    }
}
