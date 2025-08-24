use {
    rand::{Rng, SeedableRng as _, rngs::StdRng},
    ring::{aead, hkdf},
    wcn_storage_api::{MapEntry, Record, RecordBorrowed, operation as op},
};

const INFO_ENCRYPTION_KEY: &[u8] = b"encryption_key";
const KEY_SALT: &[u8] = b"wcn_client";
const KEY_LEN_ENCRYPTION: usize = 32;

#[derive(Debug, PartialEq, Eq, thiserror::Error, Clone)]
pub enum Error {
    #[error("Invalid data")]
    Data,

    #[error("Invalid secret")]
    Secret,

    #[error("Failed to encrypt")]
    Encrypt,

    #[error("Failed to decrypt")]
    Decrypt,
}

#[derive(Clone)]
pub struct Key {
    key: aead::LessSafeKey,
}

impl Key {
    pub fn new(secret: &[u8]) -> Result<Self, Error> {
        if secret.is_empty() {
            return Err(Error::Secret);
        }

        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, KEY_SALT).extract(secret);

        let mut encryption_key = [0; KEY_LEN_ENCRYPTION];

        // Both unwraps are safe as the length everywhere is guaranteed to be
        // compatible.
        prk.expand(&[INFO_ENCRYPTION_KEY], &aead::CHACHA20_POLY1305)
            .unwrap()
            .fill(&mut encryption_key)
            .unwrap();

        let key = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, &encryption_key).unwrap();
        let key = aead::LessSafeKey::new(key);

        Ok(Self { key })
    }

    fn seal(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Generate a unique nonce.
        let nonce: [u8; aead::NONCE_LEN] = StdRng::from_rng(&mut rand::rng()).random();

        // Compute the total data length for `nonce + serialized data + tag`.
        let mut out =
            Vec::with_capacity(aead::NONCE_LEN + data.len() + aead::CHACHA20_POLY1305.tag_len());

        // Write nonce bytes.
        out.extend_from_slice(&nonce);

        // Write data.
        out.extend_from_slice(data);

        // Encrypt the data.
        let tag = self
            .key
            .seal_in_place_separate_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::empty(),
                &mut out[aead::NONCE_LEN..],
            )
            .map_err(|_| Error::Encrypt)?;

        // Write tag.
        out.extend_from_slice(tag.as_ref());

        Ok(out)
    }

    fn open_in_place<'in_out>(&self, data: &'in_out mut [u8]) -> Result<&'in_out [u8], Error> {
        if data.is_empty() {
            Ok(data)
        } else if data.len() < aead::NONCE_LEN + aead::CHACHA20_POLY1305.tag_len() + 1 {
            Err(Error::Data)
        } else {
            // Safe unwrap as nonce length is guaranteed to be correct.
            let nonce = aead::Nonce::try_assume_unique_for_key(&data[..aead::NONCE_LEN]).unwrap();

            Ok(self
                .key
                .open_in_place(nonce, aead::Aad::empty(), &mut data[aead::NONCE_LEN..])
                .map_err(|_| Error::Decrypt)?)
        }
    }
}

pub(super) fn decrypt_output(output: &mut op::Output, key: &Key) -> Result<(), Error> {
    match output {
        op::Output::Record(Some(rec)) => {
            let decrypted = key.open_in_place(&mut rec.value)?.into();
            rec.value = decrypted;
        }

        op::Output::MapPage(page) => {
            for entry in &mut page.entries {
                let decrypted = key.open_in_place(&mut entry.record.value)?.into();
                entry.record.value = decrypted;
            }
        }

        _ => {}
    }

    Ok(())
}

pub(super) trait Encrypt {
    type Output;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error>;
}

impl Encrypt for RecordBorrowed<'_> {
    type Output = Record;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(Record {
            value: key.seal(self.value)?,
            expiration: self.expiration,
            version: self.version,
        })
    }
}

impl Encrypt for Record {
    type Output = Self;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(Self {
            value: key.seal(&self.value)?,
            ..self
        })
    }
}

impl Encrypt for op::SetBorrowed<'_> {
    type Output = op::Set;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(op::Set {
            namespace: self.namespace,
            key: self.key.to_owned(),
            record: self.record.encrypt(key)?,
            keyspace_version: self.keyspace_version,
        })
    }
}

impl Encrypt for op::Set {
    type Output = Self;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(Self {
            record: self.record.encrypt(key)?,
            ..self
        })
    }
}

impl Encrypt for op::HSetBorrowed<'_> {
    type Output = op::HSet;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(op::HSet {
            namespace: self.namespace,
            key: self.key.to_owned(),
            entry: MapEntry {
                field: self.entry.field.to_owned(),
                record: self.entry.record.encrypt(key)?,
            },
            keyspace_version: self.keyspace_version,
        })
    }
}

impl Encrypt for op::HSet {
    type Output = Self;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(Self {
            entry: MapEntry {
                field: self.entry.field,
                record: self.entry.record.encrypt(key)?,
            },
            ..self
        })
    }
}

impl Encrypt for op::Operation<'_> {
    type Output = Self;

    fn encrypt(self, key: &Key) -> Result<Self::Output, Error> {
        Ok(match self {
            op::Operation::Owned(op) => match op {
                op::Owned::Set(op) => op::Owned::Set(op.encrypt(key)?).into(),
                op::Owned::HSet(op) => op::Owned::HSet(op.encrypt(key)?).into(),
                _ => op.into(),
            },

            op::Operation::Borrowed(op) => match op {
                op::Borrowed::Set(op) => op::Owned::Set(op.encrypt(key)?).into(),
                op::Borrowed::HSet(op) => op::Owned::HSet(op.encrypt(key)?).into(),
                _ => op.into(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption() {
        let auth1 = Key::new(b"secret1").unwrap();
        let auth2 = Key::new(b"secret2").unwrap();

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
