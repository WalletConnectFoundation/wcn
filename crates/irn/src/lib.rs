use {
    irn_api::{client, SigningKey},
    std::str::FromStr,
};

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("Failed to initialize namespace")]
    Namespace,

    #[error("Invalid key encoding: must be base64")]
    KeyEncoding,

    #[error("Invalid key length: must be 32 byte ed25519 private key")]
    KeyLength,

    #[error("Failed to decode parameter: {0}")]
    Decoding(&'static str),

    #[error("Failed to run health check: {0}")]
    Client(#[from] client::Error),

    #[error("Failed to write data to stdout")]
    Io(#[from] std::io::Error),

    #[error("Text encoding must be utf8")]
    TextEncoding,
}

#[derive(Debug, Clone)]
pub struct PrivateKey(pub SigningKey);

impl FromStr for PrivateKey {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::BASE64
            .decode(s.as_bytes())
            .map_err(|_| CliError::KeyEncoding)?[..]
            .try_into()
            .map_err(|_| CliError::KeyLength)?;

        Ok(Self(irn_api::SigningKey::from_bytes(&bytes)))
    }
}
