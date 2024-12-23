use {
    wcn_rpc::{
        identity::ed25519::{Keypair as Ed25519Keypair, PublicKey as Ed25519PublicKey},
        PeerId,
    },
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::time::Duration,
};

const JWT_VALIDATION_TIME_LEEWAY_SECS: i64 = 120;

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    #[error("JWT decoding failed")]
    Decoding,

    #[error("JWT encoding failed")]
    Encoding,

    #[error("JWT signing failed")]
    Signing,

    #[error("Invalid JWT header")]
    Header,

    #[error("Invalid JWT signature")]
    Signature,

    #[error("Invalid namespace signature")]
    NamespaceSignature,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamespaceAuth {
    pub namespace: super::PublicKey,
    pub signature: super::Signature,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub purpose: Purpose,
    pub duration: Option<Duration>,
    pub namespaces: Vec<NamespaceAuth>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Storage,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Token(String);

impl Token {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn decode(&self) -> Result<Claims, Error> {
        let mut split_iter = self.0.rsplitn(2, '.');

        let (Some(signature), Some(message)) = (split_iter.next(), split_iter.next()) else {
            return Err(Error::Decoding);
        };

        let mut split_iter = message.split('.');

        let (Some(header), Some(claims), None) =
            (split_iter.next(), split_iter.next(), split_iter.next())
        else {
            return Err(Error::Decoding);
        };

        let header = decode::<Header>(header.as_bytes())?;
        let claims = decode::<Claims>(claims.as_bytes())?;
        let signature = data_encoding::BASE64URL_NOPAD
            .decode(signature.as_bytes())
            .map_err(|_| Error::Signature)?;

        if !header.is_valid() {
            return Err(Error::Header);
        }

        if !claims.iss.0.verify(message.as_bytes(), &signature) {
            return Err(Error::Signature);
        }

        Ok(claims)
    }
}

/// Wrapper for [`Ed25519Keypair`] to serialize it as base64-encoded string for
/// use in JSON token.
#[derive(Clone, Debug)]
pub struct PublicKey(Ed25519PublicKey);

impl From<super::PublicKey> for PublicKey {
    fn from(value: super::PublicKey) -> Self {
        // Safe unwrap, since the keys are the same length.
        Self(Ed25519PublicKey::try_from_bytes(value.as_bytes()).unwrap())
    }
}

impl From<Ed25519PublicKey> for PublicKey {
    fn from(value: Ed25519PublicKey) -> Self {
        Self(value)
    }
}

impl From<PublicKey> for super::PublicKey {
    fn from(value: PublicKey) -> Self {
        value.inner().to_bytes().into()
    }
}

impl PublicKey {
    pub fn inner(&self) -> &Ed25519PublicKey {
        &self.0
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = self.0.to_bytes();
        let encoded = data_encoding::BASE64_NOPAD.encode(&encoded);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let decoded = data_encoding::BASE64_NOPAD
            .decode(encoded.as_bytes())
            .map_err(|_| serde::de::Error::custom("invalid key encoding"))?;

        Ed25519PublicKey::try_from_bytes(&decoded)
            .map(PublicKey)
            .map_err(|_| serde::de::Error::custom("invalid key encoding"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub aud: String,
    pub sub: PeerId,
    pub iss: PublicKey,
    pub api: Purpose,
    pub iat: i64,
    pub exp: Option<i64>,
    pub nsp: Vec<PublicKey>,
}

impl Claims {
    pub fn network_id(&self) -> &str {
        &self.aud
    }

    pub fn issuer_peer_id(&self) -> PeerId {
        wcn_rpc::identity::PublicKey::from(self.iss.0.clone()).to_peer_id()
    }

    pub fn client_peer_id(&self) -> PeerId {
        self.sub
    }

    pub fn purpose(&self) -> Purpose {
        self.api
    }

    pub fn is_expired(&self) -> bool {
        let time_leeway = JWT_VALIDATION_TIME_LEEWAY_SECS;
        let now = chrono::Utc::now().timestamp();

        if matches!(self.exp, Some(exp) if now - time_leeway > exp) {
            true
        } else {
            now + time_leeway < self.iat
        }
    }

    pub fn namespaces(&self) -> Vec<super::PublicKey> {
        self.nsp
            .iter()
            .map(|key| key.inner().to_bytes().into())
            .collect()
    }

    pub fn encode(&self, keypair: &Ed25519Keypair) -> Result<Token, Error> {
        let header = encode(&Header::default())?;
        let claims = encode(self)?;
        let message = format!("{header}.{claims}");
        let signature = keypair.sign(message.as_bytes());
        let signature = data_encoding::BASE64URL_NOPAD.encode(&signature);

        Ok(Token(format!("{message}.{signature}")))
    }
}

const HEADER_TYP: &str = "JWT";
const HEADER_ALG: &str = "EdDSA";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Header {
    pub typ: String,
    pub alg: String,
}

impl Header {
    fn is_valid(&self) -> bool {
        self.typ == HEADER_TYP && self.alg == HEADER_ALG
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            typ: HEADER_TYP.to_owned(),
            alg: HEADER_ALG.to_owned(),
        }
    }
}

fn encode<T: Serialize>(data: &T) -> Result<String, Error> {
    serde_json::to_string(&data)
        .map(|data| data_encoding::BASE64URL_NOPAD.encode(data.as_bytes()))
        .map_err(|_| Error::Encoding)
}

fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T, Error> {
    let data = data_encoding::BASE64URL_NOPAD
        .decode(data)
        .map_err(|_| Error::Decoding)?;

    serde_json::from_slice(&data).map_err(|_| Error::Decoding)
}

pub fn create_timestamp(offset: Option<Duration>) -> i64 {
    let now = chrono::Utc::now().timestamp();

    if let Some(offset) = offset {
        now + offset.as_secs().try_into().unwrap_or(0)
    } else {
        now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN_AUD: &str = "test_token_aud";

    fn peer_id(public_key: Ed25519PublicKey) -> PeerId {
        wcn_rpc::identity::PublicKey::from(public_key).to_peer_id()
    }

    fn create_claims() -> (Ed25519Keypair, Claims) {
        let keypair = Ed25519Keypair::generate();

        let token = Claims {
            aud: TOKEN_AUD.to_owned(),
            iss: keypair.public().into(),
            sub: peer_id(keypair.public()),
            api: Purpose::Storage,
            iat: super::create_timestamp(None),
            exp: None,
            nsp: Default::default(),
        };

        (keypair, token)
    }

    #[test]
    fn signature() {
        let (key1, claims) = create_claims();
        let key2 = Ed25519Keypair::generate();
        let token1 = claims.encode(&key1).unwrap();
        let token2 = claims.encode(&key2).unwrap();

        assert!(token1.decode().is_ok());
        assert!(matches!(token2.decode(), Err(Error::Signature)));
    }

    #[test]
    fn claims() {
        let (key, claims) = create_claims();
        let token = claims.encode(&key).unwrap();
        let claims = token.decode().unwrap();

        assert_eq!(claims.issuer_peer_id(), peer_id(key.public()));
        assert_eq!(claims.client_peer_id(), peer_id(key.public()));
    }

    #[test]
    fn expiry() {
        // No EXP, current IAT.
        let (key, claims) = create_claims();
        let token = claims.encode(&key).unwrap();
        let claims = token.decode().unwrap();

        assert!(!claims.is_expired());

        // EXP in future, current IAT.
        let (key, mut claims) = create_claims();
        claims.exp = Some(create_timestamp(Some(Duration::from_secs(600))));
        let token = claims.encode(&key).unwrap();
        let claims = token.decode().unwrap();

        assert!(!claims.is_expired());

        // EXP in past.
        let (key, mut claims) = create_claims();
        claims.iat = create_timestamp(None) - 1000;
        claims.exp = Some(claims.iat + 600);
        let token = claims.encode(&key).unwrap();
        let claims = token.decode().unwrap();

        assert!(claims.is_expired());

        // IAT in future.
        let (key, mut claims) = create_claims();
        claims.iat = create_timestamp(None) + 1000;
        let token = claims.encode(&key).unwrap();
        let claims = token.decode().unwrap();

        assert!(claims.is_expired());
    }
}
