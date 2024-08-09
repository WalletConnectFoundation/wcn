#![allow(clippy::manual_async_fn)]

use {
    derive_more::AsRef,
    serde::{Deserialize, Serialize},
    std::{borrow::Cow, collections::HashSet, io},
};
pub use {
    ed25519_dalek::SigningKey,
    network::{Multiaddr, PeerId as NodeId},
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
pub mod auth;
#[cfg(feature = "server")]
pub mod server;

pub mod rpc {
    use {
        super::{Cardinality, Field, PubsubEventPayload, UnixTimestampSecs, Value},
        crate::Cursor,
        network::rpc,
        serde::{Deserialize, Serialize},
    };

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct StatusResponse {
        pub node_version: u64,
        pub eth_address: Option<String>,
        pub stake_amount: f64,
    }

    type Rpc<const ID: rpc::Id, Op, Out> = rpc::Unary<ID, Op, super::Result<Out>>;

    pub type Get = Rpc<{ rpc::id(b"get") }, super::Get, Option<Value>>;
    pub type Set = Rpc<{ rpc::id(b"set") }, super::Set, ()>;
    pub type Del = Rpc<{ rpc::id(b"del") }, super::Del, ()>;
    pub type GetExp = Rpc<{ rpc::id(b"get_exp") }, super::GetExp, Option<UnixTimestampSecs>>;
    pub type SetExp = Rpc<{ rpc::id(b"set_exp") }, super::SetExp, ()>;

    pub type HGet = Rpc<{ rpc::id(b"hget") }, super::HGet, Option<Value>>;
    pub type HSet = Rpc<{ rpc::id(b"hset") }, super::HSet, ()>;
    pub type HDel = Rpc<{ rpc::id(b"hdel") }, super::HDel, ()>;
    pub type HGetExp = Rpc<{ rpc::id(b"hget_exp") }, super::HGetExp, Option<UnixTimestampSecs>>;
    pub type HSetExp = Rpc<{ rpc::id(b"hset_exp") }, super::HSetExp, ()>;

    pub type HCard = Rpc<{ rpc::id(b"hcard") }, super::HCard, Cardinality>;
    pub type HFields = Rpc<{ rpc::id(b"hfields") }, super::HFields, Vec<Field>>;
    pub type HVals = Rpc<{ rpc::id(b"hvals") }, super::HVals, Vec<Value>>;
    pub type HScan = Rpc<{ rpc::id(b"hscan") }, super::HScan, (Vec<Value>, Option<Cursor>)>;

    pub type Publish = rpc::Oneshot<{ rpc::id(b"publish") }, super::Publish>;
    pub type Subscribe =
        rpc::Streaming<{ rpc::id(b"subscribe") }, super::Subscribe, PubsubEventPayload>;
    pub type Status = Rpc<{ rpc::id(b"status") }, super::Status, StatusResponse>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Key {
    pub namespace: Option<auth::PublicKey>,
    pub bytes: Vec<u8>,
}

pub type Field = Vec<u8>;
pub type Value = Vec<u8>;
pub type Cursor = Vec<u8>;
pub type Cardinality = u64;

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Error {
    #[error("Missing namespace authorization")]
    Unauthorized,

    #[error("Entry not found")]
    NotFound,

    #[error("Too many requests")]
    Throttled,

    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct InternalError {
    pub code: Cow<'static, str>,
    pub message: String,
}

impl InternalError {
    pub fn new(code: impl Into<Cow<'static, str>>, message: impl ToString) -> Self {
        Self {
            code: code.into(),
            message: message.to_string(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type UnixTimestampSecs = u64;

trait Operation {
    const NAME: &'static str;
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Get {
    #[as_ref]
    pub key: Key,
}

impl Operation for Get {
    const NAME: &'static str = "get";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Set {
    #[as_ref]
    pub key: Key,
    pub value: Value,
    pub expiration: Option<UnixTimestampSecs>,
}

impl Operation for Set {
    const NAME: &'static str = "set";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Del {
    #[as_ref]
    pub key: Key,
}

impl Operation for Del {
    const NAME: &'static str = "del";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct GetExp {
    #[as_ref]
    pub key: Key,
}

impl Operation for GetExp {
    const NAME: &'static str = "get_exp";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct SetExp {
    #[as_ref]
    pub key: Key,
    pub expiration: Option<UnixTimestampSecs>,
}

impl Operation for SetExp {
    const NAME: &'static str = "set_exp";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGet {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGet {
    const NAME: &'static str = "hget";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HSet {
    #[as_ref]
    pub key: Key,
    pub field: Field,
    pub value: Value,
    pub expiration: Option<UnixTimestampSecs>,
}

impl Operation for HSet {
    const NAME: &'static str = "hset";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HDel {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HDel {
    const NAME: &'static str = "hdel";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HCard {
    #[as_ref]
    pub key: Key,
}

impl Operation for HCard {
    const NAME: &'static str = "hcard";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HGetExp {
    #[as_ref]
    pub key: Key,
    pub field: Field,
}

impl Operation for HGetExp {
    const NAME: &'static str = "hget_exp";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HSetExp {
    #[as_ref]
    pub key: Key,
    pub field: Field,
    pub expiration: Option<UnixTimestampSecs>,
}

impl Operation for HSetExp {
    const NAME: &'static str = "hset_exp";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HFields {
    #[as_ref]
    pub key: Key,
}

impl Operation for HFields {
    const NAME: &'static str = "hfields";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HVals {
    #[as_ref]
    pub key: Key,
}

impl Operation for HVals {
    const NAME: &'static str = "hvals";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct HScan {
    #[as_ref]
    pub key: Key,
    pub count: u32,
    pub cursor: Option<Cursor>,
}

impl Operation for HScan {
    const NAME: &'static str = "hscan";
}

#[derive(AsRef, Clone, Debug, Serialize, Deserialize)]
pub struct Status;

impl Operation for Status {
    const NAME: &'static str = "status";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publish {
    pub channel: Vec<u8>,
    pub message: Vec<u8>,
}

impl Operation for Publish {
    const NAME: &'static str = "publish";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscribe {
    pub channels: HashSet<Vec<u8>>,
}

impl Operation for Subscribe {
    const NAME: &'static str = "subscribe";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceAuth {
    pub namespace: auth::PublicKey,
    pub signature: auth::Signature,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PubsubEventPayload {
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub auth_nonce: auth::Nonce,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub namespaces: Vec<NamespaceAuth>,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
enum HandshakeError {
    #[error(transparent)]
    Connection(#[from] network::ConnectionError),

    #[error(transparent)]
    Rpc(#[from] network::rpc::Error),
}

impl From<io::Error> for HandshakeError {
    fn from(e: io::Error) -> Self {
        network::rpc::Error::from(e).into()
    }
}
