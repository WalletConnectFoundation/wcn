#![allow(async_fn_in_trait)]
#![allow(clippy::manual_async_fn)]

pub use libp2p::{identity, Multiaddr, PeerId};
use {
    derive_more::Display,
    serde::{Deserialize, Serialize},
    std::{borrow::Cow, marker::PhantomData},
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

pub mod middleware;

pub mod quic;
pub mod transport;

#[cfg(test)]
mod test;

/// RPC identifier derived from a UTF-8 string of up to 16 bytes.
pub type Id = u128;

/// Remote procedure call.
pub trait Rpc {
    /// [`Id`] of this [`Rpc`].
    const ID: Id;

    /// [`Rpc`] [`kind`].
    type Kind;

    /// Request type of this [`Rpc`].
    type Request: Message;

    /// Response type of this [`Rpc`].
    type Response: Message;
}

/// [`Rpc`] kinds.
pub mod kind {
    /// Unary (request-response) RPC.
    pub struct Unary;

    /// RPC with bi-directional streaming.
    pub struct Streaming;

    /// "Fire and forget" RPC, which sends a request and doesn't wait for any
    /// response.
    pub struct Oneshot;
}

/// Builds [`Id`] from a byte slice.
///
/// Intended to be used in const contexts using `b"my_string"` notation.
///
/// # Panics
///
/// If the provided slice is larger than 16-bytes.
pub const fn id(bytes: &[u8]) -> Id {
    assert!(
        bytes.len() <= 16,
        "rpc::Id should be no longer than 16 bytes"
    );

    // loops aren't supported in const fns
    const fn copy(idx: usize, src: &[u8], mut dst: [u8; 16]) -> [u8; 16] {
        if idx == src.len() {
            return dst;
        }

        dst[idx] = src[idx];
        copy(idx + 1, src, dst)
    }

    u128::from_be_bytes(copy(0, bytes, [0u8; 16]))
}

/// RPC name derived from [`Id`].
#[derive(Clone, Copy, Debug, Display)]
#[display("{}", self.as_str())]
pub struct Name([u8; 16]);

impl Name {
    /// Derives [`Name`] from the provided [`Id`].
    pub const fn new(id: Id) -> Self {
        Self(id.to_be_bytes())
    }

    /// Returns UTF-8 representation of this [`Name`].
    pub const fn as_str(&self) -> &str {
        match std::str::from_utf8(&self.0) {
            Ok(s) => s,
            Err(_) => "invalid",
        }
    }
}

/// Message transimitted over the network.
pub trait Message: Serialize + for<'de> Deserialize<'de> + Unpin + Send {}
impl<M> Message for M where M: Serialize + for<'de> Deserialize<'de> + Unpin + Send {}

/// Unary (request-response) RPC.
pub struct Unary<const ID: Id, Req, Resp>(PhantomData<(Req, Resp)>);

impl<const ID: Id, Req, Resp> Unary<ID, Req, Resp> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Req: Message, Resp: Message> Rpc for Unary<ID, Req, Resp> {
    const ID: Id = ID;

    type Kind = kind::Unary;
    type Request = Req;
    type Response = Resp;
}

/// RPC with bi-directional streaming.
pub struct Streaming<const ID: Id, Req, Resp>(PhantomData<(Req, Resp)>);

impl<const ID: Id, Req, Resp> Streaming<ID, Req, Resp> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Req: Message, Resp: Message> Rpc for Streaming<ID, Req, Resp> {
    const ID: Id = ID;

    type Kind = kind::Streaming;
    type Request = Req;
    type Response = Resp;
}

/// "Fire and forget" RPC, which sends a request and doesn't wait for any
/// response.
pub struct Oneshot<const ID: Id, Msg>(PhantomData<Msg>);

impl<const ID: Id, Msg> Oneshot<ID, Msg> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Msg: Message> Rpc for Oneshot<ID, Msg> {
    const ID: Id = ID;

    type Kind = kind::Oneshot;
    type Request = Msg;
    type Response = ();
}

/// RPC error.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("code: {code}, description: {description:?}")]
pub struct Error {
    /// Error code.
    pub code: Cow<'static, str>,

    /// Error description.
    pub description: Option<Cow<'static, str>>,
}

impl Error {
    /// Creates a new RPC error with the provided error code.
    pub fn new(code: &'static str) -> Self {
        Self {
            code: code.into(),
            description: None,
        }
    }
}

/// RPC result.
pub type Result<T, E = Error> = std::result::Result<T, E>;
