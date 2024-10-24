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
pub use server::{IntoServer, Server};

pub mod middleware;

pub mod quic;
pub mod transport;

#[cfg(test)]
mod test;

/// Error codes produced by this module.
pub mod error_code {
    #[allow(unused_imports)]
    use super::middleware;

    /// RPC server is throttling.
    pub const THROTTLED: &str = "throttled";

    /// Error code of [`middleware::WithTimeouts`].
    pub const TIMEOUT: &str = "timeout";
}

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

    u128::from_be_bytes(copy_slice_recursive(0, bytes, [0u8; 16]))
}

// loops aren't supported in const fns
const fn copy_slice_recursive(idx: usize, src: &[u8], mut dst: [u8; 16]) -> [u8; 16] {
    if idx == src.len() {
        return dst;
    }

    dst[idx] = src[idx];
    copy_slice_recursive(idx + 1, src, dst)
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

/// RPC server name.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ServerName([u8; 16]);

impl ServerName {
    /// Creates a new [`ServerName`] from the provided `&'static str`.
    /// Intended to be used in `const` contexts only.
    ///
    /// # Panics
    ///
    /// If the provided string is larger than `16` bytes.
    pub const fn new(s: &'static str) -> Self {
        assert!(!(s.len() > 16), "`ServiceName` should be <= 16 bytes");

        Self(copy_slice_recursive(0, s.as_bytes(), [0u8; 16]))
    }

    /// Returns UTF-8 representation of this [`ServerName`].
    pub const fn as_str(&self) -> &str {
        match std::str::from_utf8(&self.0) {
            Ok(s) => s,
            Err(_) => "invalid",
        }
    }
}

/// Message transimitted over the network.
pub trait Message: Serialize + for<'de> Deserialize<'de> + Unpin + Sync + Send + 'static {}
impl<M> Message for M where M: Serialize + for<'de> Deserialize<'de> + Unpin + Sync + Send + 'static {}

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
    const THROTTLED: Self = Self::new(error_code::THROTTLED);

    /// Creates a new RPC error with the provided error code.
    pub const fn new(code: &'static str) -> Self {
        Self {
            code: Cow::Borrowed(code),
            description: None,
        }
    }
}

/// RPC result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// Workaround for this compliler bug: https://github.com/rust-lang/rust/issues/100013
// https://github.com/rust-lang/rust/issues/100013#issuecomment-2210995259
// TODO: remove when fixed
trait ForceSendFuture: core::future::Future {
    fn force_send_impl(self) -> impl core::future::Future<Output = Self::Output> + Send
    where
        Self: Sized + Send,
    {
        self
    }
}

impl<T: core::future::Future> ForceSendFuture for T {}
