#![allow(async_fn_in_trait)]
#![allow(clippy::manual_async_fn)]

pub use libp2p::{identity::Keypair, Multiaddr, PeerId};
use {
    derive_more::Display,
    serde::{Deserialize, Serialize},
    std::marker::PhantomData,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

mod middleware;

pub mod quic;
pub mod transport;

#[cfg(test)]
mod test;

/// RPC identifier derived from a UTF-8 string of up to 16 bytes.
pub type Id = u128;

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

/// RPC with bi-directional streaming.
pub struct Streaming<const ID: Id, Req, Resp>(PhantomData<(Req, Resp)>);

impl<const ID: Id, Req, Resp> Streaming<ID, Req, Resp> {
    pub const ID: Id = ID;
}

/// "Fire and forget" RPC, which sends a request and doesn't wait for any
/// response.
pub struct Oneshot<const ID: Id, Msg>(PhantomData<Msg>);

impl<const ID: Id, Msg> Oneshot<ID, Msg> {
    pub const ID: Id = ID;
}
