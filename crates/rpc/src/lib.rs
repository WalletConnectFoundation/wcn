#![allow(async_fn_in_trait)]
#![allow(clippy::manual_async_fn)]

use {
    derive_more::{derive::TryFrom, Display},
    serde::{Deserialize, Serialize},
    std::{borrow::Cow, fmt::Debug, marker::PhantomData, net::SocketAddr, str::FromStr},
    transport::Codec,
    transport2::Codec as CodecV2,
};
pub use {
    libp2p::{identity, Multiaddr, PeerId},
    transport2::PostcardCodec,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub mod client2;
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub mod server2;
use serde::de::DeserializeOwned;
#[cfg(feature = "server")]
pub use server::{IntoServer, Server};

pub mod middleware;

pub mod quic;
pub mod transport;
mod transport2;

#[cfg(test)]
mod test;

const PROTOCOL_VERSION: u32 = 0;

/// RPC API specification.
///
/// Expected to be implemented on unit types and should not contain data.
///
/// # Example
///
/// ```
/// use wcn_rpc::ApiName;
///
/// struct MyApi;
///
/// enum RpcId {
///     CreateUser = 0,
///     UpdateUser = 1,
///     DeleteUser = 2,
/// }
///
/// impl wcn_rpc::Api for MyApi {
///     const NAME: ApiName = ApiName::new("myApi");
///     type RpcId = RpcId;
/// }
/// ```
pub trait Api: Clone + Send + Sync + 'static {
    /// [`ApiName`] of this [`Api`].
    const NAME: ApiName;

    /// `enum` representation of all RPC IDs of this [`Api`].
    ///
    /// Should be convertible from/into `u8`.
    type RpcId: Copy + Into<u8> + TryFrom<u8>;
}

/// [`Api`] name.
pub type ApiName = ServerName;

/// Specification of a remote procedure call.
///
/// Expected to be implemented on unit types and should not contain any data.
///
/// See [`StreamingV2`] RPC and [`UnaryV2`] RPC.
pub trait RpcV2: Sized + 'static {
    /// ID of this [`Rpc`].
    const ID: u8;

    /// Request type of this [`Rpc`].
    type Request: Message;

    type RequestRef<'a>: Serialize + Unpin + Sync + Send + 'a;

    /// Response type of this [`Rpc`].
    type Response: Message;

    /// Serialization codec of this [`Rpc`].
    type Codec: CodecV2<Self::Request> + CodecV2<Self::Response>;
}

pub(crate) mod sealed {
    pub trait UnaryRpc: super::RpcV2 {}

    impl<const ID: u8, Req, Resp, C> UnaryRpc for super::UnaryV2<ID, Req, Resp, C> where
        Self: super::RpcV2
    {
    }
}

/// Generic [`RpcV2`] implementation.
///
/// Not intended to be used directly by the users of this crate.
///
/// Use [`StreamingV2`] or [`UnaryV2`] instead.
pub struct RpcImpl<const ID: u8, K, Req, Resp, C> {
    _marker: PhantomData<(K, Req, Resp, C)>,
}

impl<const ID: u8, K, Req, Resp, C> RpcV2 for RpcImpl<ID, K, Req, Resp, C>
where
    K: 'static,
    Req: MessageV2,
    Resp: MessageV2,
    C: CodecV2<Req> + CodecV2<Resp>,
{
    const ID: u8 = ID;
    type Request = Req;
    type RequestRef<'a> = &'a Req;
    type Response = Resp;
    type Codec = C;
}

/// Bi-directional streaming [`RpcV2`].
///
/// Client sends a stream of requests and server responds with a stream of
/// responses.
///
/// Use type aliases to conviniently define RPCs of this kind.
pub type StreamingV2<'a, const ID: u8, Req, Resp, C> = RpcImpl<ID, kind::Streaming, Req, Resp, C>;

/// Unary (request-response) [`RpcV2`].
///
/// Client sends a single request and server responds with a single response.
///
/// Use type aliases to conviniently define RPCs of this kind.
pub type UnaryV2<const ID: u8, Req, Resp, C> = RpcImpl<ID, kind::Streaming, Req, Resp, C>;

pub trait MessageRef: Serialize + DeserializeOwned + Unpin + Sync + Send {}

pub trait MessageV2: Serialize + DeserializeOwned + Unpin + Sync + Send + 'static {
    type Borrowed<'a>: Serialize + Unpin + Sync + Send + 'a;
}

// type BorrowedRequest<'a, RPC> = <<RPC as RpcV2>::Request as
// MessageV2>::Borrowed<'a>;

pub trait StaticMessage:
    Serialize + for<'de> Deserialize<'de> + Unpin + Sync + Send + 'static
{
}

impl<Msg> MessageV2 for Msg
where
    Msg: StaticMessage,
{
    type Borrowed<'a> = Self;
}

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

    /// Serialization codec of this [`Rpc`].
    type Codec: Codec;
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
#[derive(Debug, Display, Clone, Copy, Hash, PartialEq, Eq)]
#[display("{}", self.as_str())]
pub struct ServerName([u8; 16]);

impl ServerName {
    /// Creates a new [`ServerName`] from the provided `&'static str`.
    /// Intended to be used in `const` contexts only.
    ///
    /// # Panics
    ///
    /// If the provided string is larger than `16` bytes.
    pub const fn new(s: &'static str) -> Self {
        assert!(s.len() <= 16, "`ServerName` should be <= 16 bytes");

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
pub struct Unary<const ID: Id, Req, Resp, C = transport::PostcardCodec>(
    PhantomData<(Req, Resp, C)>,
);

impl<const ID: Id, Req, Resp> Unary<ID, Req, Resp> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Req: Message, Resp: Message, C: Codec> Rpc for Unary<ID, Req, Resp, C> {
    const ID: Id = ID;

    type Kind = kind::Unary;
    type Request = Req;
    type Response = Resp;
    type Codec = C;
}

/// RPC with bi-directional streaming.
pub struct Streaming<const ID: Id, Req, Resp, C = transport::PostcardCodec>(
    PhantomData<(Req, Resp, C)>,
);

impl<const ID: Id, Req, Resp> Streaming<ID, Req, Resp> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Req: Message, Resp: Message, C: Codec> Rpc for Streaming<ID, Req, Resp, C> {
    const ID: Id = ID;

    type Kind = kind::Streaming;
    type Request = Req;
    type Response = Resp;
    type Codec = C;
}

/// "Fire and forget" RPC, which sends a request and doesn't wait for any
/// response.
pub struct Oneshot<const ID: Id, Msg, C = transport::PostcardCodec>(PhantomData<(Msg, C)>);

impl<const ID: Id, Msg> Oneshot<ID, Msg> {
    pub const ID: Id = ID;
}

impl<const ID: Id, Msg: Message, C: Codec> Rpc for Oneshot<ID, Msg, C> {
    const ID: Id = ID;

    type Kind = kind::Oneshot;
    type Request = Msg;
    type Response = ();
    type Codec = C;
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
    #[cfg(feature = "server")]
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

/// RPC error.
pub struct Error2 {
    /// Error code.
    pub code: u8,

    /// Error description.
    pub description: Option<String>,
}

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

/// Peer address, which includes [`PeerId`] and [`Multiaddr`].
///
/// These can be parsed from a string of the following format:
/// `{peer_id}-{multiaddr}`.
#[derive(Display, Clone, Hash, PartialEq, Eq)]
#[display("{}-{}", self.id, self.addr)]
pub struct PeerAddr {
    pub id: PeerId,
    pub addr: Multiaddr,
}

impl PeerAddr {
    pub fn new(id: PeerId, addr: Multiaddr) -> Self {
        Self { id, addr }
    }

    pub fn quic_socketaddr(&self) -> Result<SocketAddr, quic::InvalidMultiaddrError> {
        quic::multiaddr_to_socketaddr(&self.addr)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerAddressError {
    #[error("Invalid format")]
    Format,

    #[error("Invalid peer ID")]
    Id,

    #[error("Invalid multiaddress")]
    Address,
}

impl FromStr for PeerAddr {
    type Err = PeerAddressError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let Some((id, addr)) = s.split_once("-") else {
            return Err(PeerAddressError::Format);
        };

        let id = PeerId::from_str(id).map_err(|_| PeerAddressError::Id)?;
        let addr = Multiaddr::from_str(addr).map_err(|_| PeerAddressError::Address)?;

        Ok(Self { id, addr })
    }
}

impl Debug for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.to_string(), f)
    }
}

#[derive(Clone, Copy, Debug, TryFrom)]
#[try_from(repr)]
#[repr(i32)]
enum ConnectionStatusCode {
    Ok = 0,

    UnsupportedProtocol = -1,
    UnknownApi = -2,
    Unauthorized = -3,
}

impl<T, E> MessageV2 for Result<T, E>
where
    T: MessageV2,
    E: MessageV2,
{
    type Borrowed<'a> = Result<T::Borrowed<'a>, E::Borrowed<'a>>;
}

impl<T> MessageV2 for Option<T>
where
    T: MessageV2,
{
    type Borrowed<'a> = Option<T::Borrowed<'a>>;
}

impl MessageV2 for () {
    type Borrowed<'a> = ();
}
