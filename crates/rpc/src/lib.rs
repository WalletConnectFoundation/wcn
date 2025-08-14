pub use wcn_rpc_derive::Message;
use {
    derive_more::Display,
    serde::{de::DeserializeOwned, Serialize},
    std::{borrow::Cow, fmt::Debug, marker::PhantomData, time::Duration},
    transport::Codec,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
mod connection_filter;
#[cfg(feature = "server")]
pub mod server;

pub mod metrics;
pub mod quic;
pub mod transport;

const PROTOCOL_VERSION: u32 = 0;

/// RPC API specification.
pub trait Api: Clone + Send + Sync + 'static {
    /// [`ApiName`] of this [`Api`].
    const NAME: ApiName;

    /// `enum` representation of all RPC IDs of this [`Api`].
    type RpcId: Id;

    /// Returns the timeout to use for the specified RPC.
    fn rpc_timeout(&self, rpc_id: Self::RpcId) -> Option<Duration>;
}

/// [`Rpc`] ID.
pub trait Id: Copy + Into<u8> + TryFrom<u8> + Into<&'static str> + Send + Sync + 'static {}

impl<ID> Id for ID where
    ID: Copy + Into<u8> + TryFrom<u8> + Into<&'static str> + Send + Sync + 'static
{
}

/// Remote procedure call.
pub trait Rpc: Sized + Send + Sync + 'static {
    /// ID of this [`Rpc`].
    const ID: u8;

    /// Request type of this [`Rpc`].
    type Request: Message;

    /// Response type of this [`Rpc`].
    type Response: Message + metrics::FallibleResponse;

    /// Serialization codec of this [`Rpc`].
    type Codec: Codec<Self::Request> + Codec<Self::Response>;
}

/// [`RpcV2::Request`].
pub type Request<RPC> = <RPC as Rpc>::Request;

/// [`RpcV2::Response`].
pub type Response<RPC> = <RPC as Rpc>::Response;

/// Default implementation of [`UnaryRpc`].
pub struct RpcImpl<const ID: u8, Req: Message, Resp: Message, C: Codec<Req> + Codec<Resp>> {
    _marker: PhantomData<(Req, Resp, C)>,
}

impl<const ID: u8, Req, Resp, C> Rpc for RpcImpl<ID, Req, Resp, C>
where
    Req: Message,
    Resp: Message + metrics::FallibleResponse,
    C: Codec<Req> + Codec<Resp>,
{
    const ID: u8 = ID;
    type Request = Req;
    type Response = Resp;
    type Codec = C;
}

/// RPC message.
pub trait Message: DeserializeOwned + BorrowedMessage<Owned = Self> + 'static {}

impl<T> Message for T where T: DeserializeOwned + BorrowedMessage<Owned = Self> + 'static {}

/// Borrowed [Message][`MessageV2`].
pub trait BorrowedMessage: Serialize + Unpin + Sync + Send {
    type Owned: Message;

    fn into_owned(self) -> Self::Owned;
}

// loops aren't supported in const fns
const fn copy_slice_recursive(idx: usize, src: &[u8], mut dst: [u8; 16]) -> [u8; 16] {
    if idx == src.len() {
        return dst;
    }

    dst[idx] = src[idx];
    copy_slice_recursive(idx + 1, src, dst)
}

/// RPC server name.
#[derive(Debug, Display, Clone, Copy, Hash, PartialEq, Eq)]
#[display("{}", self.as_str())]
pub struct ApiName([u8; 16]);

impl ApiName {
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

#[derive(Clone, Copy, Debug)]
enum ConnectionStatus {
    Ok,

    UnsupportedProtocol,
    UnknownApi,
    Unauthorized,

    Rejected { reason: u8 },
}

impl ConnectionStatus {
    fn try_from(code: i32) -> Option<Self> {
        Some(match code {
            0 => Self::Ok,

            -1 => Self::UnsupportedProtocol,
            -2 => Self::UnknownApi,
            -3 => Self::Unauthorized,

            -1255..=-1000 => Self::Rejected {
                reason: (-code - 1000) as u8,
            },

            _ => return None,
        })
    }

    fn code(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::UnsupportedProtocol => -1,
            Self::UnknownApi => -2,
            Self::Unauthorized => -2,
            Self::Rejected { reason } => -(reason as i32) - 1000,
        }
    }
}

impl<T, E> BorrowedMessage for Result<T, E>
where
    T: BorrowedMessage,
    E: BorrowedMessage,
{
    type Owned = Result<T::Owned, E::Owned>;

    fn into_owned(self) -> Self::Owned {
        match self {
            Ok(ok) => Ok(ok.into_owned()),
            Err(err) => Err(err.into_owned()),
        }
    }
}

impl<T> BorrowedMessage for Option<T>
where
    T: BorrowedMessage,
{
    type Owned = Option<T::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(BorrowedMessage::into_owned)
    }
}

impl<T: Clone + BorrowedMessage> BorrowedMessage for &[T] {
    type Owned = Vec<T::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.iter()
            .cloned()
            .map(BorrowedMessage::into_owned)
            .collect()
    }
}

impl<T: Message> BorrowedMessage for Vec<T> {
    type Owned = Vec<T>;

    fn into_owned(self) -> Self::Owned {
        self
    }
}

impl<T: Clone + BorrowedMessage> BorrowedMessage for &T {
    type Owned = T::Owned;

    fn into_owned(self) -> Self::Owned {
        self.clone().into_owned()
    }
}

impl BorrowedMessage for &str {
    type Owned = String;

    fn into_owned(self) -> Self::Owned {
        self.to_string()
    }
}

impl<T: Clone + BorrowedMessage> BorrowedMessage for Cow<'_, T> {
    type Owned = T::Owned;

    fn into_owned(self) -> T::Owned {
        BorrowedMessage::into_owned(Cow::into_owned(self))
    }
}

macro_rules! impl_borrowed_message {
    ( $( $t:ty ),* $(,)? ) => {
        $(
            impl BorrowedMessage for $t {
                type Owned = $t;

                fn into_owned(self) -> Self::Owned {
                    self
                }
            }
        )*
    };
}

impl_borrowed_message!((), bool, char);
impl_borrowed_message!(u8, u16, u32, u64, u128);
impl_borrowed_message!(i8, i16, i32, i64, i128);
impl_borrowed_message!(f32, f64);
impl_borrowed_message!(String);
