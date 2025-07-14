use {
    derive_more::derive::TryFrom,
    serde::{Deserialize, Serialize, de::DeserializeOwned},
    wcn_cluster::{
        Event,
        smart_contract::{Address, ClusterView},
    },
    wcn_rpc::{
        ApiName,
        BorrowedMessage,
        JsonCodec,
        Message,
        client2::{Client, Connection},
    },
};

#[cfg(feature = "rpc_client")]
pub mod client;
#[cfg(feature = "rpc_server")]
pub mod server;

#[derive(Clone, Copy, Debug, TryFrom)]
#[try_from(repr)]
#[repr(u8)]
pub enum Id {
    GetAddress = 0,
    GetClusterView = 1,
    GetEventStream = 2,
}

impl From<Id> for u8 {
    fn from(id: Id) -> Self {
        id as u8
    }
}

#[derive(Clone, Copy)]
pub struct ClusterApi;

impl wcn_rpc::Api for ClusterApi {
    const NAME: ApiName = ApiName::new("Cluster");
    type RpcId = Id;
}

/// RPC [`Client`] of [`ClusterApi`].
pub type Cluster = Client<ClusterApi>;

/// Outbound [`Connection`] to [`ClusterApi`].
pub type ClusterConnection = Connection<ClusterApi>;

type UnaryRpc<const ID: u8, Req, Resp> = wcn_rpc::UnaryV2<ID, Req, Resp, JsonCodec>;
type StreamingRpc<const ID: u8, Req, Resp, Item> =
    wcn_rpc::StreamingV2<ID, Req, Resp, Item, JsonCodec>;

type GetAddress = UnaryRpc<{ Id::GetAddress as u8 }, (), Result<MessageWrapper<Address>>>;
type GetClusterView =
    UnaryRpc<{ Id::GetClusterView as u8 }, (), Result<MessageWrapper<ClusterView>>>;
type GetEventStream =
    StreamingRpc<{ Id::GetEventStream as u8 }, (), Result<()>, Result<MessageWrapper<Event>>>;

#[derive(Clone, Debug, Serialize, Deserialize, Message, thiserror::Error)]
#[error("code = {code}, message = {message:?}")]
struct Error {
    code: u8,
    message: Option<String>,
}

impl Error {
    fn new(code: ErrorCode, details: Option<String>) -> Self {
        Self {
            code: code as u8,
            message: details,
        }
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Self::new(code, None)
    }
}

#[derive(TryFrom)]
#[try_from(repr)]
#[repr(u8)]
enum ErrorCode {
    Internal = 0,
    Unauthorized = 1,
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        use crate::ErrorKind;

        let Ok(code) = ErrorCode::try_from(err.code) else {
            return Self::new(crate::ErrorKind::Unknown)
                .with_message(format!("Unexpected error code: {}", err.code));
        };

        let kind = match code {
            ErrorCode::Unauthorized => ErrorKind::Unauthorized,
            ErrorCode::Internal => ErrorKind::Internal,
        };

        Self {
            kind,
            message: err.message,
        }
    }
}

impl From<crate::Error> for Error {
    fn from(err: crate::Error) -> Self {
        use crate::ErrorKind;

        let code = match err.kind {
            ErrorKind::Unauthorized => ErrorCode::Unauthorized,

            ErrorKind::Internal
            | ErrorKind::Timeout
            | ErrorKind::Transport
            | ErrorKind::Unknown => ErrorCode::Internal,
        };

        Error::new(code, err.message)
    }
}

#[derive(Serialize, Deserialize)]
struct MessageWrapper<T>(pub T);

impl<T> MessageWrapper<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> BorrowedMessage for MessageWrapper<T>
where
    T: Serialize + DeserializeOwned + Unpin + Send + Sync + 'static,
{
    type Owned = Self;

    fn into_owned(self) -> Self::Owned {
        self
    }
}
