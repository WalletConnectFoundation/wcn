use {
    derive_more::derive::{TryFrom, TryInto},
    serde::{Deserialize, Serialize, de::DeserializeOwned},
    std::time::Duration,
    strum::IntoStaticStr,
    wcn_cluster::{
        Event,
        smart_contract::{Address, ClusterView},
    },
    wcn_rpc::{
        ApiName,
        BorrowedMessage,
        Message,
        RpcImpl,
        metrics::FallibleResponse,
        transport::JsonCodec,
    },
};

#[cfg(feature = "rpc_client")]
pub mod client;
#[cfg(feature = "rpc_server")]
pub mod server;

#[derive(Clone, Copy, Debug, TryFrom, IntoStaticStr)]
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

#[derive(Clone, Copy, Default)]
pub struct ClusterApi<S = ()> {
    rpc_timeout: Option<Duration>,
    state: S,
}

impl ClusterApi {
    /// Creates a new [`ClusterApi`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds RPC timeout to this [`ClusterApi`].
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    /// Adds `state` to this [`ClusterApi`].
    pub fn with_state<S>(self, state: S) -> ClusterApi<S> {
        ClusterApi {
            rpc_timeout: self.rpc_timeout,
            state,
        }
    }
}

impl<S> wcn_rpc::Api for ClusterApi<S>
where
    S: Clone + Send + Sync + 'static,
{
    const NAME: ApiName = ApiName::new("Cluster");
    type RpcId = Id;

    fn rpc_timeout(&self, rpc_id: Id) -> Option<Duration> {
        match rpc_id {
            Id::GetAddress | Id::GetClusterView => self.rpc_timeout,
            Id::GetEventStream => None,
        }
    }
}

type Rpc<const ID: u8, Req, Resp> = RpcImpl<ID, Req, Resp, JsonCodec>;

type GetAddress = Rpc<{ Id::GetAddress as u8 }, (), Result<MessageWrapper<Address>>>;

type GetClusterView = Rpc<{ Id::GetClusterView as u8 }, (), Result<MessageWrapper<ClusterView>>>;

type GetEventStream = Rpc<{ Id::GetEventStream as u8 }, (), GetEventStreamItem>;

#[derive(Debug, TryInto, Serialize, Deserialize, Message)]
enum GetEventStreamItem {
    Ack(Result<()>),
    Event(Result<Event>),
}

impl FallibleResponse for GetEventStreamItem {
    fn error_kind(&self) -> Option<&'static str> {
        match self {
            Self::Ack(resp) => resp.error_kind(),
            Self::Event(resp) => resp.error_kind(),
        }
    }
}

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

    #[cfg(feature = "rpc_server")]
    fn internal(err: impl ToString) -> Self {
        Self::new(ErrorCode::Internal, Some(err.to_string()))
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Self::new(code, None)
    }
}

#[derive(TryFrom, IntoStaticStr)]
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

impl wcn_rpc::metrics::ErrorResponse for Error {
    fn kind(&self) -> &'static str {
        ErrorCode::try_from(self.code)
            .map(Into::into)
            .unwrap_or("Unknown")
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
