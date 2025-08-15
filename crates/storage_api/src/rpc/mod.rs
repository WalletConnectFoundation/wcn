use {
    crate::{operation, DataItem, MapPage, Record, RecordExpiration},
    derive_more::derive::{TryFrom, TryInto},
    serde::{Deserialize, Serialize},
    std::{marker::PhantomData, ops::RangeInclusive, time::Duration},
    strum::IntoStaticStr,
    wcn_rpc::{
        metrics::{ErrorResponse, FallibleResponse},
        transport::PostcardCodec,
        ApiName,
        Message,
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
    Get = 0,
    Set = 1,
    Del = 2,
    SetExp = 3,
    GetExp = 4,

    HGet = 5,
    HSet = 6,
    HDel = 7,
    HSetExp = 8,
    HGetExp = 9,
    HCard = 10,
    HScan = 11,

    ReadData = 12,
    WriteData = 13,
}

impl From<Id> for u8 {
    fn from(id: Id) -> Self {
        id as u8
    }
}

/// `wcn_rpc` implementation of [`StorageApi`](super::StorageApi).
#[derive(Clone, Copy, Debug, Default)]
pub struct Api<Kind, S = ()> {
    rpc_timeout: Option<Duration>,
    kind: PhantomData<Kind>,
    state: S,
}

impl<Kind> Api<Kind> {
    /// Creates a new [`Api`].
    pub fn new() -> Self {
        Self {
            rpc_timeout: None,
            kind: PhantomData,
            state: (),
        }
    }

    /// Adds RPC timeout to this [`Api`].
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    /// Adds `state` to this [`Api`].
    pub fn with_state<S>(self, state: S) -> Api<Kind, S> {
        Api {
            rpc_timeout: self.rpc_timeout,
            kind: PhantomData,
            state,
        }
    }
}

impl<Kind, S> Api<Kind, S> {
    fn rpc_timeout_(&self, id: Id) -> Option<Duration> {
        match id {
            Id::Get
            | Id::Set
            | Id::Del
            | Id::SetExp
            | Id::GetExp
            | Id::HGet
            | Id::HSet
            | Id::HDel
            | Id::HSetExp
            | Id::HGetExp
            | Id::HCard
            | Id::HScan => self.rpc_timeout,
            Id::ReadData | Id::WriteData => None,
        }
    }
}

mod api_kind {
    #[derive(Clone, Copy, Debug)]
    pub struct Coordinator;

    #[derive(Clone, Copy, Debug)]
    pub struct Replica;

    #[derive(Clone, Copy, Debug)]
    pub struct Database;
}

pub type CoordinatorApi<S = ()> = Api<api_kind::Coordinator, S>;

impl<S> wcn_rpc::Api for CoordinatorApi<S>
where
    S: Clone + Send + Sync + 'static,
{
    const NAME: ApiName = ApiName::new("Coordinator");
    type RpcId = Id;

    fn rpc_timeout(&self, rpc_id: Id) -> Option<Duration> {
        self.rpc_timeout_(rpc_id)
    }
}

pub type ReplicaApi<S = ()> = Api<api_kind::Replica, S>;

impl<S> wcn_rpc::Api for ReplicaApi<S>
where
    S: Clone + Send + Sync + 'static,
{
    const NAME: ApiName = ApiName::new("Replica");
    type RpcId = Id;

    fn rpc_timeout(&self, rpc_id: Id) -> Option<Duration> {
        self.rpc_timeout_(rpc_id)
    }
}

pub type DatabaseApi<S = ()> = Api<api_kind::Database, S>;

impl<S> wcn_rpc::Api for DatabaseApi<S>
where
    S: Clone + Send + Sync + 'static,
{
    const NAME: ApiName = ApiName::new("Database");
    type RpcId = Id;

    fn rpc_timeout(&self, rpc_id: Id) -> Option<Duration> {
        self.rpc_timeout_(rpc_id)
    }
}

type Rpc<const ID: u8, Req, Resp> = wcn_rpc::RpcImpl<ID, Req, Resp, PostcardCodec>;

type Get = Rpc<{ Id::Get as u8 }, operation::Get, Result<Option<Record>>>;
type Set = Rpc<{ Id::Set as u8 }, operation::Set, Result<()>>;
type Del = Rpc<{ Id::Del as u8 }, operation::Del, Result<()>>;

type GetExp = Rpc<{ Id::GetExp as u8 }, operation::GetExp, Result<Option<RecordExpiration>>>;
type SetExp = Rpc<{ Id::SetExp as u8 }, operation::SetExp, Result<()>>;

type HGet = Rpc<{ Id::HGet as u8 }, operation::HGet, Result<Option<Record>>>;
type HSet = Rpc<{ Id::HSet as u8 }, operation::HSet, Result<()>>;
type HDel = Rpc<{ Id::HDel as u8 }, operation::HDel, Result<()>>;

type HGetExp = Rpc<{ Id::HGetExp as u8 }, operation::HGetExp, Result<Option<RecordExpiration>>>;
type HSetExp = Rpc<{ Id::HSetExp as u8 }, operation::HSetExp, Result<()>>;

type HCard = Rpc<{ Id::HCard as u8 }, operation::HCard, Result<u64>>;
type HScan = Rpc<{ Id::HScan as u8 }, operation::HScan, Result<MapPage>>;

type ReadData = Rpc<{ Id::ReadData as u8 }, ReadDataRequest, ReadDataResponse>;
type WriteData = Rpc<{ Id::WriteData as u8 }, DataItem, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize, Message)]
struct ReadDataRequest {
    keyrange: RangeInclusive<u64>,
    keyspace_version: u64,
}

#[derive(Clone, Debug, TryInto, Serialize, Deserialize, Message)]
enum ReadDataResponse {
    Ack(Result<()>),
    Item(Result<DataItem>),
}

impl FallibleResponse for ReadDataResponse {
    fn error_kind(&self) -> Option<&'static str> {
        match self {
            ReadDataResponse::Ack(res) => res.error_kind(),
            ReadDataResponse::Item(res) => res.error_kind(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Message)]
struct ErrorBorrowed<'a> {
    code: u8,
    message: Option<&'a str>,
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

impl ErrorResponse for Error {
    fn kind(&self) -> &'static str {
        ErrorCode::try_from(self.code)
            .map(Into::into)
            .unwrap_or("Unknown")
    }
}

impl ErrorResponse for ErrorBorrowed<'_> {
    fn kind(&self) -> &'static str {
        ErrorCode::try_from(self.code)
            .map(Into::into)
            .unwrap_or("Unknown")
    }
}

#[derive(TryFrom, IntoStaticStr)]
#[try_from(repr)]
#[repr(u8)]
enum ErrorCode {
    Internal = 0,
    Unauthorized = 1,
    KeyspaceVersionMismatch = 2,
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
            ErrorCode::KeyspaceVersionMismatch => ErrorKind::KeyspaceVersionMismatch,
            ErrorCode::Internal => ErrorKind::Internal,
        };

        Self {
            kind,
            message: err.message,
        }
    }
}

impl From<crate::ErrorKind> for ErrorCode {
    fn from(kind: crate::ErrorKind) -> Self {
        use crate::ErrorKind;

        match kind {
            ErrorKind::Unauthorized => ErrorCode::Unauthorized,
            ErrorKind::KeyspaceVersionMismatch => ErrorCode::KeyspaceVersionMismatch,

            ErrorKind::Internal
            | ErrorKind::Timeout
            | ErrorKind::Transport
            | ErrorKind::Unknown => ErrorCode::Internal,
        }
    }
}

impl From<crate::Error> for Error {
    fn from(err: crate::Error) -> Self {
        Error::new(err.kind.into(), err.message)
    }
}
