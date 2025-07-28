use {
    crate::{operation, MapPage, PullDataItem, Record, RecordExpiration},
    derive_more::derive::{TryFrom, TryInto},
    serde::{Deserialize, Serialize},
    std::{marker::PhantomData, ops::RangeInclusive},
    wcn_rpc::{ApiName, Message, PostcardCodec, RpcV2},
};

#[cfg(feature = "rpc_client")]
pub mod client;
#[cfg(feature = "rpc_server")]
pub mod server;

#[derive(Clone, Copy, Debug, TryFrom)]
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

    PullData = 12,
}

impl From<Id> for u8 {
    fn from(id: Id) -> Self {
        id as u8
    }
}

/// `wcn_rpc` implementation of [`StorageApi`](super::StorageApi).
#[derive(Clone, Copy, Debug)]
pub struct Api<Kind>(PhantomData<Kind>);

mod api_kind {
    #[derive(Clone, Copy, Debug)]
    pub struct Coordinator;

    #[derive(Clone, Copy, Debug)]
    pub struct Replica;

    #[derive(Clone, Copy, Debug)]
    pub struct Database;
}

pub type CoordinatorApi = Api<api_kind::Coordinator>;

impl wcn_rpc::Api for CoordinatorApi {
    const NAME: ApiName = ApiName::new("Coordinator");
    type RpcId = Id;
}

pub type ReplicaApi = Api<api_kind::Replica>;

impl wcn_rpc::Api for ReplicaApi {
    const NAME: ApiName = ApiName::new("Replica");
    type RpcId = Id;
}

pub type DatabaseApi = Api<api_kind::Database>;

impl wcn_rpc::Api for DatabaseApi {
    const NAME: ApiName = ApiName::new("Database");
    type RpcId = Id;
}

type UnaryRpc<const ID: u8, Req, Resp> = wcn_rpc::UnaryV2<ID, Req, Resp, PostcardCodec>;

type Get = UnaryRpc<{ Id::Get as u8 }, operation::Get, Result<Option<Record>>>;
type Set = UnaryRpc<{ Id::Set as u8 }, operation::Set, Result<()>>;
type Del = UnaryRpc<{ Id::Del as u8 }, operation::Del, Result<()>>;

type GetExp = UnaryRpc<{ Id::GetExp as u8 }, operation::GetExp, Result<Option<RecordExpiration>>>;
type SetExp = UnaryRpc<{ Id::SetExp as u8 }, operation::SetExp, Result<()>>;

type HGet = UnaryRpc<{ Id::HGet as u8 }, operation::HGet, Result<Option<Record>>>;
type HSet = UnaryRpc<{ Id::HSet as u8 }, operation::HSet, Result<()>>;
type HDel = UnaryRpc<{ Id::HDel as u8 }, operation::HDel, Result<()>>;

type HGetExp =
    UnaryRpc<{ Id::HGetExp as u8 }, operation::HGetExp, Result<Option<RecordExpiration>>>;
type HSetExp = UnaryRpc<{ Id::HSetExp as u8 }, operation::HSetExp, Result<()>>;

type HCard = UnaryRpc<{ Id::HCard as u8 }, operation::HCard, Result<u64>>;
type HScan = UnaryRpc<{ Id::HScan as u8 }, operation::HScan, Result<MapPage>>;

struct PullData;

impl RpcV2 for PullData {
    const ID: u8 = Id::PullData as u8;
    type Request = PullDataRequest;
    type Response = PullDataResponse;
    type Codec = PostcardCodec;
}

#[derive(Clone, Debug, Serialize, Deserialize, Message)]
struct PullDataRequest {
    keyrange: RangeInclusive<u64>,
    keyspace_version: u64,
}

#[derive(Clone, Debug, TryInto, Serialize, Deserialize, Message)]
enum PullDataResponse {
    Ack(Result<()>),
    Item(Result<PullDataItem>),
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

#[derive(TryFrom)]
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
