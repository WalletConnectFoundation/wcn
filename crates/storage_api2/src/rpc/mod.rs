use {
    crate::{operation, MapPage, Record, RecordExpiration},
    derive_more::derive::TryFrom,
    serde::{Deserialize, Serialize},
    std::marker::PhantomData,
    wcn_rpc::{ApiName, MessageV2 as Message, PostcardCodec},
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

type Get = UnaryRpc<{ Id::Get as u8 }, operation::Get<'static>, Result<Option<Record<'static>>>>;
type Set = UnaryRpc<{ Id::Set as u8 }, operation::Set<'static>, Result<()>>;
type Del = UnaryRpc<{ Id::Del as u8 }, operation::Del<'static>, Result<()>>;

type GetExp =
    UnaryRpc<{ Id::GetExp as u8 }, operation::GetExp<'static>, Result<Option<RecordExpiration>>>;
type SetExp = UnaryRpc<{ Id::SetExp as u8 }, operation::SetExp<'static>, Result<()>>;

type HGet = UnaryRpc<{ Id::HGet as u8 }, operation::HGet<'static>, Result<Option<Record<'static>>>>;
type HSet = UnaryRpc<{ Id::HSet as u8 }, operation::HSet<'static>, Result<()>>;
type HDel = UnaryRpc<{ Id::HDel as u8 }, operation::HDel<'static>, Result<()>>;

type HGetExp =
    UnaryRpc<{ Id::HGetExp as u8 }, operation::HGetExp<'static>, Result<Option<RecordExpiration>>>;
type HSetExp = UnaryRpc<{ Id::HSetExp as u8 }, operation::HSetExp<'static>, Result<()>>;

type HCard = UnaryRpc<{ Id::HCard as u8 }, operation::HCard<'static>, Result<u64>>;
type HScan = UnaryRpc<{ Id::HScan as u8 }, operation::HScan<'static>, Result<MapPage<'static>>>;

impl Message for operation::Get<'static> {
    type Borrowed<'a> = operation::Get<'a>;
}

impl Message for operation::Set<'static> {
    type Borrowed<'a> = operation::Set<'a>;
}

impl Message for operation::Del<'static> {
    type Borrowed<'a> = operation::Del<'a>;
}

impl Message for operation::GetExp<'static> {
    type Borrowed<'a> = operation::GetExp<'a>;
}

impl Message for operation::SetExp<'static> {
    type Borrowed<'a> = operation::SetExp<'a>;
}

impl Message for operation::HSet<'static> {
    type Borrowed<'a> = operation::HSet<'a>;
}

impl Message for operation::HGet<'static> {
    type Borrowed<'a> = operation::HGet<'a>;
}

impl Message for operation::HDel<'static> {
    type Borrowed<'a> = operation::HDel<'a>;
}

impl Message for operation::HGetExp<'static> {
    type Borrowed<'a> = operation::HGetExp<'a>;
}

impl Message for operation::HSetExp<'static> {
    type Borrowed<'a> = operation::HSetExp<'a>;
}

impl Message for operation::HCard<'static> {
    type Borrowed<'a> = operation::HCard<'a>;
}

impl Message for operation::HScan<'static> {
    type Borrowed<'a> = operation::HScan<'a>;
}

impl Message for Record<'static> {
    type Borrowed<'a> = Record<'a>;
}

impl Message for RecordExpiration {
    type Borrowed<'a> = Self;
}

impl Message for MapPage<'static> {
    type Borrowed<'a> = MapPage<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Error {
    code: u8,
    details: Option<String>,
}

impl Error {
    fn new(code: ErrorCode, details: Option<String>) -> Self {
        Self {
            code: code as u8,
            details,
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
            return Self::new(
                crate::ErrorKind::Unknown,
                Some(format!("Unexpected error code: {}", err.code)),
            );
        };

        let kind = match code {
            ErrorCode::Unauthorized => ErrorKind::Unauthorized,
            ErrorCode::KeyspaceVersionMismatch => ErrorKind::KeyspaceVersionMismatch,
            ErrorCode::Internal => ErrorKind::Internal,
        };

        Self::new(kind, err.details)
    }
}

impl From<crate::Error> for Error {
    fn from(err: crate::Error) -> Self {
        use crate::ErrorKind;

        let code = match err.kind {
            ErrorKind::Unauthorized => ErrorCode::Unauthorized,
            ErrorKind::KeyspaceVersionMismatch => ErrorCode::KeyspaceVersionMismatch,

            ErrorKind::Internal
            | ErrorKind::Timeout
            | ErrorKind::Transport
            | ErrorKind::Unknown => ErrorCode::Internal,
        };

        Error::new(code, err.details)
    }
}

impl Message for Error {
    type Borrowed<'a> = Self;
}
