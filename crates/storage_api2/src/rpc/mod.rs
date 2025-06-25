use {
    crate::{Bytes, EntryExpiration, EntryVersion, Field, Key, Value},
    derive_more::derive::TryFrom,
    serde::{Deserialize, Serialize},
    std::marker::PhantomData,
    wcn_rpc::{self as rpc, Api, ApiName, MessageV2, PostcardCodec, StaticMessage},
};

#[cfg(feature = "rpc_client")]
mod client;
#[cfg(feature = "rpc_server")]
mod server;

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

#[derive(Clone, Copy, Debug)]
pub struct StorageApi<Kind>(PhantomData<Kind>);

pub mod api_kind {
    #[derive(Clone, Copy, Debug)]
    pub struct Coordinator;

    #[derive(Clone, Copy, Debug)]
    pub struct Replica;

    #[derive(Clone, Copy, Debug)]
    pub struct Database;
}

pub type StorageApiCoordinator = StorageApi<api_kind::Coordinator>;

impl Api for StorageApiCoordinator {
    const NAME: ApiName = ApiName::new("StorageApiCoordinator");
    type RpcId = Id;
}

pub type StorageApiReplica = StorageApi<api_kind::Replica>;

impl Api for StorageApiReplica {
    const NAME: ApiName = ApiName::new("StorageApiReplica");
    type RpcId = Id;
}

pub type StorageApiDatabase = StorageApi<api_kind::Database>;

impl Api for StorageApiDatabase {
    const NAME: ApiName = ApiName::new("StorageApiDatabase");
    type RpcId = Id;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Namespace {
    node_operator_id: [u8; 20],
    id: u8,
}

type UnaryRpc<const ID: u8, Req, Resp> = rpc::UnaryV2<ID, Req, Resp, PostcardCodec>;

type Get = UnaryRpc<{ Id::Get as u8 }, GetRequest, Result<Option<GetResponse>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetRequest {
    namespace: Namespace,
    // key: Bytes<'static>,
    keyspace_version: Option<u64>,
}

impl Rpc for Get {
    const ID: u8 = { Id::Get as u8 };
    type Request<'a> = GetRequest<'a>;
    type Response<'a> = GetResponse<'a>;
    type Codec = PostcardCodec;
}

impl StaticMessage for GetRequest {}

// impl MessageV2 for GetRequest<'static> {
//     type Borrowed<'a> = GetRequest<'a>;
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetResponse {
    namespace: Namespace,
    value: Bytes<'static>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl StaticMessage for GetResponse {}

type Set = UnaryRpc<{ Id::Set as u8 }, SetRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    value: Bytes<'a>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for SetRequest<'static> {
    type Borrowed<'a> = SetRequest<'a>;
}

type Del = UnaryRpc<{ Id::Del as u8 }, DelRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DelRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for DelRequest<'static> {
    type Borrowed<'a> = DelRequest<'a>;
}

type GetExp =
    UnaryRpc<{ Id::GetExp as u8 }, GetExpRequest<'static>, Result<Option<GetExpResponse>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,

    keyspace_version: Option<u64>,
}

impl MessageV2 for GetExpRequest<'static> {
    type Borrowed<'a> = GetExpRequest<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetExpResponse {
    expiration: UnixTimestampSecs,
}

impl StaticMessage for GetExpResponse {}

type SetExp = UnaryRpc<{ Id::SetExp as u8 }, SetExpRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SetExpRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for SetExpRequest<'static> {
    type Borrowed<'a> = DelRequest<'a>;
}

type HGet = UnaryRpc<{ Id::HGet as u8 }, HGetRequest<'static>, Result<Option<HGetResponse>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    field: Bytes<'a>,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HGetRequest<'static> {
    type Borrowed<'a> = HGetRequest<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetResponse {
    value: Bytes<'static>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

impl StaticMessage for HGetResponse {}

type HSet = UnaryRpc<{ Id::HSet as u8 }, HSetRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    field: Bytes<'a>,
    value: Bytes<'a>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HSetRequest<'static> {
    type Borrowed<'a> = HSetRequest<'a>;
}

type HDel = UnaryRpc<{ Id::HDel as u8 }, HDelRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HDelRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    field: Bytes<'a>,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HDelRequest<'static> {
    type Borrowed<'a> = HDelRequest<'a>;
}

type HGetExp =
    UnaryRpc<{ Id::HGetExp as u8 }, HGetExpRequest<'static>, Result<Option<HGetExpResponse>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    field: Bytes<'a>,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HGetExpRequest<'static> {
    type Borrowed<'a> = HGetExpRequest<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HGetExpResponse {
    expiration: UnixTimestampSecs,
}

impl StaticMessage for HGetExpResponse {}

type HSetExp = UnaryRpc<{ Id::HSetExp as u8 }, HSetExpRequest<'static>, Result<()>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HSetExpRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    field: Bytes<'a>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HSetExpRequest<'static> {
    type Borrowed<'a> = HSetExpRequest<'a>;
}

type HCard = UnaryRpc<{ Id::HCard as u8 }, HCardRequest<'static>, Result<HCardResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HCardRequest<'static> {
    type Borrowed<'a> = HCardRequest<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HCardResponse {
    cardinality: u64,
}

impl StaticMessage for HCardResponse {}

type HScan = UnaryRpc<{ Id::HScan as u8 }, HScanRequest<'static>, Result<HScanResponse>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanRequest<'a> {
    namespace: Namespace,
    key: Bytes<'a>,
    count: u32,
    cursor: Option<Bytes<'a>>,

    keyspace_version: Option<u64>,
}

impl MessageV2 for HScanRequest<'static> {
    type Borrowed<'a> = HScanRequest<'a>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponse {
    records: Vec<HScanResponseRecord>,
    has_more: bool,
}

impl StaticMessage for HScanResponse {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HScanResponseRecord {
    field: Bytes<'static>,
    value: Bytes<'static>,
    expiration: UnixTimestampSecs,
    version: UnixTimestampMicros,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampSecs(u64);

impl From<EntryExpiration> for UnixTimestampSecs {
    fn from(exp: EntryExpiration) -> Self {
        Self(exp.unix_timestamp_secs)
    }
}

impl From<UnixTimestampSecs> for EntryExpiration {
    fn from(timestamp: UnixTimestampSecs) -> Self {
        Self {
            unix_timestamp_secs: timestamp.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct UnixTimestampMicros(u64);

impl From<EntryVersion> for UnixTimestampMicros {
    fn from(version: EntryVersion) -> Self {
        Self(version.unix_timestamp_micros)
    }
}

impl From<UnixTimestampMicros> for EntryVersion {
    fn from(timestamp: UnixTimestampMicros) -> Self {
        Self {
            unix_timestamp_micros: timestamp.0,
        }
    }
}

impl From<crate::Namespace> for Namespace {
    fn from(ns: crate::Namespace) -> Self {
        Self {
            node_operator_id: ns.node_operator_id,
            id: ns.id,
        }
    }
}

impl From<Namespace> for crate::Namespace {
    fn from(ns: Namespace) -> Self {
        Self {
            node_operator_id: ns.node_operator_id,
            id: ns.id,
        }
    }
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
    Timeout = 3,
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
            ErrorCode::Timeout => ErrorKind::Timeout,
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
            ErrorKind::Timeout => ErrorCode::Timeout,

            ErrorKind::Internal
            | ErrorKind::Transport
            | ErrorKind::WrongOperationOutput
            | ErrorKind::Unknown => ErrorCode::Internal,
        };

        Error::new(code, err.details)
    }
}

impl StaticMessage for Error {}
