#![allow(clippy::manual_async_fn)]

pub use wcn_rpc::{identity, Multiaddr, PeerId};
use {
    raft::{
        AddMemberRequest,
        AddMemberResult,
        AppendEntriesRequest,
        AppendEntriesResult,
        InstallSnapshotRequest,
        InstallSnapshotResult,
        ProposeChangeRequest,
        ProposeChangeResult,
        RemoveMemberRequest,
        RemoveMemberResult,
        VoteRequest,
        VoteResult,
    },
    wcn_rpc::{self as rpc, transport::JsonCodec},
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

const RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("raftApi");

type Rpc<const ID: rpc::Id, Req, Resp> = rpc::Unary<ID, Req, Resp, JsonCodec>;

type AddMember<C> =
    rpc::Unary<{ rpc::id(b"addMember") }, AddMemberRequest<C>, AddMemberResult<C>, JsonCodec>;

type RemoveMember<C> =
    Rpc<{ rpc::id(b"removeMember") }, RemoveMemberRequest<C>, RemoveMemberResult<C>>;

type ProposeChange<C> =
    Rpc<{ rpc::id(b"proposeChange") }, ProposeChangeRequest<C>, ProposeChangeResult<C>>;

type AppendEntries<C> =
    Rpc<{ rpc::id(b"appendEntries") }, AppendEntriesRequest<C>, AppendEntriesResult<C>>;

type InstallSnapshot<C> =
    Rpc<{ rpc::id(b"installSnapshot") }, InstallSnapshotRequest<C>, InstallSnapshotResult<C>>;

type Vote<C> = Rpc<{ rpc::id(b"vote") }, VoteRequest<C>, VoteResult<C>>;
