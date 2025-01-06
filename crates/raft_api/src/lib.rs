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
    wcn_rpc as rpc,
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

type AddMember<C> = rpc::Unary<{ rpc::id(b"addMember") }, AddMemberRequest<C>, AddMemberResult<C>>;

type RemoveMember<C> =
    rpc::Unary<{ rpc::id(b"removeMember") }, RemoveMemberRequest<C>, RemoveMemberResult<C>>;

type ProposeChange<C> =
    rpc::Unary<{ rpc::id(b"proposeChange") }, ProposeChangeRequest<C>, ProposeChangeResult<C>>;

type AppendEntries<C> =
    rpc::Unary<{ rpc::id(b"appendEntries") }, AppendEntriesRequest<C>, AppendEntriesResult<C>>;

type InstallSnapshot<C> = rpc::Unary<
    { rpc::id(b"installSnapshot") },
    InstallSnapshotRequest<C>,
    InstallSnapshotResult<C>,
>;

type Vote<C> = rpc::Unary<{ rpc::id(b"vote") }, VoteRequest<C>, VoteResult<C>>;
