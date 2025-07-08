#![allow(clippy::manual_async_fn)]

pub use wcn_rpc::{identity, Multiaddr, PeerId};
use {
    serde::{Deserialize, Serialize},
    std::ops::RangeInclusive,
    wcn_rocks::db::migration::ExportItem,
    wcn_rpc::{self as rpc},
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

const RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("migration_api");

/// RPC error codes produced by this module.
mod error_code {
    /// Client is not a cluster member.
    pub const NOT_CLUSTER_MEMBER: &str = "not_cluster_member";

    /// Keyspace versions of the client and the server don't match.
    pub const KEYSPACE_VERSION_MISMATCH: &str = "keyspace_version_mismatch";

    /// Storage export operation failed.
    pub const STORAGE_EXPORT_FAILED: &str = "storage_export_failed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullDataRequest {
    pub keyrange: RangeInclusive<u64>,
    pub keyspace_version: u64,
}

pub type PullData = rpc::Streaming<{ rpc::id(b"pull_data") }, PullDataRequest, ExportItem>;
