#![allow(clippy::manual_async_fn)]

use {
    irn_rpc as rpc,
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, time::Duration},
};
pub use {
    irn_rpc::{identity, Multiaddr, PeerId},
    snap,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

const RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("admin_api");

type GetClusterView = rpc::Unary<{ rpc::id(b"get_cluster_view") }, (), ClusterView>;
type GetNodeStatus =
    rpc::Unary<{ rpc::id(b"get_node_status") }, (), Result<NodeStatus, GetNodeStatusError>>;

type DecommissionNode = rpc::Unary<
    { rpc::id(b"decom_node") },
    DecommissionNodeRequest,
    Result<(), DecommissionNodeError>,
>;

type GetMemoryProfile = rpc::Unary<
    { rpc::id(b"mem_profile") },
    MemoryProfileRequest,
    Result<MemoryProfile, MemoryProfileError>,
>;

pub const MEMORY_PROFILE_MAX_DURATION: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterView {
    pub nodes: HashMap<PeerId, Node>,
    pub cluster_version: u128,
    pub keyspace_version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: PeerId,
    pub state: NodeState,
    pub addr: Multiaddr,
    pub region: NodeRegion,
    pub organization: String,
    pub eth_address: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRegion {
    Eu,
    Us,
    Ap,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Pulling,
    Normal,
    Restarting,
    Decommissioning,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_version: u64,
    pub eth_address: Option<String>,
    pub stake_amount: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetNodeStatusError {
    /// Node status provider is not available.
    NotAvailable,

    /// Internal server error.
    Internal(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecommissionNodeRequest {
    pub id: PeerId,
    pub force: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DecommissionNodeError {
    /// Node is pulling data and can not be decommissioned yet.
    Pulling,

    /// Node is restarting and should be started before it can be
    /// decommissioned.
    Restarting,

    /// Consensus error.
    Consensus(String),

    /// The node serving the Admin API is not allowed to decommission the
    /// requested node.
    NotAllowed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryProfileRequest {
    pub duration: Duration,
}

#[derive(thiserror::Error, Clone, Debug, Serialize, Deserialize)]
pub enum MemoryProfileError {
    #[error("Memory profiler is already running")]
    AlreadyProfiling,

    #[error("Memory profiling is not available")]
    ProfilerNotAvailable,

    #[error("Failed to compress memory profile")]
    Compression,

    #[error("Invalid memory profile duration")]
    Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryProfile {
    pub dhat: Vec<u8>,
}
