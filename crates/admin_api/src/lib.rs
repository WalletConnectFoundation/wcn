#![allow(clippy::manual_async_fn)]

pub use irn_rpc::{identity, Multiaddr, PeerId};
use {
    irn_rpc as rpc,
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

type GetClusterView = rpc::Unary<{ rpc::id(b"get_cluster_view") }, (), ClusterView>;
type GetNodeStatus =
    rpc::Unary<{ rpc::id(b"get_node_status") }, (), Result<NodeStatus, GetNodeStatusError>>;

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
