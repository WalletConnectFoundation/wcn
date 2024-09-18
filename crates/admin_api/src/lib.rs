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

type DecommissionNode = rpc::Unary<
    { rpc::id(b"decom_node") },
    DecommissionNodeRequest,
    Result<(), DecommissionNodeError>,
>;

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
    /// Specified node is not in the cluster.
    UnknownNode,

    /// Node is already decommissioning.
    Decommissioning,

    /// Node is pulling data and can not be decommissioned yet.
    Pulling,

    /// Node is restarting and should be started before it can be
    /// decommissioned.
    Restarting,

    /// Consensus error.
    Consensus(String),
}
