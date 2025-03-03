//! Consensus functionality required for managing WCN cluster state.

use {
    super::{keyspace::Keyspace, node, Cluster},
    crate::cluster,
    std::{error::Error as StdError, future::Future, sync::Arc},
};

/// [`Consensus`] [`node::Id`].
pub type NodeId<C> = node::Id<<C as Consensus>::Node>;

/// [`Consensus::Node`].
pub type Node<C> = <C as Consensus>::Node;

/// Consensus functionality required for managing a [`Cluster`] state.
///
/// The implementor of this trait is expected to wrap a [`Cluster`] state, and
/// to guarantee it's consistency across all the nodes in the network.
pub trait Consensus: Clone + Send + Sync + 'static {
    /// [`cluster::Node`] within the underlying [`Cluster`].
    type Node: cluster::Node;

    /// [`Keyspace`] within the underlying [`Cluster`].
    type Keyspace: Keyspace<Self::Node>;

    /// Error of consistent [`Cluster`] operations.
    type Error: TryInto<cluster::Error, Error = Self::Error> + StdError + Send + Sync + 'static;

    /// Proxy to [`Cluster::add_node`].
    fn add_node(&self, node: &Self::Node) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Proxy to [`Cluster::complete_pull`].
    fn complete_pull(
        &self,
        node_id: &node::Id<Self::Node>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Proxy to [`Cluster::shutdown_node`].
    fn shutdown_node(
        &self,
        id: &node::Id<Self::Node>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Proxy to [`Cluster::startup_node`].
    fn startup_node(
        &self,
        node: &Self::Node,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Proxy to [`Cluster::decommission_node`].
    fn decommission_node(
        &self,
        id: &node::Id<Self::Node>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns a read-only copy of the underlying [`Cluster`].
    fn cluster(&self) -> Arc<Cluster<Self::Node, Self::Keyspace>>;

    /// Returns a [`cluster::View`] of the underlying [`Cluster`].
    fn cluster_view(&self) -> &cluster::View<Self::Node, Self::Keyspace>;
}
