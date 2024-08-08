use {
    super::{keyspace::Keyspace, node, Cluster},
    crate::cluster,
    std::{error::Error as StdError, future::Future, sync::Arc},
};

pub type NodeId<C> = node::Id<<C as Consensus>::Node>;
pub type Node<C> = <C as Consensus>::Node;

pub trait Consensus: Clone + Send + Sync + 'static {
    type Node: cluster::Node;
    type Keyspace: Keyspace<Self::Node>;

    type Error: TryInto<cluster::Error, Error = Self::Error> + StdError + Send + Sync + 'static;

    fn add_node(&self, node: &Self::Node) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn complete_pull(
        &self,
        node_id: &node::Id<Self::Node>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn shutdown_node(
        &self,
        id: &node::Id<Self::Node>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn startup_node(
        &self,
        node: &Self::Node,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn decommission_node(
        &self,
        id: &node::Id<Self::Node>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn cluster(&self) -> Arc<Cluster<Self::Node, Self::Keyspace>>;

    fn cluster_view(&self) -> &cluster::View<Self::Node, Self::Keyspace>;
}
