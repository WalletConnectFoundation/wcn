pub use {domain::*, wcn::cluster::Error};

pub type Cluster = wcn::Cluster<Node, Keyspace>;
pub type Snapshot<'a> = cluster::Snapshot<'a, Node, Keyspace>;
pub type Viewable = cluster::Viewable<Node, Keyspace>;
pub type View = cluster::View<Node, Keyspace>;
pub type NodeState = cluster::node::State<Node>;
