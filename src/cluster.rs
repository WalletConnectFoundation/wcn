pub use irn::cluster::Error;
use {
    irn::cluster,
    libp2p::{Multiaddr, PeerId},
    serde::{Deserialize, Serialize},
    std::convert::Infallible,
    xxhash_rust::xxh3::Xxh3Builder,
};

pub type Cluster = irn::Cluster<Node, Keyspace>;
pub type Snapshot<'a> = cluster::Snapshot<'a, Node, Keyspace>;
pub type Viewable = cluster::Viewable<Node, Keyspace>;
pub type View = cluster::View<Node, Keyspace>;
pub type NodeState = cluster::node::State<Node>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: PeerId,
    pub addr: Multiaddr,
}

impl cluster::Node for Node {
    type Id = PeerId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn can_add(&self, _nodes: &cluster::Nodes<Self>) -> Result<(), impl std::error::Error> {
        Ok::<_, Infallible>(())
    }

    fn can_update(
        &self,
        _nodes: &cluster::Nodes<Self>,
        _new_state: &Self,
    ) -> Result<(), impl std::error::Error> {
        Ok::<_, Infallible>(())
    }
}

pub type Keyspace = cluster::keyspace::Sharded<3, Xxh3Builder, sharding::DefaultStrategy>;
