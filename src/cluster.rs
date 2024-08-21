pub use irn::cluster::Error;
use {
    irn::cluster,
    libp2p::{Multiaddr, PeerId},
    serde::{Deserialize, Serialize},
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
    pub eth_address: Option<String>,
}

impl Node {
    fn validate_constraints(&self, nodes: &cluster::Nodes<Self>) -> Result<(), NodeError> {
        let err = nodes.iter().find_map(|(_, node)| {
            if self.id == node.id {
                return None;
            }

            match (&self.eth_address, &node.eth_address) {
                (Some(my_addr), Some(addr)) if my_addr == addr => {
                    Some(NodeError::EthAddressConflict {
                        addr: addr.clone(),
                        node_id: node.id,
                    })
                }
                _ => None,
            }
        });

        err.map(Err).unwrap_or(Ok(()))
    }
}

impl cluster::Node for Node {
    type Id = PeerId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn can_add(&self, nodes: &cluster::Nodes<Self>) -> Result<(), impl std::error::Error> {
        self.validate_constraints(nodes)
    }

    fn can_update(
        &self,
        nodes: &cluster::Nodes<Self>,
        new_state: &Self,
    ) -> Result<(), impl std::error::Error> {
        if self.id != new_state.id {
            return Err(NodeError::IdChanged);
        }

        new_state.validate_constraints(nodes)
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
enum NodeError {
    #[error("Trying to update a Node to a state with a different Id")]
    IdChanged,

    #[error("Eth address ({addr}) is already being used by {node_id}")]
    EthAddressConflict { addr: String, node_id: PeerId },
}

pub type Keyspace = cluster::keyspace::Sharded<3, Xxh3Builder, sharding::DefaultStrategy>;

#[cfg(test)]
mod test {
    use {super::*, cluster::Node as _};

    fn new_node(eth_addr: Option<&'static str>) -> Node {
        Node {
            id: PeerId::random(),
            addr: "/ip4/127.0.0.1/udp/3000/quic-v1".parse().unwrap(),
            eth_address: eth_addr.map(ToString::to_string),
        }
    }

    #[test]
    fn node_constraints() {
        let addr1 = "0x4d0867CdD3228feC92dd877de74FfFe3c544e40B";
        let addr2 = "0x7d35CE48b2b056FAdB99dE718CBbbbf00f221E3c";

        let node1 = new_node(None);
        let node2 = new_node(Some(addr1));

        let mut nodes = cluster::Nodes::default();
        nodes.insert(node1).unwrap();
        nodes.insert(node2.clone()).unwrap();

        assert_eq!(new_node(None).validate_constraints(&nodes), Ok(()));
        assert_eq!(new_node(Some(addr2)).validate_constraints(&nodes), Ok(()));
        assert_eq!(
            new_node(Some(addr1)).validate_constraints(&nodes),
            Err(NodeError::EthAddressConflict {
                addr: addr1.to_string(),
                node_id: node2.id
            })
        );

        assert!(new_node(None).can_update(&nodes, &new_node(None)).is_err());
    }
}
