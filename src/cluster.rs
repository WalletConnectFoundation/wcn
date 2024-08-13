pub use irn::cluster::Error;
use {
    irn::cluster::{
        self,
        keyspace::{self, ReplicaCandidate, ReplicaCandidates},
    },
    libp2p::{Multiaddr, PeerId},
    serde::{Deserialize, Serialize},
    std::iter,
    tap::TapOptional,
    xxhash_rust::xxh3::Xxh3Builder,
};

const WC_ORG: &'static str = "WalletConnect";

pub type Cluster = irn::Cluster<Node, Keyspace>;
pub type Snapshot<'a> = cluster::Snapshot<'a, Node, Keyspace>;
pub type Viewable = cluster::Viewable<Node, Keyspace>;
pub type View = cluster::View<Node, Keyspace>;
pub type NodeState = cluster::node::State<Node>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: PeerId,
    pub addr: Multiaddr,
    pub region: NodeRegion,
    pub organization: String,
    pub eth_address: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRegion {
    Eu,
    Us,
    Ap,
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

pub type Keyspace = cluster::keyspace::Sharded<3, Xxh3Builder, ReplicationStrategy>;

/// [`keyspace::ReplicationStrategy`] implementation.
///
/// Ensures that:
/// - each replica set has 1 node from each region
/// - each replica set has no more than 1 operator node
///
/// And it assumes that each region has at least one WalletConnect node.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicationStrategy {
    wallet_connet_nodes: NodesByRegion,
    operator_nodes: NodesByRegion,
}

#[derive(Clone, Copy, Debug, Default)]
struct NodesByRegion {
    eu: u8,
    us: u8,
    ap: u8,
}

impl NodesByRegion {
    fn incr(&mut self, region: NodeRegion) {
        match region {
            NodeRegion::Eu => self.eu += 1,
            NodeRegion::Us => self.us += 1,
            NodeRegion::Ap => self.ap += 1,
        }
    }

    fn has_node(&self, region: NodeRegion) -> bool {
        match region {
            NodeRegion::Eu => self.eu > 0,
            NodeRegion::Us => self.us > 0,
            NodeRegion::Ap => self.ap > 0,
        }
    }
}

impl ReplicationStrategy {
    fn choose_operator_node<'a>(
        &mut self,
        candidates: ReplicaCandidates<'a, Node>,
    ) -> Option<ReplicaCandidate<'a, Node>> {
        candidates
            .iter()
            .find(|c| c.node().organization != WC_ORG)
            .tap_some(|c| self.operator_nodes.incr(c.node().region))
    }

    fn has_node_in_region<'a>(&self, region: NodeRegion) -> bool {
        self.wallet_connet_nodes.has_node(region) || self.operator_nodes.has_node(region)
    }

    fn choose_wallet_connect_nodes<'s, 'a>(
        &'s mut self,
        candidates: ReplicaCandidates<'a, Node>,
    ) -> impl Iterator<Item = ReplicaCandidate<'a, Node>> + 's
    where
        'a: 's,
    {
        candidates.iter().filter(|c| {
            if c.node().organization != WC_ORG || self.has_node_in_region(c.node().region) {
                return false;
            }

            self.wallet_connet_nodes.incr(c.node().region);
            true
        })
    }
}

impl keyspace::ReplicationStrategy<Node> for ReplicationStrategy {
    fn choose_replicas<'a>(
        &'a mut self,
        candidates: ReplicaCandidates<'a, Node>,
    ) -> impl Iterator<Item = ReplicaCandidate<'a, Node>> {
        self.choose_operator_node(candidates)
            .into_iter()
            .chain(self.choose_wallet_connect_nodes(candidates))
    }
}

#[cfg(test)]
mod test {
    use {super::*, cluster::Node as _, sharding::ReplicationStrategy as _};

    fn new_node(
        region: &'static str,
        organization: &'static str,
        eth_addr: Option<&'static str>,
    ) -> Node {
        Node {
            id: PeerId::random(),
            addr: "/ip4/127.0.0.1/udp/3000/quic-v1".parse().unwrap(),
            region: match region {
                "eu" => NodeRegion::Eu,
                "us" => NodeRegion::Us,
                "ap" => NodeRegion::Ap,
                _ => unreachable!(),
            },
            organization: organization.to_string(),
            eth_address: eth_addr.map(ToString::to_string),
        }
    }

    #[test]
    fn node_constraints() {
        let addr1 = "0x4d0867CdD3228feC92dd877de74FfFe3c544e40B";
        let addr2 = "0x7d35CE48b2b056FAdB99dE718CBbbbf00f221E3c";

        let node1 = new_node("eu", "A", None);
        let node2 = new_node("us", "B", Some(addr1));

        let mut nodes = cluster::Nodes::default();
        nodes.insert(node1.clone()).unwrap();
        nodes.insert(node2.clone()).unwrap();

        assert_eq!(
            new_node("eu", "C", None).validate_constraints(&nodes),
            Ok(())
        );

        assert_eq!(
            new_node("ap", "A", Some(addr2)).validate_constraints(&nodes),
            Ok(())
        );

        assert_eq!(
            new_node("eu", "A", Some(addr1)).validate_constraints(&nodes),
            Err(NodeError::EthAddressConflict {
                addr: addr1.to_string(),
                node_id: node2.id
            })
        );

        assert!(node1.can_update(&nodes, &node2).is_err());
    }

    #[test]
    fn replication_strategy() {
        let mut strategy = ReplicationStrategy::default();

        // strategy.is_suitable_replica(&new_node("eu", WC_ORG))
    }
}
