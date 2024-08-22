pub use irn::cluster::Error;
use {
    irn::cluster,
    libp2p::{Multiaddr, PeerId},
    serde::{Deserialize, Serialize},
    xxhash_rust::xxh3::Xxh3Builder,
};

const WC_ORG: &str = "WalletConnect";

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
#[allow(clippy::struct_excessive_bools)]
pub struct ReplicationStrategy {
    has_operator_node: bool,

    has_eu_node: bool,
    has_us_node: bool,
    has_ap_node: bool,
}

impl sharding::ReplicationStrategy<Node> for ReplicationStrategy {
    fn is_suitable_replica(&mut self, node: &Node) -> bool {
        let is_operator_node = node.organization != WC_ORG;

        if is_operator_node && self.has_operator_node {
            return false;
        }

        match node.region {
            NodeRegion::Eu if !self.has_eu_node => self.has_eu_node = true,
            NodeRegion::Us if !self.has_us_node => self.has_us_node = true,
            NodeRegion::Ap if !self.has_ap_node => self.has_ap_node = true,
            _ => return false,
        };

        if is_operator_node {
            self.has_operator_node = true;
        }

        true
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        cluster::{Keyspace as _, Node as _},
        sharding::ReplicationStrategy as _,
    };

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
        assert!(strategy.is_suitable_replica(&new_node("eu", WC_ORG, None)));

        // nodes from the same region are no longer suitable
        assert!(!strategy.is_suitable_replica(&new_node("eu", WC_ORG, None)));
        assert!(!strategy.is_suitable_replica(&new_node("eu", "A", None)));

        assert!(strategy.is_suitable_replica(&new_node("us", "A", None)));

        // no more operator nodes are allowed
        assert!(!strategy.is_suitable_replica(&new_node("ap", "B", None)));

        assert!(strategy.is_suitable_replica(&new_node("ap", WC_ORG, None)));

        // having 3 WalletConnect nodes is fine
        let mut strategy = ReplicationStrategy::default();
        assert!(strategy.is_suitable_replica(&new_node("eu", WC_ORG, None)));
        assert!(strategy.is_suitable_replica(&new_node("us", WC_ORG, None)));
        assert!(strategy.is_suitable_replica(&new_node("ap", WC_ORG, None)));
    }

    #[test]
    fn keyspace() {
        struct Context {
            nodes: cluster::Nodes<Node>,
            keyspace: Keyspace,
            old_keyspace: Keyspace,
        }

        // bootstrap
        // {region}: {WalletConnect nodes}/{operator nodes}
        // eu: 1/0, us: 1/0, ap: 1/0
        let mut nodes = cluster::Nodes::<Node>::default();
        nodes.insert(new_node("eu", WC_ORG, None)).unwrap();
        nodes.insert(new_node("us", WC_ORG, None)).unwrap();
        nodes.insert(new_node("ap", WC_ORG, None)).unwrap();
        let keyspace = Keyspace::new(&nodes).unwrap();

        let ctx = &mut Context {
            old_keyspace: keyspace.clone(),
            nodes,
            keyspace,
        };

        let assert_distribution_and_stability =
            |ctx: &mut Context, min_max_ratio, stability_coef| {
                ctx.keyspace.sharding().assert_distribution(min_max_ratio);
                ctx.keyspace
                    .sharding()
                    .assert_stability(ctx.old_keyspace.sharding(), stability_coef);
            };

        assert_distribution_and_stability(ctx, 1.0, 1.0);

        let add_node = |ctx: &mut Context, region, org| {
            ctx.nodes.insert(new_node(region, org, None)).unwrap();
            ctx.old_keyspace = ctx.keyspace.clone();
            assert_eq!(ctx.keyspace.update(&ctx.nodes), Ok(true));
        };

        let remove_node = |ctx: &mut Context, region, org| {
            let remove = new_node(region, org, None);
            let id = ctx
                .nodes
                .iter()
                .find_map(|(_, node)| {
                    (node.region == remove.region && node.organization == remove.organization)
                        .then_some(node.id)
                })
                .unwrap();
            ctx.nodes.remove(&id);
            ctx.old_keyspace = ctx.keyspace.clone();
            assert_eq!(ctx.keyspace.update(&ctx.nodes), Ok(true));
        };

        // eu: 2/0, us: 1/0, ap: 1/0
        add_node(ctx, "eu", WC_ORG);
        assert_distribution_and_stability(ctx, 0.49, 0.83);

        // eu: 2/0, us: 2/0, ap: 1/0
        add_node(ctx, "us", WC_ORG);
        assert_distribution_and_stability(ctx, 0.49, 0.83);

        // eu: 2/0, us: 2/0, ap: 2/0
        add_node(ctx, "ap", WC_ORG);
        assert_distribution_and_stability(ctx, 0.99, 0.83);

        // eu: 3/0, us: 2/0, ap: 2/0
        add_node(ctx, "eu", WC_ORG);
        assert_distribution_and_stability(ctx, 0.66, 0.88);

        // eu: 3/0, us: 3/0, ap: 2/0
        add_node(ctx, "us", WC_ORG);
        assert_distribution_and_stability(ctx, 0.66, 0.88);

        // eu: 3/0, us: 3/0, ap: 3/0
        add_node(ctx, "ap", WC_ORG);
        assert_distribution_and_stability(ctx, 0.98, 0.88);

        // eu: 3/1, us: 3/0, ap: 3/0
        add_node(ctx, "eu", "operator_A");
        assert_distribution_and_stability(ctx, 0.74, 0.91);

        // eu: 3/1, us: 3/1, ap: 3/0
        add_node(ctx, "us", "operator_B");
        assert_distribution_and_stability(ctx, 0.65, 0.91);

        // eu: 3/1, us: 3/1, ap: 3/1
        add_node(ctx, "ap", "operator_C");
        assert_distribution_and_stability(ctx, 0.70, 0.91);

        // eu: 3/2, us: 3/1, ap: 3/1
        add_node(ctx, "eu", "operator_D");
        assert_distribution_and_stability(ctx, 0.56, 0.93);

        // eu: 3/2, us: 3/2, ap: 3/1
        add_node(ctx, "us", "operator_E");
        assert_distribution_and_stability(ctx, 0.50, 0.93);

        // eu: 3/2, us: 3/2, ap: 3/2
        add_node(ctx, "ap", "operator_F");
        assert_distribution_and_stability(ctx, 0.52, 0.93);

        // eu: 4/2, us: 3/2, ap: 3/2
        add_node(ctx, "eu", WC_ORG);
        assert_distribution_and_stability(ctx, 0.46, 0.93);

        // eu: 4/2, us: 4/2, ap: 3/2
        add_node(ctx, "us", WC_ORG);
        assert_distribution_and_stability(ctx, 0.46, 0.93);

        // eu: 4/2, us: 4/2, ap: 4/2
        add_node(ctx, "ap", WC_ORG);
        assert_distribution_and_stability(ctx, 0.59, 0.93);

        // eu: 3/2, us: 4/2, ap: 4/2
        remove_node(ctx, "eu", WC_ORG);
        assert_distribution_and_stability(ctx, 0.46, 0.93);

        // eu: 3/2, us: 3/2, ap: 4/2
        remove_node(ctx, "us", WC_ORG);
        assert_distribution_and_stability(ctx, 0.45, 0.93);

        // eu: 3/2, us: 3/2, ap: 3/2
        remove_node(ctx, "ap", WC_ORG);
        assert_distribution_and_stability(ctx, 0.51, 0.93);
    }
}
