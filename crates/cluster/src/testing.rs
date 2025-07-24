use {
    crate::{node_operator, smart_contract, Node, NodeOperator},
    libp2p::{identity::Keypair, PeerId},
    std::net::SocketAddrV4,
};

pub fn node_keypair(operator_id: u8, node_id: u8) -> Keypair {
    let mut priv_key = [0; 32];
    priv_key[30] = operator_id;
    priv_key[31] = node_id;

    Keypair::ed25519_from_bytes(priv_key).unwrap()
}

pub fn node_peer_id(operator_id: u8, node_id: u8) -> PeerId {
    node_keypair(operator_id, node_id).public().to_peer_id()
}

pub fn node_addr(operator_id: u8, node_id: u8) -> SocketAddrV4 {
    SocketAddrV4::new([10, 0, 0, operator_id].into(), node_id as u16)
}

pub fn node_operator(id: u8) -> NodeOperator {
    let nodes = (0..2).map(|node_id| Node {
        peer_id: node_peer_id(id, node_id),
        addr: node_addr(id, node_id),
    });

    NodeOperator::new(
        smart_contract::testing::account_address(id),
        node_operator::Name::new(format!("Operator{id}")).unwrap(),
        nodes.collect(),
        vec![],
    )
    .unwrap()
}
