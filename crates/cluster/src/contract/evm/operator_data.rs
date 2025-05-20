//! De/serialization for on-chain node operator data.
//!
//! IMPORTANT: The serialization is non self-describing! Every change to the
//! schema should be handled by creating a new version of the schema and bumping
//! the version byte.

use {
    super::Address,
    crate::{contract, node},
    aes::Aes256,
    anyhow::Context,
    fpe::ff1::FF1,
    libp2p::PeerId,
    serde::{Deserialize, Serialize},
    std::net::SocketAddrV4,
};

#[derive(Serialize, Deserialize)]
struct Operator {
    name: node::OperatorName,
    nodes: Vec<Node>,
    clients: Vec<PeerId>,
}

#[derive(Serialize, Deserialize)]
struct Node {
    peer_id: PeerId,
    addr: SocketAddrV4,
}

pub(super) fn serialize(operator: node::Operator) -> anyhow::Result<Vec<u8>> {
    let op = Operator {
        name: operator.name,
        nodes: operator
            .nodes
            .into_iter()
            .map(|node| Node {
                peer_id: node.peer_id,
                addr: node.addr,
            })
            .collect(),
        clients: operator.clients,
    };

    // reserve first byte for versioning
    let size = postcard::experimental::serialized_size(&op).context("serialize_size")? + 1;
    let mut buf = Vec::with_capacity(size);
    postcard::to_slice(&op, &mut buf[1..]).context("to_slice")?;
    Ok(buf)
}

pub(super) fn deserialize(operator_addr: Address, buf: &[u8]) -> anyhow::Result<node::Operator> {
    if buf.is_empty() {
        return Err(anyhow::anyhow!("empty buf"));
    }

    let operator: Operator = postcard::from_bytes(&buf[1..]).context("from_bytes")?;

    Ok(node::Operator {
        id: operator_addr.into(),
        name: operator.name,
        nodes: operator
            .nodes
            .into_iter()
            .map(|node| crate::Node {
                peer_id: node.peer_id,
                addr: node.addr,
            })
            .collect(),
        clients: operator.clients,
    })
}
