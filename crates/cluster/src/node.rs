//! Node within a WCN cluster.

use {
    crate::EncryptionKey,
    aes::Aes256,
    derive_more::derive::AsRef,
    fpe::ff1::{FlexibleNumeralString, FF1},
    libp2p_identity::PeerId,
    multihash::Multihash,
    serde::{Deserialize, Serialize},
    std::net::{Ipv4Addr, SocketAddrV4},
    tap::Pipe,
};

/// Node within a WCN cluster.
///
/// The IP address is currently being encrypted using a format-preserving
/// encryption algorithm.
#[derive(AsRef, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Node {
    /// [`PeerId`] of the [`Node`].
    ///
    /// Used for authentication. Multiple nodes managed by the same
    /// node operator are allowed to have the same [`PeerId`].
    #[as_ref]
    pub peer_id: PeerId,

    /// [`Ipv4Addr`] of the [`Node`].
    pub ipv4_addr: Ipv4Addr,

    /// Primary RPC server port.
    pub primary_port: u16,

    /// Secondary RPC server port.
    pub secondary_port: u16,
}

impl Node {
    /// Builds [`SocketAddrV4`] using [`Node::ipv4_addr`] and
    /// [`Node::primary_port`].
    pub fn primary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ipv4_addr, self.primary_port)
    }

    /// Builds [`SocketAddrV4`] using [`Node::ipv4_addr`] and
    /// [`Node::secondary_port`].
    pub fn secondary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ipv4_addr, self.secondary_port)
    }

    pub(crate) fn encrypt(&mut self, key: &EncryptionKey) {
        self.ipv4_addr = encrypt_ip(key, &self.peer_id, self.ipv4_addr);
    }

    pub(crate) fn decrypt(&mut self, key: &EncryptionKey) {
        self.ipv4_addr = decrypt_ip(key, &self.peer_id, self.ipv4_addr);
    }
}

// NOTE: The on-chain serialization is non self-describing!
// This `struct` can not be changed, a `struct` with a new schema version should
// be created instead.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct V0 {
    pub peer_id: PeerId,
    pub ipv4_addr: Ipv4Addr,
    pub primary_port: u16,
    pub secondary_port: u16,
}

impl From<Node> for V0 {
    fn from(node: Node) -> Self {
        Self {
            peer_id: node.peer_id,
            ipv4_addr: node.ipv4_addr,
            primary_port: node.primary_port,
            secondary_port: node.secondary_port,
        }
    }
}

impl From<V0> for Node {
    fn from(node: V0) -> Self {
        Self {
            peer_id: node.peer_id,
            ipv4_addr: node.ipv4_addr,
            primary_port: node.primary_port,
            secondary_port: node.secondary_port,
        }
    }
}

struct Ipv4AddrNumeral(FlexibleNumeralString);

impl From<Ipv4Addr> for Ipv4AddrNumeral {
    fn from(addr: Ipv4Addr) -> Self {
        addr.octets()
            .into_iter()
            .map(|oct| oct as u16)
            .collect::<Vec<_>>()
            .pipe(|vec| Self(vec.into()))
    }
}

impl From<Ipv4AddrNumeral> for Ipv4Addr {
    fn from(addr: Ipv4AddrNumeral) -> Self {
        let octets: Vec<u16> = addr.0.into();
        let octets: [u8; 4] = std::array::from_fn(|idx| octets[idx] as u8);
        octets.into()
    }
}

fn fpe_ff1(key: &EncryptionKey) -> FF1<Aes256> {
    // NOTE(unwrap): `256` is valid `radix`
    FF1::<Aes256>::new(key.0.as_slice(), 256).unwrap()
}

/// Extracts `digest` from a [`PeerId`] to be used as a `tweak` for FPE.
fn fpe_tweak(peer_id: &PeerId) -> &[u8] {
    let multihash: &Multihash<64> = peer_id.as_ref();
    multihash.digest()
}

fn encrypt_ip(key: &EncryptionKey, peer_id: &PeerId, ip: Ipv4Addr) -> Ipv4Addr {
    // NOTE(unwrap): any IP addr octet (`u8`) is valid for radix `256`
    fpe_ff1(key)
        .encrypt(fpe_tweak(peer_id), &Ipv4AddrNumeral::from(ip).0)
        .map(Ipv4AddrNumeral)
        .unwrap()
        .into()
}

fn decrypt_ip(key: &EncryptionKey, peer_id: &PeerId, ip: Ipv4Addr) -> Ipv4Addr {
    // NOTE(unwrap): any IP addr octet (`u8`) is valid for radix `256`
    fpe_ff1(key)
        .decrypt(fpe_tweak(peer_id), &Ipv4AddrNumeral::from(ip).0)
        .map(Ipv4AddrNumeral)
        .unwrap()
        .into()
}

#[cfg(test)]
mod tests {
    use {super::*, proptest::prelude::*, std::net::Ipv4Addr};

    #[test]
    fn ip_encryption() {
        let peer_id = PeerId::random();
        let ip = Ipv4Addr::new(8, 255, 0, 1);

        let key: [u8; 32] = std::array::from_fn(|idx| idx as u8);
        let key = EncryptionKey(key);

        let encrypted = encrypt_ip(&key, &peer_id, ip);
        assert_ne!(ip, encrypted);

        let decrypted = decrypt_ip(&key, &peer_id, encrypted);
        assert_eq!(ip, decrypted);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10000, // has been tested with 1mil
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_ip_encryption(ip in any::<Ipv4Addr>(), key_bytes in prop::array::uniform32(any::<u8>())) {
            let peer_id = PeerId::random();
            let key = EncryptionKey(key_bytes);

            let encrypted = encrypt_ip(&key, &peer_id, ip);
            let decrypted = decrypt_ip(&key, &peer_id, encrypted);

            prop_assert_eq!(decrypted, ip);
        }
    }
}
