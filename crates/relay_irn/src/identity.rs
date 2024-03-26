use {
    derive_more::{AsRef, Display},
    rand::Rng,
    serde_with::{DeserializeFromStr, SerializeDisplay},
    std::str::FromStr,
};

#[derive(Debug, thiserror::Error)]
#[error("Invalid peer ID")]
pub struct InvalidPeerId;

// N.B. Display format must not change as it'll break serialization, which
// relies on `Display` and `FromStr` implementations.
#[derive(
    AsRef,
    Clone,
    Copy,
    Debug,
    Display,
    Hash,
    SerializeDisplay,
    DeserializeFromStr,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
#[display("{}_{}", id, group)]
pub struct PeerId {
    #[as_ref]
    pub id: libp2p::PeerId,

    pub group: u16,
}

impl Default for PeerId {
    fn default() -> Self {
        Self {
            id: libp2p::PeerId::from_multihash(Default::default()).unwrap(),
            group: 0,
        }
    }
}

impl FromStr for PeerId {
    type Err = InvalidPeerId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split('_');

        let (Some(id), Some(group), None) = (iter.next(), iter.next(), iter.next()) else {
            return Err(InvalidPeerId);
        };

        Ok(Self {
            id: libp2p::PeerId::from_str(id).map_err(|_| InvalidPeerId)?,
            group: group.parse().map_err(|_| InvalidPeerId)?,
        })
    }
}

impl PeerId {
    pub fn from_parts(id: &str, group: u16) -> Result<Self, InvalidPeerId> {
        let id = libp2p::PeerId::from_str(id).map_err(|_| InvalidPeerId)?;

        Ok(Self { id, group })
    }

    pub fn from_bytes(data: &[u8], group: u16) -> Result<Self, InvalidPeerId> {
        libp2p::PeerId::from_bytes(data)
            .map(|id| Self { id, group })
            .map_err(|_| InvalidPeerId)
    }

    pub fn from_public_key(key: &libp2p::identity::PublicKey, group: u16) -> Self {
        Self {
            id: libp2p::PeerId::from_public_key(key),
            group,
        }
    }

    pub fn random() -> Self {
        Self {
            id: libp2p::PeerId::random(),
            group: rand::thread_rng().gen(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_serde() {
        let serialized = "1Ad95d9fUWtiYuLmGFAKagi47rADsqpeHcwbkpc9RivHTq_12345";
        let id = serialized.parse::<PeerId>().unwrap();
        assert_eq!(serialized, format!("{id}"));
    }
}
