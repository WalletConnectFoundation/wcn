use {
    core::net::Ipv4Addr,
    serde::{Deserialize, Deserializer},
    std::collections::HashSet,
    wcn_rpc::{identity::Keypair, PeerAddr},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ed25519 private key decoding failed")]
    KeyDecodingFailed,

    #[error("Invalid storage node address")]
    InvalidNodeAddress,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub keypair: Keypair,

    pub server_addr: Ipv4Addr,
    pub replica_api_server_port: u16,
    pub client_api_server_port: u16,
    pub metrics_server_port: u16,

    pub client_api_max_concurrent_connections: u32,
    pub client_api_max_concurrent_rpcs: u32,

    pub replica_api_max_concurrent_connections: u32,
    pub replica_api_max_concurrent_rpcs: u32,

    pub storage_nodes: HashSet<PeerAddr>,
}

impl Config {
    pub fn from_env() -> envy::Result<Self> {
        let raw = envy::from_env::<RawConfig>()?;

        Ok(Self {
            keypair: raw.keypair,

            server_addr: raw.server_addr,
            replica_api_server_port: raw.replica_api_server_port.unwrap_or(3011),
            client_api_server_port: raw.client_api_server_port.unwrap_or(3014),
            metrics_server_port: raw.metrics_server_port.unwrap_or(3014),

            client_api_max_concurrent_connections: raw
                .client_api_max_concurrent_connections
                .unwrap_or(500),
            client_api_max_concurrent_rpcs: raw.client_api_max_concurrent_rpcs.unwrap_or(2000),

            replica_api_max_concurrent_connections: raw
                .replica_api_max_concurrent_connections
                .unwrap_or(500),
            replica_api_max_concurrent_rpcs: raw.replica_api_max_concurrent_rpcs.unwrap_or(4500),

            storage_nodes: parse_nodes(&raw.storage_nodes)?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(deserialize_with = "deserialize_keypair")]
    #[serde(rename = "secret_key")]
    keypair: Keypair,

    server_addr: Ipv4Addr,
    replica_api_server_port: Option<u16>,
    client_api_server_port: Option<u16>,
    metrics_server_port: Option<u16>,

    client_api_max_concurrent_connections: Option<u32>,
    client_api_max_concurrent_rpcs: Option<u32>,

    replica_api_max_concurrent_connections: Option<u32>,
    replica_api_max_concurrent_rpcs: Option<u32>,

    storage_nodes: Vec<String>,
}

fn parse_nodes(nodes: &[String]) -> envy::Result<HashSet<PeerAddr>> {
    nodes
        .iter()
        .map(|addr| addr.parse())
        .collect::<Result<_, _>>()
        .map_err(|_| envy::Error::Custom(Error::InvalidNodeAddress.to_string()))
}

fn deserialize_keypair<'de, D>(deserializer: D) -> Result<Keypair, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error as _;

    String::deserialize(deserializer)
        .and_then(|s| base64::decode(s).map_err(D::Error::custom))
        .and_then(|bytes| Keypair::ed25519_from_bytes(bytes).map_err(D::Error::custom))
}
