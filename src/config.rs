use {
    irn::{cluster::replication::Strategy, PeerId},
    network::Multiaddr,
    serde::{de::Error, Deserialize, Deserializer},
    std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        path::PathBuf,
        str::FromStr,
    },
};

/// Local [`Node`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`PeerId`] of the local [`Node`].
    pub id: PeerId,

    /// [`network::Keypair`] of the local [`Node`].
    pub keypair: network::Keypair,

    /// [`network::Multiaddr`] of the local [`Node`].
    pub addr: network::Multiaddr,

    /// Address of the external API.
    pub api_addr: network::Multiaddr,

    /// Service metrics HTTP address.
    pub metrics_addr: String,

    /// Indicator of whether the node to run should be a Raft member.
    pub is_raft_member: bool,

    /// List of known peers.
    pub known_peers: HashMap<PeerId, Multiaddr>,

    /// List of nodes performing the initial bootstrap of the cluster.
    /// Each node in this list should also be in [`Config::known_peers`].
    ///
    /// Can be omitted if the cluster is already running.
    pub bootstrap_nodes: Option<HashMap<PeerId, Multiaddr>>,

    /// Path to the [`Raft`] directory.
    pub raft_dir: PathBuf,

    /// Path to the rocksdb directory.
    pub rocksdb_dir: PathBuf,

    /// Number of rocksdb batch threads.
    pub rocksdb_num_batch_threads: usize,

    /// Number of rocksdb callback threads.
    pub rocksdb_num_callback_threads: usize,

    /// Replication strategy.
    pub replication_strategy: Strategy,

    /// API and inter-node storage request concurrency limit.
    pub request_concurrency_limit: usize,

    /// API and inter-node storage request queue size.
    pub request_limiter_queue: usize,

    /// Inter-node connection timeout in milliseconds.
    pub network_connection_timeout: u64,

    /// Inter-node request timeout in milliseconds.
    pub network_request_timeout: u64,

    /// Replication request timeout in milliseconds.
    ///
    /// It doesn't make sense to have it larger than
    /// [`Config::network_request_timeout`].
    pub replication_request_timeout: u64,

    /// Extra time a node should take to "warmup" before transitioning from
    /// `Restarting` to `Normal`. (in milliseconds)
    pub warmup_delay: u64,

    /// List of authorized client ids.
    /// If `None` the client authorization is going to be disabled.
    pub authorized_clients: Option<HashSet<libp2p::PeerId>>,
}

impl Config {
    pub fn from_env() -> envy::Result<Self> {
        let raw: RawConfig = envy::from_env()?;

        let mut cfg = Config {
            id: PeerId::from_public_key(&raw.keypair.public(), raw.group),
            keypair: raw.keypair,
            addr: raw.addr,
            api_addr: raw.api_addr,
            metrics_addr: raw.metrics_addr,
            is_raft_member: raw.is_raft_member.unwrap_or(true),
            bootstrap_nodes: None,
            known_peers: known_peers_from_env()?,
            raft_dir: raw.raft_dir,
            rocksdb_dir: raw.rocksdb_dir,
            rocksdb_num_batch_threads: raw.rocksdb_num_batch_threads.unwrap_or(8),
            rocksdb_num_callback_threads: raw.rocksdb_num_callback_threads.unwrap_or(8),
            replication_strategy: envy::prefixed("REPLICATION_STRATEGY_").from_env()?,
            request_concurrency_limit: raw.request_concurrency_limit.unwrap_or(10000),
            request_limiter_queue: raw.request_limiter_queue.unwrap_or(32768),
            network_connection_timeout: raw.network_connection_timeout.unwrap_or(1000),
            network_request_timeout: raw.network_request_timeout.unwrap_or(1000),
            replication_request_timeout: raw.replication_request_timeout.unwrap_or(1000),
            warmup_delay: raw.warmup_delay.unwrap_or(30_000),
            authorized_clients: raw.authorized_clients,
        };

        if let Some(nodes) = raw.bootstrap_nodes {
            let map_fn = |id| {
                if id == cfg.id {
                    return Ok((id, cfg.addr.clone()));
                };

                cfg.known_peers
                    .get(&id)
                    .map(|addr| (id, addr.clone()))
                    .ok_or_else(|| envy::Error::custom(format!("Missing address for {id} peer")))
            };

            cfg.bootstrap_nodes = Some(nodes.into_iter().map(map_fn).collect::<Result<_, _>>()?);
        }

        Ok(cfg)
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(deserialize_with = "deserialize_keypair")]
    #[serde(rename = "secret_key")]
    keypair: network::Keypair,
    group: u16,

    #[serde(rename = "raft_member")]
    is_raft_member: Option<bool>,

    addr: network::Multiaddr,
    api_addr: network::Multiaddr,
    metrics_addr: String,
    bootstrap_nodes: Option<Vec<PeerId>>,
    raft_dir: PathBuf,
    rocksdb_dir: PathBuf,
    rocksdb_num_batch_threads: Option<usize>,
    rocksdb_num_callback_threads: Option<usize>,
    request_concurrency_limit: Option<usize>,
    request_limiter_queue: Option<usize>,
    network_connection_timeout: Option<u64>,
    network_request_timeout: Option<u64>,
    replication_request_timeout: Option<u64>,
    warmup_delay: Option<u64>,
    authorized_clients: Option<HashSet<libp2p::PeerId>>,
}

#[derive(Debug, thiserror::Error)]
enum PeerParseError {
    #[error("Invalid peer ID")]
    Id,

    #[error("Invalid peer address")]
    Address,
}

fn known_peers_from_env() -> envy::Result<HashMap<PeerId, Multiaddr>> {
    std::env::vars()
        .filter_map(|(key, val)| {
            let id = key.strip_prefix("PEER_")?;

            parse_known_peer(id, &val)
                .map(Some)
                .map_err(envy::Error::custom)
                .transpose()
        })
        .collect()
}

fn parse_known_peer(id: &str, addr: &str) -> Result<(PeerId, Multiaddr), PeerParseError> {
    let id = PeerId::from_str(id).map_err(|_| PeerParseError::Id)?;
    let addr = Multiaddr::from_str(addr).map_err(|_| PeerParseError::Address)?;
    Ok((id, addr))
}

fn deserialize_keypair<'de, D>(deserializer: D) -> Result<network::Keypair, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error as _;

    String::deserialize(deserializer)
        .and_then(|s| base64::decode(s).map_err(D::Error::custom))
        .and_then(|bytes| network::Keypair::ed25519_from_bytes(bytes).map_err(D::Error::custom))
}
