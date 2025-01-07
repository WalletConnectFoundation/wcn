pub use relay_rocks::RocksdbDatabaseConfig;
use {
    domain::NodeRegion,
    libp2p::PeerId,
    serde::{de::Error, Deserialize, Deserializer},
    std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        net::Ipv4Addr,
        path::PathBuf,
        str::FromStr,
    },
    tap::TapOptional,
    wcn_rpc::{identity::Keypair, Multiaddr},
};

/// Local [`Node`] config.
#[derive(Clone, Debug)]
pub struct Config {
    /// [`PeerId`] of the local [`Node`].
    pub id: PeerId,

    /// [`Keypair`] of the local [`Node`].
    pub keypair: Keypair,

    pub is_raft_voter: bool,

    /// [`Ipv4Addr`] to bind the servers to.
    pub server_addr: Ipv4Addr,

    /// Port of the Raft server.
    pub raft_server_port: u16,

    /// Port of the replica API server.
    pub replica_api_server_port: u16,

    /// Port of the client API server.
    pub client_api_server_port: u16,

    /// Port of the admin API server.
    pub admin_api_server_port: u16,

    /// Port of the Prometheus metrics server.
    pub metrics_server_port: u16,

    /// Maximum number of concurrent Client API connections.
    pub client_api_max_concurrent_connections: u32,

    /// Maximum number of concurrent Client API RPCs.
    pub client_api_max_concurrent_rpcs: u32,

    /// Maximum number of concurrent Replica API connections.
    pub replica_api_max_concurrent_connections: u32,

    /// Maximum number of concurrent Replica API RPCs.
    pub replica_api_max_concurrent_rpcs: u32,

    /// Maximum number of concurrent Coordinator API connections.
    pub coordinator_api_max_concurrent_connections: u32,

    /// Maximum number of concurrent Coordinator API RPCs.
    pub coordinator_api_max_concurrent_rpcs: u32,

    /// List of known peers.
    pub known_peers: HashMap<PeerId, Multiaddr>,

    /// List of nodes performing the initial bootstrap of the cluster.
    /// Each node in this list should also be in [`Config::known_peers`].
    ///
    /// Can be omitted if the cluster is already running.
    pub bootstrap_nodes: Option<Vec<PeerId>>,

    /// Path to the [`Raft`] directory.
    pub raft_dir: PathBuf,

    /// Path to the rocksdb directory.
    pub rocksdb_dir: PathBuf,

    /// Performance-related RocksDB configuration.
    pub rocksdb: RocksdbDatabaseConfig,

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

    /// List of authorized client ids for Admin API.
    pub authorized_admin_api_clients: HashSet<libp2p::PeerId>,

    /// List of authorized Raft candidates.
    /// If `None` the Raft authorization is going to be disabled.
    pub authorized_raft_candidates: Option<HashSet<libp2p::PeerId>>,

    /// Operator config.
    pub eth_address: Option<String>,

    /// Bootstrap node config.
    pub smart_contract: Option<SmartContractConfig>,

    /// Region in which the node is being deployed.
    pub region: NodeRegion,

    /// Organization that operates this node.
    pub organization: String,

    /// Network ID. E.g. `wcn_mainnet` or `wcn_testnet`.
    pub network_id: String,
}

#[derive(Clone, Debug)]
pub struct SmartContractConfig {
    pub eth_rpc_url: String,
    pub config_address: String,
    pub performance_reporter: Option<PerformanceReporterConfig>,
}

#[derive(Clone, Debug)]
pub struct PerformanceReporterConfig {
    pub signer_mnemonic: String,
    pub tracker_dir: PathBuf,
}

impl Config {
    pub fn from_env() -> envy::Result<Self> {
        let raw: RawConfig = envy::from_env()?;
        let rocksdb = create_rocksdb_config(&raw);

        tracing::info!(config = ?rocksdb, "rocksdb configuration");

        Ok(Self {
            id: PeerId::from_public_key(&raw.keypair.public()),
            keypair: raw.keypair,
            region: raw.region,
            organization: raw.organization,
            network_id: raw.network_id,
            is_raft_voter: raw.is_raft_voter.unwrap_or_default(),
            server_addr: raw.server_addr,
            raft_server_port: raw.raft_server_port.unwrap_or(3010),
            replica_api_server_port: raw.replica_api_server_port.unwrap_or(3011),
            client_api_server_port: raw.client_api_server_port.unwrap_or(3014),
            admin_api_server_port: raw.admin_api_server_port.unwrap_or(3013),
            metrics_server_port: raw.metrics_server_port.unwrap_or(3014),
            client_api_max_concurrent_connections: raw
                .client_api_max_concurrent_connections
                .unwrap_or(500),
            client_api_max_concurrent_rpcs: raw.client_api_max_concurrent_rpcs.unwrap_or(2000),
            replica_api_max_concurrent_connections: raw
                .replica_api_max_concurrent_connections
                .unwrap_or(500),
            replica_api_max_concurrent_rpcs: raw.replica_api_max_concurrent_rpcs.unwrap_or(4500),
            coordinator_api_max_concurrent_connections: raw
                .coordinator_api_max_concurrent_connections
                .unwrap_or(500),
            coordinator_api_max_concurrent_rpcs: raw
                .coordinator_api_max_concurrent_rpcs
                .unwrap_or(4500),
            bootstrap_nodes: raw.bootstrap_nodes,
            known_peers: known_peers_from_env()?,
            raft_dir: raw.raft_dir,
            rocksdb_dir: raw.rocksdb_dir,
            rocksdb,
            request_concurrency_limit: raw.request_concurrency_limit.unwrap_or(10000),
            request_limiter_queue: raw.request_limiter_queue.unwrap_or(32768),
            network_connection_timeout: raw.network_connection_timeout.unwrap_or(1000),
            network_request_timeout: raw.network_request_timeout.unwrap_or(1000),
            replication_request_timeout: raw.replication_request_timeout.unwrap_or(1000),
            warmup_delay: raw.warmup_delay.unwrap_or(30_000),
            authorized_clients: raw.authorized_clients,
            authorized_admin_api_clients: raw.authorized_admin_api_clients.unwrap_or_default(),
            authorized_raft_candidates: raw.authorized_raft_candidates,
            eth_address: raw.eth_address,
            smart_contract: if let Some(address) = raw.config_smart_contract_address {
                let performance_reporter = raw
                    .performance_tracker_dir
                    .map(|dir| {
                        let signer_mnemonic =
                            raw.smart_contract_signer_mnemonic.ok_or_else(|| {
                                envy::Error::custom("missing SMART_CONTRACT_SIGNER_MNEMONIC")
                            })?;
                        Ok(PerformanceReporterConfig {
                            signer_mnemonic,
                            tracker_dir: dir,
                        })
                    })
                    .transpose()?;

                Some(SmartContractConfig {
                    config_address: address,
                    eth_rpc_url: raw
                        .eth_rpc_url
                        .ok_or_else(|| envy::Error::custom("missing ETH_RPC_URL"))?,
                    performance_reporter,
                })
            } else {
                None
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(deserialize_with = "deserialize_keypair")]
    #[serde(rename = "secret_key")]
    keypair: Keypair,

    region: NodeRegion,
    organization: String,
    network_id: String,

    is_raft_voter: Option<bool>,

    server_addr: Ipv4Addr,
    raft_server_port: Option<u16>,
    replica_api_server_port: Option<u16>,
    client_api_server_port: Option<u16>,
    admin_api_server_port: Option<u16>,
    metrics_server_port: Option<u16>,

    client_api_max_concurrent_connections: Option<u32>,
    client_api_max_concurrent_rpcs: Option<u32>,
    replica_api_max_concurrent_connections: Option<u32>,
    replica_api_max_concurrent_rpcs: Option<u32>,
    coordinator_api_max_concurrent_connections: Option<u32>,
    coordinator_api_max_concurrent_rpcs: Option<u32>,

    bootstrap_nodes: Option<Vec<PeerId>>,
    raft_dir: PathBuf,
    rocksdb_dir: PathBuf,
    performance_tracker_dir: Option<PathBuf>,

    request_concurrency_limit: Option<usize>,
    request_limiter_queue: Option<usize>,
    network_connection_timeout: Option<u64>,
    network_request_timeout: Option<u64>,
    replication_request_timeout: Option<u64>,
    warmup_delay: Option<u64>,
    authorized_clients: Option<HashSet<libp2p::PeerId>>,
    authorized_admin_api_clients: Option<HashSet<libp2p::PeerId>>,
    authorized_raft_candidates: Option<HashSet<libp2p::PeerId>>,

    eth_address: Option<String>,

    config_smart_contract_address: Option<String>,
    smart_contract_signer_mnemonic: Option<String>,
    eth_rpc_url: Option<String>,

    rocksdb_num_batch_threads: Option<usize>,
    rocksdb_num_callback_threads: Option<usize>,
    rocksdb_max_subcompactions: Option<usize>,
    rocksdb_max_background_jobs: Option<usize>,
    rocksdb_ratelimiter: Option<usize>,
    rocksdb_increase_parallelism: Option<usize>,
    rocksdb_write_buffer_size: Option<usize>,
    rocksdb_max_write_buffer_number: Option<usize>,
    rocksdb_min_write_buffer_number_to_merge: Option<usize>,
    rocksdb_block_cache_size: Option<usize>,
    rocksdb_block_size: Option<usize>,
    rocksdb_row_cache_size: Option<usize>,
    rocksdb_enable_metrics: Option<bool>,
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

fn deserialize_keypair<'de, D>(deserializer: D) -> Result<Keypair, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error as _;

    String::deserialize(deserializer)
        .and_then(|s| base64::decode(s).map_err(D::Error::custom))
        .and_then(|bytes| Keypair::ed25519_from_bytes(bytes).map_err(D::Error::custom))
}

fn create_rocksdb_config(raw: &RawConfig) -> RocksdbDatabaseConfig {
    let defaults = RocksdbDatabaseConfig::default();

    RocksdbDatabaseConfig {
        num_batch_threads: raw
            .rocksdb_num_batch_threads
            .unwrap_or(defaults.num_batch_threads),
        num_callback_threads: raw
            .rocksdb_num_callback_threads
            .unwrap_or(defaults.num_callback_threads),
        max_subcompactions: raw
            .rocksdb_max_subcompactions
            .unwrap_or(defaults.max_subcompactions),
        max_background_jobs: raw
            .rocksdb_max_background_jobs
            .unwrap_or(defaults.max_background_jobs),

        ratelimiter: raw
            .rocksdb_ratelimiter
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.ratelimiter,
                    "rocksdb `ratelimiter` param not set, using default value"
                );
            })
            .unwrap_or(defaults.ratelimiter),

        increase_parallelism: raw
            .rocksdb_increase_parallelism
            .unwrap_or(defaults.increase_parallelism),

        write_buffer_size: raw
            .rocksdb_write_buffer_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.write_buffer_size,
                    "rocksdb `write_buffer_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.write_buffer_size),

        max_write_buffer_number: raw
            .rocksdb_max_write_buffer_number
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.max_write_buffer_number,
                    "rocksdb `max_write_buffer_number` param not set, using default value"
                );
            })
            .unwrap_or(defaults.max_write_buffer_number),

        min_write_buffer_number_to_merge: raw
            .rocksdb_min_write_buffer_number_to_merge
            .unwrap_or(defaults.min_write_buffer_number_to_merge),

        block_cache_size: raw
            .rocksdb_block_cache_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.block_cache_size,
                    "rocksdb `block_cache_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.block_cache_size),

        block_size: raw.rocksdb_block_size.unwrap_or(defaults.block_size),

        row_cache_size: raw
            .rocksdb_row_cache_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.row_cache_size,
                    "rocksdb `row_cache_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.row_cache_size),

        enable_metrics: raw
            .rocksdb_enable_metrics
            .unwrap_or(defaults.enable_metrics),
    }
}
