use {
    network::{Keypair, PeerId},
    node::cluster::NodeRegion,
    serde::{Deserialize, Serialize},
    std::net::{Ipv4Addr, SocketAddr},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Identity {
    #[serde(with = "keypair_as_base64")]
    pub private_key: Keypair,
    pub region: NodeRegion,
    pub organization: String,
    pub eth_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Peer {
    pub id: PeerId,
}

impl From<Peer> for PeerId {
    fn from(value: Peer) -> Self {
        value.id
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KnownPeer {
    #[serde(flatten)]
    pub peer: Peer,
    pub address: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Server {
    pub bind_address: Ipv4Addr,
    pub server_port: u16,
    pub client_port: u16,
    pub raft_port: u16,
    pub admin_port: u16,
    pub metrics_port: u16,
    pub warmup_delay: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Authorization {
    pub enable: bool,
    pub clients: Vec<PeerId>,
    pub consensus_candidates: Vec<PeerId>,
    pub is_consensus_member: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Rocksdb {
    pub num_batch_threads: Option<usize>,
    pub num_callback_threads: Option<usize>,
    pub max_subcompactions: Option<usize>,
    pub max_background_jobs: Option<usize>,
    pub ratelimiter: Option<usize>,
    pub increase_parallelism: Option<usize>,
    pub write_buffer_size: Option<usize>,
    pub max_write_buffer_number: Option<usize>,
    pub min_write_buffer_number_to_merge: Option<usize>,
    pub block_cache_size: Option<usize>,
    pub block_size: Option<usize>,
    pub row_cache_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Storage {
    pub data_dir: String,
    pub consensus_dir: String,
    pub rocksdb: Rocksdb,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Network {
    pub request_concurrency_limit: usize,
    pub request_limiter_queue: usize,
    pub connection_timeout: u64,
    pub request_timeout: u64,
    pub replication_request_timeout: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SmartContractConfig {
    pub eth_rpc_url: String,
    pub config_address: String,
}

impl From<SmartContractConfig> for node::config::SmartContractConfig {
    fn from(value: SmartContractConfig) -> Self {
        Self {
            eth_rpc_url: value.eth_rpc_url,
            config_address: value.config_address,
            performance_reporter: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub identity: Identity,
    pub known_peers: Vec<KnownPeer>,
    pub bootstrap_nodes: Vec<Peer>,
    pub server: Server,
    pub authorization: Authorization,
    pub storage: Storage,
    pub network: Network,
    pub smart_contract: Option<SmartContractConfig>,
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(
                include_str!("../../../default_config.toml"),
                config::FileFormat::Toml,
            ))
            .add_source(config::File::new(path, config::FileFormat::Toml).required(true))
            .build()?
            .try_deserialize()
    }
}

mod keypair_as_base64 {
    use {
        network::Keypair,
        serde::{Deserialize, Deserializer, Serialize, Serializer},
    };

    pub fn serialize<S>(data: &Keypair, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::Error;

        let key = data
            .clone()
            .try_into_ed25519()
            .map_err(|_| S::Error::custom("invalid key type: must be ed25519"))?
            .secret();

        data_encoding::BASE64
            .encode(key.as_ref())
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Keypair, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let key_str = String::deserialize(deserializer)?;

        let decoded = data_encoding::BASE64
            .decode(key_str.as_bytes())
            .map_err(|_| D::Error::custom("invalid key encoding: must be base64"))?;

        network::Keypair::ed25519_from_bytes(decoded)
            .map_err(|_| D::Error::custom("invalid key length: must be 32 bytes"))
    }
}
