use {
    super::{Error, Lockfile, LogFormat},
    irn_core::{cluster::replication::Strategy, PeerId},
    node::RocksdbDatabaseConfig,
    std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
        path::PathBuf,
        process::{Command, Stdio},
    },
    tap::TapOptional as _,
};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum LogOutput {
    Stdout,
    File,
}

#[derive(Debug, clap::Args)]
pub struct StartCmd {
    #[clap(short, long)]
    /// Working directory for this node instance.
    ///
    /// Each running instance should use its own working directory, and the
    /// directories are locked to that node's process ID.
    ///
    /// The following paths are considered relative to the working directory:
    /// `config` (CLI arg), `data_dir` (config param), `consensus_dir` (config
    /// param).
    ///
    /// The directory will be created if it doesn't exist.
    working_dir: Option<String>,

    #[clap(short, long, default_value = "config.toml")]
    /// Node configuration file.
    ///
    /// The path is relative to the working directory.
    config: String,

    #[clap(short, long)]
    /// Whether to start the node in a detached state.
    ///
    /// Detaching would spawn a background process running the node. Note that
    /// `log-output` is set to `file` if running in a detached node.
    detached: bool,

    #[clap(long, default_value = "info")]
    /// Log filtering.
    ///
    /// Can be used to filter per module. Example: `warn,irn_node=info` would
    /// set the default level for all modules to `warn`, while overriding it
    /// for `irn_node` to `info`.
    log_filter: String,

    #[clap(long, value_enum, default_value_t = LogFormat::Text)]
    /// Log format.
    ///
    /// Either human readable `text`, or structured `json`.
    log_format: LogFormat,

    #[clap(long, value_enum, default_value_t = LogOutput::Stdout)]
    /// Log output stream.
    ///
    /// By default all logs are being written to `stdout`. Switching to `file`
    /// would store node logs in the working directory.
    log_output: LogOutput,
}

pub async fn exec(args: StartCmd) -> anyhow::Result<()> {
    let initial_wd = std::env::current_dir().map_err(|_| Error::InvalidWorkingDir)?;

    if let Some(working_dir) = &args.working_dir {
        std::fs::create_dir_all(working_dir).map_err(|_| Error::InvalidWorkingDir)?;
        std::env::set_current_dir(working_dir).map_err(|_| Error::InaccessibleWorkingDir)?;
    }

    let is_detached = get_detached_state();
    let log_to_file = is_detached || matches!(args.log_output, LogOutput::File);

    let log_file = log_to_file.then(|| {
        let mut log_file = std::env::current_dir().unwrap();
        log_file.push(format!("irn.{}.log", std::process::id()));
        log_file
    });

    let _logger = node::Logger::init(args.log_format.into(), Some(&args.log_filter), log_file);

    let config = super::config::Config::load_from_file(&args.config).map_err(Error::Config)?;

    let mut lockfile = Lockfile::new();

    lockfile.acquire().map_err(|err| {
        tracing::error!(pid = ?lockfile.owner(), "working directory is locked by another instance");
        err
    })?;

    let data_dir = PathBuf::from(&config.storage.data_dir);
    std::fs::create_dir_all(&data_dir).map_err(|_| Error::InvalidDataDir)?;

    let consensus_dir = PathBuf::from(&config.storage.consensus_dir);
    std::fs::create_dir_all(&consensus_dir).map_err(|_| Error::InvalidConsensusDir)?;

    let known_peers = config
        .known_peers
        .into_iter()
        .map(|node| {
            (
                PeerId::from(node.peer),
                network::socketaddr_to_multiaddr(node.address),
            )
        })
        .collect::<HashMap<_, _>>();

    let bootstrap_nodes = if !config.bootstrap_nodes.is_empty() {
        let nodes = config
            .bootstrap_nodes
            .into_iter()
            .map(|node| {
                let id = PeerId::from(node);
                let address = known_peers
                    .get(&id)
                    .cloned()
                    .ok_or(Error::PeerAddressMissing)?;

                Ok((id, address))
            })
            .collect::<Result<_, Error>>()?;

        Some(nodes)
    } else {
        None
    };

    let (authorized_clients, authorized_raft_candidates) = if config.authorization.enable {
        (
            Some(HashSet::from_iter(config.authorization.clients)),
            Some(HashSet::from_iter(
                config.authorization.consensus_candidates,
            )),
        )
    } else {
        (None, None)
    };

    // This is a bit hacky because of the limitations imposed by `fork()`. So the
    // idea here is to run normally up to this point, then chek if the internal env
    // variable is set, and if not, initiate forking. The child fork would then
    // spawn a subprocess with the internal env variable set, causing the spawned
    // process to fully skip the fork-check part. The parent process would exit
    // immediately after forking, and the child would wait for the spawned process
    // to finish before exiting.
    if args.detached && !is_detached {
        drop(lockfile);

        // Set the internal env var for the spawned process to skip the fork part.
        set_detached_state();

        // Restore the initial working directory, so that the paths are all relative to
        // it, as it was in the parent process.
        std::env::set_current_dir(initial_wd).unwrap();

        // Reconstruct the command line.
        let args = std::env::args().collect::<Vec<_>>();
        let (_, args) = args.split_first().unwrap();
        let exec = std::env::current_exe().unwrap();

        let mut command = Command::new(exec);

        // Make sure the spawned process doesn't inherit IO handles from the original
        // process.
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .args(args);

        if let fork::Fork::Child = fork::daemon(true, true).map_err(Error::Fork)? {
            let _ = command.spawn();
        }

        std::process::exit(0);
    };

    let rocksdb = create_rocksdb_config(&config.storage.rocksdb);

    let config = node::Config {
        id: PeerId::from_public_key(&config.identity.private_key.public(), config.identity.group),
        keypair: config.identity.private_key,
        addr: network::socketaddr_to_multiaddr(SocketAddr::new(
            config.server.bind_address,
            config.server.server_port,
        )),
        api_addr: network::socketaddr_to_multiaddr(SocketAddr::new(
            config.server.bind_address,
            config.server.client_port,
        )),
        metrics_addr: SocketAddr::new(config.server.bind_address, config.server.metrics_port)
            .to_string(),
        is_raft_member: config.authorization.is_consensus_member,
        bootstrap_nodes,
        known_peers,
        raft_dir: consensus_dir,
        rocksdb_dir: data_dir,
        rocksdb,
        replication_strategy: Strategy::new(
            config.replication.factor,
            config.replication.consistency_level,
        ),
        request_concurrency_limit: config.network.request_concurrency_limit,
        request_limiter_queue: config.network.request_limiter_queue,
        network_connection_timeout: config.network.connection_timeout,
        network_request_timeout: config.network.request_timeout,
        replication_request_timeout: config.network.replication_request_timeout,
        warmup_delay: config.server.warmup_delay,
        authorized_clients,
        authorized_raft_candidates,
        eth_address: config.identity.eth_address,
        smart_contract: config.smart_contract.map(Into::into),
    };

    node::run(node::signal::shutdown_listener()?, &config)
        .await?
        .await;

    Ok(())
}

const DETACH_STATE_ENV_VAR: &str = "IRN_INTERNAL_DETACHED";

fn set_detached_state() {
    std::env::set_var(DETACH_STATE_ENV_VAR, "true");
}

fn get_detached_state() -> bool {
    std::env::var(DETACH_STATE_ENV_VAR).is_ok()
}

fn create_rocksdb_config(raw: &super::config::Rocksdb) -> RocksdbDatabaseConfig {
    let defaults = RocksdbDatabaseConfig::default();

    RocksdbDatabaseConfig {
        num_batch_threads: raw.num_batch_threads.unwrap_or(defaults.num_batch_threads),
        num_callback_threads: raw
            .num_callback_threads
            .unwrap_or(defaults.num_callback_threads),
        max_subcompactions: raw
            .max_subcompactions
            .unwrap_or(defaults.max_subcompactions),
        max_background_jobs: raw
            .max_background_jobs
            .unwrap_or(defaults.max_background_jobs),

        ratelimiter: raw
            .ratelimiter
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.ratelimiter,
                    "rocksdb `ratelimiter` param not set, using default value"
                );
            })
            .unwrap_or(defaults.ratelimiter),

        increase_parallelism: raw
            .increase_parallelism
            .unwrap_or(defaults.increase_parallelism),

        write_buffer_size: raw
            .write_buffer_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.write_buffer_size,
                    "rocksdb `write_buffer_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.write_buffer_size),

        max_write_buffer_number: raw
            .max_write_buffer_number
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.max_write_buffer_number,
                    "rocksdb `max_write_buffer_number` param not set, using default value"
                );
            })
            .unwrap_or(defaults.max_write_buffer_number),

        min_write_buffer_number_to_merge: raw
            .min_write_buffer_number_to_merge
            .unwrap_or(defaults.min_write_buffer_number_to_merge),

        block_cache_size: raw
            .block_cache_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.block_cache_size,
                    "rocksdb `block_cache_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.block_cache_size),

        block_size: raw.block_size.unwrap_or(defaults.block_size),

        row_cache_size: raw
            .row_cache_size
            .tap_none(|| {
                tracing::warn!(
                    default = defaults.row_cache_size,
                    "rocksdb `row_cache_size` param not set, using default value"
                );
            })
            .unwrap_or(defaults.row_cache_size),
    }
}
