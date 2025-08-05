use {
    derive_more::AsRef,
    derive_where::derive_where,
    futures::future::OptionFuture,
    futures_concurrency::future::Join as _,
    metrics_exporter_prometheus::PrometheusHandle,
    std::{
        io,
        net::{Ipv4Addr, SocketAddrV4},
        sync::Arc,
        time::Duration,
    },
    tap::Pipe,
    wcn_cluster::{
        keyspace,
        node_operator,
        smart_contract::{self, evm},
        Cluster,
    },
    wcn_rpc::{
        client2::{self as rpc_client},
        identity::Keypair,
        server2::{self as rpc_server, Server as _, ShutdownSignal},
        PeerId,
    },
    wcn_storage_api::rpc::{client as storage_api_client, server as storage_api_server},
};

mod metrics;

/// Configuration options of a WCN Node.
#[derive(Clone)]
#[derive_where(Debug)]
pub struct Config {
    /// [`Keypair`] of the node.
    pub keypair: Keypair,

    /// Port of the primary WCN RPC server.
    pub primary_rpc_server_port: u16,

    /// Port of the secondary WCN RPC server.
    pub secondary_rpc_server_port: u16,

    /// Port of the Prometheus metrics server.
    pub metrics_server_port: u16,

    /// [`Ipv4Addr`] of the local WCN Database to connect to.
    ///
    /// WCN Databases don't have authorization and MUST not be exposed to the
    /// public internet. So, this address should be `127.0.0.1` or `10.a.b.c`
    /// etc.
    pub database_rpc_server_address: Ipv4Addr,

    /// [`PeerId`] of the local WCN Database to connect to.
    // TODO: We don't need server auth for this, but it's baked into the RPC machinery rn.
    pub database_peer_id: PeerId,

    /// Port of the primary WCN RPC server of the local WCN Database to
    /// connect to.
    pub database_primary_rpc_server_port: u16,

    /// Port of the secondary WCN RPC server of the local WCN Database to
    /// connect to.
    pub database_secondary_rpc_server_port: u16,

    /// Address of the WCN Cluster Smart-Contract.
    pub smart_contract_address: smart_contract::Address,

    /// Signer to be used for the WCN Cluster Smart-Contract calls.
    ///
    /// Only one node within node operator infrastructure is supposed to have
    /// this set.
    pub smart_contract_signer: Option<smart_contract::Signer>,

    /// URL of the Optimism RPC provider.
    pub rpc_provider_url: smart_contract::RpcUrl,

    /// [`ShutdownSignal`] to use for RPC servers.
    pub shutdown_signal: ShutdownSignal,

    /// [`PrometheusHandle`] to use for getting metrics in metrics server.
    #[derive_where(skip)]
    pub prometheus_handle: PrometheusHandle,
}

impl Config {
    fn try_into_app(self) -> Result<AppConfig, ErrorInner> {
        let replica_client_cfg = rpc_client::Config {
            keypair: self.keypair.clone(),
            connection_timeout: Duration::from_secs(2),
            reconnect_interval: Duration::from_millis(100),
            max_concurrent_rpcs: 10000,
            priority: wcn_rpc::transport::Priority::High,
        };

        let replica_low_prio_client_cfg = rpc_client::Config {
            keypair: self.keypair.clone(),
            connection_timeout: Duration::from_secs(10),
            reconnect_interval: Duration::from_secs(1),
            max_concurrent_rpcs: 200,
            priority: wcn_rpc::transport::Priority::Low,
        };

        let database_client_cfg = rpc_client::Config {
            keypair: self.keypair.clone(),
            connection_timeout: Duration::from_secs(1),
            reconnect_interval: Duration::from_millis(100),
            max_concurrent_rpcs: 5000,
            priority: wcn_rpc::transport::Priority::High,
        };

        let database_low_prio_client_cfg = rpc_client::Config {
            keypair: self.keypair.clone(),
            connection_timeout: Duration::from_secs(1),
            reconnect_interval: Duration::from_millis(100),
            max_concurrent_rpcs: 200,
            priority: wcn_rpc::transport::Priority::Low,
        };

        let database_client = storage_api_client::database(database_client_cfg)?;
        let database_low_prio_client = storage_api_client::database(database_low_prio_client_cfg)?;

        Ok(AppConfig {
            replica_client: storage_api_client::replica(replica_client_cfg)?,
            replica_low_prio_client: storage_api_client::replica(replica_low_prio_client_cfg)?,

            database_connection: database_client.new_connection(
                self.database_primary_socket_addr(),
                &self.database_peer_id,
                (),
            ),

            database_low_prio_connection: database_low_prio_client.new_connection(
                self.database_secondary_socket_addr(),
                &self.database_peer_id,
                (),
            ),
        })
    }

    fn database_primary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(
            self.database_rpc_server_address,
            self.primary_rpc_server_port,
        )
    }

    fn database_secondary_socket_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(
            self.database_rpc_server_address,
            self.database_secondary_rpc_server_port,
        )
    }
}

#[derive(Clone)]
struct AppConfig {
    replica_client: storage_api_client::Replica,
    replica_low_prio_client: storage_api_client::Replica,

    database_connection: storage_api_client::DatabaseConnection,
    database_low_prio_connection: storage_api_client::DatabaseConnection,
}

#[derive(AsRef, Clone)]
struct Node {
    #[as_ref]
    peer_id: PeerId,

    #[as_ref]
    replica_connection: storage_api_client::ReplicaConnection,

    replica_low_prio_connection: storage_api_client::ReplicaConnection,
}

impl wcn_cluster::Config for AppConfig {
    type SmartContract = evm::SmartContract;
    type KeyspaceShards = keyspace::Shards;
    type Node = Node;

    fn new_node(&self, _operator_id: node_operator::Id, node: wcn_cluster::Node) -> Self::Node {
        Node {
            peer_id: node.peer_id,
            replica_connection: self.replica_client.new_connection(
                node.primary_socket_addr(),
                &node.peer_id,
                (),
            ),
            replica_low_prio_connection: self.replica_low_prio_client.new_connection(
                node.secondary_socket_addr(),
                &node.peer_id,
                (),
            ),
        }
    }
}

impl wcn_replication::coordinator::Config for AppConfig {
    type OutboundReplicaConnection = storage_api_client::ReplicaConnection;
}

impl wcn_replication::replica::Config for AppConfig {
    type OutboundDatabaseConnection = storage_api_client::DatabaseConnection;
}

impl wcn_migration::manager::Config for AppConfig {
    type OutboundReplicaConnection = storage_api_client::ReplicaConnection;
    type OutboundDatabaseConnection = storage_api_client::DatabaseConnection;

    fn get_replica_connection<'a>(
        &self,
        node: &'a Self::Node,
    ) -> &'a Self::OutboundReplicaConnection {
        &node.replica_low_prio_connection
    }

    fn concurrency(&self) -> usize {
        100
    }
}

pub async fn run(config: Config) -> Result<()> {
    run_(config).await.map_err(Into::into)
}

async fn run_(config: Config) -> Result<(), ErrorInner> {
    let app_cfg = config.clone().try_into_app()?;

    let rpc_provider = if let Some(signer) = config.smart_contract_signer.clone() {
        smart_contract::evm::RpcProvider::new(config.rpc_provider_url.clone(), signer).await
    } else {
        smart_contract::evm::RpcProvider::new_ro(config.rpc_provider_url.clone()).await
    }?;

    let cluster = Cluster::connect(
        app_cfg.clone(),
        &rpc_provider,
        config.smart_contract_address,
    )
    .await?;

    let coordinator = wcn_replication::Coordinator::new(app_cfg.clone(), cluster.clone());

    let replica = wcn_replication::Replica::new(
        Arc::new(app_cfg.clone()),
        cluster.clone(),
        app_cfg.database_connection.clone(),
    );

    let migration_manager = wcn_migration::Manager::new(
        app_cfg.clone(),
        cluster,
        app_cfg.database_low_prio_connection,
    );

    let primary_rpc_server_cfg = wcn_rpc::server2::Config {
        name: "primary",
        port: config.primary_rpc_server_port,
        keypair: config.keypair.clone(),
        connection_timeout: Duration::from_secs(2),
        max_connections: 500,
        max_connections_per_ip: 50,
        max_connection_rate_per_ip: 50,
        max_concurrent_rpcs: 10000,
        priority: wcn_rpc::transport::Priority::High,
        shutdown_signal: config.shutdown_signal.clone(),
    };

    let secondary_rcp_server_cfg = wcn_rpc::server2::Config {
        name: "secondary",
        port: config.secondary_rpc_server_port,
        keypair: config.keypair.clone(),
        connection_timeout: Duration::from_secs(2),
        max_connections: 100,
        max_connections_per_ip: 10,
        max_connection_rate_per_ip: 10,
        max_concurrent_rpcs: 1000,
        priority: wcn_rpc::transport::Priority::Low,
        shutdown_signal: config.shutdown_signal.clone(),
    };

    let primary_rpc_server_fut = storage_api_server::coordinator(coordinator.clone())
        .multiplex(storage_api_server::replica(replica.clone()))
        .serve(primary_rpc_server_cfg)?;

    let secondary_rpc_server_fut = storage_api_server::coordinator(coordinator)
        .multiplex(storage_api_server::replica(replica))
        .serve(secondary_rcp_server_cfg)?;

    let metrics_server_fut = metrics::serve(&config).await?;

    let migration_manager_fut = migration_manager
        .map(|manager| manager.run(async { config.shutdown_signal.wait().await }))
        .pipe(OptionFuture::from);

    (
        primary_rpc_server_fut,
        secondary_rpc_server_fut,
        metrics_server_fut,
        migration_manager_fut,
    )
        .join()
        .await;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] ErrorInner);

#[derive(Debug, thiserror::Error)]
pub enum ErrorInner {
    #[error("RPC client: {0}")]
    RpcClient(#[from] rpc_client::Error),

    #[error("Failed to create Smart-Contract RPC provider: {0}")]
    RpcProvider(#[from] smart_contract::evm::RpcProviderCreationError),

    #[error("Failed to connect to WCN Cluster: {0}")]
    ClusterConnection(#[from] wcn_cluster::ConnectionError),

    #[error("RPC server: {0}")]
    RpcServer(#[from] rpc_server::Error),

    #[error("Metrics server: {0}")]
    MetricsServer(#[from] io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
