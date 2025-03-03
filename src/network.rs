use {
    crate::{cluster, consensus, Config, Error, Node, TypeConfig},
    admin_api::Server as _,
    anyerror::AnyError,
    client_api::Server,
    derive_more::AsRef,
    domain::HASHER,
    futures::{
        future,
        stream::{self, Map},
        Future,
        FutureExt,
        SinkExt,
        Stream,
        StreamExt,
        TryFutureExt,
    },
    libp2p::PeerId,
    metrics_exporter_prometheus::PrometheusHandle,
    pin_project::{pin_project, pinned_drop},
    raft::Raft,
    relay_rocks::{
        self as rocksdb,
        db::{
            cf::DbColumn,
            migration::ExportItem,
            schema::{self, GenericKey},
            types::{
                common::iterators::ScanOptions,
                map::Pair,
                MapStorage as _,
                StringStorage as _,
            },
        },
    },
    std::{
        collections::{HashMap, HashSet},
        fmt::Debug,
        hash::BuildHasher,
        pin::Pin,
        sync::{Arc, RwLock},
        time::Duration,
    },
    tap::Pipe,
    tokio::sync::mpsc,
    wc::metrics::{future_metrics, FutureExt as _},
    wcn::cluster::Consensus,
    wcn_rpc::{
        client::middleware::{MeteredExt as _, WithTimeoutsExt as _},
        middleware::{Metered, Timeouts, WithTimeouts},
        quic::{self, socketaddr_to_multiaddr},
        server::{
            self,
            middleware::{MeteredExt as _, WithTimeoutsExt as _},
            ClientConnectionInfo,
        },
        transport::{
            self,
            BiDirectionalStream,
            NoHandshake,
            PostcardCodec,
            RecvStream,
            SendStream,
        },
        Multiaddr,
        PeerAddr,
        ServerName,
    },
};

pub const REPLICA_API_SERVER_NAME: ServerName = ServerName::new("replica_api");

pub mod rpc {
    pub mod migration {
        use {
            relay_rocks::db::migration::ExportItem,
            serde::{Deserialize, Serialize},
            std::ops::RangeInclusive,
            wcn_rpc as rpc,
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct PullDataRequest {
            pub keyrange: RangeInclusive<u64>,
            pub keyspace_version: u64,
        }

        #[derive(Clone, Debug, thiserror::Error, Serialize, Deserialize)]
        pub enum PullDataError {
            #[error("Keyspace versions of puller and pullee don't match")]
            KeyspaceVersionMismatch,
            #[error("Storage export failed: {0}")]
            StorageExport(String),
            #[error("Puller is not a cluster member")]
            NotClusterMember,
        }

        pub type PullDataResponse = Result<ExportItem, PullDataError>;

        pub type PullData =
            rpc::Streaming<{ rpc::id(b"pull_data") }, PullDataRequest, PullDataResponse>;
    }

    pub mod broadcast {
        use {
            libp2p::PeerId,
            serde::{Deserialize, Serialize},
            wcn_rpc as rpc,
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Message(pub Vec<u8>);

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct PubsubEventPayload {
            pub channel: Vec<u8>,
            pub payload: Vec<u8>,
        }

        pub type Pubsub = rpc::Oneshot<{ rpc::id(b"pubsub") }, PubsubEventPayload>;

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct HeartbeatMessage(pub PeerId);

        pub type Heartbeat = rpc::Oneshot<{ rpc::id(b"heartbeat") }, HeartbeatMessage>;
    }

    pub use health::Health;
    pub mod health {
        use {
            serde::{Deserialize, Serialize},
            wcn_rpc as rpc,
        };

        pub type Request = ();

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct HealthResponse {
            pub node_version: u64,
        }

        pub type Health = rpc::Unary<{ rpc::id(b"health") }, Request, HealthResponse>;
    }

    pub use metrics::Metrics;
    pub mod metrics {
        use wcn_rpc as rpc;

        pub type Request = ();

        pub type Response = String;

        pub type Metrics = rpc::Unary<{ rpc::id(b"metrics") }, Request, Response>;
    }
}

#[derive(AsRef, Clone)]
struct ReplicaApiServer {
    #[as_ref]
    node: Arc<Node>,

    pubsub: Pubsub,

    prometheus: PrometheusHandle,

    config: wcn_rpc::server::Config,
}

impl wcn_rpc::Server for ReplicaApiServer {
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: wcn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        Self::handle_internal_rpc(self, id, stream, conn_info)
    }
}

#[derive(AsRef, Clone)]
struct StorageApiServer {
    #[as_ref]
    node: Arc<Node>,
}

impl StorageApiServer {
    fn string_storage(&self) -> &DbColumn<schema::StringColumn> {
        &self.node.storage().string
    }

    fn map_storage(&self) -> &DbColumn<schema::MapColumn> {
        &self.node.storage().map
    }
}

fn generic_key(key: storage_api::Key) -> GenericKey {
    GenericKey::new(HASHER.hash_one(key.as_bytes()), key.into_bytes())
}

impl storage_api::Server for StorageApiServer {
    fn keyspace_version(&self) -> u64 {
        self.node.consensus().cluster().keyspace_version()
    }

    fn get(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::Record>>> + Send {
        async move { self.string_storage().get(&generic_key(key)).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "get"))
            .map_ok(|opt| {
                opt.map(|rec| storage_api::Record {
                    value: rec.value,
                    expiration: storage_api::EntryExpiration::from_unix_timestamp_secs(
                        rec.expiration,
                    ),
                    version: storage_api::EntryVersion::from_unix_timestamp_micros(rec.version),
                })
            })
            .map_err(storage_api::server::Error::new)
    }

    fn set(
        &self,
        entry: storage_api::Entry,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(entry.key);
        let val = entry.value;
        let exp = entry.expiration.unix_timestamp_secs();
        let ver = entry.version.unix_timestamp_micros();

        async move { self.string_storage().set(&key, &val, exp, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "set"))
            .map_err(storage_api::server::Error::new)
    }

    fn del(
        &self,
        key: storage_api::Key,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(key);
        let ver = version.unix_timestamp_micros();

        async move { self.string_storage().del(&key, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "del"))
            .map_err(storage_api::server::Error::new)
    }

    fn get_exp(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::EntryExpiration>>> + Send
    {
        async move { self.string_storage().exp(&generic_key(key)).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "get_exp"))
            .map(|res| match res {
                Ok(timestamp) => Ok(Some(
                    storage_api::EntryExpiration::from_unix_timestamp_secs(timestamp),
                )),
                Err(rocksdb::Error::EntryNotFound) => Ok(None),
                Err(err) => Err(storage_api::server::Error::new(err)),
            })
    }

    fn set_exp(
        &self,
        key: storage_api::Key,
        expiration: impl Into<storage_api::EntryExpiration>,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(key);
        let exp = expiration.into().unix_timestamp_secs();
        let ver = version.unix_timestamp_micros();

        async move { self.string_storage().setexp(&key, exp, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "set_exp"))
            .map_err(storage_api::server::Error::new)
    }

    fn hget(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::Record>>> + Send {
        async move { self.map_storage().hget(&generic_key(key), &field).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hget"))
            .map_ok(|opt| {
                opt.map(|rec| storage_api::Record {
                    value: rec.value,
                    expiration: storage_api::EntryExpiration::from_unix_timestamp_secs(
                        rec.expiration,
                    ),
                    version: storage_api::EntryVersion::from_unix_timestamp_micros(rec.version),
                })
            })
            .map_err(storage_api::server::Error::new)
    }

    fn hset(
        &self,
        entry: storage_api::MapEntry,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(entry.key);
        let pair = Pair::new(entry.field, entry.value);
        let exp = entry.expiration.unix_timestamp_secs();
        let ver = entry.version.unix_timestamp_micros();

        async move { self.map_storage().hset(&key, &pair, exp, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hset"))
            .map_err(storage_api::server::Error::new)
    }

    fn hdel(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(key);
        let ver = version.unix_timestamp_micros();

        async move { self.map_storage().hdel(&key, &field, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hdel"))
            .map_err(storage_api::server::Error::new)
    }

    fn hget_exp(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::EntryExpiration>>> + Send
    {
        async move { self.map_storage().hexp(&generic_key(key), &field).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hget_exp"))
            .map(|res| match res {
                Ok(timestamp) => Ok(Some(
                    storage_api::EntryExpiration::from_unix_timestamp_secs(timestamp),
                )),
                Err(rocksdb::Error::EntryNotFound) => Ok(None),
                Err(err) => Err(storage_api::server::Error::new(err)),
            })
    }

    fn hset_exp(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
        expiration: impl Into<storage_api::EntryExpiration>,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        let key = generic_key(key);
        let exp = expiration.into().unix_timestamp_secs();
        let ver = version.unix_timestamp_micros();

        async move { self.map_storage().hsetexp(&key, &field, exp, ver).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hset_exp"))
            .map_err(storage_api::server::Error::new)
    }

    fn hcard(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<u64>> + Send {
        let key = generic_key(key);

        async move { self.map_storage().hcard(&key).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hcard"))
            .map_ok(|card| card as u64)
            .map_err(storage_api::server::Error::new)
    }

    fn hscan(
        &self,
        key: storage_api::Key,
        count: u32,
        cursor: Option<storage_api::Field>,
    ) -> impl Future<Output = storage_api::server::Result<storage_api::MapPage>> + Send {
        let key = generic_key(key);
        let opts = ScanOptions::new(count as usize).with_cursor(cursor);

        async move { self.map_storage().hscan(&key, opts).await }
            .with_metrics(future_metrics!("storage_operation", "op_name" => "hscan"))
            .map_ok(|res| storage_api::MapPage {
                records: res
                    .items
                    .into_iter()
                    .map(|rec| storage_api::MapRecord {
                        field: rec.field,
                        value: rec.value,
                        expiration: storage_api::EntryExpiration::from_unix_timestamp_secs(
                            rec.expiration,
                        ),
                        version: storage_api::EntryVersion::from_unix_timestamp_micros(rec.version),
                    })
                    .collect(),
                has_next: res.has_more,
            })
            .map_err(storage_api::server::Error::new)
    }
}

impl ReplicaApiServer {
    async fn handle_internal_rpc(
        &self,
        id: wcn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ClientConnectionInfo<Self>,
    ) {
        let peer_id = &conn_info.peer_id;

        let _ = match id {
            rpc::migration::PullData::ID => {
                rpc::migration::PullData::handle(stream, |rx, tx| {
                    self.handle_pull_data(peer_id, rx, tx)
                })
                .await
            }

            rpc::broadcast::Pubsub::ID => {
                rpc::broadcast::Pubsub::handle(stream, |evt| async { self.pubsub.publish(evt) })
                    .await
            }

            rpc::broadcast::Heartbeat::ID => {
                rpc::broadcast::Heartbeat::handle(stream, |_heartbeat| async {}).await
            }

            rpc::Health::ID => {
                rpc::Health::handle(stream, |_req| async {
                    Ok(rpc::health::HealthResponse {
                        node_version: crate::NODE_VERSION,
                    })
                })
                .await
            }

            rpc::Metrics::ID => {
                rpc::Metrics::handle(stream, |_req| async { Ok(self.prometheus.render()) }).await
            }

            id => {
                return tracing::warn!(
                    "Unexpected internal RPC: {}",
                    wcn_rpc::Name::new(id).as_str()
                )
            }
        }
        .map_err(|err| {
            tracing::debug!(
                name = wcn_rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle internal RPC"
            )
        });
    }
}

impl ReplicaApiServer {
    async fn handle_pull_data(
        &self,
        peer_id: &libp2p::PeerId,
        mut rx: RecvStream<rpc::migration::PullDataRequest>,
        mut tx: SendStream<wcn_rpc::Result<rpc::migration::PullDataResponse>>,
    ) -> server::Result<()> {
        tx.set_low_priority();

        let req = rx.recv_message().await?;

        let resp = self
            .node
            .migration_manager()
            .handle_pull_request(peer_id, req.keyrange, req.keyspace_version)
            .await;

        match resp {
            Ok(data) => tx.send_all(&mut data.map(Ok).map(Ok).map(Ok)).await?,
            Err(e) => tx.send(Ok(Err(e.into()))).await?,
        };

        Ok(())
    }
}

#[derive(Clone)]
struct ClientApiServer {
    node: Arc<Node>,
    pubsub: Pubsub,
}

impl client_api::Server for ClientApiServer {
    fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> impl Future<Output = ()> + Send {
        let evt = rpc::broadcast::PubsubEventPayload {
            channel,
            payload: message,
        };

        self.pubsub.publish(evt.clone());

        let cluster = self.node.consensus().cluster();

        async move {
            stream::iter(cluster.nodes().filter(|n| &n.id != self.node.id()))
                .for_each_concurrent(None, |node| async {
                    let addr = PeerAddr::new(node.id, node.addr.clone());

                    rpc::broadcast::Pubsub::send(
                        &self.node.network().replica_api_client,
                        &addr,
                        &evt,
                    )
                    .map(drop)
                    .await
                })
                .await
        }
    }

    fn subscribe(
        &self,
        channels: HashSet<Vec<u8>>,
    ) -> impl Future<
        Output = client_api::server::Result<
            impl stream::Stream<Item = client_api::SubscriptionEvent> + Send + 'static,
        >,
    > + Send {
        self.pubsub
            .subscribe(channels)
            .map(|payload| client_api::SubscriptionEvent {
                channel: payload.channel,
                message: payload.payload,
            })
            .pipe(future::ok)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PullDataError {
    #[error(transparent)]
    Transport(#[from] transport::Error),

    #[error(transparent)]
    Rpc(rpc::migration::PullDataError),
}

impl wcn::migration::Network<cluster::Node> for Network {
    type DataStream = Map<
        RecvStream<wcn_rpc::Result<rpc::migration::PullDataResponse>>,
        fn(
            transport::Result<wcn_rpc::Result<rpc::migration::PullDataResponse>>,
        ) -> transport::Result<ExportItem>,
    >;

    fn pull_keyrange(
        &self,
        from: &cluster::Node,
        range: std::ops::RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<Self::DataStream, impl wcn::migration::AnyError>> + Send {
        async move {
            let range = &range;

            rpc::migration::PullData::send(
                &self.replica_api_client,
                &from.addr(),
                &move |mut tx, rx| async move {
                    tx.send(rpc::migration::PullDataRequest {
                        keyrange: range.clone(),
                        keyspace_version,
                    })
                    .await?;

                    // flatten error by converting the inner ones into the outer `io::Error`
                    fn flatten_err(
                        res: transport::Result<wcn_rpc::Result<rpc::migration::PullDataResponse>>,
                    ) -> transport::Result<ExportItem> {
                        match res {
                            Ok(Ok(Ok(item))) => Ok(item),
                            Ok(Ok(Err(err))) => Err(transport::Error::Other(
                                wcn::migration::PullKeyrangeError::from(err).to_string(),
                            )),
                            Ok(Err(err)) => Err(transport::Error::Other(err.to_string())),
                            Err(err) => Err(err),
                        }
                    }
                    let rx = rx.map(flatten_err as _);

                    Ok(rx)
                },
            )
            .await
            .map_err(|e| AnyError::new(&e))
        }
    }
}

#[derive(Clone)]
struct RaftRpcServer {
    raft: consensus::Raft,
}

impl raft_api::Server<TypeConfig> for RaftRpcServer {
    fn is_member(&self, peer_id: &PeerId) -> bool {
        self.raft.is_member(peer_id)
    }

    fn add_member(
        &self,
        peer_id: &PeerId,
        req: consensus::AddMemberRequest,
    ) -> impl Future<Output = consensus::AddMemberResult> + Send {
        self.raft.add_member(peer_id, req)
    }

    fn remove_member(
        &self,
        peer_id: &PeerId,
        req: consensus::RemoveMemberRequest,
    ) -> impl Future<Output = consensus::RemoveMemberResult> + Send {
        self.raft.remove_member(peer_id, req)
    }

    fn propose_change(
        &self,
        req: consensus::ProposeChangeRequest,
    ) -> impl Future<Output = consensus::ProposeChangeResult> + Send {
        self.raft.propose_change(req)
    }

    fn append_entries(
        &self,
        req: consensus::AppendEntriesRequest,
    ) -> impl Future<Output = consensus::AppendEntriesResult> + Send {
        self.raft.append_entries(req)
    }

    fn install_snapshot(
        &self,
        req: consensus::InstallSnapshotRequest,
    ) -> impl Future<Output = consensus::InstallSnapshotResult> + Send {
        self.raft.install_snapshot(req)
    }

    fn vote(
        &self,
        req: consensus::VoteRequest,
    ) -> impl Future<Output = consensus::VoteResult> + Send {
        self.raft.vote(req)
    }
}

#[derive(Clone)]
struct AdminApiServer {
    node: Arc<Node>,
    eth_address: Option<Arc<str>>,
}

impl admin_api::Server for AdminApiServer {
    fn get_cluster_view(&self) -> impl Future<Output = admin_api::ClusterView> + Send {
        use admin_api::NodeState;

        let cluster = self.node.consensus().cluster();

        let nodes = cluster
            .nodes()
            .filter_map(|node| {
                let state = match cluster.node_state(&node.id)? {
                    cluster::NodeState::Pulling(_) => NodeState::Pulling,
                    cluster::NodeState::Normal => NodeState::Normal,
                    cluster::NodeState::Restarting => NodeState::Restarting,
                    cluster::NodeState::Decommissioning => NodeState::Decommissioning,
                };
                let node = admin_api::Node {
                    id: node.id,
                    state,
                    addr: node.addr.clone(),
                    region: match node.region {
                        cluster::NodeRegion::Eu => admin_api::NodeRegion::Eu,
                        cluster::NodeRegion::Us => admin_api::NodeRegion::Us,
                        cluster::NodeRegion::Ap => admin_api::NodeRegion::Ap,
                    },
                    organization: node.organization.clone(),
                    eth_address: node.eth_address.clone(),
                };
                Some((node.id, node))
            })
            .collect();

        future::ready(admin_api::ClusterView {
            nodes,
            cluster_version: cluster.version(),
            keyspace_version: cluster.keyspace_version(),
        })
    }

    fn get_node_status(
        &self,
    ) -> impl Future<Output = admin_api::server::GetNodeStatusResult> + Send {
        async {
            Ok(admin_api::NodeStatus {
                node_version: crate::NODE_VERSION,
                eth_address: self.eth_address.as_ref().map(|s| s.to_string()),
            })
        }
    }

    fn decommission_node(
        &self,
        id: PeerId,
        force: bool,
    ) -> impl Future<Output = admin_api::server::DecommissionNodeResult> + Send {
        use {admin_api::DecommissionNodeError as Error, cluster::NodeState};

        let consensus = self.node.consensus();
        let cluster = consensus.cluster();

        let decommission_fut = async move {
            // Regular nodes are only allowed to decommission themselves, voter nodes can
            // decommission any node.
            let is_allowed = &id == self.node.id() || consensus.is_voter(self.node.id());
            if !is_allowed {
                return Err(Error::NotAllowed);
            }

            let (Some(node), Some(node_state)) = (cluster.node(&id), cluster.node_state(&id))
            else {
                return Ok(());
            };

            // If node is stuck in some intermediate state, restore it to `Normal`, then
            // decommission.

            match node_state {
                NodeState::Pulling(_) if force => {
                    consensus
                        .complete_pull(&node.id, cluster.keyspace_version())
                        .await
                }
                NodeState::Restarting if force => consensus.startup_node(node).await,

                NodeState::Pulling(_) => return Err(Error::Pulling),
                NodeState::Restarting => return Err(Error::Restarting),
                NodeState::Decommissioning => return Ok(()),

                NodeState::Normal => Ok(()),
            }
            .map_err(|err| Error::Consensus(err.to_string()))?;

            consensus
                .decommission_node(&id)
                .await
                .map_err(|err| Error::Consensus(err.to_string()))?;

            Ok(())
        };

        let consensus = self.node.consensus();

        let remove_raft_member_fut = async move {
            if !consensus.is_member(&id) {
                return Ok(());
            }

            consensus
                .remove_member(&id, consensus::RemoveMemberRequest {
                    node_id: consensus::NodeId(id),
                    is_learner: !consensus.is_voter(&id),
                })
                .await
                .map(drop)
                .map_err(|err| Error::Consensus(err.to_string()))
        };

        decommission_fut.and_then(|_| remove_raft_member_fut)
    }

    #[allow(unused)]
    fn memory_profile(
        &self,
        duration: Duration,
    ) -> impl Future<Output = admin_api::server::MemoryProfileResult> + Send {
        use admin_api::MemoryProfileError as Error;

        async move {
            #[cfg(feature = "memory_profiler")]
            {
                use {
                    admin_api::{snap, MemoryProfile},
                    std::io::Write,
                };

                if duration.is_zero() || duration > admin_api::MEMORY_PROFILE_MAX_DURATION {
                    return Err(Error::Duration);
                }

                let profile = wc::alloc::profiler::record(duration)
                    .await
                    .map_err(|_| Error::AlreadyProfiling)?;

                let mut writer = snap::write::FrameEncoder::new(Vec::new());
                writer
                    .write(profile.as_bytes())
                    .map_err(|_| Error::Compression)?;

                Ok(MemoryProfile {
                    dhat: writer.into_inner().map_err(|_| Error::Compression)?,
                })
            }

            #[cfg(not(feature = "memory_profiler"))]
            {
                Err(Error::ProfilerNotAvailable)
            }
        }
    }
}

pub type Client = Metered<WithTimeouts<quic::Client>>;

/// Network adapter.
#[derive(Debug, Clone)]
pub struct Network {
    pub local_id: PeerId,
    pub replica_api_client: Client,
}

impl Network {
    pub fn new(cfg: &Config) -> Result<Self, Error> {
        // Set timeouts for everything except `PullData`.
        let rpc_timeouts = Timeouts::new()
            .with_default(Duration::from_millis(cfg.network_request_timeout))
            .with::<{ rpc::migration::PullData::ID }>(None);

        let known_peers = cfg
            .known_peers
            .iter()
            .map(|(id, addr)| PeerAddr::new(*id, addr.clone()))
            .collect();

        Ok(Self {
            local_id: cfg.id,
            replica_api_client: quic::Client::new(wcn_rpc::client::Config {
                keypair: cfg.keypair.clone(),
                known_peers,
                handshake: NoHandshake,
                connection_timeout: Duration::from_millis(cfg.network_connection_timeout),
                server_name: REPLICA_API_SERVER_NAME,
                priority: transport::Priority::High,
            })?
            .with_timeouts(rpc_timeouts)
            .metered(),
        })
    }

    pub fn spawn_raft_server(
        cfg: &Config,
        server_addr: Multiaddr,
        raft: consensus::Raft,
    ) -> Result<tokio::task::JoinHandle<()>, quic::Error> {
        let quic_server_config = wcn_rpc::quic::server::Config {
            name: "raft_api",
            addr: server_addr,
            keypair: cfg.keypair.clone(),
            max_concurrent_connections: 500,
            max_concurrent_streams: 1000,
            priority: transport::Priority::High,
        };

        let server = RaftRpcServer { raft };

        let operation_timeout = Duration::from_secs(2);

        let server =
            raft_api::Server::<TypeConfig>::into_rpc_server(server, raft_api::server::Config {
                operation_timeout,
            });

        wcn_rpc::quic::server::run(server, quic_server_config).map(tokio::spawn)
    }

    pub fn spawn_servers(
        cfg: &Config,
        node: Node,
        prometheus: PrometheusHandle,
    ) -> Result<tokio::task::JoinHandle<()>, Error> {
        let replica_api_server_config = wcn_rpc::server::Config {
            name: const { wcn_rpc::ServerName::new("replica_api") },
            handshake: NoHandshake,
        };
        let replica_api_quic_server_config = wcn_rpc::quic::server::Config {
            name: "replica_api",
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.replica_api_server_port)),
            keypair: cfg.keypair.clone(),
            max_concurrent_connections: cfg.replica_api_max_concurrent_connections,
            max_concurrent_streams: cfg.replica_api_max_concurrent_rpcs,
            priority: transport::Priority::High,
        };

        let default_timeout = Duration::from_millis(cfg.network_request_timeout);

        let admin_api_server_config = admin_api::server::Config {
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.admin_api_server_port)),
            keypair: cfg.keypair.clone(),
            operation_timeout: default_timeout,
            authorized_clients: cfg.authorized_admin_api_clients.clone(),
        };

        let rpc_timeouts = Timeouts::new()
            .with_default(default_timeout)
            .with::<{ rpc::migration::PullData::ID }>(None);

        let node = Arc::new(node);

        let pubsub = Pubsub::new();

        let replica_api_server = ReplicaApiServer {
            node: node.clone(),
            pubsub: pubsub.clone(),
            prometheus,
            config: replica_api_server_config,
        }
        .with_timeouts(rpc_timeouts)
        .metered();

        let client_api_cfg = client_api::server::Config {
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.client_api_server_port)),
            keypair: cfg.keypair.clone(),
            operation_timeout: default_timeout,
            authorized_clients: cfg.authorized_clients.clone(),
            network_id: cfg.network_id.clone(),
            cluster_view: node.consensus().cluster_view().clone(),
            max_concurrent_connections: cfg.client_api_max_concurrent_connections,
            max_concurrent_streams: cfg.client_api_max_concurrent_rpcs,
        };

        let client_api_server = ClientApiServer {
            node: node.clone(),
            pubsub,
        }
        .serve(client_api_cfg)?;

        let admin_api_server = AdminApiServer {
            node: node.clone(),
            eth_address: cfg.eth_address.clone().map(Into::into),
        }
        .serve(admin_api_server_config)?;

        let storage_api_server = storage_api::Server::into_rpc_server(
            StorageApiServer { node: node.clone() },
            storage_api::server::Config {
                operation_timeout: default_timeout,
                authenticator: StorageApiAuthenticator {
                    network_id: cfg.network_id.clone().into(),
                    node,
                },
            },
        );

        let replica_and_storage_api_servers = wcn_rpc::quic::server::multiplex(
            (replica_api_server, storage_api_server),
            replica_api_quic_server_config,
        )?;

        let echo_server = echo_api::server::spawn(echo_api::server::Config {
            address: (cfg.server_addr, cfg.replica_api_server_port).into(),
            max_connections: 512,
            max_rate: std::num::NonZeroU32::new(5).unwrap(), // Safe unwrap, obviously.
        });

        Ok(async move {
            tokio::join!(
                replica_and_storage_api_servers,
                client_api_server,
                admin_api_server,
                echo_server
            )
        }
        .map(drop)
        .pipe(tokio::spawn))
    }
}

#[derive(Clone, Debug, Default)]
pub struct Pubsub {
    inner: Arc<RwLock<PubSubInner>>,
}

#[derive(Debug, Default)]
struct PubSubInner {
    next_id: u64,
    subscribers: HashMap<u64, Subscriber>,
}

impl Pubsub {
    pub fn new() -> Self {
        Self::default()
    }

    fn subscribe(&self, channels: HashSet<Vec<u8>>) -> Subscription {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let mut inner = self.inner.write().unwrap();

        let (tx, rx) = mpsc::channel(512);

        let subscription_id = inner.next_id;
        let _ = inner
            .subscribers
            .insert(subscription_id, Subscriber { channels, tx });

        inner.next_id += 1;

        Subscription {
            id: subscription_id,
            pubsub: self.clone(),
            rx,
        }
    }

    fn publish(&self, evt: rpc::broadcast::PubsubEventPayload) {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let inner = self.inner.read().unwrap();

        for sub in inner.subscribers.values() {
            if sub.channels.contains(&evt.channel) {
                match sub.tx.try_send(evt.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        metrics::counter!("wcn_pubsub_channel_full").increment(1)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        metrics::counter!("wcn_pubsub_channel_closed").increment(1)
                    }
                };
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Subscriber {
    channels: HashSet<Vec<u8>>,
    tx: mpsc::Sender<rpc::broadcast::PubsubEventPayload>,
}

#[pin_project(PinnedDrop)]
struct Subscription {
    id: u64,
    pubsub: Pubsub,

    #[pin]
    rx: mpsc::Receiver<rpc::broadcast::PubsubEventPayload>,
}

impl Stream for Subscription {
    type Item = rpc::broadcast::PubsubEventPayload;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().rx.poll_recv(cx)
    }
}

#[pinned_drop]
impl PinnedDrop for Subscription {
    fn drop(self: Pin<&mut Self>) {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let _ = self
            .pubsub
            .inner
            .write()
            .unwrap()
            .subscribers
            .remove(&self.id);
    }
}

impl From<wcn::migration::PullKeyrangeError> for rpc::migration::PullDataError {
    fn from(err: wcn::migration::PullKeyrangeError) -> Self {
        use wcn::migration::PullKeyrangeError as E;

        match err {
            E::KeyspaceVersionMismatch => Self::KeyspaceVersionMismatch,
            E::StorageExport(s) => Self::StorageExport(s),
            E::NotClusterMember => Self::NotClusterMember,
        }
    }
}

impl From<rpc::migration::PullDataError> for wcn::migration::PullKeyrangeError {
    fn from(err: rpc::migration::PullDataError) -> Self {
        use rpc::migration::PullDataError as E;

        match err {
            E::KeyspaceVersionMismatch => Self::KeyspaceVersionMismatch,
            E::StorageExport(s) => Self::StorageExport(s),
            E::NotClusterMember => Self::NotClusterMember,
        }
    }
}

#[derive(Clone)]
struct StorageApiAuthenticator {
    network_id: Arc<str>,
    node: Arc<Node>,
}

impl storage_api::server::Authenticator for StorageApiAuthenticator {
    fn is_authorized_token_issuer(&self, peer_id: PeerId) -> bool {
        self.node.consensus().cluster().contains_node(&peer_id)
    }

    fn network_id(&self) -> &str {
        &self.network_id
    }
}
