pub use network::{Multiaddr, Multihash};
use {
    crate::{
        cluster,
        consensus,
        storage::{self, Cursor, Storage, Value},
        Config,
        Error,
        Node,
    },
    anyerror::AnyError,
    api::{auth, server::HandshakeData},
    derive_more::AsRef,
    futures::{
        future,
        stream::{Map, Peekable},
        Future,
        FutureExt,
        SinkExt,
        StreamExt,
        TryFutureExt,
    },
    irn::{
        cluster::Consensus,
        replication::{self, CoordinatorError, ReplicaError, ReplicaResponse, StorageOperation},
    },
    libp2p::PeerId,
    metrics_exporter_prometheus::PrometheusHandle,
    network::{
        inbound::{self, ConnectionInfo},
        outbound,
        rpc::Send as _,
        BiDirectionalStream,
        Metered,
        MeteredExt as _,
        NoHandshake,
        RecvStream,
        Rpc as _,
        SendStream,
        WithTimeouts,
        WithTimeoutsExt as _,
    },
    raft::Raft,
    relay_rocks::{db::migration::ExportItem, util::timestamp_micros, StorageError},
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        fmt::{self, Debug},
        io,
        net::SocketAddr,
        pin::Pin,
        sync::{Arc, RwLock},
        time::Duration,
    },
    tap::Pipe,
    tokio::sync::mpsc,
};

pub mod rpc {
    pub use network::rpc::{Id, Name, Result, Send, Timeouts};

    pub mod raft {
        use {crate::consensus::*, network::rpc};

        pub type AddMember =
            rpc::Unary<{ rpc::id(b"add_member") }, AddMemberRequest, AddMemberResult>;

        pub type RemoveMember =
            rpc::Unary<{ rpc::id(b"remove_member") }, RemoveMemberRequest, RemoveMemberResult>;

        pub type ProposeChange =
            rpc::Unary<{ rpc::id(b"propose_change") }, ProposeChangeRequest, ProposeChangeResult>;

        pub type AppendEntries =
            rpc::Unary<{ rpc::id(b"append_entries") }, AppendEntriesRequest, AppendEntriesResult>;

        pub type InstallSnapshot = rpc::Unary<
            { rpc::id(b"install_snapshot") },
            InstallSnapshotRequest,
            InstallSnapshotResult,
        >;

        pub type Vote = rpc::Unary<{ rpc::id(b"vote") }, VoteRequest, VoteResult>;
    }

    pub mod replica {
        use {
            crate::storage,
            irn::replication::{ReplicaError, StorageOperation},
            network::rpc,
            relay_rocks::StorageError,
            serde::{Deserialize, Serialize},
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Request<Op> {
            pub operation: Op,
            pub keyspace_version: u64,
        }

        pub type Result<T> = std::result::Result<T, ReplicaError<StorageError>>;

        pub type Rpc<const ID: rpc::Id, Op> =
            rpc::Unary<ID, Request<Op>, Result<<Op as StorageOperation>::Output>>;

        pub type Get = Rpc<{ rpc::id(b"r_get") }, storage::Get>;
        pub type Set = Rpc<{ rpc::id(b"r_set") }, storage::Set>;
        pub type Del = Rpc<{ rpc::id(b"r_del") }, storage::Del>;
        pub type GetExp = Rpc<{ rpc::id(b"r_get_exp") }, storage::GetExp>;
        pub type SetExp = Rpc<{ rpc::id(b"r_set_exp") }, storage::SetExp>;

        pub type HGet = Rpc<{ rpc::id(b"r_hget") }, storage::HGet>;
        pub type HSet = Rpc<{ rpc::id(b"r_hset") }, storage::HSet>;
        pub type HDel = Rpc<{ rpc::id(b"r_hdel") }, storage::HDel>;
        pub type HGetExp = Rpc<{ rpc::id(b"r_hget_exp") }, storage::HGetExp>;
        pub type HSetExp = Rpc<{ rpc::id(b"r_hset_exp") }, storage::HSetExp>;

        pub type HCard = Rpc<{ rpc::id(b"r_hcard") }, storage::HCard>;
        pub type HFields = Rpc<{ rpc::id(b"r_hfields") }, storage::HFields>;
        pub type HVals = Rpc<{ rpc::id(b"r_hvals") }, storage::HVals>;
        pub type HScan = Rpc<{ rpc::id(b"r_hscan") }, storage::HScan>;
    }

    pub mod migration {
        use {
            network::rpc,
            relay_rocks::db::migration::ExportItem,
            serde::{Deserialize, Serialize},
            std::ops::RangeInclusive,
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
            network::rpc,
            serde::{Deserialize, Serialize},
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Message(pub Vec<u8>);

        pub type Pubsub = rpc::Oneshot<{ rpc::id(b"pubsub") }, api::PubsubEventPayload>;

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct HeartbeatMessage(pub PeerId);

        pub type Heartbeat = rpc::Oneshot<{ rpc::id(b"heartbeat") }, HeartbeatMessage>;
    }

    pub use health::Health;
    pub mod health {
        use {
            network::rpc,
            serde::{Deserialize, Serialize},
        };

        pub type Request = ();

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Response {
            pub node_version: u64,
            pub eth_address: Option<String>,
        }

        pub type Health = rpc::Unary<{ rpc::id(b"health") }, Request, Response>;
    }

    pub use metrics::Metrics;
    pub mod metrics {
        use network::rpc;

        pub type Request = ();

        pub type Response = String;

        pub type Metrics = rpc::Unary<{ rpc::id(b"metrics") }, Request, Response>;
    }

    pub mod admin {
        use {
            libp2p::PeerId,
            network::rpc,
            serde::{Deserialize, Serialize},
            std::collections::HashMap,
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct ClusterView {
            pub nodes: HashMap<PeerId, Node>,
            pub cluster_version: u128,
            pub keyspace_version: u64,
        }

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Node {
            pub id: PeerId,
            pub state: NodeState,
        }

        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        pub enum NodeState {
            Pulling,
            Normal,
            Restarting,
            Decommissioning,
        }

        #[derive(Clone, Debug, thiserror::Error, Serialize, Deserialize)]
        pub enum GetClusterViewError {
            #[error("Unauthorized")]
            Unauthorized,
        }

        pub type GetClusterViewResponse = Result<ClusterView, GetClusterViewError>;

        pub type GetClusterView =
            rpc::Unary<{ rpc::id(b"get_cluster_view") }, (), GetClusterViewResponse>;
    }
}

#[derive(AsRef, Clone)]
struct RpcHandler {
    #[as_ref]
    node: Arc<Node>,
    rpc_timeouts: rpc::Timeouts,

    pubsub: Pubsub,

    eth_address: Option<Arc<str>>,

    prometheus: Option<PrometheusHandle>,
}

impl RpcHandler {
    fn node(&self) -> &Node {
        &self.node
    }
}

impl inbound::RpcHandler<HandshakeData> for RpcHandler {
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<HandshakeData>,
    ) -> impl Future<Output = ()> + Send {
        Self::handle_coordinator_rpc(self, id, stream, conn_info)
    }
}

impl inbound::RpcHandler for RpcHandler {
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        Self::handle_internal_rpc(self, id, stream, conn_info)
    }
}

fn prepare_key(key: api::Key, conn_info: &ConnectionInfo<HandshakeData>) -> api::Result<Vec<u8>> {
    if let Some(namespace) = &key.namespace {
        if !conn_info.handshake_data.namespaces.contains(namespace) {
            return Err(api::Error::Unauthorized);
        }
    }

    Ok(namespaced_key(key))
}

// Creates the internal representation of the data key by combining namespace
// information with the user key etc.
pub fn namespaced_key(key: api::Key) -> storage::Key {
    #[repr(u8)]
    enum KeyKind {
        Shared = 0,
        Private = 1,
    }

    let prefix_len = if key.namespace.is_some() {
        auth::PUBLIC_KEY_LEN
    } else {
        0
    };

    let mut data = Vec::with_capacity(1 + prefix_len + key.bytes.len());

    if let Some(namespace) = key.namespace {
        data.push(KeyKind::Private as u8);
        data.extend_from_slice(namespace.as_ref());
    } else {
        data.push(KeyKind::Shared as u8);
    };

    data.extend_from_slice(&key.bytes);
    data
}

impl RpcHandler {
    async fn handle_coordinator_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<HandshakeData>,
    ) {
        let stream = stream.with_timeouts(self.rpc_timeouts).metered();
        let client_id = &conn_info.peer_id;
        let coordinator = self.node().coordinator();

        let _ = match id {
            api::rpc::Get::ID => {
                api::rpc::Get::handle(stream, |req| async {
                    let op = storage::Get {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::Set::ID => {
                api::rpc::Set::handle(stream, |req| async move {
                    let op = storage::Set {
                        key: prepare_key(req.key, conn_info)?,
                        value: req.value,
                        expiration: req.expiration,
                        version: timestamp_micros(),
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::Del::ID => {
                api::rpc::Del::handle(stream, |req| async {
                    let op = storage::Del {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::GetExp::ID => {
                api::rpc::GetExp::handle(stream, |req| async {
                    let op = storage::GetExp {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::SetExp::ID => {
                api::rpc::SetExp::handle(stream, |req| async move {
                    let op = storage::SetExp {
                        key: prepare_key(req.key, conn_info)?,
                        expiration: req.expiration,
                        version: timestamp_micros(),
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HGet::ID => {
                api::rpc::HGet::handle(stream, |req| async {
                    let op = storage::HGet {
                        key: prepare_key(req.key, conn_info)?,
                        field: req.field,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HSet::ID => {
                api::rpc::HSet::handle(stream, |req| async move {
                    let op = storage::HSet {
                        key: prepare_key(req.key, conn_info)?,
                        field: req.field,
                        value: req.value,
                        expiration: req.expiration,
                        version: timestamp_micros(),
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HDel::ID => {
                api::rpc::HDel::handle(stream, |req| async {
                    let op = storage::HDel {
                        key: prepare_key(req.key, conn_info)?,
                        field: req.field,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HGetExp::ID => {
                api::rpc::HGetExp::handle(stream, |req| async {
                    let op = storage::HGetExp {
                        key: prepare_key(req.key, conn_info)?,
                        field: req.field,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HSetExp::ID => {
                api::rpc::HSetExp::handle(stream, |req| async move {
                    let op = storage::HSetExp {
                        key: prepare_key(req.key, conn_info)?,
                        field: req.field,
                        expiration: req.expiration,
                        version: timestamp_micros(),
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HCard::ID => {
                api::rpc::HCard::handle(stream, |req| async {
                    let op = storage::HCard {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HFields::ID => {
                api::rpc::HFields::handle(stream, |req| async {
                    let op = storage::HFields {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HVals::ID => {
                api::rpc::HVals::handle(stream, |req| async {
                    let op = storage::HVals {
                        key: prepare_key(req.key, conn_info)?,
                    };

                    coordinator.replicate(client_id, op).map(api_result).await
                })
                .await
            }
            api::rpc::HScan::ID => {
                api::rpc::HScan::handle(stream, |req| async move {
                    let op = storage::HScan {
                        key: prepare_key(req.key, conn_info)?,
                        count: req.count,
                        cursor: req.cursor,
                    };

                    // Client only needs `(Vec<Value>, Option<Cursor>)`, however storage
                    // basically returns `Vec<(Cursor, Value)>`.
                    // TODO: Change storage return type after client api migration is done.
                    #[derive(Debug)]
                    struct Page(irn::replication::Page<(Cursor, Value)>);

                    #[allow(clippy::from_over_into)]
                    impl Into<(Vec<Value>, Option<Cursor>)> for Page {
                        fn into(self) -> (Vec<Value>, Option<Cursor>) {
                            let cursor = self.0.items.last().map(|t| t.0.clone());
                            let items = self.0.items.into_iter().map(|t| t.1).collect();
                            (items, cursor)
                        }
                    }

                    coordinator
                        .replicate(client_id, op)
                        .map_ok(|res| res.map(Page))
                        .map(api_result)
                        .await
                })
                .await
            }

            api::rpc::Publish::ID => {
                api::rpc::Publish::handle(stream, |req| self.handle_pubsub_publish(req)).await
            }

            api::rpc::Subscribe::ID => {
                api::rpc::Subscribe::handle(stream, |tx, rx| {
                    self.handle_pubsub_subscribe(tx, rx, conn_info)
                })
                .await
            }

            id => {
                return tracing::warn!(
                    name = rpc::Name::new(id).as_str(),
                    "Unexpected coordinator RPC"
                )
            }
        }
        .map_err(|err| {
            tracing::debug!(
                name = rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle coordinator RPC"
            )
        });
    }

    async fn handle_internal_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) {
        let stream = stream.with_timeouts(self.rpc_timeouts).metered();
        let peer_id = &conn_info.peer_id;
        let replica = self.node().replica();

        let _ = match id {
            rpc::replica::Get::ID => {
                rpc::replica::Get::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::Set::ID => {
                rpc::replica::Set::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::Del::ID => {
                rpc::replica::Del::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::GetExp::ID => {
                rpc::replica::GetExp::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::SetExp::ID => {
                rpc::replica::SetExp::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HGet::ID => {
                rpc::replica::HGet::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HSet::ID => {
                rpc::replica::HSet::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HDel::ID => {
                rpc::replica::HDel::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HGetExp::ID => {
                rpc::replica::HGetExp::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HSetExp::ID => {
                rpc::replica::HSetExp::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HCard::ID => {
                rpc::replica::HCard::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HFields::ID => {
                rpc::replica::HFields::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HVals::ID => {
                rpc::replica::HVals::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }
            rpc::replica::HScan::ID => {
                rpc::replica::HScan::handle(stream, |req| {
                    replica.handle_replication(peer_id, req.operation, req.keyspace_version)
                })
                .await
            }

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
                    rpc::health::Response {
                        node_version: crate::NODE_VERSION,
                        eth_address: self.eth_address.as_ref().map(|s| s.to_string()),
                    }
                })
                .await
            }

            rpc::Metrics::ID => {
                rpc::Metrics::handle(stream, |_req| async {
                    self.prometheus
                        .as_ref()
                        .map(|p| p.render())
                        .unwrap_or_default()
                })
                .await
            }

            id => {
                return tracing::warn!("Unexpected internal RPC: {}", rpc::Name::new(id).as_str())
            }
        }
        .map_err(|err| {
            tracing::debug!(
                name = rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle internal RPC"
            )
        });
    }

    async fn handle_pubsub_publish(&self, req: api::Publish) {
        let evt = api::PubsubEventPayload {
            channel: req.channel,
            payload: req.message,
        };

        self.pubsub.publish(evt.clone());

        self.node().network().broadcast_pubsub(evt).await;
    }

    async fn handle_pubsub_subscribe(
        &self,
        mut rx: RecvStream<api::Subscribe>,
        mut tx: SendStream<api::PubsubEventPayload>,
        conn_info: &ConnectionInfo<HandshakeData>,
    ) -> rpc::Result<()> {
        let req = rx.recv_message().await?;

        let mut subscription = self
            .pubsub
            .subscribe(conn_info.remote_address, req.channels);

        while let Some(msg) = subscription.rx.recv().await {
            tx.send(msg).await?;
        }

        Ok(())
    }

    async fn handle_pull_data(
        &self,
        peer_id: &libp2p::PeerId,
        mut rx: RecvStream<rpc::migration::PullDataRequest>,
        mut tx: SendStream<rpc::migration::PullDataResponse>,
    ) -> rpc::Result<()> {
        let req = rx.recv_message().await?;

        let resp = self
            .node()
            .migration_manager()
            .handle_pull_request(peer_id, req.keyrange, req.keyspace_version)
            .await;

        match resp {
            Ok(data) => tx.send_all(&mut data.map(Ok).map(Ok)).await?,
            Err(e) => tx.send(Err(e.into())).await?,
        };

        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PullDataError {
    #[error(transparent)]
    Network(#[from] network::outbound::Error),

    #[error(transparent)]
    Rpc(rpc::migration::PullDataError),
}

impl irn::migration::Network<cluster::Node> for Network {
    type DataStream = Map<
        Peekable<RecvStream<rpc::migration::PullDataResponse>>,
        fn(io::Result<rpc::migration::PullDataResponse>) -> io::Result<ExportItem>,
    >;

    fn pull_keyrange(
        &self,
        from: &cluster::Node,
        range: std::ops::RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<Self::DataStream, impl irn::migration::AnyError>> + Send {
        let rpc_fut = rpc::migration::PullData::send(
            &self.client,
            (&from.id, &from.addr),
            move |mut tx, rx| async move {
                tx.send(rpc::migration::PullDataRequest {
                    keyrange: range,
                    keyspace_version,
                })
                .await?;

                let mut rx = rx.peekable();

                // if the first response message is an error, return the error
                let err_item = Pin::new(&mut rx)
                    .next_if(|item| matches!(item, Err(_) | Ok(Err(_))))
                    .await;
                if let Some(item) = err_item {
                    if let Err(e) = item? {
                        return Ok(Err(e));
                    }
                };

                // flatten error by converting the inner one into the outer `io::Error`
                let map_fn = |res| match res {
                    Ok(Ok(item)) => Ok(item),
                    Ok(Err(e)) => Err(io::Error::other(irn::migration::PullKeyrangeError::from(e))),
                    Err(e) => Err(e),
                };
                let rx = rx.map(map_fn as fn(_) -> _);

                Ok(Ok(rx))
            },
        );

        async move {
            rpc_fut
                .await
                .map_err(|e| AnyError::new(&e))?
                .map_err(|e| AnyError::new(&e))
        }
    }
}

#[derive(Clone)]
struct RaftRpcHandler {
    raft: consensus::Raft,
    rpc_timeouts: rpc::Timeouts,
}

impl inbound::RpcHandler for RaftRpcHandler {
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        Self::handle_rpc(self, id, stream, conn_info)
    }
}

impl RaftRpcHandler {
    async fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) {
        let stream = stream.with_timeouts(self.rpc_timeouts).metered();
        let peer_id = &conn_info.peer_id;

        let _ = match id {
            rpc::raft::AddMember::ID => {
                rpc::raft::AddMember::handle(stream, |req| self.raft.add_member(peer_id, req)).await
            }
            rpc::raft::RemoveMember::ID => {
                rpc::raft::RemoveMember::handle(stream, |req| self.raft.remove_member(peer_id, req))
                    .await
            }

            // Adding `Unauthorized` error to responses of these RPCs is total PITA, as the types
            // are defined in `openraft` itself.
            // So if the requestor is not a member we just drop the request.
            // This should generally never happen under normal circumstances, unless we are dealing
            // with a malicious actor.
            rpc::raft::ProposeChange::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::ProposeChange::handle(stream, |req| self.raft.propose_change(req)).await
            }
            rpc::raft::AppendEntries::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::AppendEntries::handle(stream, |req| self.raft.append_entries(req)).await
            }
            rpc::raft::InstallSnapshot::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::InstallSnapshot::handle(stream, |req| self.raft.install_snapshot(req))
                    .await
            }
            rpc::raft::Vote::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::Vote::handle(stream, |req| self.raft.vote(req)).await
            }

            id => return tracing::warn!("Unexpected raft RPC: {}", rpc::Name::new(id).as_str()),
        }
        .map_err(|err| {
            tracing::debug!(
                name = rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle raft RPC"
            )
        });
    }
}

#[derive(Clone)]
struct AdminRpcHandler {
    node: Arc<Node>,
    rpc_timeouts: rpc::Timeouts,
}

impl inbound::RpcHandler for AdminRpcHandler {
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        Self::handle_rpc(self, id, stream, conn_info)
    }
}

impl AdminRpcHandler {
    async fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        _conn_info: &ConnectionInfo,
    ) {
        let stream = stream.with_timeouts(self.rpc_timeouts).metered();

        let _ = match id {
            // TODO: auth
            rpc::admin::GetClusterView::ID => {
                rpc::admin::GetClusterView::handle(stream, |_| {
                    future::ready(Ok(self.get_cluster_view()))
                })
                .await
            }

            id => return tracing::warn!("Unexpected admin RPC: {}", rpc::Name::new(id).as_str()),
        }
        .map_err(|err| {
            tracing::debug!(
                name = rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle admin RPC"
            )
        });
    }

    fn get_cluster_view(&self) -> rpc::admin::ClusterView {
        use rpc::admin::{self, NodeState};

        let cluster = self.node.consensus().cluster();

        let nodes = cluster
            .nodes()
            .filter_map(|n| {
                let state = match cluster.node_state(&n.id)? {
                    cluster::NodeState::Pulling(_) => NodeState::Pulling,
                    cluster::NodeState::Normal => NodeState::Normal,
                    cluster::NodeState::Restarting => NodeState::Restarting,
                    cluster::NodeState::Decommissioning => NodeState::Decommissioning,
                };
                let node = admin::Node { id: n.id, state };
                Some((node.id, node))
            })
            .collect();

        admin::ClusterView {
            nodes,
            cluster_version: cluster.version(),
            keyspace_version: cluster.keyspace_version(),
        }
    }
}

pub type Client = Metered<WithTimeouts<network::Client>>;

/// Network adapter.
#[derive(Debug, Clone)]
pub struct Network {
    pub local_id: PeerId,
    pub client: Client,
}

impl Network {
    pub fn new(cfg: &Config) -> Result<Self, Error> {
        let rpc_timeout = Duration::from_millis(cfg.network_request_timeout);
        let rpc_timeouts = rpc::Timeouts {
            unary: Some(rpc_timeout),
            streaming: None,
            oneshot: Some(rpc_timeout),
        };

        Ok(Self {
            local_id: cfg.id,
            client: ::network::Client::new(::network::ClientConfig {
                keypair: cfg.keypair.clone(),
                known_peers: cfg
                    .known_peers
                    .iter()
                    .map(|(id, addr)| (id.clone(), addr.clone()))
                    .collect(),
                handshake: NoHandshake,
                connection_timeout: Duration::from_millis(cfg.network_connection_timeout),
            })?
            .with_timeouts(rpc_timeouts)
            .metered(),
        })
    }

    pub fn spawn_raft_server(
        cfg: &Config,
        raft: consensus::Raft,
    ) -> Result<tokio::task::JoinHandle<()>, network::Error> {
        let server_config = ::network::ServerConfig {
            addr: cfg.raft_server_addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        let rpc_timeouts = rpc::Timeouts {
            unary: Some(Duration::from_secs(2)),
            streaming: None,
            oneshot: None,
        };

        let handler = RaftRpcHandler { raft, rpc_timeouts };
        ::network::run_server(server_config, NoHandshake, handler.clone()).map(tokio::spawn)
    }

    pub fn spawn_servers(
        cfg: &Config,
        node: Node,
        prometheus: Option<PrometheusHandle>,
    ) -> Result<tokio::task::JoinHandle<()>, Error> {
        let server_config = ::network::ServerConfig {
            addr: cfg.replica_api_server_addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        let api_server_config = ::network::ServerConfig {
            addr: cfg.coordinator_api_server_addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        let admin_api_server_config = ::network::ServerConfig {
            addr: cfg.admin_api_server_addr.clone(),
            keypair: cfg.keypair.clone(),
        };

        let rpc_timeout = Duration::from_millis(cfg.network_request_timeout);
        let rpc_timeouts = rpc::Timeouts {
            unary: Some(rpc_timeout),
            streaming: None,
            oneshot: Some(rpc_timeout),
        };

        let handler = RpcHandler {
            node: Arc::new(node),
            rpc_timeouts,
            pubsub: Pubsub::new(),
            eth_address: cfg.eth_address.clone().map(Into::into),
            prometheus,
        };

        let admin_api_handler = AdminRpcHandler {
            node: handler.node.clone(),
            rpc_timeouts,
        };

        let server = ::network::run_server(server_config, NoHandshake, handler.clone())?;
        let api_server = ::api::server::run(api_server_config, handler)?;
        let admin_api_server =
            ::network::run_server(admin_api_server_config, NoHandshake, admin_api_handler)?;

        Ok(
            async move { tokio::join!(server, api_server, admin_api_server) }
                .map(drop)
                .pipe(tokio::spawn),
        )
    }

    pub(crate) fn get_peer<'a>(
        &self,
        node_id: &'a PeerId,
        multiaddr: &'a Multiaddr,
    ) -> RemoteNode<'a> {
        RemoteNode {
            id: Cow::Borrowed(node_id),
            multiaddr: Cow::Borrowed(multiaddr),
            client: self.client.clone(),
        }
    }

    pub(crate) async fn broadcast_pubsub(&self, evt: api::PubsubEventPayload) {
        rpc::broadcast::Pubsub::broadcast(&self.client, evt).await;
    }
}

#[derive(Clone)]
pub struct RemoteNode<'a> {
    id: Cow<'a, PeerId>,
    multiaddr: Cow<'a, Multiaddr>,
    client: Client,
}

impl<'a> RemoteNode<'a> {
    pub fn into_owned(self) -> RemoteNode<'static> {
        RemoteNode {
            id: Cow::Owned(self.id.into_owned()),
            multiaddr: Cow::Owned(self.multiaddr.into_owned()),
            client: self.client,
        }
    }
}

impl<Op: StorageOperation + MapRpc> irn::replication::Network<cluster::Node, Storage, Op>
    for Network
where
    for<'a> Client: rpc::Send<
        Op::Rpc,
        (&'a PeerId, &'a Multiaddr),
        rpc::replica::Request<Op>,
        Ok = rpc::replica::Result<Op::Output>,
    >,
    Storage: replication::Storage<Op, Error = StorageError>,
{
    type Error = outbound::Error;

    fn send(
        &self,
        to: &cluster::Node,
        operation: Op,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<ReplicaResponse<Storage, Op>, Self::Error>> + Send {
        async move {
            let req = rpc::replica::Request {
                operation,
                keyspace_version,
            };
            self.client.send((&to.id, &to.addr), req).await
        }
    }
}

pub trait MapRpc {
    type Rpc;
}

impl MapRpc for storage::Get {
    type Rpc = rpc::replica::Get;
}

impl MapRpc for storage::Set {
    type Rpc = rpc::replica::Set;
}

impl MapRpc for storage::Del {
    type Rpc = rpc::replica::Del;
}

impl MapRpc for storage::GetExp {
    type Rpc = rpc::replica::GetExp;
}

impl MapRpc for storage::SetExp {
    type Rpc = rpc::replica::SetExp;
}

impl MapRpc for storage::HGet {
    type Rpc = rpc::replica::HGet;
}

impl MapRpc for storage::HSet {
    type Rpc = rpc::replica::HSet;
}

impl MapRpc for storage::HDel {
    type Rpc = rpc::replica::HDel;
}

impl MapRpc for storage::HGetExp {
    type Rpc = rpc::replica::HGetExp;
}

impl MapRpc for storage::HSetExp {
    type Rpc = rpc::replica::HSetExp;
}

impl MapRpc for storage::HCard {
    type Rpc = rpc::replica::HCard;
}

impl MapRpc for storage::HFields {
    type Rpc = rpc::replica::HFields;
}

impl MapRpc for storage::HVals {
    type Rpc = rpc::replica::HVals;
}

impl MapRpc for storage::HScan {
    type Rpc = rpc::replica::HScan;
}

impl<'a> RemoteNode<'a> {
    pub fn id(&self) -> PeerId {
        self.id.clone().into_owned()
    }

    pub(super) async fn send_rpc<RPC, Args>(
        &'a self,
        args: Args,
    ) -> Result<<Client as rpc::Send<RPC, (&'a PeerId, &'a Multiaddr), Args>>::Ok, outbound::Error>
    where
        Client: rpc::Send<RPC, (&'a PeerId, &'a Multiaddr), Args>,
    {
        self.client.send((&self.id, &self.multiaddr), args).await
    }
}

fn api_result<T, U>(
    resp: Result<Result<T, ReplicaError<StorageError>>, CoordinatorError>,
) -> api::Result<U>
where
    T: Into<U> + fmt::Debug,
{
    use StorageError as S;

    Err(match resp {
        Ok(Ok(v)) => return Ok(v.into()),

        Ok(Err(ReplicaError::Throttled)) => api::Error::Throttled,
        Ok(Err(ReplicaError::Storage(S::EntryNotFound))) => api::Error::NotFound,
        Ok(Err(ReplicaError::Storage(S::IrnBackend { kind, message }))) => {
            api::InternalError::new(kind, message).into()
        }

        Err(CoordinatorError::Throttled) => api::Error::Throttled,
        Err(e @ CoordinatorError::Timeout) => api::InternalError::new("timeout", e).into(),
        Err(e @ CoordinatorError::InconsistentOperation) => {
            api::InternalError::new("inconsistent_operation", e).into()
        }
        Err(CoordinatorError::Network(e)) => api::InternalError::new("network", e).into(),

        resp => api::Error::Internal(api::InternalError::new("other", format!("{resp:?}"))),
    })
}

#[derive(Clone, Debug, Default)]
pub struct Pubsub {
    subscribers: Arc<RwLock<HashMap<SocketAddr, Subscriber>>>,
}

impl Pubsub {
    pub fn new() -> Self {
        Self::default()
    }

    fn subscribe(&self, subscriber_addr: SocketAddr, channels: HashSet<Vec<u8>>) -> Subscription {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let mut subscribers = self.subscribers.write().unwrap();

        let (tx, rx) = mpsc::channel(512);

        let _ = subscribers.insert(subscriber_addr, Subscriber { channels, tx });

        Subscription {
            addr: subscriber_addr,
            pubsub: self.clone(),
            rx,
        }
    }

    fn publish(&self, evt: api::PubsubEventPayload) {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let subscribers = self.subscribers.read().unwrap().clone();

        for sub in subscribers.values() {
            if sub.channels.contains(&evt.channel) {
                match sub.tx.try_send(evt.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        metrics::counter!("irn_pubsub_channel_full").increment(1)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        metrics::counter!("irn_pubsub_channel_closed").increment(1)
                    }
                };
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Subscriber {
    channels: HashSet<Vec<u8>>,
    tx: mpsc::Sender<api::PubsubEventPayload>,
}

struct Subscription {
    addr: SocketAddr,
    pubsub: Pubsub,

    rx: mpsc::Receiver<api::PubsubEventPayload>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let _ = self.pubsub.subscribers.write().unwrap().remove(&self.addr);
    }
}

impl From<irn::migration::PullKeyrangeError> for rpc::migration::PullDataError {
    fn from(err: irn::migration::PullKeyrangeError) -> Self {
        use irn::migration::PullKeyrangeError as E;

        match err {
            E::KeyspaceVersionMismatch => Self::KeyspaceVersionMismatch,
            E::StorageExport(s) => Self::StorageExport(s),
            E::NotClusterMember => Self::NotClusterMember,
        }
    }
}

impl From<rpc::migration::PullDataError> for irn::migration::PullKeyrangeError {
    fn from(err: rpc::migration::PullDataError) -> Self {
        use rpc::migration::PullDataError as E;

        match err {
            E::KeyspaceVersionMismatch => Self::KeyspaceVersionMismatch,
            E::StorageExport(s) => Self::StorageExport(s),
            E::NotClusterMember => Self::NotClusterMember,
        }
    }
}
