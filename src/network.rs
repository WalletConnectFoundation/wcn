use {
    crate::{
        cluster,
        consensus,
        contract::StatusReporter,
        storage::{self, Cursor, Storage, Value},
        Config,
        Error,
        Node,
    },
    admin_api::Server as _,
    anyerror::AnyError,
    api::server::Handshake,
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
    irn::{
        cluster::Consensus,
        replication::{
            self,
            CoordinatorError,
            ReplicaError,
            ReplicaResponse,
            Storage as _,
            StorageOperation,
        },
    },
    irn_rpc::{
        client::{
            self,
            middleware::{MeteredExt as _, WithTimeoutsExt as _},
        },
        middleware::{Metered, Timeouts, WithTimeouts},
        quic::{self, socketaddr_to_multiaddr},
        server::{
            self,
            middleware::{MeteredExt as _, WithTimeoutsExt as _},
            ClientConnectionInfo,
        },
        transport::{self, BiDirectionalStream, NoHandshake, RecvStream, SendStream},
        Client as _,
        Multiaddr,
    },
    libp2p::PeerId,
    metrics_exporter_prometheus::PrometheusHandle,
    pin_project::{pin_project, pinned_drop},
    raft::Raft,
    relay_rocks::{db::migration::ExportItem, util::timestamp_micros, StorageError},
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        fmt::{self, Debug},
        hash::BuildHasher,
        io,
        pin::Pin,
        sync::{Arc, RwLock},
        time::Duration,
    },
    tap::Pipe,
    tokio::sync::mpsc,
};

pub mod rpc {

    pub mod raft {
        use {crate::consensus::*, irn_rpc as rpc};

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
            irn_rpc as rpc,
            relay_rocks::StorageError,
            serde::{Deserialize, Serialize},
        };

        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct Request<Op> {
            pub operation: Op,
            pub keyspace_version: u64,
        }

        impl<Op> Request<Op> {
            pub fn new(operation: Op, keyspace_version: u64) -> Self {
                Self {
                    operation,
                    keyspace_version,
                }
            }
        }

        pub type Result<T> = std::result::Result<T, ReplicaError<StorageError>>;

        pub type Rpc<const ID: rpc::Id, Op> =
            rpc::Unary<ID, Request<Op>, Result<<Op as StorageOperation>::Output>>;

        pub type Get = Rpc<{ rpc::id(b"r_get") }, storage::Get>;
        pub type Set = Rpc<{ rpc::id(b"r_set") }, storage::Set>;
        pub type SetVal = Rpc<{ rpc::id(b"r_set_val") }, storage::SetVal>;
        pub type Del = Rpc<{ rpc::id(b"r_del") }, storage::Del>;
        pub type GetExp = Rpc<{ rpc::id(b"r_get_exp") }, storage::GetExp>;
        pub type SetExp = Rpc<{ rpc::id(b"r_set_exp") }, storage::SetExp>;

        pub type HGet = Rpc<{ rpc::id(b"r_hget") }, storage::HGet>;
        pub type HSet = Rpc<{ rpc::id(b"r_hset") }, storage::HSet>;
        pub type HSetVal = Rpc<{ rpc::id(b"r_hset_val") }, storage::HSetVal>;
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
            irn_rpc as rpc,
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
            irn_rpc as rpc,
            libp2p::PeerId,
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
            irn_rpc as rpc,
            serde::{Deserialize, Serialize},
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
        use irn_rpc as rpc;

        pub type Request = ();

        pub type Response = String;

        pub type Metrics = rpc::Unary<{ rpc::id(b"metrics") }, Request, Response>;
    }
}

#[derive(AsRef, Clone)]
struct CoordinatorApiServer {
    #[as_ref]
    node: Arc<Node>,

    pubsub: Pubsub,

    config: irn_rpc::server::Config<Handshake>,
}

impl irn_rpc::Server for CoordinatorApiServer {
    type Handshake = Handshake;
    type ConnectionData = ();

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: irn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        Self::handle_coordinator_rpc(self, id, stream, conn_info)
    }
}

#[derive(AsRef, Clone)]
struct ReplicaApiServer {
    #[as_ref]
    node: Arc<Node>,

    pubsub: Pubsub,

    prometheus: PrometheusHandle,

    config: irn_rpc::server::Config,
}

impl irn_rpc::Server for ReplicaApiServer {
    type Handshake = NoHandshake;
    type ConnectionData = ();

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: irn_rpc::Id,
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

impl storage_api::Server for StorageApiServer {
    fn keyspace_version(&self) -> u64 {
        self.node.consensus().cluster().keyspace_version()
    }

    fn get(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::Record>>> + Send {
        self.node
            .storage()
            .get(HASHER.hash_one(key.as_bytes()), key.into_bytes())
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
        self.node
            .storage()
            .exec(HASHER.hash_one(entry.key.as_bytes()), storage::Set {
                key: entry.key.into_bytes(),
                value: entry.value,
                expiration: entry.expiration.unix_timestamp_secs(),
                version: entry.version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn del(
        &self,
        key: storage_api::Key,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::Del {
                key: key.into_bytes(),
                version: version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn get_exp(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::EntryExpiration>>> + Send
    {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::GetExp {
                key: key.into_bytes(),
            })
            .map(|res| match res {
                Ok(timestamp) => Ok(Some(
                    storage_api::EntryExpiration::from_unix_timestamp_secs(timestamp),
                )),
                Err(StorageError::EntryNotFound) => Ok(None),
                Err(err) => Err(storage_api::server::Error::new(err)),
            })
    }

    fn set_exp(
        &self,
        key: storage_api::Key,
        expiration: impl Into<storage_api::EntryExpiration>,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::SetExp {
                key: key.into_bytes(),
                expiration: expiration.into().unix_timestamp_secs(),
                version: version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn hget(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::Record>>> + Send {
        self.node
            .storage()
            .hget(HASHER.hash_one(key.as_bytes()), key.into_bytes(), field)
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
        self.node
            .storage()
            .exec(HASHER.hash_one(entry.key.as_bytes()), storage::HSet {
                key: entry.key.into_bytes(),
                field: entry.field,
                value: entry.value,
                expiration: entry.expiration.unix_timestamp_secs(),
                version: entry.version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn hdel(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::HDel {
                key: key.into_bytes(),
                field,
                version: version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn hget_exp(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> impl Future<Output = storage_api::server::Result<Option<storage_api::EntryExpiration>>> + Send
    {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::HGetExp {
                key: key.into_bytes(),
                field,
            })
            .map(|res| match res {
                Ok(timestamp) => Ok(Some(
                    storage_api::EntryExpiration::from_unix_timestamp_secs(timestamp),
                )),
                Err(StorageError::EntryNotFound) => Ok(None),
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
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::HSetExp {
                key: key.into_bytes(),
                field,
                expiration: expiration.into().unix_timestamp_secs(),
                version: version.unix_timestamp_micros(),
            })
            .map_err(storage_api::server::Error::new)
    }

    fn hcard(
        &self,
        key: storage_api::Key,
    ) -> impl Future<Output = storage_api::server::Result<u64>> + Send {
        self.node
            .storage()
            .exec(HASHER.hash_one(key.as_bytes()), storage::HCard {
                key: key.into_bytes(),
            })
            .map_ok(|card| card.0)
            .map_err(storage_api::server::Error::new)
    }

    fn hscan(
        &self,
        key: storage_api::Key,
        count: u32,
        cursor: Option<storage_api::Field>,
    ) -> impl Future<Output = storage_api::server::Result<storage_api::MapPage>> + Send {
        self.node
            .storage()
            .hscan(
                HASHER.hash_one(key.as_bytes()),
                key.into_bytes(),
                count,
                cursor,
            )
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

fn prepare_key(
    key: api::Key,
    conn_info: &ClientConnectionInfo<CoordinatorApiServer>,
) -> api::Result<Vec<u8>> {
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

impl CoordinatorApiServer {
    fn handle_coordinator_rpc<'a>(
        &'a self,
        id: irn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + 'a {
        async move {
            let client_id = &conn_info.peer_id;
            let coordinator = self.node.coordinator();

            let _ = match id {
                api::rpc::Get::ID => {
                    api::rpc::Get::handle_legacy(stream, |req| async {
                        let op = storage::Get {
                            key: prepare_key(req.key, conn_info)?,
                        };

                        coordinator
                            .replicate(client_id, op)
                            .map(api_result)
                            .await
                            .map(|opt: Option<(Value, _)>| opt.map(|(value, _)| value))
                    })
                    .await
                }
                api::rpc::Set::ID => {
                    api::rpc::Set::handle_legacy(stream, |req| async move {
                        if let Some(expiration) = req.expiration {
                            let op = storage::Set {
                                key: prepare_key(req.key, conn_info)?,
                                value: req.value,
                                expiration,
                                version: timestamp_micros(),
                            };

                            return coordinator.replicate(client_id, op).map(api_result).await;
                        }

                        let op = storage::SetVal {
                            key: prepare_key(req.key, conn_info)?,
                            value: req.value,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::Del::ID => {
                    api::rpc::Del::handle_legacy(stream, |req| async {
                        let op = storage::Del {
                            key: prepare_key(req.key, conn_info)?,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::GetExp::ID => {
                    api::rpc::GetExp::handle_legacy(stream, |req| async {
                        let op = storage::GetExp {
                            key: prepare_key(req.key, conn_info)?,
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::SetExp::ID => {
                    api::rpc::SetExp::handle_legacy(stream, |req| async move {
                        let Some(expiration) = req.expiration else {
                            return Err(api::Error::Internal(api::InternalError {
                                code: "invalid_request".into(),
                                message: "`None` expiration is no longer allowed".into(),
                            }));
                        };

                        let op = storage::SetExp {
                            key: prepare_key(req.key, conn_info)?,
                            expiration,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HGet::ID => {
                    api::rpc::HGet::handle_legacy(stream, |req| async {
                        let op = storage::HGet {
                            key: prepare_key(req.key, conn_info)?,
                            field: req.field,
                        };

                        coordinator
                            .replicate(client_id, op)
                            .map(api_result)
                            .await
                            .map(|opt: Option<(Value, _)>| opt.map(|(value, _)| value))
                    })
                    .await
                }
                api::rpc::HSet::ID => {
                    api::rpc::HSet::handle_legacy(stream, |req| async move {
                        if let Some(expiration) = req.expiration {
                            let op = storage::HSet {
                                key: prepare_key(req.key, conn_info)?,
                                field: req.field,
                                value: req.value,
                                expiration,
                                version: timestamp_micros(),
                            };

                            return coordinator.replicate(client_id, op).map(api_result).await;
                        }

                        let op = storage::HSetVal {
                            key: prepare_key(req.key, conn_info)?,
                            field: req.field,
                            value: req.value,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HDel::ID => {
                    api::rpc::HDel::handle_legacy(stream, |req| async {
                        let op = storage::HDel {
                            key: prepare_key(req.key, conn_info)?,
                            field: req.field,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HGetExp::ID => {
                    api::rpc::HGetExp::handle_legacy(stream, |req| async {
                        let op = storage::HGetExp {
                            key: prepare_key(req.key, conn_info)?,
                            field: req.field,
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HSetExp::ID => {
                    api::rpc::HSetExp::handle_legacy(stream, |req| async move {
                        let Some(expiration) = req.expiration else {
                            return Err(api::Error::Internal(api::InternalError {
                                code: "invalid_request".into(),
                                message: "`None` expiration is no longer allowed".into(),
                            }));
                        };

                        let op = storage::HSetExp {
                            key: prepare_key(req.key, conn_info)?,
                            field: req.field,
                            expiration,
                            version: timestamp_micros(),
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HCard::ID => {
                    api::rpc::HCard::handle_legacy(stream, |req| async {
                        let op = storage::HCard {
                            key: prepare_key(req.key, conn_info)?,
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HFields::ID => {
                    api::rpc::HFields::handle_legacy(stream, |req| async {
                        let op = storage::HFields {
                            key: prepare_key(req.key, conn_info)?,
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HVals::ID => {
                    api::rpc::HVals::handle_legacy(stream, |req| async {
                        let op = storage::HVals {
                            key: prepare_key(req.key, conn_info)?,
                        };

                        coordinator.replicate(client_id, op).map(api_result).await
                    })
                    .await
                }
                api::rpc::HScan::ID => {
                    api::rpc::HScan::handle_legacy(stream, |req| {
                        async move {
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
                        }
                    })
                    .await
                }

                api::rpc::Publish::ID => {
                    api::rpc::Publish::handle(stream, |req| self.handle_pubsub_publish(req)).await
                }

                api::rpc::Subscribe::ID => {
                    api::rpc::Subscribe::handle_legacy(stream, |tx, rx| {
                        self.handle_pubsub_subscribe(tx, rx, conn_info)
                    })
                    .await
                }

                id => {
                    return tracing::warn!(
                        name = irn_rpc::Name::new(id).as_str(),
                        "Unexpected coordinator RPC"
                    )
                }
            }
            .map_err(|err| {
                tracing::debug!(
                    name = irn_rpc::Name::new(id).as_str(),
                    ?err,
                    "Failed to handle coordinator RPC"
                )
            });
        }
    }
}

impl ReplicaApiServer {
    async fn handle_internal_rpc(
        &self,
        id: irn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ClientConnectionInfo<Self>,
    ) {
        let peer_id = &conn_info.peer_id;
        let replica = self.node.replica();

        let _ = match id {
            rpc::replica::Get::ID => {
                rpc::replica::Get::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::Set::ID => {
                rpc::replica::Set::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::SetVal::ID => {
                rpc::replica::SetVal::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::Del::ID => {
                rpc::replica::Del::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::GetExp::ID => {
                rpc::replica::GetExp::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::SetExp::ID => {
                rpc::replica::SetExp::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HGet::ID => {
                rpc::replica::HGet::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HSet::ID => {
                rpc::replica::HSet::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HSetVal::ID => {
                rpc::replica::HSetVal::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HDel::ID => {
                rpc::replica::HDel::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HGetExp::ID => {
                rpc::replica::HGetExp::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HSetExp::ID => {
                rpc::replica::HSetExp::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HCard::ID => {
                rpc::replica::HCard::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HFields::ID => {
                rpc::replica::HFields::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HVals::ID => {
                rpc::replica::HVals::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
                })
                .await
            }
            rpc::replica::HScan::ID => {
                rpc::replica::HScan::handle(stream, |req| {
                    replica
                        .handle_replication(peer_id, req.operation, req.keyspace_version)
                        .map(Ok)
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
                    irn_rpc::Name::new(id).as_str()
                )
            }
        }
        .map_err(|err| {
            tracing::debug!(
                name = irn_rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle internal RPC"
            )
        });
    }
}

impl CoordinatorApiServer {
    async fn handle_pubsub_publish(&self, req: api::Publish) {
        let evt = &api::PubsubEventPayload {
            channel: req.channel,
            payload: req.message,
        };

        self.pubsub.publish(evt.clone());

        let cluster = self.node.consensus().cluster();

        stream::iter(cluster.nodes().filter(|n| &n.id != self.node.id()))
            .for_each_concurrent(None, |n| {
                rpc::broadcast::Pubsub::send(&self.node.network().client, &n.addr, evt).map(drop)
            })
            .await;
    }

    async fn handle_pubsub_subscribe(
        &self,
        mut rx: RecvStream<api::Subscribe>,
        mut tx: SendStream<api::PubsubEventPayload>,
        _conn_info: &ClientConnectionInfo<Self>,
    ) -> server::Result<()> {
        let req = rx.recv_message().await?;

        let mut subscription = self.pubsub.subscribe(req.channels);

        while let Some(msg) = subscription.rx.recv().await {
            tx.send(&msg).await?;
        }

        Ok(())
    }
}

impl ReplicaApiServer {
    async fn handle_pull_data(
        &self,
        peer_id: &libp2p::PeerId,
        mut rx: RecvStream<rpc::migration::PullDataRequest>,
        mut tx: SendStream<irn_rpc::Result<rpc::migration::PullDataResponse>>,
    ) -> server::Result<()> {
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
    pubsub: Pubsub,
}

impl client_api::Server for ClientApiServer {
    fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> impl Future<Output = ()> + Send {
        self.pubsub.publish(api::PubsubEventPayload {
            channel,
            payload: message,
        });

        future::ready(())
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

impl irn::migration::Network<cluster::Node> for Network {
    type DataStream = Map<
        RecvStream<irn_rpc::Result<rpc::migration::PullDataResponse>>,
        fn(io::Result<irn_rpc::Result<rpc::migration::PullDataResponse>>) -> io::Result<ExportItem>,
    >;

    fn pull_keyrange(
        &self,
        from: &cluster::Node,
        range: std::ops::RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<Self::DataStream, impl irn::migration::AnyError>> + Send {
        async move {
            let range = &range;
            rpc::migration::PullData::send(
                &self.client,
                &from.addr,
                &move |mut tx, rx| async move {
                    tx.send(rpc::migration::PullDataRequest {
                        keyrange: range.clone(),
                        keyspace_version,
                    })
                    .await?;

                    // flatten error by converting the inner ones into the outer `io::Error`
                    let map_fn = |res| match res {
                        Ok(Ok(Ok(item))) => Ok(item),
                        Ok(Ok(Err(err))) => Err(io::Error::other(
                            irn::migration::PullKeyrangeError::from(err),
                        )),
                        Ok(Err(err)) => Err(io::Error::other(err)),
                        Err(err) => Err(err),
                    };
                    let rx = rx.map(map_fn as fn(_) -> _);

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
    config: irn_rpc::server::Config,
}

impl irn_rpc::Server for RaftRpcServer {
    type Handshake = NoHandshake;
    type ConnectionData = ();

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: irn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        Self::handle_rpc(self, id, stream, conn_info)
    }
}

impl RaftRpcServer {
    async fn handle_rpc(
        &self,
        id: irn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &ClientConnectionInfo<Self>,
    ) {
        let peer_id = &conn_info.peer_id;

        let _ = match id {
            rpc::raft::AddMember::ID => {
                rpc::raft::AddMember::handle(stream, |req| {
                    self.raft.add_member(peer_id, req).map(Ok)
                })
                .await
            }
            rpc::raft::RemoveMember::ID => {
                rpc::raft::RemoveMember::handle(stream, |req| {
                    self.raft.remove_member(peer_id, req).map(Ok)
                })
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
                rpc::raft::ProposeChange::handle(stream, |req| {
                    self.raft.propose_change(req).map(Ok)
                })
                .await
            }
            rpc::raft::AppendEntries::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::AppendEntries::handle(stream, |req| {
                    self.raft.append_entries(req).map(Ok)
                })
                .await
            }
            rpc::raft::InstallSnapshot::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::InstallSnapshot::handle(stream, |req| {
                    self.raft.install_snapshot(req).map(Ok)
                })
                .await
            }
            rpc::raft::Vote::ID => {
                if !self.raft.is_member(peer_id) {
                    return;
                }
                rpc::raft::Vote::handle(stream, |req| self.raft.vote(req).map(Ok)).await
            }

            id => {
                return tracing::warn!("Unexpected raft RPC: {}", irn_rpc::Name::new(id).as_str())
            }
        }
        .map_err(|err| {
            tracing::debug!(
                name = irn_rpc::Name::new(id).as_str(),
                ?err,
                "Failed to handle raft RPC"
            )
        });
    }
}

#[derive(Clone)]
struct AdminApiServer<S> {
    node: Arc<Node>,
    status_reporter: Option<S>,
    eth_address: Option<Arc<str>>,
}

impl<S: StatusReporter> admin_api::Server for AdminApiServer<S> {
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
        use admin_api::GetNodeStatusError as Error;

        async {
            let reporter = self.status_reporter.as_ref().ok_or(Error::NotAvailable)?;

            let report = reporter
                .report_status()
                .await
                .map_err(|err| Error::Internal(format!("{err:?}")))?;

            Ok(admin_api::NodeStatus {
                node_version: crate::NODE_VERSION,
                eth_address: self.eth_address.as_ref().map(|s| s.to_string()),
                stake_amount: report.stake,
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
                    io::Write,
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
    pub client: Client,
}

impl Network {
    pub fn new(cfg: &Config) -> Result<Self, Error> {
        // Set timeouts for everything except `PullData`.
        let rpc_timeouts = Timeouts::new()
            .with_default(Duration::from_millis(cfg.network_request_timeout))
            .with::<{ rpc::migration::PullData::ID }>(None);

        Ok(Self {
            local_id: cfg.id,
            client: quic::Client::new(irn_rpc::client::Config {
                keypair: cfg.keypair.clone(),
                known_peers: cfg.known_peers.values().cloned().collect(),
                handshake: NoHandshake,
                connection_timeout: Duration::from_millis(cfg.network_connection_timeout),
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
        let server_config = irn_rpc::server::Config {
            name: const { irn_rpc::ServerName::new("raft_api") },
            handshake: NoHandshake,
        };

        let quic_server_config = irn_rpc::quic::server::Config {
            name: "raft_api",
            addr: server_addr,
            keypair: cfg.keypair.clone(),
            max_concurrent_connections: 500,
            max_concurrent_streams: 1000,
        };

        let server = RaftRpcServer {
            raft,
            config: server_config,
        }
        .with_timeouts(Timeouts::new().with_default(Duration::from_secs(2)))
        .metered();

        irn_rpc::quic::server::run(server, quic_server_config).map(tokio::spawn)
    }

    pub fn spawn_servers<S: StatusReporter>(
        cfg: &Config,
        node: Node,
        prometheus: PrometheusHandle,
        status_reporter: Option<S>,
    ) -> Result<tokio::task::JoinHandle<()>, Error> {
        let replica_api_server_config = irn_rpc::server::Config {
            name: const { irn_rpc::ServerName::new("replica_api") },
            handshake: NoHandshake,
        };
        let replica_api_quic_server_config = irn_rpc::quic::server::Config {
            name: "replica_api",
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.replica_api_server_port)),
            keypair: cfg.keypair.clone(),
            max_concurrent_connections: cfg.replica_api_max_concurrent_connections,
            max_concurrent_streams: cfg.replica_api_max_concurrent_rpcs,
        };

        let coordinator_api_server_config = irn_rpc::server::Config {
            name: const { irn_rpc::ServerName::new("coordinator_api") },
            handshake: Handshake,
        };
        let coordinator_api_quic_server_config = irn_rpc::quic::server::Config {
            name: "coordinator_api",
            addr: socketaddr_to_multiaddr((cfg.server_addr, cfg.coordinator_api_server_port)),
            keypair: cfg.keypair.clone(),
            max_concurrent_connections: cfg.coordinator_api_max_concurrent_connections,
            max_concurrent_streams: cfg.coordinator_api_max_concurrent_rpcs,
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
            .with::<{ rpc::migration::PullData::ID }>(None)
            .with::<{ api::rpc::Subscribe::ID }>(None);

        let node = Arc::new(node);

        let pubsub = Pubsub::new();

        let coordinator_api_server = CoordinatorApiServer {
            node: node.clone(),
            pubsub: pubsub.clone(),
            config: coordinator_api_server_config,
        }
        .with_timeouts(rpc_timeouts.clone())
        .metered();

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
        };

        let client_api_server = ClientApiServer { pubsub }.serve(client_api_cfg)?;

        let admin_api_server = AdminApiServer {
            node: node.clone(),
            status_reporter,
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

        let replica_and_storage_api_servers = irn_rpc::quic::server::multiplex(
            (replica_api_server, storage_api_server),
            replica_api_quic_server_config,
        )?;
        let coordinator_api_server =
            ::api::server::run(coordinator_api_server, coordinator_api_quic_server_config)?;

        Ok(async move {
            tokio::join!(
                replica_and_storage_api_servers,
                coordinator_api_server,
                client_api_server,
                admin_api_server
            )
        }
        .map(drop)
        .pipe(tokio::spawn))
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
}

#[derive(Clone)]
pub struct RemoteNode<'a> {
    pub id: Cow<'a, PeerId>,
    pub multiaddr: Cow<'a, Multiaddr>,
    pub client: Client,
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
    Op::Rpc: irn_rpc::Rpc<
        Kind = irn_rpc::kind::Unary,
        Request = rpc::replica::Request<Op>,
        Response = ReplicaResponse<Storage, Op>,
    >,
    Storage: replication::Storage<Op, Error = StorageError>,
{
    type Error = client::Error;

    fn send(
        &self,
        to: &cluster::Node,
        operation: Op,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<ReplicaResponse<Storage, Op>, Self::Error>> + Send {
        async move {
            self.client
                .send_unary::<Op::Rpc>(&to.addr, &rpc::replica::Request {
                    operation,
                    keyspace_version,
                })
                .await
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

impl MapRpc for storage::SetVal {
    type Rpc = rpc::replica::SetVal;
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

impl MapRpc for storage::HSetVal {
    type Rpc = rpc::replica::HSetVal;
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

    fn publish(&self, evt: api::PubsubEventPayload) {
        // `Err` can't happen here, if the lock is poisoned then we have already crashed
        // as we don't handle panics.
        let subscribers = self.inner.read().unwrap().subscribers.clone();

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

#[pin_project(PinnedDrop)]
struct Subscription {
    id: u64,
    pubsub: Pubsub,

    #[pin]
    rx: mpsc::Receiver<api::PubsubEventPayload>,
}

impl Stream for Subscription {
    type Item = api::PubsubEventPayload;

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
