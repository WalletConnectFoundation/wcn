use {
    crate::PeerId,
    async_trait::async_trait,
    libp2p::Multiaddr,
    std::{error::Error as StdError, fmt},
};

#[async_trait]
pub trait SendRequest<R>: Clone + Send + Sync + 'static {
    type Response;
    type Error: StdError + Send + Sync + 'static;

    async fn send_request(&self, peer_id: PeerId, req: R) -> Result<Self::Response, Self::Error>;
}

#[async_trait]
pub trait HandleRequest<R>: Send + Sync + 'static {
    type Response;

    async fn handle_request(&self, req: R) -> Self::Response;
}

/// Handling network interactions between cluster members.
#[async_trait]
pub trait Network: fmt::Debug + Clone + Send + Sync + 'static {
    /// Error of broadcasting messages to the network;
    type BroadcastError: std::error::Error + Send + Sync + 'static;

    /// Registers a known [`Multiaddr`] of a peer.
    async fn register_peer_address(&self, peer_id: PeerId, addr: Multiaddr);

    /// Unregisters a [`Multiaddr`] of a peer.
    async fn unregister_peer_address(&self, peer_id: PeerId, addr: Multiaddr);

    /// Broadcasts heartbeat of this node to the entire network, indicating that
    /// the node is alive.
    async fn broadcast_heartbeat(&self) -> Result<(), Self::BroadcastError>;
}

#[cfg(any(feature = "testing", test))]
pub use stub::Network as Stub;
#[cfg(any(feature = "testing", test))]
pub mod stub {
    use {
        super::{async_trait, Multiaddr, PeerId, SendRequest},
        crate::{
            cluster::VersionedRequest,
            migration::{
                self,
                booting::{PullDataRequest, PullDataRequestArgs, PullDataResponse},
                leaving::{PushDataRequest, PushDataRequestArgs, PushDataResponse},
            },
            replication::{Replica, ReplicaError, ReplicatedRequest},
            storage::{
                self,
                stub::{Data, Del, Get, Set},
            },
            StubbedNode,
        },
        std::{
            collections::{HashMap, HashSet},
            sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
        },
    };

    #[derive(Clone, Debug, Default)]
    pub struct Registry {
        nodes: Arc<Mutex<HashMap<Multiaddr, StubbedNode>>>,
        migration_managers: Arc<Mutex<HashMap<Multiaddr, migration::StubbedManager>>>,
    }

    impl Registry {
        pub fn new_network_handle(
            &self,
            local_id: PeerId,
            multiaddr: Multiaddr,
            peers: HashMap<PeerId, HashSet<Multiaddr>>,
        ) -> Network {
            Network {
                local_id,
                inner: Arc::new(RwLock::new(Inner {
                    multiaddr,
                    peers,
                    registry: self.clone(),
                })),
            }
        }

        pub fn register_node(&self, multiaddr: Multiaddr, node: StubbedNode) {
            self.nodes.lock().unwrap().insert(multiaddr, node);
        }

        pub fn register_migration_manager(
            &self,
            multiaddr: Multiaddr,
            manager: migration::StubbedManager,
        ) {
            self.migration_managers
                .lock()
                .unwrap()
                .insert(multiaddr, manager);
        }
    }

    #[derive(Clone, Debug)]
    pub struct Network {
        local_id: PeerId,
        inner: Arc<RwLock<Inner>>,
    }

    #[derive(Debug)]
    struct Inner {
        multiaddr: Multiaddr,
        peers: HashMap<PeerId, HashSet<Multiaddr>>,

        registry: Registry,
    }

    impl Network {
        fn read(&self) -> RwLockReadGuard<Inner> {
            self.inner.read().unwrap()
        }

        fn write(&self) -> RwLockWriteGuard<Inner> {
            self.inner.write().unwrap()
        }

        fn peer_addr(&self, peer_id: PeerId) -> Result<Multiaddr, Error> {
            self.read()
                .peers
                .get(&peer_id)
                .and_then(|addrs| addrs.iter().next().cloned())
                .ok_or_else(|| Error("Unknown peer".to_string()))
        }

        fn node(&self, peer_id: PeerId) -> Result<StubbedNode, Error> {
            self.resolve_node(&self.peer_addr(peer_id)?)
                .ok_or_else(|| Error("Node not registered".to_string()))
        }

        fn resolve_node(&self, addr: &Multiaddr) -> Option<StubbedNode> {
            self.inner
                .read()
                .unwrap()
                .registry
                .nodes
                .lock()
                .unwrap()
                .get(addr)
                .cloned()
        }

        fn resolve_migration_manager(&self, addr: &Multiaddr) -> Option<migration::StubbedManager> {
            self.inner
                .read()
                .unwrap()
                .registry
                .migration_managers
                .lock()
                .unwrap()
                .get(addr)
                .cloned()
        }

        pub fn local_multiaddr(&self) -> Multiaddr {
            self.read().multiaddr.clone()
        }
    }

    #[async_trait]
    impl super::Network for Network {
        type BroadcastError = Error;

        async fn register_peer_address(&self, peer_id: PeerId, addr: Multiaddr) {
            self.write().peers.entry(peer_id).or_default().insert(addr);
        }

        async fn unregister_peer_address(&self, peer_id: PeerId, addr: Multiaddr) {
            self.write().peers.entry(peer_id).or_default().remove(&addr);
        }

        async fn broadcast_heartbeat(&self) -> Result<(), Self::BroadcastError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
    #[error("{0}")]
    pub struct Error(String);

    #[async_trait]
    impl SendRequest<ReplicatedRequest<Get>> for Network {
        type Response = Result<Option<u64>, ReplicaError<storage::stub::Error>>;
        type Error = Error;

        async fn send_request(
            &self,
            peer_id: PeerId,
            req: ReplicatedRequest<Get>,
        ) -> Result<Self::Response, Self::Error> {
            Ok(self
                .node(peer_id)?
                .handle_replication(&self.local_id.id, req)
                .await)
        }
    }

    #[async_trait]
    impl SendRequest<ReplicatedRequest<Set>> for Network {
        type Response = Result<(), ReplicaError<storage::stub::Error>>;
        type Error = Error;

        async fn send_request(
            &self,
            peer_id: PeerId,
            req: ReplicatedRequest<Set>,
        ) -> Result<Self::Response, Self::Error> {
            Ok(self
                .node(peer_id)?
                .handle_replication(&self.local_id.id, req)
                .await)
        }
    }

    #[async_trait]
    impl SendRequest<ReplicatedRequest<Del>> for Network {
        type Response = Result<(), ReplicaError<storage::stub::Error>>;
        type Error = Error;

        async fn send_request(
            &self,
            peer_id: PeerId,
            req: ReplicatedRequest<Del>,
        ) -> Result<Self::Response, Self::Error> {
            Ok(self
                .node(peer_id)?
                .handle_replication(&self.local_id.id, req)
                .await)
        }
    }

    #[async_trait]
    impl SendRequest<PullDataRequest> for Network {
        type Response = PullDataResponse<Data>;
        type Error = Error;

        async fn send_request(
            &self,
            peer_id: PeerId,
            req: PullDataRequest,
        ) -> Result<Self::Response, Self::Error> {
            let args = PullDataRequestArgs {
                key_range: req.inner.key_range,
            };
            let req = VersionedRequest::new(args, req.cluster_view_version);

            let id = &self.local_id.id;
            let addr = self.peer_addr(peer_id)?;
            Ok(if let Some(node) = self.resolve_node(&addr) {
                node.handle_pull_data_request(id, req).await
            } else if let Some(mgr) = self.resolve_migration_manager(&addr) {
                mgr.handle_pull_data_request(id, req).await
            } else {
                panic!("Not registered");
            })
        }
    }

    #[async_trait]
    impl SendRequest<PushDataRequest<Data>> for Network {
        type Response = PushDataResponse;
        type Error = Error;

        async fn send_request(
            &self,
            peer_id: PeerId,
            req: PushDataRequest<Data>,
        ) -> Result<Self::Response, Self::Error> {
            let args = PushDataRequestArgs {
                key_range: req.inner.key_range,
                data: req.inner.data,
            };
            let req = VersionedRequest::new(args, req.cluster_view_version);

            let id = &self.local_id.id;
            let addr = self.peer_addr(peer_id)?;
            Ok(if let Some(node) = self.resolve_node(&addr) {
                node.handle_push_data_request(id, req).await
            } else if let Some(mgr) = self.resolve_migration_manager(&addr) {
                mgr.handle_push_data_request(id, req).await
            } else {
                panic!("Not registered");
            })
        }
    }
}
