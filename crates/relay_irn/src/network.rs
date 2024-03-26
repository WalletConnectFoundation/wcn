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

#[cfg(test)]
pub use stub::Network as Stub;
#[cfg(test)]
pub mod stub {
    use {
        super::{async_trait, Multiaddr, PeerId, SendRequest},
        crate::{
            cluster::VersionedRequest,
            migration::{
                booting::{PullDataRequest, PullDataRequestArgs, PullDataResponse},
                leaving::{PushDataRequest, PushDataRequestArgs, PushDataResponse},
            },
            replication::{ReplicaError, ReplicatedRequest},
            storage::{
                self,
                stub::{Data, Del, Get, Set},
            },
            stub::StubbedNode,
        },
        once_cell::sync::Lazy,
        std::{
            collections::{HashMap, HashSet},
            sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
        },
    };

    static REGISTRY: Lazy<Mutex<HashMap<Multiaddr, StubbedNode>>> =
        Lazy::new(|| Mutex::new(HashMap::new()));

    #[derive(Debug)]
    struct RegisteredMultiaddr {
        inner: Multiaddr,
    }

    impl Drop for RegisteredMultiaddr {
        fn drop(&mut self) {
            REGISTRY.lock().unwrap().remove(&self.inner);
        }
    }

    fn random_multiaddr() -> Multiaddr {
        let port = rand::random::<u16>();
        format!("/ip4//udp/{port}/quic-v1")
            .parse()
            .unwrap()
    }

    #[derive(Clone, Debug)]
    pub struct Network {
        inner: Arc<RwLock<Inner>>,
    }

    impl Default for Network {
        fn default() -> Self {
            Self::new()
        }
    }

    #[derive(Debug)]
    struct Inner {
        multiaddr: Option<RegisteredMultiaddr>,
        peers: HashMap<PeerId, HashSet<Multiaddr>>,
    }

    impl Network {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(RwLock::new(Inner {
                    multiaddr: None,
                    peers: HashMap::new(),
                })),
            }
        }

        fn read(&self) -> RwLockReadGuard<Inner> {
            self.inner.read().unwrap()
        }

        fn write(&self) -> RwLockWriteGuard<Inner> {
            self.inner.write().unwrap()
        }

        fn resolve(&self, peer_id: PeerId) -> Result<StubbedNode, Error> {
            let addr = self
                .read()
                .peers
                .get(&peer_id)
                .and_then(|addrs| addrs.iter().next().cloned())
                .ok_or_else(|| Error("Unknown peer".to_string()))?;

            REGISTRY
                .lock()
                .unwrap()
                .get(&addr)
                .cloned()
                .ok_or_else(|| Error("Node not registered".to_string()))
        }

        pub fn local_multiaddr(&self) -> Multiaddr {
            self.read()
                .multiaddr
                .as_ref()
                .expect("Not registered")
                .inner
                .clone()
        }
    }

    impl StubbedNode {
        pub fn register(self) -> Self {
            let mut registry = REGISTRY.lock().unwrap();
            assert!(
                registry.len() <= (u16::MAX / 2) as usize,
                "Too many REGISTRY entries"
            );

            let addr = loop {
                let addr = random_multiaddr();
                if !registry.contains_key(&addr) {
                    registry.insert(addr.clone(), self.clone());
                    break RegisteredMultiaddr { inner: addr };
                }
            };

            let _ = self.network.write().multiaddr.insert(addr);
            self
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
            Ok(self.resolve(peer_id)?.exec_replicated(req).await)
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
            Ok(self.resolve(peer_id)?.exec_replicated(req).await)
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
            Ok(self.resolve(peer_id)?.exec_replicated(req).await)
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

            Ok(self.resolve(peer_id)?.handle_pull_data_request(req).await)
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

            Ok(self.resolve(peer_id)?.handle_push_data_request(req).await)
        }
    }
}
