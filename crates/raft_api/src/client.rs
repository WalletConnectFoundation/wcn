use {
    super::*,
    raft::{
        AddMemberRpcResult,
        AppendEntriesRpcResult,
        InstallSnapshotRpcResult,
        ProposeChangeRpcResult,
        RemoteError,
        RemoveMemberRpcResult,
        RpcError,
        TypeConfig,
        VoteRpcResult,
    },
    std::{
        collections::HashSet,
        future::Future,
        marker::PhantomData,
        result::Result as StdResult,
        time::Duration,
    },
    wcn_rpc::{
        client::{self, middleware::MeteredExt},
        identity::Keypair,
        middleware::Metered,
        transport::NoHandshake,
    },
};

/// Raft API client.
#[derive(Clone, Debug)]
pub struct Client<C> {
    rpc_client: RpcClient,
    _type_config: PhantomData<C>,
}

type RpcClient = Metered<wcn_rpc::quic::Client>;

/// [`Client`] config.
#[derive(Clone)]
pub struct Config {
    /// [`Keypair`] of the [`Client`].
    pub keypair: Keypair,

    /// Timeout of establishing a network connection.
    pub connection_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config::new()
    }
}

impl Config {
    pub fn new() -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            connection_timeout: Duration::from_secs(5),
        }
    }

    /// Overwrites [`Config::keypair`].
    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = keypair;
        self
    }
}

impl<C: TypeConfig> Client<C> {
    /// Creates a new [`Client`].
    pub fn new(config: Config) -> StdResult<Self, CreationError> {
        let rpc_client_config = wcn_rpc::client::Config {
            keypair: config.keypair,
            known_peers: HashSet::new(),
            handshake: NoHandshake,
            connection_timeout: config.connection_timeout,
            server_name: crate::RPC_SERVER_NAME,
        };

        let rpc_client = wcn_rpc::quic::Client::new(rpc_client_config)
            .map_err(|err| CreationError(err.to_string()))?
            .metered();

        Ok(Self {
            rpc_client,
            _type_config: PhantomData,
        })
    }

    /// Adds a member to the Raft network.
    pub async fn add_member(
        &self,
        destination: &Multiaddr,
        req: &AddMemberRequest<C>,
    ) -> client::Result<AddMemberResult<C>> {
        AddMember::<C>::send(&self.rpc_client, destination, req).await
    }

    /// Removes a member from the Raft network.
    pub async fn remove_member(
        &self,
        destination: &Multiaddr,
        req: &RemoveMemberRequest<C>,
    ) -> client::Result<RemoveMemberResult<C>> {
        RemoveMember::<C>::send(&self.rpc_client, destination, req).await
    }

    /// Proposes a change to the Raft state.
    pub async fn propose_change(
        &self,
        destination: &Multiaddr,
        req: &ProposeChangeRequest<C>,
    ) -> client::Result<ProposeChangeResult<C>> {
        ProposeChange::<C>::send(&self.rpc_client, destination, req).await
    }

    /// Appends entries to the Raft log.
    pub async fn append_entries(
        &self,
        destination: &Multiaddr,
        req: &AppendEntriesRequest<C>,
    ) -> client::Result<AppendEntriesResult<C>> {
        AppendEntries::<C>::send(&self.rpc_client, destination, req).await
    }

    /// Installs a new snapshot of the Raft state.
    pub async fn install_snapshot(
        &self,
        destination: &Multiaddr,
        req: &InstallSnapshotRequest<C>,
    ) -> client::Result<InstallSnapshotResult<C>> {
        InstallSnapshot::<C>::send(&self.rpc_client, destination, req).await
    }

    /// Makes a candidate vote.
    pub async fn vote(
        &self,
        destination: &Multiaddr,
        req: &VoteRequest<C>,
    ) -> client::Result<VoteResult<C>> {
        Vote::<C>::send(&self.rpc_client, destination, req).await
    }
}

impl<C: TypeConfig> raft::Network<C> for Client<C>
where
    C::NodeId: Into<PeerId>,
    C::Node: AsRef<Multiaddr>,
    PeerId: Into<C::NodeId>,
{
    type Client = RemotePeer<C>;

    fn new_client(&self, target: C::NodeId, node: &C::Node) -> Self::Client {
        RemotePeer {
            id: target.into(),
            multiaddr: node.as_ref().clone(),
            api_client: self.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemotePeer<C> {
    id: PeerId,
    multiaddr: Multiaddr,
    api_client: Client<C>,
}

impl<C: TypeConfig> raft::Raft<C, raft::RpcApi> for RemotePeer<C>
where
    PeerId: Into<C::NodeId>,
{
    fn add_member(
        &self,
        req: AddMemberRequest<C>,
    ) -> impl Future<Output = AddMemberRpcResult<C>> + Send {
        async move {
            self.api_client
                .add_member(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }

    fn remove_member(
        &self,
        req: RemoveMemberRequest<C>,
    ) -> impl Future<Output = RemoveMemberRpcResult<C>> + Send {
        async move {
            self.api_client
                .remove_member(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }

    fn propose_change(
        &self,
        req: ProposeChangeRequest<C>,
    ) -> impl Future<Output = ProposeChangeRpcResult<C>> + Send {
        async move {
            self.api_client
                .propose_change(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }

    fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> impl Future<Output = AppendEntriesRpcResult<C>> + Send {
        async move {
            self.api_client
                .append_entries(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }

    fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> impl Future<Output = InstallSnapshotRpcResult<C>> + Send {
        async move {
            self.api_client
                .install_snapshot(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }

    fn vote(&self, req: VoteRequest<C>) -> impl Future<Output = VoteRpcResult<C>> + Send {
        async move {
            self.api_client
                .vote(&self.multiaddr, &req)
                .await
                .map_err(|e| RpcError::<C, _>::Unreachable(raft::UnreachableError::new(&e)))?
                .map_err(|e| RemoteError::new(self.id.into(), e).into())
        }
    }
}

/// Error of [`Client::new`].
#[derive(Clone, Debug, thiserror::Error)]
#[error("{_0}")]
pub struct CreationError(String);
