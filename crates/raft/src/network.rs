//! Raft network abstraction.

use {
    super::{
        AppendEntriesRequest,
        AppendEntriesRpcResult,
        InstallSnapshotRequest,
        InstallSnapshotRpcResult,
        Raft,
        RpcApi,
        TypeConfig,
        VoteRequest,
        VoteRpcResult,
    },
    crate::OpenRaft,
    async_trait::async_trait,
};

/// Creating connections between cluster members.
#[async_trait]
pub trait Network<C: TypeConfig>: Clone + Send + Sync + 'static {
    type Client: Raft<C, RpcApi>;

    /// Creates a new client instance sending RPCs to the target node.
    async fn new_client(&self, target: C::NodeId, node: &C::Node) -> Self::Client;
}

#[derive(Clone, Debug)]
pub(crate) struct Adapter<T>(pub T);

#[async_trait]
impl<C: TypeConfig, N: Network<C>> openraft::RaftNetworkFactory<OpenRaft<C>> for Adapter<N> {
    type Network = Adapter<N::Client>;

    async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network {
        Adapter(self.0.new_client(target, node).await)
    }
}

#[async_trait]
impl<C: TypeConfig, R: Raft<C, RpcApi>> openraft::RaftNetwork<OpenRaft<C>> for Adapter<R> {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<C>,
    ) -> AppendEntriesRpcResult<C> {
        self.0.append_entries(req).await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<C>,
    ) -> InstallSnapshotRpcResult<C> {
        self.0.install_snapshot(req).await
    }

    async fn send_vote(&mut self, args: VoteRequest<C>) -> VoteRpcResult<C> {
        self.0.vote(args).await
    }
}
