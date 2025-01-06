use {
    super::*,
    futures::FutureExt as _,
    raft::TypeConfig,
    std::{future::Future, marker::PhantomData, time::Duration},
    wcn_rpc::{
        self as rpc,
        middleware::Timeouts,
        server::{
            self,
            middleware::{MeteredExt as _, WithTimeoutsExt as _},
            ClientConnectionInfo,
        },
        transport::{BiDirectionalStream, JsonCodec, NoHandshake},
        PeerId,
        Rpc as _,
    },
};

/// [`Server`] config.
pub struct Config {
    /// Timeout of a [`Server`] operation.
    pub operation_timeout: Duration,
}

/// Raft API server.
pub trait Server<C: TypeConfig>: Clone + Send + Sync + 'static {
    /// Indicates whether the specified node is member.
    fn is_member(&self, peer_id: &PeerId) -> bool;

    /// Adds a member to the Raft network.
    fn add_member(
        &self,
        peer_id: &PeerId,
        req: AddMemberRequest<C>,
    ) -> impl Future<Output = AddMemberResult<C>> + Send;

    /// Removes a member from the Raft network.
    fn remove_member(
        &self,
        peer_id: &PeerId,
        req: RemoveMemberRequest<C>,
    ) -> impl Future<Output = RemoveMemberResult<C>> + Send;

    /// Proposes a change to the Raft state.
    fn propose_change(
        &self,
        req: ProposeChangeRequest<C>,
    ) -> impl Future<Output = ProposeChangeResult<C>> + Send;

    /// Appends entries to the Raft log.
    fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> impl Future<Output = AppendEntriesResult<C>> + Send;

    /// Installs a new snapshot of the Raft state.
    fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> impl Future<Output = InstallSnapshotResult<C>> + Send;

    /// Makes a candidate vote.
    fn vote(&self, req: VoteRequest<C>) -> impl Future<Output = VoteResult<C>> + Send;

    /// Converts this Raft API [`Server`] into an [`rpc::Server`].
    fn into_rpc_server(self, cfg: Config) -> impl rpc::Server {
        let timeouts = Timeouts::new().with_default(cfg.operation_timeout);

        let rpc_server_config = wcn_rpc::server::Config {
            name: crate::RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        RpcServer {
            api_server: self,
            config: rpc_server_config,
            _type_config: PhantomData,
        }
        .with_timeouts(timeouts)
        .metered()
    }
}

#[derive(Clone, Debug)]
struct RpcServer<S, C> {
    api_server: S,
    config: rpc::server::Config,

    _type_config: PhantomData<C>,
}

impl<S, C: TypeConfig> rpc::Server for RpcServer<S, C>
where
    S: Server<C>,
{
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = JsonCodec;

    fn config(&self) -> &server::Config<Self::Handshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: wcn_rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let peer_id = &conn_info.peer_id;

            let _ = match id {
                AddMember::<C>::ID => {
                    AddMember::handle(stream, |req| {
                        self.api_server.add_member(peer_id, req).map(Ok)
                    })
                    .await
                }
                RemoveMember::<C>::ID => {
                    RemoveMember::handle(stream, |req| {
                        self.api_server.remove_member(peer_id, req).map(Ok)
                    })
                    .await
                }

                // Adding `Unauthorized` error to responses of these RPCs is total PITA, as the
                // types are defined in `openraft` itself.
                // So if the requestor is not a member we just drop the request.
                // This should generally never happen under normal circumstances, unless we are
                // dealing with a malicious actor.
                ProposeChange::<C>::ID => {
                    if !self.api_server.is_member(peer_id) {
                        return;
                    }
                    ProposeChange::handle(stream, |req| self.api_server.propose_change(req).map(Ok))
                        .await
                }
                AppendEntries::<C>::ID => {
                    if !self.api_server.is_member(peer_id) {
                        return;
                    }
                    AppendEntries::handle(stream, |req| self.api_server.append_entries(req).map(Ok))
                        .await
                }
                InstallSnapshot::<C>::ID => {
                    if !self.api_server.is_member(peer_id) {
                        return;
                    }
                    InstallSnapshot::handle(stream, |req| {
                        self.api_server.install_snapshot(req).map(Ok)
                    })
                    .await
                }
                Vote::<C>::ID => {
                    if !self.api_server.is_member(peer_id) {
                        return;
                    }
                    Vote::<C>::handle(stream, |req| self.api_server.vote(req).map(Ok)).await
                }

                id => {
                    return tracing::warn!(
                        "Unexpected raft RPC: {}",
                        wcn_rpc::Name::new(id).as_str()
                    )
                }
            }
            .map_err(|err| {
                tracing::debug!(
                    name = wcn_rpc::Name::new(id).as_str(),
                    ?err,
                    "Failed to handle raft RPC"
                )
            });
        }
    }
}
