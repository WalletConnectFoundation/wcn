use {
    super::*,
    futures::FutureExt as _,
    irn_rpc::{
        identity::Keypair,
        middleware::Timeouts,
        server::{
            middleware::{Auth, MeteredExt as _, WithAuthExt as _, WithTimeoutsExt as _},
            ConnectionInfo,
        },
        transport::{BiDirectionalStream, NoHandshake},
    },
    std::{collections::HashSet, future::Future, time::Duration},
};

/// [`Server`] config.
pub struct Config {
    /// [`Multiaddr`] of the server.
    pub addr: Multiaddr,

    /// [`Keypair`] of the server.
    pub keypair: Keypair,

    /// Timeout of a [`Server`] operation.
    pub operation_timeout: Duration,

    /// A list of clients authorized to use the Admin API.
    pub authorized_clients: HashSet<PeerId>,
}

/// Admin API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Gets [`ClusterView`].
    fn get_cluster_view(&self) -> impl Future<Output = ClusterView> + Send;

    /// Gets [`NodeStatus`].
    fn get_node_status(&self) -> impl Future<Output = GetNodeStatusResult> + Send;

    /// Decommissions a node.
    ///
    /// If `force` is true the node must be decommissioned even if it's not in
    /// the `Normal` state.
    fn decommission_node(
        &self,
        id: PeerId,
        force: bool,
    ) -> impl Future<Output = DecommissionNodeResult> + Send;

    fn memory_profile(
        &self,
        duration: Duration,
    ) -> impl Future<Output = MemoryProfileResult> + Send;

    /// Runs this [`Server`] using the provided [`Config`].
    fn serve(self, cfg: Config) -> Result<impl Future<Output = ()>, Error> {
        let timeouts = Timeouts::new()
            .with::<{ GetMemoryProfile::ID }>(MEMORY_PROFILE_MAX_DURATION)
            .with_default(cfg.operation_timeout);

        let rpc_server = Adapter { server: self }
            .with_auth(Auth {
                authorized_clients: cfg.authorized_clients,
            })
            .with_timeouts(timeouts)
            .metered();

        let rpc_server_config = irn_rpc::server::Config {
            name: "admin_api",
            addr: cfg.addr,
            keypair: cfg.keypair,
            max_concurrent_connections: 10,
            max_concurrent_rpcs: 100,
        };

        irn_rpc::quic::server::run(rpc_server, rpc_server_config, NoHandshake).map_err(Error)
    }
}

pub type GetNodeStatusResult = Result<NodeStatus, GetNodeStatusError>;
pub type DecommissionNodeResult = Result<(), DecommissionNodeError>;
pub type MemoryProfileResult = Result<MemoryProfile, MemoryProfileError>;

#[derive(Clone, Debug)]
struct Adapter<S> {
    server: S,
}

impl<S> rpc::Server for Adapter<S>
where
    S: Server,
{
    fn handle_rpc(
        &self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        _conn_info: &ConnectionInfo,
    ) -> impl Future<Output = ()> + Send {
        async move {
            let _ = match id {
                GetClusterView::ID => {
                    GetClusterView::handle(stream, |()| self.server.get_cluster_view().map(Ok))
                        .await
                }
                GetNodeStatus::ID => {
                    GetNodeStatus::handle(stream, |()| self.server.get_node_status().map(Ok)).await
                }
                DecommissionNode::ID => {
                    DecommissionNode::handle(stream, |req| {
                        self.server.decommission_node(req.id, req.force).map(Ok)
                    })
                    .await
                }
                GetMemoryProfile::ID => {
                    GetMemoryProfile::handle(stream, |req| {
                        self.server.memory_profile(req.duration).map(Ok)
                    })
                    .await
                }

                id => return tracing::warn!("Unexpected RPC: {}", rpc::Name::new(id)),
            }
            .map_err(
                |err| tracing::debug!(name = %rpc::Name::new(id), ?err, "Failed to handle RPC"),
            );
        }
    }
}

impl<S> rpc::server::Marker for Adapter<S> {}

#[derive(Debug, thiserror::Error)]
#[error("{_0:?}")]
pub struct Error(irn_rpc::quic::Error);
