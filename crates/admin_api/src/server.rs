use {
    super::*,
    futures::FutureExt as _,
    std::{collections::HashSet, future::Future, time::Duration},
    wcn_rpc::{
        middleware::Timeouts,
        server::{
            middleware::{Auth, MeteredExt as _, WithAuthExt as _, WithTimeoutsExt as _},
            ClientConnectionInfo,
            Transport,
        },
        transport::{BiDirectionalStream, NoHandshake, PostcardCodec, Read, Write},
        Server as _,
    },
};

/// [`Server`] config.
pub struct Config {
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
    fn serve(self, transport: impl Transport, cfg: Config) -> impl Future<Output = ()> {
        let timeouts = Timeouts::new()
            .with::<{ GetMemoryProfile::ID }>(MEMORY_PROFILE_MAX_DURATION)
            .with_default(cfg.operation_timeout);

        let rpc_server_config = wcn_rpc::server::Config {
            name: &crate::RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        let transport_config = wcn_rpc::server::TransportConfig {
            max_concurrent_connections: 10,
            max_concurrent_streams: 100,
        };

        RpcServer {
            api_server: self,
            config: rpc_server_config,
        }
        .with_auth(Auth::new(cfg.authorized_clients))
        .with_timeouts(timeouts)
        .metered()
        .serve(transport, transport_config)
    }
}

pub type GetNodeStatusResult = Result<NodeStatus, GetNodeStatusError>;
pub type DecommissionNodeResult = Result<(), DecommissionNodeError>;
pub type MemoryProfileResult = Result<MemoryProfile, MemoryProfileError>;

#[derive(Clone, Debug)]
struct RpcServer<S> {
    api_server: S,
    config: rpc::server::Config,
}

impl<S> rpc::Server for RpcServer<S>
where
    S: Server,
{
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &rpc::server::Config {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream<impl Read, impl Write>,
        _conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = match id {
                GetClusterView::ID => {
                    GetClusterView::handle(stream, |()| self.api_server.get_cluster_view().map(Ok))
                        .await
                }
                GetNodeStatus::ID => {
                    GetNodeStatus::handle(stream, |()| self.api_server.get_node_status().map(Ok))
                        .await
                }
                DecommissionNode::ID => {
                    DecommissionNode::handle(stream, |req| {
                        self.api_server.decommission_node(req.id, req.force).map(Ok)
                    })
                    .await
                }
                GetMemoryProfile::ID => {
                    GetMemoryProfile::handle(stream, |req| {
                        self.api_server.memory_profile(req.duration).map(Ok)
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

#[derive(Debug, thiserror::Error)]
#[error("{_0:?}")]
pub struct Error(wcn_rpc::quic::Error);
