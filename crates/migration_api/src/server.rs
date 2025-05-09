use {
    super::*,
    futures::{SinkExt as _, Stream, StreamExt as _},
    std::{future::Future, pin::pin},
    wcn_rpc::{
        server::{middleware::MeteredExt, ClientConnectionInfo},
        transport::{BiDirectionalStream, NoHandshake, PostcardCodec},
    },
};

/// Migration API server.
pub trait Server: Clone + Send + Sync + 'static {
    /// Pulls data from this [`Server`].
    fn pull_data(
        &self,
        peer_id: &PeerId,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<impl Stream<Item = ExportItem> + Send>> + Send;

    /// Converts this Migration API [`Server`] into an [`rpc::Server`].
    fn into_rpc_server(self) -> impl rpc::Server {
        let rpc_server_config = wcn_rpc::server::Config {
            name: crate::RPC_SERVER_NAME,
            handshake: NoHandshake,
        };

        RpcServer {
            api_server: self,
            config: rpc_server_config,
        }
        .metered()
    }
}

#[derive(Clone, Debug)]
struct RpcServer<S> {
    api_server: S,
    config: rpc::server::Config,
}

impl<S: Server> rpc::Server for RpcServer<S> {
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &wcn_rpc::server::Config<NoHandshake> {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = match id {
                PullData::ID => {
                    PullData::handle(stream, |mut rx, mut tx| async move {
                        let req = rx.recv_message().await?;

                        let resp = self
                            .api_server
                            .pull_data(&conn_info.peer_id, req.keyrange, req.keyspace_version)
                            .await;

                        match resp {
                            Ok(data) => {
                                let data = pin!(data);
                                tx.send_all(&mut data.map(Ok).map(Ok)).await?
                            }
                            Err(e) => tx.send(Err(e.into_rpc_error())).await?,
                        };

                        Ok(())
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

/// Error of a [`Server`] operation.
#[derive(Clone, Debug)]
pub enum Error {
    /// Client is not a cluster member.
    NotClusterMember,

    /// Keyspace versions of client and server don't match.
    KeyspaceVersionMismatch,

    /// Storage export operation error.
    StorageExport(String),
}

impl Error {
    fn into_rpc_error(self) -> wcn_rpc::Error {
        match self {
            Self::NotClusterMember => wcn_rpc::Error::new(error_code::NOT_CLUSTER_MEMBER),
            Self::KeyspaceVersionMismatch => {
                wcn_rpc::Error::new(error_code::KEYSPACE_VERSION_MISMATCH)
            }
            Self::StorageExport(desc) => wcn_rpc::Error {
                code: error_code::STORAGE_EXPORT_FAILED.into(),
                description: Some(desc.into()),
            },
        }
    }
}

/// [`Server`] operation [`Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
