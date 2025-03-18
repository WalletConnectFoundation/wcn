use {
    super::*,
    std::future::Future,
    wcn_rpc::{
        server::ClientConnectionInfo,
        transport::{BiDirectionalStream, NoHandshake, PostcardCodec},
    },
};

pub fn new_rpc_server() -> impl rpc::Server {
    RpcServer {
        config: rpc::server::Config {
            name: RPC_SERVER_NAME,
            handshake: NoHandshake,
        },
    }
}

#[derive(Clone, Debug)]
struct RpcServer {
    config: rpc::server::Config,
}

impl rpc::Server for RpcServer {
    type Handshake = NoHandshake;
    type ConnectionData = ();
    type Codec = PostcardCodec;

    fn config(&self) -> &rpc::server::Config {
        &self.config
    }

    fn handle_rpc<'a>(
        &'a self,
        id: rpc::Id,
        stream: BiDirectionalStream,
        _conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = match id {
                Heartbeat::ID => Heartbeat::handle(stream, |()| async { Ok(()) }).await,
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
