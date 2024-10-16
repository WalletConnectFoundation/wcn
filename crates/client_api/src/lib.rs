#![allow(clippy::manual_async_fn)]

pub use irn_rpc::{identity, Multiaddr, PeerId};
use {
    auth::token,
    irn_rpc as rpc,
    serde::{Deserialize, Serialize},
};

#[cfg(feature = "client")]
pub mod client;
pub mod domain;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::Server;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterUpdate(Vec<u8>);

type CreateAuthNonce = rpc::Unary<{ rpc::id(b"create_nonce") }, (), auth::Nonce>;
type CreateAuthToken =
    rpc::Unary<{ rpc::id(b"create_auth") }, token::Config, Result<token::Token, token::Error>>;
type GetCluster = rpc::Unary<{ rpc::id(b"get_cluster") }, (), Result<ClusterUpdate, Error>>;
type ClusterUpdates = rpc::Streaming<{ rpc::id(b"cluster_updates") }, (), ClusterUpdate>;

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum Error {
    #[error("Serialization failed")]
    Serialization,
}
