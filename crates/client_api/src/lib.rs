#![allow(clippy::manual_async_fn)]

use {
    auth::token,
    irn_rpc as rpc,
    serde::{Deserialize, Serialize},
    std::collections::HashSet,
};

#[cfg(feature = "client")]
pub mod client;
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

/// Storage subscription event.
#[derive(Clone, Debug)]
pub struct SubscriptionEvent {
    /// Channel the message has been published to.
    pub channel: Vec<u8>,

    /// Published message.
    pub message: Vec<u8>,
}

type Publish = rpc::Oneshot<{ rpc::id(b"publish") }, PublishRequest>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PublishRequest {
    channel: Vec<u8>,
    message: Vec<u8>,
}

type Subscribe = rpc::Streaming<{ rpc::id(b"subscribe") }, SubscribeRequest, SubscribeResponse>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SubscribeRequest {
    channels: HashSet<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SubscribeResponse {
    channel: Vec<u8>,
    message: Vec<u8>,
}
