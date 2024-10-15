#![allow(clippy::manual_async_fn)]

pub use {
    api::auth as ns_auth,
    irn_rpc::{identity, Multiaddr, PeerId},
};
use {
    irn_rpc as rpc,
    serde::{Deserialize, Serialize},
};

pub mod auth;
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
pub struct KeyspaceUpdate(Vec<u8>);

type CreateAuthNonce =
    rpc::Unary<{ rpc::id(b"create_nonce") }, (), Result<ns_auth::Nonce, auth::Error>>;
type CreateAuthToken =
    rpc::Unary<{ rpc::id(b"create_auth") }, auth::TokenConfig, Result<auth::Token, auth::Error>>;
type GetKeyspace = rpc::Unary<{ rpc::id(b"get_keyspace") }, (), Result<KeyspaceUpdate, Error>>;
type KeyspaceUpdates = rpc::Streaming<{ rpc::id(b"keyspace_updates") }, (), KeyspaceUpdate>;

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum Error {
    #[error("Serialization failed")]
    Serialization,
}
