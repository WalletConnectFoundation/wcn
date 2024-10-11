#![allow(clippy::manual_async_fn)]

use irn_rpc as rpc;
pub use irn_rpc::{identity, Multiaddr, PeerId};

pub mod auth;
#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
pub use api::auth as ns_auth;
#[cfg(feature = "server")]
pub use server::Server;

type CreateAuthNonce =
    rpc::Unary<{ rpc::id(b"create_nonce") }, (), Result<ns_auth::Nonce, auth::Error>>;
type CreateAuthToken =
    rpc::Unary<{ rpc::id(b"create_auth") }, auth::TokenConfig, Result<auth::Token, auth::Error>>;
