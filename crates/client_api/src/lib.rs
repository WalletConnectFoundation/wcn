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
#[cfg(feature = "server")]
pub use server::Server;

type CreateAuthToken =
    rpc::Unary<{ rpc::id(b"create_auth") }, auth::TokenConfig, Result<auth::Token, auth::Error>>;
