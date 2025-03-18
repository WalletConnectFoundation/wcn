#![allow(clippy::manual_async_fn)]

use wcn_rpc as rpc;
pub use wcn_rpc::{identity, Multiaddr, PeerId};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub use client::Client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub use server::new_rpc_server;

const RPC_SERVER_NAME: rpc::ServerName = rpc::ServerName::new("pulseApi");

type Heartbeat = rpc::Unary<{ rpc::id(b"heartbeat") }, (), ()>;
