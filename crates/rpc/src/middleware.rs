//! Shared RPC client/server middleware.

use {
    crate::Id as RpcId,
    std::{collections::HashMap, sync::Arc, time::Duration},
};

/// Metered RPC client or server, enabling outbound and inbound RPC metrics
/// respectively.
#[derive(Clone, Debug)]
pub struct Metered<T> {
    pub(crate) inner: T,
    pub(crate) tag: &'static str,
}

/// RPC client or server with configured RPC timeouts.
#[derive(Clone, Debug)]
pub struct WithTimeouts<T> {
    pub(crate) inner: T,
    pub(crate) timeouts: Arc<Timeouts>,
}

/// Inbound/outbound RPC timeouts.
#[derive(Clone, Debug, Default)]
pub struct Timeouts {
    pub default: Option<Duration>,
    pub rpc: HashMap<RpcId, Option<Duration>>,
}

impl Timeouts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.default = timeout.into();
        self
    }

    pub fn with<const RPC_ID: RpcId>(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.rpc.insert(RPC_ID, timeout.into());
        self
    }

    pub fn get(&self, rpc_id: RpcId) -> Option<Duration> {
        if let Some(timeout) = self.rpc.get(&rpc_id) {
            return *timeout;
        }

        self.default
    }
}
