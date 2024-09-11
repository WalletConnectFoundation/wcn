use {
    super::{ConnectionInfo, Server},
    crate::{
        transport::{BiDirectionalStream, Handshake},
        Id as RpcId,
        Name as RpcName,
    },
    std::{future::Future, sync::Arc},
    wc::{
        future::FutureExt as _,
        metrics::{future_metrics, FutureExt as _, StringLabel},
    },
};

pub use crate::middleware::*;

/// Error codes produced by middleware defined in this module.
pub mod error_code {
    pub use crate::middleware::error_code::*;
}

/// Extension trait wrapping [`Server`]s with [`Metered`] middleware.
pub trait MeteredExt: Sized {
    /// Wraps `Self` with [`Metered`].
    fn metered(self) -> Metered<Self> {
        Metered { inner: self }
    }
}

impl<S> MeteredExt for S where S: super::Marker {}

impl<H: Handshake, S> Server<H> for Metered<S>
where
    S: Server<H>,
{
    fn handle_rpc(
        &self,
        id: RpcId,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<H::Ok>,
    ) -> impl Future<Output = ()> {
        self.inner
            .handle_rpc(id, stream, conn_info)
            .with_metrics(future_metrics!(
                "inbound_rpc",
                StringLabel<"rpc_name"> => RpcName::new(id).as_str()
            ))
    }
}

impl<S> super::Marker for Metered<S> {}

/// Extension trait wrapping [`Server`]s with [`WithTimeouts`] middleware.
pub trait WithTimeoutsExt: Sized {
    /// Wraps `Self` with [`WithTimeouts`].
    fn with_timeouts(self, timeouts: Timeouts) -> WithTimeouts<Self> {
        WithTimeouts {
            inner: self,
            timeouts: Arc::new(timeouts),
        }
    }
}

impl<S> WithTimeoutsExt for S where S: super::Marker {}

impl<H, S> Server<H> for WithTimeouts<S>
where
    H: Handshake,
    S: Server<H>,
{
    fn handle_rpc(
        &self,
        id: RpcId,
        stream: BiDirectionalStream,
        conn_info: &ConnectionInfo<H::Ok>,
    ) -> impl Future<Output = ()> {
        async move {
            if let Some(timeout) = self.timeouts.get(id) {
                let _ = self
                    .inner
                    .handle_rpc(id, stream, conn_info)
                    .with_timeout(timeout)
                    .await
                    .map_err(|_| tracing::warn!("inbound RPC timeout"));
            } else {
                self.inner.handle_rpc(id, stream, conn_info).await
            }
        }
    }
}

impl<S> super::Marker for WithTimeouts<S> {}
