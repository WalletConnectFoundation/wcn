use {
    super::{Client, Error, Result},
    crate::{transport::BiDirectionalStream, Id as RpcId, Name as RpcName},
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

/// Extension trait wrapping [`Client`]s with [`Metered`] middleware.
pub trait MeteredExt: Sized {
    /// Wraps `Self` with [`Metered`].
    fn metered(self) -> Metered<Self> {
        Metered { inner: self }
    }
}

impl<C> MeteredExt for C where C: super::Marker {}

impl<A, C> Client<A> for Metered<C>
where
    A: Sync,
    C: Client<A>,
{
    fn send_rpc<Fut: Future<Output = Result<Ok>> + Send, Ok>(
        &self,
        addr: &A,
        rpc_id: RpcId,
        f: impl FnOnce(BiDirectionalStream) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send {
        self.inner
            .send_rpc(addr, rpc_id, f)
            .with_metrics(future_metrics!(
                "outbound_rpc",
                StringLabel<"rpc_name"> => RpcName::new(rpc_id).as_str()
            ))
    }
}

impl<C: super::Marker> super::Marker for Metered<C> {}

/// Extension trait wrapping [`Client`]s with [`WithTimeouts`] middleware.
pub trait WithTimeoutsExt: Sized {
    /// Wraps `Self` with [`WithTimeouts`].
    fn with_timeouts(self, timeouts: Timeouts) -> WithTimeouts<Self> {
        WithTimeouts {
            inner: self,
            timeouts: Arc::new(timeouts),
        }
    }
}

impl<C> WithTimeoutsExt for C where C: super::Marker {}

impl<A, C> Client<A> for WithTimeouts<C>
where
    A: Sync,
    C: Client<A>,
{
    fn send_rpc<Fut: Future<Output = Result<Ok>> + Send, Ok>(
        &self,
        addr: &A,
        rpc_id: RpcId,
        f: impl FnOnce(BiDirectionalStream) -> Fut + Send,
    ) -> impl Future<Output = Result<Ok>> + Send {
        async move {
            if let Some(timeout) = self.timeouts.get(rpc_id) {
                self.inner
                    .send_rpc(addr, rpc_id, f)
                    .with_timeout(timeout)
                    .await
                    .map_err(|_| Error::rpc(error_code::TIMEOUT))?
            } else {
                self.inner.send_rpc(addr, rpc_id, f).await
            }
        }
    }
}

impl<C: super::Marker> super::Marker for WithTimeouts<C> {}
