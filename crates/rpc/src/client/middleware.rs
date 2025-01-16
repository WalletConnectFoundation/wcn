use {
    super::{BiDirectionalStream, Client, Error, Result},
    crate::{error_code, ForceSendFuture as _, Id as RpcId, Name as RpcName},
    futures::FutureExt as _,
    libp2p::Multiaddr,
    std::{future::Future, sync::Arc, time::Duration},
    wc::{
        future::FutureExt as _,
        metrics::{self, future_metrics, FutureExt as _, StringLabel},
    },
};

pub use crate::middleware::*;

/// Extension trait wrapping [`Client`]s with [`Metered`] middleware.
pub trait MeteredExt: Sized {
    /// Wraps `Self` with [`Metered`].
    fn metered(self) -> Metered<Self> {
        Metered { inner: self }
    }
}

impl<C> MeteredExt for C where C: super::Marker {}

impl<C> Client for Metered<C>
where
    C: Client,
{
    type Transport = C::Transport;

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok: Send>(
        &'a self,
        addr: &'a Multiaddr,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<Self::Transport>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        self.inner
            .send_rpc(addr, rpc_id, f)
            .with_metrics(future_metrics!(
                "outbound_rpc",
                StringLabel<"rpc_name"> => RpcName::new(rpc_id).as_str(),
                StringLabel<"destination", Multiaddr> => addr
            ))
            .map(move |res| {
                let error_kind = match &res {
                    Ok(_) => return res,
                    Err(Error::NoAvailablePeers) => "no_available_peers",
                    Err(Error::Lock) => "lock",
                    Err(Error::Rng) => "rng",
                    Err(Error::Transport(err)) => err.kind(),
                    Err(Error::Rpc { error, .. }) => error.code.as_ref(),
                };

                metrics::counter!(
                    "outbound_rpc_errors",
                    StringLabel<"rpc_name"> => RpcName::new(rpc_id).as_str(),
                    StringLabel<"destination", Multiaddr> => addr,
                    StringLabel<"error_kind"> => error_kind
                )
                .increment(1);

                res
            })
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
    type Transport = C::Transport;

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok: Send>(
        &'a self,
        addr: &'a A,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<Self::Transport>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        async move {
            if let Some(timeout) = self.timeouts.get(rpc_id) {
                self.inner
                    .send_rpc(addr, rpc_id, f)
                    .with_timeout(timeout)
                    .force_send_impl()
                    .await
                    .map_err(|_| Error::rpc(error_code::TIMEOUT))?
            } else {
                self.inner.send_rpc(addr, rpc_id, f).force_send_impl().await
            }
        }
    }
}

impl<C: super::Marker> super::Marker for WithTimeouts<C> {}

/// RPC client with configured RPC retries.
#[derive(Clone, Debug)]
pub struct WithRetries<T, R> {
    inner: T,
    strategy: R,
}

/// Retry strategy for [`WithRetries`] middleware.
pub trait RetryStrategy: Clone + Send + Sync + 'static {
    /// Specifies whether to perform an RPC retry.
    ///
    /// `attempts` counter starts from `1`.
    ///
    /// Returned [`Duration`] is going to be used as a delay before attempting
    /// the retry. `None` means "don't retry".
    fn requires_retry(&self, rpc_id: RpcId, error: &Error, attempt: usize) -> Option<Duration>;
}

impl<F> RetryStrategy for F
where
    F: Fn(RpcId, &Error, usize) -> Option<Duration> + Clone + Send + Sync + 'static,
{
    fn requires_retry(&self, rpc_id: RpcId, error: &Error, attempt: usize) -> Option<Duration> {
        (self)(rpc_id, error, attempt)
    }
}

/// Extension trait wrapping [`Client`]s with [`WithRetries`] middleware.
pub trait WithRetriesExt: Sized {
    /// Wraps `Self` with [`WithRetries`].
    fn with_retries<S: RetryStrategy>(self, strategy: S) -> WithRetries<Self, S> {
        WithRetries {
            inner: self,
            strategy,
        }
    }
}

impl<C> WithRetriesExt for C where C: super::Marker {}

impl<A, C, R> Client<A> for WithRetries<C, R>
where
    A: Sync,
    C: Client<A>,
    R: RetryStrategy,
{
    type Transport = C::Transport;

    fn send_rpc<'a, Fut: Future<Output = Result<Ok>> + Send + 'a, Ok: Send>(
        &'a self,
        addr: &'a A,
        rpc_id: RpcId,
        f: &'a (impl Fn(BiDirectionalStream<Self::Transport>) -> Fut + Send + Sync + 'a),
    ) -> impl Future<Output = Result<Ok>> + Send + 'a {
        async move {
            let mut attempt = 1;
            loop {
                match self.inner.send_rpc(addr, rpc_id, f).force_send_impl().await {
                    Ok(ok) => return Ok(ok),
                    Err(err) => match self.strategy.requires_retry(rpc_id, &err, attempt) {
                        Some(dur) => tokio::time::sleep(dur).force_send_impl().await,
                        None => return Err(err),
                    },
                }
                attempt += 1;
            }
        }
    }
}

impl<C: super::Marker, R> super::Marker for WithRetries<C, R> {}
