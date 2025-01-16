use {
    super::{ClientConnectionInfo, Server},
    crate::{
        transport::{BiDirectionalStream, Read, Write},
        Id as RpcId,
        Name as RpcName,
    },
    libp2p::PeerId,
    std::{collections::HashSet, future::Future, sync::Arc},
    wc::{
        future::FutureExt as _,
        metrics::{future_metrics, FutureExt as _, StringLabel},
    },
};

pub use crate::middleware::*;

/// Extension trait wrapping [`Server`]s with [`Metered`] middleware.
pub trait MeteredExt: Sized {
    /// Wraps `Self` with [`Metered`].
    fn metered(self) -> Metered<Self> {
        Metered { inner: self }
    }
}

impl<S> MeteredExt for S where S: Server {}

impl<S> Server for Metered<S>
where
    S: Server,
{
    type Handshake = S::Handshake;
    type ConnectionData = S::ConnectionData;
    type Codec = S::Codec;

    fn config(&self) -> &super::Config<Self::Handshake> {
        self.inner.config()
    }

    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream<impl Read, impl Write>,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + 'a {
        self.inner
            .handle_rpc(id, stream, conn_info)
            .with_metrics(future_metrics!(
                "inbound_rpc",
                StringLabel<"rpc_name"> => RpcName::new(id).as_str()
            ))
    }
}

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

impl<S> WithTimeoutsExt for S where S: Server {}

impl<S> Server for WithTimeouts<S>
where
    S: Server,
{
    type Handshake = S::Handshake;
    type ConnectionData = S::ConnectionData;
    type Codec = S::Codec;

    fn config(&self) -> &super::Config<Self::Handshake> {
        self.inner.config()
    }

    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream<impl Read, impl Write>,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + 'a {
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

/// RPC server with configured RPC authorization.
#[derive(Clone, Debug)]
pub struct WithAuth<S> {
    server: S,
    auth: Auth,
}

/// RPC authorization config.
#[derive(Clone, Debug)]
pub struct Auth {
    /// A list of clients authorized to use the RPC server.
    pub authorized_clients: HashSet<PeerId>,

    /// Disable auth for testing purposes.
    pub disabled: bool,
}

impl Auth {
    pub fn new(authorized_clients: impl Into<HashSet<PeerId>>) -> Self {
        Self {
            authorized_clients: authorized_clients.into(),
            disabled: false,
        }
    }

    pub fn disabled() -> Self {
        Self {
            authorized_clients: Default::default(),
            disabled: true,
        }
    }
}

/// Extension trait wrapping [`Server`]s with [`WithAuth`] middleware.
pub trait WithAuthExt: Sized {
    /// Wraps `Self` with [`WithAuth`].
    fn with_auth(self, auth: Auth) -> WithAuth<Self> {
        WithAuth { server: self, auth }
    }
}

impl<S> WithAuthExt for S where S: Server {}

impl<S> Server for WithAuth<S>
where
    S: Server,
{
    type Handshake = S::Handshake;
    type ConnectionData = S::ConnectionData;
    type Codec = S::Codec;

    fn config(&self) -> &super::Config<Self::Handshake> {
        self.server.config()
    }

    fn handle_rpc<'a>(
        &'a self,
        id: RpcId,
        stream: BiDirectionalStream<impl Read, impl Write>,
        conn_info: &'a ClientConnectionInfo<Self>,
    ) -> impl Future<Output = ()> + 'a {
        async move {
            let auth = &self.auth;

            if !auth.disabled && !auth.authorized_clients.contains(&conn_info.peer_id) {
                tracing::warn!(%conn_info.peer_id, "unauthorized");
                return;
            }

            self.server.handle_rpc(id, stream, conn_info).await
        }
    }
}
