use futures::{Stream, TryStreamExt};
pub use wcn_cluster::{
    Event,
    smart_contract::{Address, ClusterView, Read},
};

#[cfg(any(feature = "rpc_client", feature = "rpc_server"))]
pub mod rpc;

pub trait ClusterApi: Clone + Send + Sync + 'static {
    fn address(&self) -> impl Future<Output = Result<Address>> + Send;

    fn cluster_view(&self) -> impl Future<Output = Result<ClusterView>> + Send;

    fn events(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Event>> + Send + 'static>> + Send;
}

/// [`ClusterApi`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SmartContractReader<C> {
    smart_contract: C,
}

impl<C> SmartContractReader<C>
where
    C: Read,
{
    pub fn new(smart_contract: C) -> Self {
        Self { smart_contract }
    }
}

impl<C: Read + Clone> ClusterApi for SmartContractReader<C> {
    async fn address(&self) -> Result<Address> {
        Ok(self.smart_contract.address())
    }

    async fn cluster_view(&self) -> Result<ClusterView> {
        self.smart_contract
            .cluster_view()
            .await
            .map_err(Error::internal)
    }

    async fn events(&self) -> Result<impl Stream<Item = Result<Event>> + Send + 'static> {
        Ok(self
            .smart_contract
            .events()
            .await
            .map_err(Error::internal)?
            .map_err(Error::internal))
    }
}

/// [`ClusterApi`] error.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("{kind:?}({message:?})")]
pub struct Error {
    kind: ErrorKind,
    message: Option<String>,
}

impl Error {
    pub fn internal<E: ToString>(err: E) -> Self {
        Self {
            kind: ErrorKind::Internal,
            message: Some(err.to_string()),
        }
    }

    pub fn transport<E: ToString>(err: E) -> Self {
        Self {
            kind: ErrorKind::Transport,
            message: Some(err.to_string()),
        }
    }
}

/// [`Error`] kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Client is not authorized to perform an [`Operation`].
    Unauthorized,

    /// [`Operation`] timeout.
    Timeout,

    /// Internal error.
    Internal,

    /// Transport error.
    Transport,

    /// Unable to determine [`ErrorKind`] of an [`Error`].
    Unknown,
}

impl Error {
    /// Creates a new [`Error`].
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }

    pub fn with_message(mut self, message: impl ToString) -> Self {
        self.message = Some(message.to_string());
        self
    }

    /// Returns [`ErrorKind`] of this [`Error`].
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }
}
