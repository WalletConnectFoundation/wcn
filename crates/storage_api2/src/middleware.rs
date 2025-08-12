use {
    crate::{
        operation,
        Callback,
        ConnectionInfo,
        ConnectionType,
        DataItem,
        Error,
        ErrorKind,
        Factory,
        KeyspaceVersion,
        Kind,
        Operation,
        Result,
        StorageApi,
    },
    derive_more::derive::Deref,
    futures::{Stream, TryFutureExt as _},
    std::{
        future::Future,
        net::Ipv4Addr,
        ops::{self, Deref, RangeInclusive},
        time::Duration,
    },
    wc::{
        future::FutureExt,
        metrics::{self, future_metrics, EnumLabel, FutureExt as _, FutureMetrics, StringLabel},
    },
};

pub struct Middleware {
    timeouts: Duration,
}

pub struct MetricLabels {}

impl Middleware {
    pub fn with_operation_timeout(self, timeout: Duration) -> Self {
        todo!()
    }
}

/// Information about a network connection backing a [`StorageApi`]
/// implementation.
pub trait ConnectionInfo: Send + Sync + Sized {
    /// Returns [`ConnectionType`] of the connection.
    fn connection_type(&self) -> ConnectionType;

    /// Returns [`StorageApi`] [`Kind`] of the connection.
    fn api_kind(&self) -> Kind;

    /// Returns [`Ipv4Addr`] of the remote peer.
    fn remote_addr(&self) -> Ipv4Addr;
}

/// [`Connection`] type.
#[derive(Clone, Copy, Debug, IntoStaticStr, PartialEq, Eq, Ordinalize)]
pub enum ConnectionType {
    /// Client connection.
    Client,

    /// Server connection.
    Server,
}

impl metrics::Enum for ConnectionType {
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Deref)]
#[deref(forward)]
pub struct WithTimeout<S> {
    pub inner: S,

    #[deref(ignore)]
    pub timeout: Duration,
}

impl<F: Factory<Args>, Args> Factory<Args> for WithTimeout<F> {
    type StorageApi = WithTimeout<F::StorageApi>;

    fn new_storage_api(&self, args: Args) -> Result<Self::StorageApi> {
        Ok(WithTimeout {
            inner: self.inner.new_storage_api(args)?,
            timeout: self.timeout,
        })
    }
}

impl<S: StorageApi> StorageApi for WithTimeout<S> {
    async fn execute_ref(&self, operation: &Operation<'_>) -> Result<operation::Output> {
        self.inner
            .execute_ref(operation)
            .with_timeout(self.timeout)
            .await
            .map_err(|_| Error::timeout())?
    }

    async fn execute_callback<C: Callback>(
        &self,
        operation: Operation<'_>,
        callback: C,
    ) -> Result<(), C::Error> {
        let mut callback = Some(callback);

        let borrowed_callback = BorrowedCallback {
            inner: &mut callback,
        };

        let res = self
            .inner
            .execute_callback(operation, borrowed_callback)
            .with_timeout(self.timeout)
            .await;

        match res {
            Ok(res) => res,
            Err(_) => {
                BorrowedCallback {
                    inner: &mut callback,
                }
                .send_result(&Err(Error::timeout()))
                .await
            }
        }
    }

    async fn execute(&self, operation: Operation<'_>) -> Result<operation::Output> {
        self.inner
            .execute(operation)
            .with_timeout(self.timeout)
            .await
            .map_err(|_| Error::timeout())?
    }

    fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: KeyspaceVersion,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<DataItem>> + Send>> + Send {
        self.inner.read_data(keyrange, keyspace_version)
    }

    fn write_data(
        &self,
        stream: impl Stream<Item = Result<DataItem>> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        self.inner.write_data(stream)
    }
}

struct BorrowedCallback<'a, C> {
    inner: &'a mut Option<C>,
}

impl<'a, C: Callback> Callback for BorrowedCallback<'a, C> {
    type Error = C::Error;

    async fn send_result(self, result: &Result<operation::Output>) -> Result<(), Self::Error> {
        if let Some(cb) = self.inner.take() {
            cb.send_result(result).await
        } else {
            Ok(())
        }
    }
}

/// [`StorageApi`] with metrics.
#[derive(Clone, Deref)]
#[deref(forward)]
pub struct WithMetrics<S> {
    inner: S,
}

impl<S> WithMetrics<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

fn operation_metrics(
    name: operation::Name,
    conn_info: &impl ConnectionInfo,
) -> &'static FutureMetrics {
    future_metrics!("wcn_storage_api_operation",
        StringLabel<"remote_addr", Ipv4Addr> => &conn_info.remote_addr(),
        EnumLabel<"connection_type", ConnectionType> => conn_info.connection_type(),
        EnumLabel<"api_kind", Kind> => conn_info.api_kind(),
        EnumLabel<"name", operation::Name> => name
    )
}

fn meter_error_ref(err: &Error, operation_name: operation::Name, conn_info: &impl ConnectionInfo) {
    metrics::counter!("wcn_storage_api_errors",
        StringLabel<"remote_addr", Ipv4Addr> => &conn_info.remote_addr(),
        EnumLabel<"connection_type", ConnectionType> => conn_info.connection_type(),
        EnumLabel<"api_kind", Kind> => conn_info.api_kind(),
        EnumLabel<"operation_name", operation::Name> => operation_name,
        EnumLabel<"kind", ErrorKind> => err.kind
    )
    .increment(1);
}

fn meter_error(
    err: Error,
    operation_name: operation::Name,
    conn_info: &impl ConnectionInfo,
) -> Error {
    meter_error_ref(&err, operation_name, conn_info);
    err
}

impl<F: Factory<Args>, Args> Factory<Args> for WithMetrics<F>
where
    F::StorageApi: Deref<Target: ConnectionInfo>,
{
    type StorageApi = WithMetrics<F::StorageApi>;

    fn new_storage_api(&self, args: Args) -> Result<Self::StorageApi> {
        Ok(WithMetrics {
            inner: self.inner.new_storage_api(args)?,
        })
    }
}

impl<S: StorageApi + ops::Deref> StorageApi for WithMetrics<S>
where
    S::Target: ConnectionInfo,
{
    fn execute_ref(
        &self,
        operation: &Operation<'_>,
    ) -> impl Future<Output = Result<operation::Output>> + Send {
        self.inner
            .execute_ref(operation)
            .with_metrics(operation_metrics(operation.name(), self.deref()))
            .map_err(|err| meter_error(err, operation.name(), self.deref()))
    }

    fn execute_callback<C: Callback>(
        &self,
        operation: Operation<'_>,
        callback: C,
    ) -> impl Future<Output = Result<(), C::Error>> + Send {
        let operation_name = operation.name();

        async move {
            let callback = MeteredCallback {
                inner: callback,
                conn_info: self.deref(),
                operation_name,
            };

            let result = self.execute(operation).await;
            callback.send_result(&result).await
        }
        .with_metrics(operation_metrics(operation_name, self.deref()))
    }

    fn execute(
        &self,
        operation: Operation<'_>,
    ) -> impl Future<Output = Result<operation::Output>> + Send {
        let operation_name = operation.name();

        self.inner
            .execute(operation)
            .with_metrics(operation_metrics(operation_name, self.deref()))
            .map_err(move |err| meter_error(err, operation_name, self.deref()))
    }

    fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: KeyspaceVersion,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<DataItem>> + Send>> + Send {
        self.inner
            .read_data(keyrange, keyspace_version)
            .with_metrics(future_metrics!("wcn_storage_api_read_data",
                StringLabel<"remote_addr", Ipv4Addr> => &self.remote_addr(),
                EnumLabel<"connection_type", ConnectionType> => self.connection_type(),
                EnumLabel<"api_kind", Kind> => self.api_kind()
            ))
    }

    fn write_data(
        &self,
        stream: impl Stream<Item = Result<DataItem>> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        self.inner
            .write_data(stream)
            .with_metrics(future_metrics!("wcn_storage_api_write_data",
                StringLabel<"remote_addr", Ipv4Addr> => &self.remote_addr(),
                EnumLabel<"connection_type", ConnectionType> => self.connection_type(),
                EnumLabel<"api_kind", Kind> => self.api_kind()
            ))
    }
}

struct MeteredCallback<'a, C, I> {
    inner: C,
    conn_info: &'a I,
    operation_name: operation::Name,
}

impl<C, I> Callback for MeteredCallback<'_, C, I>
where
    C: Callback,
    I: ConnectionInfo,
{
    type Error = C::Error;

    fn send_result(
        self,
        result: &Result<operation::Output>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Err(err) = result {
            meter_error_ref(err, self.operation_name, self.conn_info);
        }

        self.inner.send_result(result)
    }
}
