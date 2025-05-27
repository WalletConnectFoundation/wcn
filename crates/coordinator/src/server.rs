use {
    client_api::SubscriptionEvent,
    core::future::Future,
    futures::{FutureExt, Stream},
    replication::Driver,
    std::sync::Arc,
    tap::TapFallible,
    tokio_stream::StreamExt,
};

struct Inner {
    driver: Driver,
}

#[derive(Clone)]
struct Server {
    inner: Arc<Inner>,
}

impl client_api::Server for Server {
    async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) {
        let _ = self
            .inner
            .driver
            .publish(channel, message)
            .await
            .tap_err(|err| {
                tracing::info!(?err, "publish failed");
            });
    }

    fn subscribe(
        &self,
        channels: std::collections::HashSet<Vec<u8>>,
    ) -> impl Future<
        Output = client_api::server::Result<impl Stream<Item = SubscriptionEvent> + Send + 'static>,
    > + Send {
        self.inner.driver.subscribe(channels).map(|res| {
            res.map(|stream| stream.filter_map(Result::ok))
                .map_err(client_err)
        })
    }
}

impl storage_api::Server for Server {
    fn keyspace_version(&self) -> u64 {
        todo!()
    }

    async fn get(
        &self,
        key: storage_api::Key,
    ) -> storage_api::server::Result<Option<storage_api::Record>> {
        todo!()
    }

    async fn set(&self, entry: storage_api::Entry) -> storage_api::server::Result<()> {
        todo!()
    }

    async fn del(
        &self,
        key: storage_api::Key,
        version: storage_api::EntryVersion,
    ) -> storage_api::server::Result<()> {
        todo!()
    }

    async fn get_exp(
        &self,
        key: storage_api::Key,
    ) -> storage_api::server::Result<Option<storage_api::EntryExpiration>> {
        todo!()
    }

    fn set_exp(
        &self,
        key: storage_api::Key,
        expiration: impl Into<storage_api::EntryExpiration>,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        async { todo!() }
    }

    async fn hget(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> storage_api::server::Result<Option<storage_api::Record>> {
        todo!()
    }

    async fn hset(&self, entry: storage_api::MapEntry) -> storage_api::server::Result<()> {
        todo!()
    }

    async fn hdel(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
        version: storage_api::EntryVersion,
    ) -> storage_api::server::Result<()> {
        todo!()
    }

    async fn hcard(&self, key: storage_api::Key) -> storage_api::server::Result<u64> {
        todo!()
    }

    async fn hget_exp(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
    ) -> storage_api::server::Result<Option<storage_api::EntryExpiration>> {
        todo!()
    }

    fn hset_exp(
        &self,
        key: storage_api::Key,
        field: storage_api::Field,
        expiration: impl Into<storage_api::EntryExpiration>,
        version: storage_api::EntryVersion,
    ) -> impl Future<Output = storage_api::server::Result<()>> + Send {
        async { todo!() }
    }

    async fn hscan(
        &self,
        key: storage_api::Key,
        count: u32,
        cursor: Option<storage_api::Field>,
    ) -> storage_api::server::Result<storage_api::MapPage> {
        todo!()
    }
}

#[inline]
fn client_err(err: impl ToString) -> client_api::server::Error {
    client_api::server::Error::new(err)
}

#[inline]
fn storage_err(err: impl ToString) -> storage_api::server::Error {
    storage_api::server::Error::new(err)
}
