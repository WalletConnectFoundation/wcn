use {
    crate::{
        keyspace,
        node_operator,
        smart_contract,
        view,
        Inner,
        Keyspace,
        SerializedEvent,
        View,
    },
    futures::{Stream, StreamExt},
    std::{pin::pin, sync::Arc, time::Duration},
    tokio::sync::watch,
};

pub(super) struct Task<SC, Shards, Events> {
    pub initial_events: Option<Events>,
    pub inner: Arc<Inner<SC, Shards>>,
    pub watch: watch::Sender<()>,
}

pub(super) struct Guard(tokio::task::JoinHandle<()>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.abort();
        tracing::info!("aborted");
    }
}

impl<SC, Shards, Events> Task<SC, Shards, Events>
where
    SC: smart_contract::Read,
    Shards: Clone + Send + Sync + 'static,
    Events: Stream<Item = smart_contract::ReadResult<SerializedEvent>> + Send + 'static,
    Keyspace: keyspace::sealed::Calculate<Shards>,
{
    pub(super) fn spawn(self) -> Guard {
        let guard = Guard(tokio::spawn(self.run()));
        tracing::info!("spawned");
        guard
    }

    async fn run(mut self) {
        loop {
            // apply initial events until they finish / first error
            if let Some(events) = self.initial_events.take() {
                match self.apply_events(events).await {
                    Ok(()) => tracing::warn!("Initial event stream finished"),
                    Err(err) => tracing::error!(%err, "Failed to apply initial events"),
                }
            }

            loop {
                // when we fail for whatever reason - subscribe again and refetch the whole
                // state
                match self.update_view().await {
                    Ok(()) => tracing::warn!("Event stream finished"),
                    Err(err) => {
                        tracing::error!(%err, "Failed to update cluster::View");
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            }
        }
    }

    async fn update_view(&mut self) -> Result<()> {
        let events = self.inner.smart_contract.events().await?;

        let new_view = View::fetch(&self.inner.smart_contract).await?;
        if self.inner.view.load().cluster_version != new_view.cluster_version {
            let new_view = Arc::new(new_view.calculate_keyspace().await);
            self.inner.view.store(new_view);
            let _ = self.watch.send(());
        }

        self.apply_events(events).await
    }

    async fn apply_events(
        &mut self,
        events: impl Stream<Item = smart_contract::ReadResult<SerializedEvent>>,
    ) -> Result<()> {
        let mut events = pin!(events);

        while let Some(res) = events.next().await {
            let event = res?.deserialize()?;
            tracing::info!(?event, "received");

            let view = self.inner.view.load_full();
            let view = Arc::new((*view).clone().apply_event(event).await?);
            self.inner.view.store(view);
            let _ = self.watch.send(());
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    ApplyEvent(#[from] view::Error),

    #[error(transparent)]
    DataDeserialization(#[from] node_operator::DataDeserializationError),

    #[error(transparent)]
    ViewFetch(#[from] view::FetchError),

    #[error(transparent)]
    SmartContractRead(#[from] smart_contract::ReadError),
}

type Result<T, E = Error> = std::result::Result<T, E>;
