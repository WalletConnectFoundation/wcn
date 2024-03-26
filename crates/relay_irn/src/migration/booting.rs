use {
    super::{CommitHintedOperations, Export, Import},
    crate::{
        cluster::{
            self,
            keyspace::{
                pending_ranges::{PendingRange, PendingRanges},
                KeyPosition,
                KeyRange,
            },
            VersionedRequest,
        },
        migration,
        network::SendRequest,
        BootingMigrations,
        Node,
        PeerId,
        Storage,
    },
    async_trait::async_trait,
    backoff::{future::retry_notify, ExponentialBackoffBuilder},
    futures::{stream, TryFutureExt, TryStreamExt},
    serde::{Deserialize, Serialize},
    std::time::Duration,
    tap::TapFallible,
    tracing::error,
};

/// [`Network`] request for pulling data from a remote node.
pub type PullDataRequest = VersionedRequest<PullDataRequestArgs>;

/// [`PullDataRequest`] arguments.
pub struct PullDataRequestArgs {
    pub key_range: KeyRange<KeyPosition>,
}

/// Error response to a [`PullDataRequest`].
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullDataError {
    #[error(transparent)]
    ClusterViewVersionMismatch(#[from] cluster::ViewVersionMismatch),

    #[error("Export operation failed: {0}")]
    Export(String),
}

/// Response to a [`PullDataRequest`].
pub type PullDataResponse<Data> = Result<Data, PullDataError>;

impl<C, N, S> Node<C, N, S> {
    pub async fn handle_pull_data_request(&self, req: PullDataRequest) -> PullDataResponse<S::Ok>
    where
        S: Storage<Export>,
    {
        let req = req.validate_and_unpack(self.cluster.read().await.view().version())?;

        let export = Export {
            key_range: req.key_range,
        };

        self.migration_manager
            .storage
            .exec(export)
            .await
            .tap_err(|err| error!(?err, "Export operation failed"))
            .map_err(|err| PullDataError::Export(err.to_string()))
    }
}

#[async_trait]
impl<N, S, Data> BootingMigrations for migration::Manager<N, S>
where
    N: SendRequest<PullDataRequest, Response = PullDataResponse<Data>>,
    S: Storage<Import<Data>> + Storage<CommitHintedOperations>,
    Data: Send,
{
    async fn run_booting_migrations(self) {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(None)
            .build();

        let pull_pending = || async {
            let (pending_ranges, cluster_view_version) = {
                let cluster = self.cluster.read().await;
                let ranges = cluster.pending_ranges(&self.id).cloned();
                let ver = cluster.view().version();
                (ranges, ver)
            };

            let Some(ranges) = pending_ranges else {
                tracing::warn!(id = %self.id, "no pending ranges");
                return Ok(());
            };

            self.pull_pending_ranges(ranges, cluster_view_version)
                .await
                .map_err(backoff::Error::transient)
        };

        let notify = |err, _| error!(?err, "Failed to pull pending ranges, retrying");
        retry_notify(backoff, pull_pending, notify).await.ok();

        self.key_range_statuses.lock().unwrap().clear();
    }
}

impl<N, S, Data> migration::Manager<N, S>
where
    N: SendRequest<PullDataRequest, Response = PullDataResponse<Data>>,
    S: Storage<Import<Data>> + Storage<CommitHintedOperations>,
    Data: Send,
{
    async fn pull_pending_ranges(
        &self,
        ranges: PendingRanges,
        cluster_view_version: u128,
    ) -> Result<(), PullError<N::Error>> {
        // Since we're pulling data from other nodes, we're not constrained by disk read
        // speed, so concurrency here should be higher than on 'push' migrations.
        const CONCURRENCY: usize = 16;

        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(None)
            .build();
        let backoff = &backoff;

        stream::iter(ranges.into_iter().map(Ok))
            .try_for_each_concurrent(CONCURRENCY, |range| async move {
                let PendingRange::Pull { range, sources } = range else {
                    // TODO: Shouldn't be possible on the type level
                    tracing::warn!(?range, "push range in node booting migration");
                    return Ok(());
                };

                // Pick the first source available. Ideally, we want to prioritize based on
                // proximity for optimal network usage.
                // TODO: prioritize based on the node being replaced, so that consistency
                // guarantees are not violated. The most straightforward way to do this is to
                // make sure that pending ranges return sources in the order of priority.
                let Some(source) = sources.iter().next() else {
                    tracing::warn!(?range, "no valid sources in booting migration");
                    return Ok(());
                };

                let pull = || {
                    self.pull_range(range, *source, cluster_view_version)
                        .map_err(|e| match e {
                            e @ PullError::Response(PullDataError::ClusterViewVersionMismatch(
                                _,
                            )) => backoff::Error::permanent(e),
                            e => backoff::Error::transient(e),
                        })
                };

                let notify = |err, _| error!(?err, ?range, "Failed to pull keyrange, retrying");
                retry_notify(backoff.clone(), pull, notify).await
            })
            .await
    }

    async fn pull_range(
        &self,
        key_range: KeyRange<KeyPosition>,
        source: PeerId,
        cluster_view_version: u128,
    ) -> Result<(), PullError<N::Error>> {
        let args = PullDataRequestArgs { key_range };

        let data = self
            .network
            .send_request(source, VersionedRequest::new(args, cluster_view_version))
            .await
            .map_err(PullError::Network)??;

        self.storage
            .exec(Import { key_range, data })
            .await
            .map_err(|e| PullError::Import(e.to_string()))?;

        self.commit_hinted_operations(key_range)
            .await
            .map_err(|e| PullError::CommitHintedOperations(e.to_string()))
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
enum PullError<N> {
    #[error("Network error: {0}")]
    Network(N),

    #[error("Download request failed: {0}")]
    Response(#[from] PullDataError),

    #[error("Import operation failed: {0}")]
    Import(String),

    #[error("CommitHintedOperations failed: {0}")]
    CommitHintedOperations(String),
}

#[cfg(test)]
mod test {
    use {
        crate::{
            cluster::keyspace::{hashring::Positioned, pending_ranges::PendingRange},
            storage::stub::Set,
            stub,
            BootingMigrations,
        },
        std::time::Duration,
    };

    #[tokio::test]
    async fn all_pending_keyranges_migrated() {
        let mut cluster = stub::Cluster::new_dummy(5).await;
        cluster.populate_storages().await;
        cluster.add_dummy_node(None).await;

        let booting_node = cluster.nodes.last().unwrap();

        let pending_ranges = booting_node
            .cluster
            .read()
            .await
            .pending_ranges(&booting_node.id)
            .cloned()
            .expect("Pending ranges");

        let pull_ranges: Vec<_> = pending_ranges
            .into_iter()
            .map(|range| match range {
                PendingRange::Push { .. } => unreachable!(),
                PendingRange::Pull { range, .. } => range,
            })
            .collect();

        booting_node
            .migration_manager
            .clone()
            .run_booting_migrations()
            .await;

        let expected_data = cluster.nodes[0].storage.data_from_ranges(&pull_ranges);
        assert!(!expected_data.entries.is_empty());

        assert_eq!(expected_data, booting_node.storage.data());
    }

    #[tokio::test]
    async fn hinted_ops_not_stored_during_commit() {
        // Invariant of this test case is being ensured inside `storage::Stub` impl

        let mut cluster = stub::Cluster::new_dummy(5).await;
        cluster.populate_storages().await;
        cluster.add_dummy_node(None).await;

        let booting_node = cluster.nodes.last().unwrap();

        let manager = booting_node.migration_manager.clone();
        tokio::spawn(async move {
            for _ in 0..100 {
                let op = Positioned {
                    position: rand::random(),
                    inner: Set(rand::random(), rand::random()),
                };

                manager.store_hinted(op).await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        booting_node
            .migration_manager
            .clone()
            .run_booting_migrations()
            .await;
    }
}
