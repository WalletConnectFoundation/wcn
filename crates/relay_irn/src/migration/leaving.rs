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
        LeavingMigrations,
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

/// [`Network`] request for pushing data to a remote node.
pub type PushDataRequest<Data> = VersionedRequest<PushDataRequestArgs<Data>>;

/// [`PushDataRequest`] arguments.
pub struct PushDataRequestArgs<Data> {
    pub key_range: KeyRange<KeyPosition>,
    pub data: Data,
}

/// Error response to [`PushDataRequest`].
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum PushDataError {
    #[error(transparent)]
    ClusterViewVersionMismatch(#[from] cluster::ViewVersionMismatch),

    #[error("Import operation failed: {0}")]
    Import(String),

    #[error("CommitHintedOperations failed: {0}")]
    CommitHintedOperations(String),
}

/// Response to [`PushRequest`].
pub type PushDataResponse = Result<(), PushDataError>;

impl<C, N, S> Node<C, N, S> {
    pub async fn handle_push_data_request<Data>(
        &self,
        req: PushDataRequest<Data>,
    ) -> PushDataResponse
    where
        S: Storage<Import<Data>> + Storage<CommitHintedOperations>,
    {
        self.migration_manager.handle_push_data_request(req).await
    }
}

impl<N, S> migration::Manager<N, S> {
    pub async fn handle_push_data_request<Data>(
        &self,
        req: PushDataRequest<Data>,
    ) -> PushDataResponse
    where
        S: Storage<Import<Data>> + Storage<CommitHintedOperations>,
    {
        let req = req.validate_and_unpack(self.cluster.read().await.view().version())?;

        let key_range = req.key_range;

        let import = Import {
            key_range,
            data: req.data,
        };

        self.storage
            .exec(import)
            .await
            .tap_err(|err| tracing::warn!(?err, "Import operation failed"))
            .map_err(|err| PushDataError::Import(err.to_string()))?;

        self.commit_hinted_operations(key_range)
            .await
            .tap_err(|err| error!(?err, "Failed to commit hinted ops"))
            .map_err(|err| PushDataError::CommitHintedOperations(err.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl<N, S> LeavingMigrations for migration::Manager<N, S>
where
    N: SendRequest<PushDataRequest<S::Ok>, Response = PushDataResponse>,
    S: Storage<Export>,
    S::Ok: Send,
{
    async fn run_leaving_migrations(self) {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(None)
            .build();

        let push_pending = || async {
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

            self.push_pending_ranges(ranges, cluster_view_version)
                .await
                .map_err(backoff::Error::transient)
        };

        let notify = |err, _| error!(?err, "Failed to push pending ranges, retrying");
        retry_notify(backoff, push_pending, notify).await.ok();
    }
}

impl<N, S> migration::Manager<N, S>
where
    N: SendRequest<PushDataRequest<S::Ok>, Response = PushDataResponse>,
    S: Storage<Export>,
    S::Ok: Send,
{
    async fn push_pending_ranges(
        &self,
        ranges: PendingRanges,
        cluster_view_version: u128,
    ) -> Result<(), PushError<N::Error>> {
        // Pushing the data to other nodes means a lot of disk reading. So concurrency
        // here is lower than in the 'pull' migrations.
        // TODO: Balance migration network and disk usage?
        const CONCURRENCY: usize = 8;

        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(None)
            .build();
        let backoff = &backoff;

        stream::iter(ranges.into_iter().map(Ok))
            .try_for_each_concurrent(CONCURRENCY, |range| async move {
                let PendingRange::Push { range, destination } = range else {
                    // TODO: Shouldn't be possible on the type level
                    tracing::warn!(?range, "pull range in node leaving migration");
                    return Ok(());
                };

                let push = || {
                    self.push_range(range, destination, cluster_view_version)
                        .map_err(|e| match e {
                            e @ PushError::Response(PushDataError::ClusterViewVersionMismatch(
                                _,
                            )) => backoff::Error::permanent(e),
                            e => backoff::Error::transient(e),
                        })
                };

                let notify = |err, _| error!(?err, ?range, "Failed to push keyrange, retrying");
                retry_notify(backoff.clone(), push, notify).await
            })
            .await
    }

    async fn push_range(
        &self,
        key_range: KeyRange<KeyPosition>,
        destination: PeerId,
        cluster_view_version: u128,
    ) -> Result<(), PushError<N::Error>> {
        let data = self
            .storage
            .exec(Export { key_range })
            .await
            .map_err(|e| PushError::Export(e.to_string()))?;

        let args = PushDataRequestArgs { key_range, data };

        self.network
            .send_request(
                destination,
                VersionedRequest::new(args, cluster_view_version),
            )
            .await
            .map_err(PushError::Network)??;

        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
enum PushError<N> {
    #[error("Export operation failed: {0}")]
    Export(String),

    #[error("Network error: {0}")]
    Network(N),

    #[error(transparent)]
    Response(#[from] PushDataError),
}

#[cfg(test)]
mod test {
    use {
        crate::{
            cluster::{
                keyspace::{hashring::Positioned, pending_ranges::PendingRange},
                NodeOperationMode,
            },
            migration,
            storage::stub::Set,
            LeavingMigrations,
        },
        std::{collections::HashMap, time::Duration},
    };

    #[tokio::test]
    async fn all_pending_keyranges_migrated() {
        let managers = migration::stub::cluster(NodeOperationMode::Leaving);

        let leaving_mgr = managers.last().unwrap();

        let pending_ranges = leaving_mgr
            .cluster
            .read()
            .await
            .pending_ranges(&leaving_mgr.id)
            .cloned()
            .expect("Pending ranges");

        let mut push_ranges =
            pending_ranges
                .into_iter()
                .fold(HashMap::<_, Vec<_>>::new(), |mut push, range| match range {
                    PendingRange::Push { range, destination } => {
                        push.entry(destination).or_default().push(range);
                        push
                    }
                    PendingRange::Pull { .. } => unreachable!(),
                });
        assert!(!push_ranges.is_empty());

        leaving_mgr.clone().run_leaving_migrations().await;

        for node in &managers[0..managers.len() - 1] {
            let Some(ranges) = push_ranges.remove(&node.id) else {
                continue;
            };

            let expected_data = leaving_mgr.storage.data_from_ranges(&ranges);
            assert!(!expected_data.entries.is_empty());

            assert_eq!(expected_data, node.storage.data());
        }

        assert!(push_ranges.is_empty());
    }

    #[tokio::test]
    async fn hinted_ops_not_stored_during_commit() {
        // Invariant of this test case is being ensured inside `storage::Stub` impl

        let managers = migration::stub::cluster(NodeOperationMode::Booting);

        let leaving_mgr = managers.last().unwrap();

        let peers: Vec<_> = managers[0..managers.len() - 1].to_vec();
        tokio::spawn(async move {
            for _ in 0..100 {
                let op = Positioned {
                    position: rand::random(),
                    inner: Set(rand::random(), rand::random()),
                };

                let futs = peers.iter().map(|peer| peer.store_hinted(op.clone()));
                futures::future::try_join_all(futs).await.unwrap();

                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        leaving_mgr.clone().run_leaving_migrations().await;
    }
}
