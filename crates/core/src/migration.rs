use {
    crate::cluster::{consensus::NodeId, keyspace::MigrationPlan, Consensus},
    anyhow::Context as _,
    backoff::{future::retry, ExponentialBackoff},
    futures::stream,
    std::{
        error::Error as StdError,
        future::Future,
        ops::RangeInclusive,
        sync::Arc,
        time::Duration,
    },
    tracing::Instrument as _,
};

pub trait Network<Node>: Clone + Send + Sync + 'static {
    type DataStream;

    fn pull_keyrange(
        &self,
        from: &Node,
        range: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> impl Future<Output = Result<Self::DataStream, impl AnyError>> + Send;
}

pub trait StorageImport<Stream>: Clone + Send + Sync + 'static {
    fn import(&self, stream: Stream) -> impl Future<Output = Result<(), impl AnyError>> + Send;
}

pub trait StorageExport: Clone + Send + Sync + 'static {
    type Stream;

    fn export(
        &self,
        keyrange: RangeInclusive<u64>,
    ) -> impl Future<Output = Result<Self::Stream, impl AnyError>>;
}

#[derive(Clone, Debug)]
pub struct Manager<C: Consensus, N, S> {
    node_id: NodeId<C>,

    consensus: C,
    network: N,
    storage: S,
}

impl<C: Consensus, N, S> Manager<C, N, S> {
    pub fn new(node_id: NodeId<C>, consensus: C, network: N, storage: S) -> Self {
        Self {
            node_id,
            consensus,
            network,
            storage,
        }
    }

    pub async fn handle_pull_request(
        &self,
        puller_id: &NodeId<C>,
        range: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> Result<S::Stream, PullKeyrangeError>
    where
        S: StorageExport,
    {
        self.consensus.cluster_view().peek(|cluster| {
            if !cluster.contains_node(puller_id) {
                return Err(PullKeyrangeError::NotClusterMember);
            }

            if cluster.keyspace_version() != keyspace_version {
                return Err(PullKeyrangeError::KeyspaceVersionMismatch);
            }

            Ok(())
        })?;

        self.storage
            .export(range)
            .await
            .map_err(|e| PullKeyrangeError::StorageExport(e.to_string()))
    }

    pub fn pull_ranges(&self, plan: Arc<MigrationPlan<C::Node>>) -> impl Future<Output = ()> + '_
    where
        N: Network<C::Node>,
        S: StorageImport<N::DataStream>,
    {
        use futures::StreamExt as _;

        async move {
            // TODO: configure concurrency
            stream::iter(plan.pending_ranges(&self.node_id))
                .for_each_concurrent(100, |range| {
                    let backoff = ExponentialBackoff {
                        initial_interval: Duration::from_secs(1),
                        max_interval: Duration::from_secs(60),
                        max_elapsed_time: None,
                        ..Default::default()
                    };

                    async move {
                        let _ = retry(backoff, || async {
                            let cluster = self.consensus.cluster();

                            // TODO: we need to pull either:
                            // - from the replica being replaced, or
                            // - from the quorum of other replicas
                            // otherwise we may violate consistency
                            for node_id in &range.replicas {
                                let res = async {
                                    let node = cluster.node(node_id).context("Missing node")?;
                                    let data = self
                                        .network
                                        .pull_keyrange(
                                            node,
                                            range.keys.clone(),
                                            cluster.keyspace_version(),
                                        )
                                        .await?;

                                    self.storage.import(data).await?;
                                    Ok::<_, anyhow::Error>(())
                                }
                                .await;

                                match res {
                                    Ok(_) => return Ok(()),
                                    Err(err) => tracing::warn!(?node_id, ?err),
                                }
                            }

                            tracing::error!("Failed to pull from all replicas");
                            Err::<(), _>(backoff::Error::transient(()))
                        })
                        .await;
                    }
                    .instrument(tracing::error_span!("pulling", keyrange = ?range.keys))
                })
                .await;
        }
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PullKeyrangeError {
    #[error("Keyspace versions of puller and pullee don't match")]
    KeyspaceVersionMismatch,

    #[error("Storage::export: {0}")]
    StorageExport(String),

    #[error("Puller is not a cluster member")]
    NotClusterMember,
}

pub trait AnyError: StdError + Send + Sync + 'static {}
impl<E> AnyError for E where E: StdError + Send + Sync + 'static {}
