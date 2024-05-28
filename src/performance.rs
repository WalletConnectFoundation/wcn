use {
    crate::{contract, network::rpc, Network},
    anyhow::Result,
    backoff::ExponentialBackoffBuilder,
    chrono::Utc,
    futures::{stream, FutureExt, StreamExt, TryFutureExt},
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, io, path::PathBuf, time::Duration},
};

pub struct Tracker<R> {
    reporter: R,
    network: Network,
    storage: Storage,
    state: State,
    expected_node_version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct State {
    current_epoch: u64,

    total_health_checks: u64,
    health_scores: HashMap<String, f64>,

    epoch_start_timestamp: i64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            current_epoch: 1,
            total_health_checks: 0,
            health_scores: HashMap::default(),
            epoch_start_timestamp: Utc::now().timestamp(),
        }
    }
}

impl State {
    fn next_epoch(&mut self) {
        self.current_epoch += 1;
        self.total_health_checks = 0;
        self.health_scores.clear();
        self.epoch_start_timestamp = Utc::now().timestamp();
    }
}

impl<R: contract::PerformanceReporter> Tracker<R> {
    const EPOCH_DURATION_SECS: i64 = 60 * 60; // an hour

    pub async fn new(
        network: Network,
        reporter: R,
        storage_dir: PathBuf,
        expected_node_version: u64,
    ) -> Result<Self> {
        let storage = Storage::new(storage_dir).await?;
        let state = storage.read_state().await?.unwrap_or_default();

        Ok(Self {
            reporter,
            network,
            storage,
            state,
            expected_node_version,
        })
    }

    pub async fn run(mut self) {
        tracing::info!("Tracker started");

        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if Utc::now().timestamp() >= self.epoch_end_timestamp() {
                self.publish_report().await;
                self.next_epoch().await;
            }

            self.healthcheck().await;

            interval.tick().await;
        }
    }

    fn epoch_end_timestamp(&self) -> i64 {
        if self.state.current_epoch <= 6 {
            return Utc::now().timestamp();
        }

        self.state.epoch_start_timestamp + Self::EPOCH_DURATION_SECS
    }

    async fn healthcheck(&mut self) {
        let expected_node_version = self.expected_node_version;

        stream::iter(self.network.peers().await)
            .map(|id| {
                rpc::Send::<rpc::Health, _, _>::send(&self.network.client, id, ())
                    .map(move |r| (id, r))
            })
            .buffer_unordered(100)
            .filter_map(|(id, res)| async move {
                let resp = res
                    .map_err(|err| tracing::warn!(%id, ?err, "healthcheck failed"))
                    .ok()?;

                let score_diff = if resp.node_version == expected_node_version {
                    1.0
                } else {
                    tracing::warn!(
                        version = %resp.node_version,
                        expected_version = %expected_node_version,
                        "outdated node detected",
                    );
                    0.5
                };

                resp.eth_address.map(|addr| (addr, score_diff))
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .for_each(|(addr, diff)| *self.state.health_scores.entry(addr).or_default() += diff);

        self.state.total_health_checks += 1;
        self.flush_state().await;

        tracing::info!(?self.state, "Healthcheck");
    }

    async fn publish_report(&self) {
        let epoch = self.state.current_epoch;
        let scores = &self.state.health_scores;

        let nodes: Vec<_> = scores
            .iter()
            .filter_map(|(addr, score)| {
                let performance = (score / self.state.total_health_checks as f64 * 100.0).ceil();
                contract::NodePerformanceData::new(addr, performance as u8)
                    .map_err(|err| tracing::warn!(?err, "invalid performance data, skipping"))
                    .ok()
            })
            .collect();

        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(10 * 60))
            .with_max_interval(Duration::from_secs(60 * 60))
            .with_max_elapsed_time(None)
            .build();

        let _ = backoff::future::retry(backoff, || {
            let nodes = nodes.clone();
            self.reporter
                .report_performance(contract::PerformanceData { epoch, nodes })
                .map_err(|err| tracing::error!(?err, "failed to publish performance report"))
                .map_err(backoff::Error::transient)
        })
        .await;

        tracing::info!(?self.state, "Report published");
    }

    async fn next_epoch(&mut self) {
        self.state.next_epoch();
        self.flush_state().await;
    }

    async fn flush_state(&self) {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(10))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None)
            .build();

        let _ = backoff::future::retry(backoff, || {
            self.storage
                .write_state(&self.state)
                .map_err(|err| tracing::error!(?err, "failed to flush state"))
                .map_err(backoff::Error::transient)
        })
        .await;
    }
}

struct Storage {
    dir: PathBuf,
}

impl Storage {
    const STATE_FILE_NAME: &'static str = "state.json";

    async fn new(dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&dir).await?;
        Ok(Self { dir })
    }

    async fn read_state(&self) -> Result<Option<State>> {
        let bytes = match tokio::fs::read(self.dir.join(Self::STATE_FILE_NAME)).await {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn write_state(&self, state: &State) -> Result<()> {
        let bytes = serde_json::to_vec(state)?;

        tokio::fs::write(self.dir.join(Self::STATE_FILE_NAME), bytes)
            .await
            .map_err(Into::into)
    }
}
