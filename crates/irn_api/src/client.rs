use {
    crate::{
        auth,
        rpc,
        Cursor,
        Field,
        HandshakeRequest,
        HandshakeResponse,
        Key,
        NamespaceAuth,
        PubsubEventPayload,
        UnixTimestampSecs,
        Value,
    },
    ed25519_dalek::SigningKey,
    futures::{FutureExt, TryFutureExt},
    futures_util::{SinkExt, Stream},
    network::{
        outbound,
        rpc::AnyPeer,
        Keypair,
        Metered,
        MeteredExt,
        Multiaddr,
        PeerId,
        WithTimeouts,
        WithTimeoutsExt,
    },
    rand::seq::SliceRandom,
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        sync::Arc,
        time::Duration,
    },
    tap::Tap,
    wc::{
        future::{FutureExt as _, StaticFutureExt},
        metrics::{self, otel, TaskMetrics},
    },
};

static METRICS: TaskMetrics = TaskMetrics::new("irn_api_client_operation");

#[derive(Clone)]
pub struct Config {
    pub key: SigningKey,

    /// The list of nodes to be used for executing operations.
    pub nodes: HashMap<PeerId, Multiaddr>,

    /// The list of nodes to shadow operations to.
    pub shadowing_nodes: HashMap<PeerId, Multiaddr>,

    /// [0.0, 1.0] representing 0-100% shadowing.
    pub shadowing_factor: f64,

    /// Timeout of a single network request.
    pub request_timeout: Duration,

    /// Maximum time a client operation may take. The mechanism ensures that
    /// this allowance is never exceeded.
    ///
    /// Should be greater than [`Config::request_timeout`] in order to have
    /// enough time for retries of slow network requests.
    pub max_operation_time: Duration,

    pub connection_timeout: Duration,
    pub udp_socket_count: usize,
    pub namespaces: Vec<auth::Auth>,
}

#[derive(Clone)]
pub struct Client {
    /// Each [`network::Client`] is build around a single UDP socket. We need
    /// multiple of them because our current infrastructure doesn't support
    /// UDP buffer size configuration, so in order to be able to sustain high
    /// throughput we need to spread the load across multiple UDP sockets.
    inner: Arc<[NetworkClient]>,
    namespaces: Arc<HashMap<auth::PublicKey, auth::Auth>>,

    shadowing: Option<Shadowing>,

    request_timeout: Duration,
    max_operation_time: Duration,

    metrics_tag: otel::KeyValue,
}

#[derive(Clone)]
struct Shadowing {
    client: Box<Client>,
    max_hash: u64,
}

type NetworkClient = Metered<WithTimeouts<network::Client<Handshake>>>;

#[derive(Debug, thiserror::Error, Clone)]
pub enum Error {
    #[error("Network: {0:?}")]
    Network(#[from] outbound::Error),

    #[error("API: {0:?}")]
    Api(super::Error),
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl Client {
    pub fn new(cfg: Config) -> Result<Self, network::Error> {
        let shadowing = if !cfg.shadowing_nodes.is_empty() {
            let mut cfg = cfg.clone();
            cfg.nodes = std::mem::take(&mut cfg.shadowing_nodes);

            Some(Shadowing {
                max_hash: (u64::MAX as f64 * cfg.shadowing_factor) as u64,
                client: Box::new(Self::new_inner(cfg, "shadowing", None)?),
            })
        } else {
            None
        };

        Self::new_inner(cfg, "", shadowing)
    }

    pub fn namespaces(&self) -> &HashMap<auth::PublicKey, auth::Auth> {
        &self.namespaces
    }

    fn new_inner(
        mut cfg: Config,
        metrics_tag: &'static str,
        shadowing: Option<Shadowing>,
    ) -> Result<Self, network::Error> {
        // Safe unwrap, as we know that the bytes are a valid ed25519 key.
        let keypair = Keypair::ed25519_from_bytes(cfg.key.to_bytes()).unwrap();

        // 0 is not valid, let's just make it 1
        if cfg.udp_socket_count == 0 {
            cfg.udp_socket_count = 1;
        }

        let namespaces = cfg
            .namespaces
            .iter()
            .map(|auth| (auth.public_key(), auth.clone()))
            .collect::<HashMap<_, _>>();
        let namespaces = Arc::new(namespaces);

        let timeouts = network::rpc::Timeouts {
            unary: Some(cfg.request_timeout),
            streaming: Some(cfg.request_timeout),
            oneshot: Some(cfg.request_timeout),
        };

        Ok(Self {
            inner: (0..cfg.udp_socket_count)
                .map(|_| {
                    network::Client::new(network::ClientConfig {
                        keypair: keypair.clone(),
                        known_peers: cfg.nodes.clone(),
                        connection_timeout: cfg.connection_timeout,
                        handshake: Handshake {
                            namespaces: namespaces.clone(),
                        },
                    })
                    .map(|c| c.with_timeouts(timeouts).metered())
                })
                .collect::<Result<Vec<_>, _>>()?
                .into(),
            namespaces,
            request_timeout: cfg.request_timeout,
            max_operation_time: cfg.max_operation_time,
            metrics_tag: otel::KeyValue::new("client_tag", metrics_tag),
            shadowing,
        })
    }

    fn random_client(&self) -> &NetworkClient {
        // This vector can't be empty, we checked it in the constructor.
        self.inner.choose(&mut rand::thread_rng()).unwrap()
    }

    /// Checks whether the provided key requires shadowing, if so returnes the
    /// shadowing client.
    fn requires_shadowing(&self, key: &Key) -> Option<Client> {
        use {std::hash::BuildHasher, xxhash_rust::xxh3::Xxh3Builder};

        static HASHER: Xxh3Builder = Xxh3Builder::new();

        let shadowing = self.shadowing.as_ref()?;

        if HASHER.hash_one(&key.bytes) > shadowing.max_hash {
            return None;
        }

        Some((*shadowing.client).clone())
    }

    async fn exec<'a, Op, Fut, T>(
        &'a self,
        op_name: &'static str,
        rpc: impl Fn(NetworkClient, AnyPeer, Op) -> Fut + Clone + Send + Sync + 'static,
        op: Op,
    ) -> Result<T>
    where
        Op: AsRef<Key> + Clone + Send + Sync + 'static,
        T: Send,
        Fut: Future<Output = Result<super::Result<T>, outbound::Error>> + Send + 'static,
    {
        if let Some(s) = self.requires_shadowing(op.as_ref()) {
            let rpc = rpc.clone();
            let op = op.clone();
            async move { s.retry(op_name, rpc, op).await }.spawn("irn_api_shadowing");
        }

        self.retry(op_name, rpc, op).await
    }

    async fn retry<'a, Op, Fut, T>(
        &'a self,
        op_name: &'static str,
        rpc: impl Fn(NetworkClient, AnyPeer, Op) -> Fut,
        op: Op,
    ) -> Result<T>
    where
        Op: Clone,
        Fut: Future<Output = Result<super::Result<T>, outbound::Error>> + 'a,
    {
        use {super::Error as ApiError, tryhard::RetryPolicy};

        let started_at = std::time::Instant::now();

        // We don't really need to specify the upper bound of retries, because we
        // control the max duration. But `tryhard` requires it, so let's set
        // `u32::MAX`.
        let retries = u32::MAX;

        tryhard::retry_fn(|| {
            rpc(self.random_client().clone(), AnyPeer, op.clone()).map(|res| match res {
                Ok(Ok(out)) => Ok(out),
                Ok(Err(api)) => Err(Error::Api(api)),
                Err(net) => Err(Error::Network(net)),
            })
        })
        .retries(retries)
        .custom_backoff(|attempt, err: &_| {
            let delay = match err {
                Error::Api(ApiError::NotFound | ApiError::Unauthorized) => {
                    return RetryPolicy::Break;
                }
                Error::Api(ApiError::Throttled) => Duration::from_millis(200),

                // On the first attempt retry immediately.
                _ if attempt == 1 => Duration::ZERO,
                Error::Api(ApiError::Internal(_)) => Duration::from_millis(100),
                Error::Network(_) => Duration::from_millis(50),
            };

            // Make sure that the operation won't take more time than allowed.
            if started_at.elapsed() + delay + self.request_timeout > self.max_operation_time {
                RetryPolicy::Break
            } else {
                RetryPolicy::Delay(delay)
            }
        })
        .with_metrics(
            METRICS
                .with_name(op_name)
                .with_attributes([self.metrics_tag.clone()]),
        )
        .await
        .tap(|res| meter_operation_result(&self.metrics_tag, op_name, res))
    }

    pub async fn get(&self, key: Key) -> Result<Option<Value>> {
        let op = super::Get { key };

        self.exec("get", rpc::Get::send_owned, op).await
    }

    pub async fn set(
        &self,
        key: Key,
        value: Value,
        expiration: Option<UnixTimestampSecs>,
    ) -> Result<()> {
        let op = super::Set {
            key,
            value,
            expiration,
        };

        self.exec("set", rpc::Set::send_owned, op).await
    }

    pub async fn del(&self, key: Key) -> Result<()> {
        let op = super::Del { key };

        self.exec("del", rpc::Del::send_owned, op).await
    }

    pub async fn get_exp(&self, key: Key) -> Result<Option<UnixTimestampSecs>> {
        let op = super::GetExp { key };

        self.exec("get_exp", rpc::GetExp::send_owned, op).await
    }

    pub async fn set_exp(&self, key: Key, expiration: Option<UnixTimestampSecs>) -> Result<()> {
        let op = super::SetExp { key, expiration };

        self.exec("set_exp", rpc::SetExp::send_owned, op).await
    }

    pub async fn hget(&self, key: Key, field: Field) -> Result<Option<Value>> {
        let op = super::HGet { key, field };

        self.exec("hget", rpc::HGet::send_owned, op).await
    }

    pub async fn hset(
        &self,
        key: Key,
        field: Field,
        value: Value,
        expiration: Option<UnixTimestampSecs>,
    ) -> Result<()> {
        let op = super::HSet {
            key,
            field,
            value,
            expiration,
        };

        self.exec("hset", rpc::HSet::send_owned, op).await
    }

    pub async fn hdel(&self, key: Key, field: Field) -> Result<()> {
        let op = super::HDel { key, field };

        self.exec("hdel", rpc::HDel::send_owned, op).await
    }

    pub async fn hget_exp(&self, key: Key, field: Field) -> Result<Option<UnixTimestampSecs>> {
        let op = super::HGetExp { key, field };

        self.exec("hget_exp", rpc::HGetExp::send_owned, op).await
    }

    pub async fn hset_exp(
        &self,
        key: Key,
        field: Field,
        expiration: Option<UnixTimestampSecs>,
    ) -> Result<()> {
        let op = super::HSetExp {
            key,
            field,
            expiration,
        };

        self.exec("hset_exp", rpc::HSetExp::send_owned, op).await
    }

    pub async fn hcard(&self, key: Key) -> Result<u64> {
        let op = super::HCard { key };

        self.exec("hcard", rpc::HCard::send_owned, op).await
    }

    pub async fn hfields(&self, key: Key) -> Result<Vec<Field>> {
        let op = super::HFields { key };

        self.exec("hfields", rpc::HFields::send_owned, op).await
    }

    pub async fn hvals(&self, key: Key) -> Result<Vec<Value>> {
        let op = super::HVals { key };

        self.exec("hvals", rpc::HVals::send_owned, op).await
    }

    pub async fn hscan(
        &self,
        key: Key,
        count: u32,
        cursor: Option<Cursor>,
    ) -> Result<(Vec<Value>, Option<Cursor>)> {
        let op = super::HScan { key, count, cursor };

        self.exec("hscan", rpc::HScan::send_owned, op).await
    }

    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        let op = super::Publish { channel, message };

        self.retry(
            "publish",
            |client, to, op| rpc::Publish::send_owned(client, to, op).map_ok(Ok),
            op,
        )
        .await
    }

    pub async fn subscribe(
        &self,
        channels: HashSet<Vec<u8>>,
    ) -> impl Stream<Item = PubsubEventPayload> + 'static {
        let this = self.clone();

        meter_operation_result(&self.metrics_tag, "subscribe", &Ok(()));

        async_stream::stream! {
            let op = &super::Subscribe { channels };

            loop {
                let subscribe =
                    rpc::Subscribe::send(this.random_client(), AnyPeer, |mut tx, rx| async move {
                        tx.send(op.clone()).await?;
                        Ok(rx)
                    });

                let mut rx = match subscribe.await {
                    Ok(rx) => rx,
                    Err(err) => {
                        tracing::error!(?err, "Failed to subscribe to any peer");
                        metrics::counter!("irn_api_client_subscribe_failures", 1, &[this.metrics_tag.clone()]);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                loop {
                    match rx.recv_message().await {
                        Ok(msg) => yield msg,
                        Err(err) => {
                            tracing::warn!(?err, "Failed to receive pubsub message, resubscribing...");
                            metrics::counter!("irn_api_client_subscription_failures", 1, &[this.metrics_tag.clone()]);
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// Client part of the [`network::Handshake`].
#[derive(Clone)]
struct Handshake {
    namespaces: Arc<HashMap<auth::PublicKey, auth::Auth>>,
}

impl network::Handshake for Handshake {
    type Ok = ();
    type Err = super::HandshakeError;

    fn handle(
        &self,
        conn: network::PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .accept_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let req = rx.recv_message().await?;

            let namespaces = self
                .namespaces
                .iter()
                .map(|(pub_key, auth)| NamespaceAuth {
                    namespace: *pub_key,
                    signature: auth.sign(req.auth_nonce.as_ref()),
                })
                .collect();

            tx.send(HandshakeResponse { namespaces }).await?;

            Ok(())
        }
    }
}

fn meter_operation_result<T>(
    client_tag: &otel::KeyValue,
    operation: &'static str,
    res: &Result<T>,
) {
    use network::{outbound::Error as NetworkError, rpc::Error as RpcError};

    let err = res.as_ref().err().and_then(|e| {
        Some(match e {
            Error::Network(NetworkError::ConnectionHandler(_)) => "connection_handler".into(),
            Error::Network(NetworkError::Rpc(RpcError::IO(e))) => e.to_string().into(),
            Error::Network(NetworkError::Rpc(RpcError::StreamFinished)) => "stream_finished".into(),
            Error::Network(NetworkError::Rpc(RpcError::Timeout)) => "timeout".into(),
            Error::Api(super::Error::Unauthorized) => "unauthorized".into(),
            Error::Api(super::Error::NotFound) => return None,
            Error::Api(super::Error::Throttled) => "throttled".into(),
            Error::Api(super::Error::Internal(e)) => e.code.clone(),
        })
    });

    metrics::counter!("irn_api_client_operation_results", 1, &[
        client_tag.clone(),
        otel::KeyValue::new("operation", operation),
        otel::KeyValue::new("err", err.unwrap_or_default())
    ]);
}
