use {
    crate::{
        auth,
        rpc::{self, StatusResponse},
        Cursor,
        Field,
        HandshakeRequest,
        HandshakeResponse,
        Key,
        NamespaceAuth,
        Operation,
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
    tap::{Pipe, Tap},
    wc::metrics::{future_metrics, FutureExt as _, StringLabel},
};

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
pub struct Client<Kind = kind::Basic> {
    /// Each [`network::Client`] is build around a single UDP socket. We need
    /// multiple of them because our current infrastructure doesn't support
    /// UDP buffer size configuration, so in order to be able to sustain high
    /// throughput we need to spread the load across multiple UDP sockets.
    inner: Arc<[NetworkClient]>,
    namespaces: Arc<HashMap<auth::PublicKey, auth::Auth>>,

    request_timeout: Duration,
    max_operation_time: Duration,

    kind: Kind,
}

type ShadowingClient = Client<kind::Shadowing>;

mod kind {
    #[derive(Clone)]
    pub struct Basic {
        pub(super) shadowing: Option<super::ShadowingClient>,
    }

    #[derive(Clone)]
    pub struct Shadowing {
        pub(super) max_hash: u64,
    }
}

pub trait Kind: Sized + Clone + 'static {
    const METRICS_TAG: &'static str;

    /// Checks whether the provided key requires shadowing, if so returns the
    /// shadowing client.
    fn requires_shadowing(_client: &Client<Self>, _key: &Key) -> Option<ShadowingClient> {
        None
    }
}

type NetworkClient = Metered<WithTimeouts<network::Client<Handshake>>>;

#[derive(Debug, Eq, PartialEq, thiserror::Error, Clone)]
pub enum Error {
    #[error("Network: {0:?}")]
    Network(#[from] outbound::Error),

    #[error("API: {0:?}")]
    Api(super::Error),

    #[error("Encryption: {0:?}")]
    Encryption(#[from] super::auth::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Client {
    pub fn new(cfg: Config) -> Result<Self, network::Error> {
        let shadowing = if !cfg.shadowing_nodes.is_empty() {
            let mut cfg = cfg.clone();
            cfg.nodes = std::mem::take(&mut cfg.shadowing_nodes);

            let max_hash = (u64::MAX as f64 * cfg.shadowing_factor) as u64;
            Some(Client::new_inner(cfg, kind::Shadowing { max_hash })?)
        } else {
            None
        };

        Self::new_inner(cfg, kind::Basic { shadowing })
    }
}

impl Kind for kind::Basic {
    const METRICS_TAG: &'static str = "";

    fn requires_shadowing(client: &Client<Self>, key: &Key) -> Option<ShadowingClient> {
        use {std::hash::BuildHasher, xxhash_rust::xxh3::Xxh3Builder};

        static HASHER: Xxh3Builder = Xxh3Builder::new();

        let shadowing = client.kind.shadowing.as_ref()?;

        if HASHER.hash_one(&key.bytes) > shadowing.kind.max_hash {
            return None;
        }

        Some((*shadowing).clone())
    }
}

impl Kind for kind::Shadowing {
    const METRICS_TAG: &'static str = "shadowing";
}

impl<K: Kind> Client<K> {
    fn new_inner(mut cfg: Config, kind: K) -> Result<Self, network::Error> {
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
            kind,
        })
    }

    pub fn namespaces(&self) -> &HashMap<auth::PublicKey, auth::Auth> {
        &self.namespaces
    }

    fn random_client(&self) -> &NetworkClient {
        // This vector can't be empty, we checked it in the constructor.
        self.inner.choose(&mut rand::thread_rng()).unwrap()
    }

    async fn exec<Op, Fut, T>(
        &self,
        rpc: impl Fn(NetworkClient, AnyPeer, Op) -> Fut + Clone + Send + Sync + 'static,
        op: Op,
    ) -> Result<T>
    where
        Op: Operation + AsRef<Key> + Clone + Send + Sync + 'static,
        T: Send + 'static,
        Fut: Future<Output = Result<super::Result<T>, outbound::Error>> + Send + 'static,
    {
        if let Some(s) = Kind::requires_shadowing(self, op.as_ref()) {
            let rpc = rpc.clone();
            let op = op.clone();
            async move { s.retry(rpc, op).await }
                .with_metrics(future_metrics!("irn_api_shadowing"))
                .pipe(tokio::spawn);
        }

        self.retry(rpc, op).await
    }

    async fn retry<'a, Op, Fut, T>(
        &'a self,
        rpc: impl Fn(NetworkClient, AnyPeer, Op) -> Fut,
        op: Op,
    ) -> Result<T>
    where
        Op: Operation + Clone,
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
                Error::Api(ApiError::NotFound | ApiError::Unauthorized) | Error::Encryption(_) => {
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
        .with_metrics(future_metrics!(
            "irn_client_operation",
            StringLabel<"op_name"> => Op::NAME,
            StringLabel<"client_tag"> => K::METRICS_TAG
        ))
        .await
        .tap(|res| Self::meter_operation_result::<Op, _>(res))
    }

    #[inline]
    fn try_seal(&self, ns: &Option<auth::PublicKey>, value: Value) -> Result<Value> {
        if let Some(ns) = ns {
            Ok(self
                .namespaces
                .get(ns)
                .ok_or(Error::Api(super::Error::Unauthorized))?
                .seal(value)?)
        } else {
            Ok(value)
        }
    }

    #[inline]
    fn try_open(&self, ns: &Option<auth::PublicKey>, mut value: Value) -> Result<Value> {
        if let Some(ns) = ns {
            Ok(self
                .namespaces
                .get(ns)
                .ok_or(Error::Api(super::Error::Unauthorized))?
                .open_in_place(&mut value)?
                .into())
        } else {
            Ok(value)
        }
    }

    #[inline]
    fn try_open_collection(
        &self,
        ns: &Option<auth::PublicKey>,
        mut values: Vec<Value>,
    ) -> Result<Vec<Value>> {
        if let Some(ns) = ns {
            let auth = self
                .namespaces
                .get(ns)
                .ok_or(Error::Api(super::Error::Unauthorized))?;

            for value in &mut values {
                *value = auth.open_in_place(value)?.into();
            }
        }

        Ok(values)
    }

    pub async fn get(&self, key: Key) -> Result<Option<Value>> {
        let ns = key.namespace;
        let op = super::Get { key };

        self.exec(rpc::Get::send_owned, op)
            .await?
            .map(|value| self.try_open(&ns, value))
            .transpose()
    }

    pub async fn set(&self, key: Key, value: Value, expiration: UnixTimestampSecs) -> Result<()> {
        let value = self.try_seal(&key.namespace, value)?;

        let op = super::Set {
            key,
            value,
            expiration: Some(expiration),
        };

        self.exec(rpc::Set::send_owned, op).await
    }

    pub async fn del(&self, key: Key) -> Result<()> {
        let op = super::Del { key };

        self.exec(rpc::Del::send_owned, op).await
    }

    pub async fn get_exp(&self, key: Key) -> Result<UnixTimestampSecs> {
        let op = super::GetExp { key };

        self.exec(rpc::GetExp::send_owned, op)
            .await?
            .ok_or_else(|| Error::Api(super::Error::NotFound))
    }

    pub async fn set_exp(&self, key: Key, expiration: UnixTimestampSecs) -> Result<()> {
        let op = super::SetExp {
            key,
            expiration: Some(expiration),
        };

        self.exec(rpc::SetExp::send_owned, op).await
    }

    pub async fn hget(&self, key: Key, field: Field) -> Result<Option<Value>> {
        let ns = key.namespace;
        let op = super::HGet { key, field };

        self.exec(rpc::HGet::send_owned, op)
            .await?
            .map(|value| self.try_open(&ns, value))
            .transpose()
    }

    pub async fn hset(
        &self,
        key: Key,
        field: Field,
        value: Value,
        expiration: UnixTimestampSecs,
    ) -> Result<()> {
        let value = self.try_seal(&key.namespace, value)?;

        let op = super::HSet {
            key,
            field,
            value,
            expiration: Some(expiration),
        };

        self.exec(rpc::HSet::send_owned, op).await
    }

    pub async fn hdel(&self, key: Key, field: Field) -> Result<()> {
        let op = super::HDel { key, field };

        self.exec(rpc::HDel::send_owned, op).await
    }

    pub async fn hget_exp(&self, key: Key, field: Field) -> Result<UnixTimestampSecs> {
        let op = super::HGetExp { key, field };

        self.exec(rpc::HGetExp::send_owned, op)
            .await?
            .ok_or_else(|| Error::Api(super::Error::NotFound))
    }

    pub async fn hset_exp(
        &self,
        key: Key,
        field: Field,
        expiration: UnixTimestampSecs,
    ) -> Result<()> {
        let op = super::HSetExp {
            key,
            field,
            expiration: Some(expiration),
        };

        self.exec(rpc::HSetExp::send_owned, op).await
    }

    pub async fn hcard(&self, key: Key) -> Result<u64> {
        let op = super::HCard { key };

        self.exec(rpc::HCard::send_owned, op).await
    }

    pub async fn hfields(&self, key: Key) -> Result<Vec<Field>> {
        let op = super::HFields { key };

        self.exec(rpc::HFields::send_owned, op).await
    }

    pub async fn hvals(&self, key: Key) -> Result<Vec<Value>> {
        let ns = key.namespace;
        let op = super::HVals { key };
        let values = self.exec(rpc::HVals::send_owned, op).await?;

        self.try_open_collection(&ns, values)
    }

    pub async fn hscan(
        &self,
        key: Key,
        count: u32,
        cursor: Option<Cursor>,
    ) -> Result<(Vec<Value>, Option<Cursor>)> {
        let ns = key.namespace;
        let op = super::HScan { key, count, cursor };
        let (values, cursor) = self.exec(rpc::HScan::send_owned, op).await?;

        self.try_open_collection(&ns, values)
            .map(|values| (values, cursor))
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        let op = super::Status;
        self.retry(rpc::Status::send_owned, op).await
    }

    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        let op = super::Publish { channel, message };

        self.retry(
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

        Self::meter_operation_result::<super::Subscribe, _>(&Ok(()));

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
                        metrics::counter!("irn_api_client_subscribe_failures", "client_tag" => K::METRICS_TAG).increment(1);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                loop {
                    match rx.recv_message().await {
                        Ok(msg) => yield msg,
                        Err(err) => {
                            tracing::warn!(?err, "Failed to receive pubsub message, resubscribing...");
                            metrics::counter!("irn_api_client_subscription_failures", "client_tag" => K::METRICS_TAG).increment(1);
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

impl<K: Kind> Client<K> {
    fn meter_operation_result<Op: Operation, T>(res: &Result<T>) {
        use network::{outbound::Error as NetworkError, rpc::Error as RpcError};

        let err = res.as_ref().err().and_then(|e| {
            Some(match e {
                Error::Network(NetworkError::ConnectionHandler(_)) => "connection_handler".into(),
                Error::Network(NetworkError::Rpc(RpcError::IO(e))) => e.to_string().into(),
                Error::Network(NetworkError::Rpc(RpcError::StreamFinished)) => {
                    "stream_finished".into()
                }
                Error::Network(NetworkError::Rpc(RpcError::Timeout)) => "timeout".into(),
                Error::Api(super::Error::Unauthorized) => "unauthorized".into(),
                Error::Api(super::Error::NotFound) => return None,
                Error::Api(super::Error::Throttled) => "throttled".into(),
                Error::Api(super::Error::Internal(e)) => e.code.clone(),
                Error::Encryption(_) => "encryption".into(),
            })
        });

        metrics::counter!("irn_api_client_operation_results",
            "client_tag" => K::METRICS_TAG,
            "operation" => Op::NAME,
            "err" => err.unwrap_or_default()
        )
        .increment(1);
    }
}
