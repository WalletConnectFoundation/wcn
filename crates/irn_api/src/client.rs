pub use irn_rpc::{identity, Multiaddr, PeerId};
use {
    crate::{
        rpc,
        Cursor,
        Field,
        HandshakeRequest,
        HandshakeResponse,
        Key,
        Operation,
        PubsubEventPayload,
        UnixTimestampSecs,
        Value,
    },
    futures::FutureExt,
    futures_util::{SinkExt, Stream},
    irn_rpc::{
        client::{
            self,
            middleware::{Metered, MeteredExt as _, Timeouts, WithTimeouts, WithTimeoutsExt as _},
            AnyPeer,
        },
        quic,
        transport::{self, PendingConnection},
        Client as _,
        Rpc,
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
    pub keypair: identity::Keypair,

    /// The list of nodes to be used for executing operations.
    pub nodes: HashMap<PeerId, Multiaddr>,

    /// The list of nodes to shadow operations to.
    pub shadowing_nodes: HashMap<PeerId, Multiaddr>,

    /// [0.0, 1.0] representing 0-100% shadowing.
    pub shadowing_factor: f64,

    /// The default namespace to use with the shadowing requests if no namespace
    /// is specified.
    pub shadowing_default_namespace: Option<auth::PublicKey>,

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
    inner: Arc<[RpcClient]>,
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
        pub(super) extra_requests: usize,
        pub(super) max_hash: u64,
        pub(super) default_namespace: Option<auth::PublicKey>,
    }
}

pub trait Kind: Sized + Clone + Send + 'static {
    const METRICS_TAG: &'static str;

    /// Checks whether the provided key requires shadowing, if so returns the
    /// shadowing client.
    fn requires_shadowing(_client: &Client<Self>, _key: &Key) -> Option<(ShadowingClient, usize)> {
        None
    }
}

type RpcClient = Metered<WithTimeouts<quic::Client<Handshake>>>;

#[derive(Debug, Eq, PartialEq, thiserror::Error, Clone)]
pub enum Error {
    #[error("RPC client: {0:?}")]
    RpcClient(#[from] irn_rpc::client::Error),

    #[error("API: {0:?}")]
    Api(super::Error),

    #[error("Encryption: {0:?}")]
    Encryption(#[from] auth::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Client {
    pub fn new(cfg: Config) -> Result<Self, quic::Error> {
        let shadowing = if !cfg.shadowing_nodes.is_empty() {
            let default_namespace = cfg.shadowing_default_namespace;
            let factor = cfg.shadowing_factor;
            let extra_requests = factor.floor() as usize;

            let mut cfg = cfg.clone();
            cfg.nodes = std::mem::take(&mut cfg.shadowing_nodes);

            let max_hash = (u64::MAX as f64 * factor.fract()) as u64;
            Some(Client::new_inner(cfg, kind::Shadowing {
                extra_requests,
                max_hash,
                default_namespace,
            })?)
        } else {
            None
        };

        Self::new_inner(cfg, kind::Basic { shadowing })
    }
}

impl Kind for kind::Basic {
    const METRICS_TAG: &'static str = "";

    fn requires_shadowing(client: &Client<Self>, key: &Key) -> Option<(ShadowingClient, usize)> {
        use {std::hash::BuildHasher, xxhash_rust::xxh3::Xxh3Builder};

        static HASHER: Xxh3Builder = Xxh3Builder::new();

        let shadowing = client.kind.shadowing.as_ref()?;
        let mut num_requests = shadowing.kind.extra_requests;

        if HASHER.hash_one(&key.bytes) <= shadowing.kind.max_hash {
            num_requests += 1;
        }

        if num_requests > 0 {
            Some((shadowing.clone(), num_requests))
        } else {
            None
        }
    }
}

impl Kind for kind::Shadowing {
    const METRICS_TAG: &'static str = "shadowing";
}

impl<K: Kind> Client<K> {
    fn new_inner(mut cfg: Config, kind: K) -> Result<Self, quic::Error> {
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

        let timeouts = Timeouts::new()
            .with_default(cfg.request_timeout)
            .with::<{ rpc::Subscribe::ID }>(None);

        Ok(Self {
            inner: (0..cfg.udp_socket_count)
                .map(|_| {
                    quic::Client::new(irn_rpc::client::Config {
                        keypair: cfg.keypair.clone(),
                        known_peers: cfg.nodes.values().cloned().collect(),
                        connection_timeout: cfg.connection_timeout,
                        handshake: Handshake {
                            namespaces: namespaces.clone(),
                        },
                        server_name: crate::RPC_SERVER_NAME,
                    })
                    .map(|c| c.with_timeouts(timeouts.clone()).metered())
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

    fn random_client(&self) -> &RpcClient {
        // This vector can't be empty, we checked it in the constructor.
        self.inner.choose(&mut rand::thread_rng()).unwrap()
    }

    fn exec<RPC, Op, T>(&self, op: Op) -> impl Future<Output = Result<T>> + '_
    where
        RPC: Rpc<Kind = irn_rpc::kind::Unary, Request = Op, Response = super::Result<T>> + 'static,
        Op: Operation + AsRef<Key> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        if let Some((client, num_requests)) = Kind::requires_shadowing(self, op.as_ref()) {
            for _ in 0..num_requests {
                let client = client.clone();
                let mut op = op.clone();

                if let (Some(ns), Some(key)) = (&client.kind.default_namespace, op.key_mut()) {
                    key.set_default_namespace(ns);
                }

                async move { client.retry::<RPC, _, _>(op).await }
                    .with_metrics(future_metrics!("irn_api_shadowing"))
                    .pipe(tokio::spawn);
            }
        };

        self.retry::<RPC, _, _>(op)
    }

    async fn retry<RPC, Op, T>(&self, op: Op) -> Result<T>
    where
        RPC: Rpc<Kind = irn_rpc::kind::Unary, Request = Op, Response = super::Result<T>> + 'static,
        Op: Operation + Clone + 'static,
    {
        use {super::Error as ApiError, tryhard::RetryPolicy};

        let started_at = std::time::Instant::now();

        // We don't really need to specify the upper bound of retries, because we
        // control the max duration. But `tryhard` requires it, so let's set
        // `u32::MAX`.
        let retries = u32::MAX;

        tryhard::retry_fn(|| {
            self.random_client()
                .send_unary::<RPC>(&AnyPeer, &op)
                .map(|res| {
                    match res {
                        Ok(Ok(out)) => Ok(out),
                        Ok(Err(api)) => Err(Error::Api(api)),
                        Err(rpc) => Err(Error::RpcClient(rpc)),
                    }
                    .tap(|res| {
                        Self::meter_operation_result::<Op, _>(res, ResultKind::Attempt);
                    })
                })
        })
        .retries(retries)
        .custom_backoff(move |attempt, err: &_| {
            let delay = match err {
                // Note: Because of the way errors are implemented in the current RPC, the
                // `StreamFinished` error here means the same as `Throttled`.
                Error::Api(ApiError::NotFound | ApiError::Unauthorized | ApiError::Throttled)
                | Error::Encryption(_)
                | Error::RpcClient(irn_rpc::client::Error::Transport(
                    irn_rpc::transport::Error::StreamFinished,
                )) => {
                    return RetryPolicy::Break;
                }

                // On the first attempt retry immediately.
                _ if attempt == 1 => Duration::ZERO,

                Error::Api(ApiError::Internal(_)) => Duration::from_millis(100),

                Error::RpcClient(_) => Duration::from_millis(50),
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
        .tap(|res| Self::meter_operation_result::<Op, _>(res, ResultKind::Final))
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

        self.exec::<rpc::Get, _, _>(op)
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

        self.exec::<rpc::Set, _, _>(op).await
    }

    pub async fn del(&self, key: Key) -> Result<()> {
        let op = super::Del { key };

        self.exec::<rpc::Del, _, _>(op).await
    }

    pub async fn get_exp(&self, key: Key) -> Result<UnixTimestampSecs> {
        let op = super::GetExp { key };

        self.exec::<rpc::GetExp, _, _>(op)
            .await?
            .ok_or(Error::Api(super::Error::NotFound))
    }

    pub async fn set_exp(&self, key: Key, expiration: UnixTimestampSecs) -> Result<()> {
        let op = super::SetExp {
            key,
            expiration: Some(expiration),
        };

        self.exec::<rpc::SetExp, _, _>(op).await
    }

    pub async fn hget(&self, key: Key, field: Field) -> Result<Option<Value>> {
        let ns = key.namespace;
        let op = super::HGet { key, field };

        self.exec::<rpc::HGet, _, _>(op)
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

        self.exec::<rpc::HSet, _, _>(op).await
    }

    pub async fn hdel(&self, key: Key, field: Field) -> Result<()> {
        let op = super::HDel { key, field };

        self.exec::<rpc::HDel, _, _>(op).await
    }

    pub async fn hget_exp(&self, key: Key, field: Field) -> Result<UnixTimestampSecs> {
        let op = super::HGetExp { key, field };

        self.exec::<rpc::HGetExp, _, _>(op)
            .await?
            .ok_or(Error::Api(super::Error::NotFound))
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

        self.exec::<rpc::HSetExp, _, _>(op).await
    }

    pub async fn hcard(&self, key: Key) -> Result<u64> {
        let op = super::HCard { key };

        self.exec::<rpc::HCard, _, _>(op).await
    }

    pub async fn hfields(&self, key: Key) -> Result<Vec<Field>> {
        let op = super::HFields { key };

        self.exec::<rpc::HFields, _, _>(op).await
    }

    pub async fn hvals(&self, key: Key) -> Result<Vec<Value>> {
        let ns = key.namespace;
        let op = super::HVals { key };
        let values = self.exec::<rpc::HVals, _, _>(op).await?;

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
        let (values, cursor) = self.exec::<rpc::HScan, _, _>(op).await?;

        self.try_open_collection(&ns, values)
            .map(|values| (values, cursor))
    }

    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> Result<()> {
        self.random_client()
            .send_oneshot::<rpc::Publish>(&AnyPeer, &super::Publish { channel, message })
            .await
            .map_err(Into::into)
    }

    pub async fn subscribe(
        &self,
        channels: HashSet<Vec<u8>>,
    ) -> impl Stream<Item = PubsubEventPayload> + Send + 'static {
        let this = self.clone();

        Self::meter_operation_result::<super::Subscribe, _>(&Ok(()), ResultKind::Final);

        async_stream::stream! {
            let op = &super::Subscribe { channels: channels.clone() };

            loop {
                let res =
                    rpc::Subscribe::send(this.random_client(), &AnyPeer, &|mut tx, rx| async  move {
                        tx.send(op).await?;
                        Ok(rx)
                    })
                    .await;

                let mut rx = match res {
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
                        Ok(Ok(msg)) => yield msg,
                        res @ (Err(_) | Ok(Err(_))) => {
                            tracing::warn!(?res, "Failed to receive pubsub message, resubscribing...");
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

impl transport::Handshake for Handshake {
    type Ok = ();
    type Err = transport::Error;

    fn handle(
        &self,
        _peer_id: PeerId,
        conn: PendingConnection,
    ) -> impl Future<Output = Result<Self::Ok, Self::Err>> + Send {
        async move {
            let (mut rx, mut tx) = conn
                .accept_handshake::<HandshakeRequest, HandshakeResponse>()
                .await?;

            let req = rx.recv_message().await?;

            let namespaces = self
                .namespaces
                .iter()
                .map(|(pub_key, auth)| auth::token::NamespaceAuth {
                    namespace: *pub_key,
                    signature: auth.sign(req.auth_nonce.as_ref()),
                })
                .collect();

            tx.send(&HandshakeResponse { namespaces }).await?;

            Ok(())
        }
    }
}

enum ResultKind {
    Attempt,
    Final,
}

impl<K: Kind> Client<K> {
    fn meter_operation_result<Op: Operation, T>(res: &Result<T>, kind: ResultKind) {
        let err = res.as_ref().err().and_then(|e| {
            Some(match e {
                Error::RpcClient(client::Error::Transport(_)) => "transport".into(),
                Error::RpcClient(client::Error::Rpc { error, .. }) => error.code.clone(),
                Error::Api(super::Error::Unauthorized) => "unauthorized".into(),
                Error::Api(super::Error::NotFound) => return None,
                Error::Api(super::Error::Throttled) => "throttled".into(),
                Error::Api(super::Error::Internal(e)) => e.code.clone(),
                Error::Encryption(_) => "encryption".into(),
            })
        });

        let kind = match kind {
            ResultKind::Attempt => "attempt",
            ResultKind::Final => "final",
        };

        metrics::counter!("irn_api_client_operation_results",
            "client_tag" => K::METRICS_TAG,
            "operation" => Op::NAME,
            "kind" => kind,
            "err" => err.unwrap_or_default()
        )
        .increment(1);
    }
}
