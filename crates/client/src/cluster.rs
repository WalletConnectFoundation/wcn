use {
    crate::{Error, PeerAddr},
    arc_swap::ArcSwap,
    derive_more::derive::AsRef,
    futures::{Stream, StreamExt as _, TryStreamExt as _, future::Either, stream},
    std::{sync::Arc, time::Duration},
    tokio::sync::oneshot,
    wc::future::FutureExt,
    wcn_cluster::{
        EncryptionKey,
        node_operator,
        smart_contract::{ReadError, ReadResult},
    },
    wcn_cluster_api::{
        Address,
        ClusterApi,
        ClusterView,
        Read,
        rpc::client::{Cluster as ClusterClient, ClusterConnection},
    },
    wcn_rpc::PeerId,
    wcn_storage_api2::rpc::client::{Coordinator as CoordinatorClient, CoordinatorConnection},
};

#[derive(Clone)]
pub struct Node {
    pub operator_id: node_operator::Id,
    pub node: wcn_cluster::Node,
    pub cluster_conn: ClusterConnection,
    pub coordinator_conn: CoordinatorConnection,
}

impl AsRef<PeerId> for Node {
    fn as_ref(&self) -> &PeerId {
        &self.node.peer_id
    }
}

#[derive(Clone, AsRef)]
pub(crate) struct Config {
    #[as_ref]
    pub(crate) encryption_key: EncryptionKey,
    pub(crate) cluster_api: ClusterClient,
    pub(crate) coordinator_api: CoordinatorClient,
}

impl wcn_cluster::Config for Config {
    type SmartContract = SmartContract;
    type KeyspaceShards = ();
    type Node = Node;

    fn new_node(&self, operator_id: node_operator::Id, node: wcn_cluster::Node) -> Self::Node {
        let cluster_conn =
            self.cluster_api
                .new_connection(node.primary_socket_addr(), &node.peer_id, ());
        let coordinator_conn =
            self.coordinator_api
                .new_connection(node.primary_socket_addr(), &node.peer_id, ());

        Node {
            operator_id,
            node,
            cluster_conn,
            coordinator_conn,
        }
    }
}

async fn select_open_connection(cluster: &ArcSwap<View>) -> ClusterConnection {
    loop {
        let conn = cluster
            .load()
            .node_operators()
            .next()
            .next_node()
            .cluster_conn
            .clone();

        let success = conn
            .wait_open()
            .with_timeout(Duration::from_millis(1500))
            .await
            .is_ok();

        if success {
            return conn;
        }
    }
}

// Smart contract here is implemented as an enum because we intend to use the
// same [`wcn_cluster::Config`] for both clusters: initialized from a bootstrap
// cluster view, and from a dynamically updated cluster.
//
// The reason why we're using the same [`wcn_cluster::Config`] for both clusters
// is that [`wcn_cluster::View`] is also parametrized over this config, and we
// need them to be compatible.
pub(crate) enum SmartContract {
    Static(ClusterView),
    Dynamic(Arc<ArcSwap<View>>),
}

#[derive(Debug, thiserror::Error)]
#[error("Method is not available")]
struct MethodNotAvailable;

impl Read for SmartContract {
    fn address(&self) -> ReadResult<Address> {
        Err(ReadError::Other(MethodNotAvailable.to_string()))
    }

    async fn cluster_view(&self) -> ReadResult<ClusterView> {
        match self {
            Self::Static(view) => Ok(view.clone()),

            Self::Dynamic(cluster) => select_open_connection(cluster)
                .await
                .cluster_view()
                .await
                .map_err(transport_err),
        }
    }

    async fn events(
        &self,
    ) -> ReadResult<impl Stream<Item = ReadResult<wcn_cluster::Event>> + Send + use<>> {
        match self {
            Self::Static(_) => Ok(Either::Left(stream::pending())),

            Self::Dynamic(cluster) => {
                let stream = select_open_connection(cluster)
                    .await
                    .events()
                    .await
                    .map_err(transport_err)?
                    .map_err(transport_err);

                Ok(Either::Right(stream))
            }
        }
    }
}

#[inline]
fn transport_err(err: impl ToString) -> ReadError {
    ReadError::Transport(err.to_string())
}

pub(crate) type Cluster = wcn_cluster::Cluster<Config>;
pub(crate) type View = wcn_cluster::View<Config>;

pub(crate) async fn update_task(
    shutdown_rx: oneshot::Receiver<()>,
    cluster: Cluster,
    view: Arc<ArcSwap<View>>,
) {
    let cluster_update_fut = async {
        let mut updates = cluster.updates();

        loop {
            view.store(cluster.view());

            if updates.next().await.is_none() {
                break;
            }
        }
    };

    tokio::select! {
        _ = cluster_update_fut => {},
        _ = shutdown_rx => {},
    };
}

pub(crate) async fn fetch_cluster_view(
    client: &ClusterClient,
    nodes: &[PeerAddr],
) -> Result<ClusterView, Error> {
    let num_nodes = nodes.len();
    let offset = rand::random_range(0..num_nodes);

    for idx in 0..num_nodes {
        let idx = (idx + offset) % num_nodes;

        match try_fetch_cluster_view(client, &nodes[idx]).await {
            Ok(view) => return Ok(view),

            Err(err) => {
                tracing::warn!(?err, "failed to fetch cluster view");
            }
        }
    }

    Err(Error::NoAvailableNodes)
}

async fn try_fetch_cluster_view(
    client: &ClusterClient,
    peer_addr: &PeerAddr,
) -> Result<ClusterView, Error> {
    let view = client
        .connect(peer_addr.addr, &peer_addr.id, ())
        .await
        .map_err(Error::internal)?
        .cluster_view()
        .await?;

    Ok(view)
}
