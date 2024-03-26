//! Consensus related types shared between different IRN crates.

use {
    crate::{
        cluster::{self, ClusterView},
        PeerId,
    },
    async_trait::async_trait,
    futures::Stream,
};

/// Consensus algorithm managing consistency of the [`ClusterView`].
#[async_trait]
pub trait Consensus: Clone + Send + Sync + 'static {
    /// [`Stream`] of [`ClusterView`] changes.
    type Stream: Stream<Item = ClusterView> + Send;

    /// Error of proposing changed to the [`ClusterView`].
    type Error: std::error::Error;

    /// Proposes a new [`cluster::NodeOperationMode`] for a node.
    async fn update_node_op_mode(
        &self,
        id: PeerId,
        mode: cluster::NodeOperationMode,
    ) -> Result<cluster::UpdateNodeOpModeResult, Self::Error>;

    /// Returns a [`Stream`] of [`ClusterView`] changes.
    fn changes(&self) -> Self::Stream;
}

#[cfg(test)]
pub use stub::Consensus as Stub;
#[cfg(test)]
pub mod stub {
    use {
        super::{async_trait, cluster, ClusterView, PeerId},
        std::{
            convert::Infallible,
            sync::{Arc, Mutex},
        },
        tap::Tap,
        tokio::sync::watch,
        tokio_stream::wrappers::WatchStream,
    };

    #[derive(Clone, Debug)]
    pub struct Consensus {
        rx: watch::Receiver<ClusterView>,
        tx: Arc<watch::Sender<ClusterView>>,

        cluster_view: Arc<Mutex<ClusterView>>,
    }

    impl Default for Consensus {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Consensus {
        pub fn new() -> Self {
            let view = ClusterView::default();

            let (tx, rx) = watch::channel(view.clone());

            Self {
                rx,
                tx: Arc::new(tx),
                cluster_view: Arc::new(Mutex::new(view)),
            }
        }

        pub fn set_node(&self, node: cluster::Node) {
            let mut view = self.cluster_view.lock().unwrap();
            let mut peers = view.nodes().clone();
            peers.insert(node.peer_id, node);
            view.set_peers(peers);

            self.tx.send(view.clone()).unwrap();
        }

        pub fn get_view(&self) -> ClusterView {
            self.cluster_view.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl super::Consensus for Consensus {
        type Stream = WatchStream<ClusterView>;
        type Error = Infallible;

        async fn update_node_op_mode(
            &self,
            id: PeerId,
            mode: cluster::NodeOperationMode,
        ) -> Result<cluster::UpdateNodeOpModeResult, Self::Error> {
            tracing::info!(%id, ?mode, "update_node_op_mode");

            let mut view = self.cluster_view.lock().unwrap();
            Ok(view
                .update_node_op_mode(id, mode)
                .tap(|_| self.tx.send(view.clone()).unwrap()))
        }

        fn changes(&self) -> Self::Stream {
            WatchStream::from_changes(self.rx.clone())
        }
    }
}
