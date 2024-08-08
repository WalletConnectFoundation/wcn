use {
    super::{Cluster, Node},
    crate::cluster,
    arc_swap::ArcSwap,
    futures::Stream,
    std::sync::Arc,
    tokio::sync::watch,
    tokio_stream::wrappers::WatchStream,
};

#[derive(Debug, Clone)]
pub struct Viewable<N: Node, K> {
    inner: Cluster<N, K>,
    view: View<N, K>,
    update_notifications: watch::Sender<()>,
}

impl<N: Node, K: Clone> Viewable<N, K> {
    pub(super) fn new(cluster: Cluster<N, K>) -> Self {
        let (tx, rx) = watch::channel(());
        let view = View::new(&cluster, rx);

        Viewable {
            inner: cluster,
            view,
            update_notifications: tx,
        }
    }

    pub fn modify(
        &mut self,
        f: impl FnOnce(&mut Cluster<N, K>) -> Result<bool, cluster::Error>,
    ) -> Result<bool, cluster::Error> {
        let modified = f(&mut self.inner)?;
        if modified {
            self.view.cluster.store(Arc::new(self.inner.clone()));
            let _ = self.update_notifications.send(());
        }
        Ok(modified)
    }

    pub fn view(&self) -> View<N, K> {
        self.view.clone()
    }

    pub fn inner(&self) -> &Cluster<N, K> {
        &self.inner
    }
}

#[derive(Debug, Clone)]
pub struct View<N: Node, K> {
    cluster: Arc<ArcSwap<Cluster<N, K>>>,
    update_notifications: watch::Receiver<()>,
}

impl<N: Node, K> View<N, K> {
    pub(super) fn new(cluster: &Cluster<N, K>, update_notifications: watch::Receiver<()>) -> Self
    where
        K: Clone,
    {
        Self {
            cluster: Arc::new(ArcSwap::from_pointee(cluster.clone())),
            update_notifications,
        }
    }

    pub fn cluster(&self) -> Arc<Cluster<N, K>> {
        self.cluster.load_full()
    }

    pub fn peek<T>(&self, f: impl FnOnce(&Cluster<N, K>) -> T) -> T {
        f(&self.cluster.load())
    }

    pub fn updates(&self) -> impl Stream<Item = ()> + 'static {
        WatchStream::new(self.update_notifications.clone())
    }
}
