use {
    crate::{db::cf::ColumnFamilyName, util::serde::deserialize, Error},
    itertools::Itertools,
    rocksdb::DBPinnableSlice,
    serde::de::DeserializeOwned,
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
            Arc,
        },
        thread::{self, JoinHandle},
        time::Instant,
    },
    tokio::sync::oneshot,
};

const CHANNEL_CAPACITY: usize = 65535;

#[derive(Debug, thiserror::Error)]
#[error("Invalid number of reader threads")]
struct InvalidThreadNumber;

type ReadCallbackFn = Box<dyn FnOnce(Result<Option<DBPinnableSlice<'_>>, Error>) + Send + 'static>;
type RawCallbackFn = Box<dyn FnOnce(&rocksdb::DB) + Send + 'static>;

struct ReadRequest {
    cf_name: &'static str,
    key: Vec<u8>,
    callback: ReadCallbackFn,
    timestamp: Instant,
}

struct RawCallbackRequest {
    callback: RawCallbackFn,
    timestamp: Instant,
}

pub struct Config {
    pub num_batch_threads: usize,
    pub num_callback_threads: usize,
}

pub struct Reader {
    batch_workers: Vec<(mpsc::SyncSender<ReadRequest>, JoinHandle<()>)>,
    raw_cb_workers: Vec<(mpsc::SyncSender<RawCallbackRequest>, JoinHandle<()>)>,
    batch_worker_idx: AtomicUsize,
    raw_cb_worker_idx: AtomicUsize,
    batch_queue_size: Arc<AtomicUsize>,
    raw_cb_queue_size: Arc<AtomicUsize>,
}

impl Reader {
    pub fn new(db: Arc<rocksdb::DB>, config: Config) -> Result<Self, Error> {
        if config.num_batch_threads == 0 || config.num_callback_threads == 0 {
            return Err(Error::Other(InvalidThreadNumber.to_string()));
        }

        let batch_queue_size = Arc::new(AtomicUsize::from(0));
        let raw_cb_queue_size = Arc::new(AtomicUsize::from(0));

        let batch_workers = (0..config.num_batch_threads)
            .map(|idx| {
                let (tx, rx) = mpsc::sync_channel(CHANNEL_CAPACITY);
                let db = db.clone();
                let batch_queue_size = batch_queue_size.clone();

                let handle = thread::Builder::new()
                    .name(format!("rocksdb_batch_read_thread_{idx}"))
                    .spawn(move || reader_thread(db, rx, batch_queue_size))
                    .expect("failed to spawn reader thread");

                (tx, handle)
            })
            .collect();

        let raw_cb_workers = (0..config.num_callback_threads)
            .map(|idx| {
                let (tx, rx) = mpsc::sync_channel(CHANNEL_CAPACITY);
                let db = db.clone();
                let raw_cb_queue_size = raw_cb_queue_size.clone();

                let handle = thread::Builder::new()
                    .name(format!("rocksdb_raw_cb_thread_{idx}"))
                    .spawn(move || raw_cb_thread(db, rx, raw_cb_queue_size))
                    .expect("failed to spawn reader thread");

                (tx, handle)
            })
            .collect();

        Ok(Self {
            batch_workers,
            raw_cb_workers,
            batch_worker_idx: 0.into(),
            raw_cb_worker_idx: 0.into(),
            batch_queue_size,
            raw_cb_queue_size,
        })
    }

    pub async fn read<T>(
        &self,
        cf_name: ColumnFamilyName,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.send_request(ReadRequest {
            cf_name: cf_name.as_str(),
            key: key.into(),
            callback: Box::new(move |res| {
                let _ = tx.send(convert_result(res));
            }),
            timestamp: Instant::now(),
        })?;

        rx.await.map_err(|_| Error::WorkerChannelClosed)?
    }

    pub async fn exec_raw<T, U>(&self, cb: T) -> Result<U, Error>
    where
        T: FnOnce(&rocksdb::DB) -> U + Send + 'static,
        U: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.send_raw_callback(RawCallbackRequest {
            callback: Box::new(move |db| {
                let _ = tx.send(cb(db));
            }),
            timestamp: Instant::now(),
        })?;

        rx.await.map_err(|_| Error::WorkerChannelClosed)
    }

    fn send_request(&self, req: ReadRequest) -> Result<(), Error> {
        self.batch_queue_size.fetch_add(1, Ordering::Relaxed);

        let idx = self.batch_worker_idx.fetch_add(1, Ordering::Relaxed) % self.batch_workers.len();

        self.batch_workers[idx]
            .0
            .try_send(req)
            .map_err(|_| Error::WorkerQueueOverrun)
    }

    fn send_raw_callback(&self, req: RawCallbackRequest) -> Result<(), Error> {
        self.raw_cb_queue_size.fetch_add(1, Ordering::Relaxed);

        let idx =
            self.raw_cb_worker_idx.fetch_add(1, Ordering::Relaxed) % self.raw_cb_workers.len();

        self.raw_cb_workers[idx]
            .0
            .try_send(req)
            .map_err(|_| Error::WorkerQueueOverrun)
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // Drop worker channels to signal shutdown.
        while let Some((tx, handle)) = self.batch_workers.pop() {
            drop(tx);
            let _ = handle.join();
        }

        while let Some((tx, handle)) = self.raw_cb_workers.pop() {
            drop(tx);
            let _ = handle.join();
        }
    }
}

fn reader_thread(
    db: Arc<rocksdb::DB>,
    rx: mpsc::Receiver<ReadRequest>,
    queue_size: Arc<AtomicUsize>,
) {
    let thread = thread::current();
    let thread_id = thread.id();
    let thread_name = thread.name();

    tracing::trace!(?thread_id, ?thread_name, "batch reader thread started");

    // Use blocking `recv()` here to let the thread suspend if there's no work.
    while let Ok(req) = rx.recv() {
        // Pull all pending requests from the queue.
        let mut requests = vec![req];
        requests.extend(rx.try_iter());

        let queue_size = queue_size.fetch_sub(requests.len(), Ordering::Relaxed);
        metrics::gauge!("irn_rocksdb_reader_thread_queue").set(queue_size as f64);

        // Group the requests by column family name.
        // Note: This is likely unnecessary, if `multi_get_cf_opt()` performs grouping
        // under the hood. But for now let's manually group requests into batches and
        // use `batched_multi_get_cf()`.
        let groups = requests.into_iter().into_group_map_by(|req| req.cf_name);

        for (cf_name, reqs) in groups {
            if let Some(cf_handle) = db.cf_handle(cf_name) {
                let keys = reqs.iter().map(|req| &req.key);
                let result = db.batched_multi_get_cf(cf_handle, keys, false).into_iter();

                for (req, res) in reqs.into_iter().zip(result) {
                    metrics::histogram!("irn_rocksdb_reader_thread_queue_time")
                        .record(req.timestamp.elapsed().as_secs_f64());

                    (req.callback)(res.map_err(Into::into));
                }
            } else {
                tracing::warn!(?thread_id, ?thread_name, %cf_name, "column family not available. dropping request batch");

                for req in reqs {
                    (req.callback)(Err(Error::InvalidColumnFamily));
                }
            }
        }
    }

    tracing::trace!(?thread_id, ?thread_name, "batch reader thread finished");
}

fn raw_cb_thread(
    db: Arc<rocksdb::DB>,
    rx: mpsc::Receiver<RawCallbackRequest>,
    queue_size: Arc<AtomicUsize>,
) {
    let thread = thread::current();
    let thread_id = thread.id();
    let thread_name = thread.name();

    tracing::trace!(?thread_id, ?thread_name, "raw callback thread started");

    while let Ok(req) = rx.recv() {
        let queue_size = queue_size.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!("irn_rocksdb_raw_cb_thread_queue").set(queue_size as f64);
        metrics::histogram!("irn_rocksdb_raw_cb_thread_queue_time")
            .record(req.timestamp.elapsed().as_secs_f64());

        (req.callback)(&db);
    }

    tracing::trace!(?thread_id, ?thread_name, "raw callback thread finished");
}

fn convert_result<T>(res: Result<Option<DBPinnableSlice<'_>>, Error>) -> Result<Option<T>, Error>
where
    T: DeserializeOwned + Send + 'static,
{
    res?.as_ref()
        .map(AsRef::as_ref)
        .map(deserialize::<T>)
        .transpose()
}
