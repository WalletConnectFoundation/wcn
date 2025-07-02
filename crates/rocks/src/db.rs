//! Interfaces, adapters and utilities for `RocksDB` storage backend.

#![allow(clippy::manual_async_fn)]

use {
    crate::{
        db::{
            compaction::{CompactionFilterFactory, MergeOperator},
            schema::GenericKey,
        },
        Error,
    },
    cf::ColumnFamilyName,
    futures_util::{FutureExt, TryFutureExt},
    serde::de::DeserializeOwned,
    std::{
        collections::HashMap,
        fmt::Debug,
        io,
        ops::Deref,
        path::{Path, PathBuf},
        sync::Arc,
    },
};
pub use {
    context::DataContext,
    rocksdb::{perf::MemoryUsageStats, Error as NativeError},
    statistics::Statistic,
};

pub(crate) mod batch;
pub mod cf;
pub mod compaction;
pub mod context;
pub mod migration;
mod reader;
pub mod schema;
pub mod statistics;
pub mod types;

const LOG_FILE_NAME: &str = "LOG";

pub struct RocksBackendInner {
    db: Arc<rocksdb::DB>,
    opts: rocksdb::Options,
    reader: reader::Reader,

    // We need to stop log consumer task before shutting down RocksDB, otherwise it hangs
    // (sometimes!).
    log_consumer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for RocksBackendInner {
    fn drop(&mut self) {
        if let Some(handle) = self.log_consumer_handle.take() {
            handle.abort();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    let _ = handle.await;
                })
            });
        }
    }
}

/// Main entry point for database clients, wraps backend, and exposes access to
/// column families.
///
/// Abstracts database methods, allows different data structures to have a
/// common API, when interacting with the underlying database.
#[derive(Clone)]
pub struct RocksBackend {
    inner: Arc<RocksBackendInner>,
}

impl Deref for RocksBackend {
    type Target = RocksBackendInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Debug for RocksBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksBackend")
            .field("db", &self.inner.db)
            .finish_non_exhaustive()
    }
}

impl RocksBackend {
    /// Gets a value from a given column family, using provided key.
    ///
    /// To avoid unnecessary memory copy, `get_pinned_cf` is used.
    async fn get<V>(
        &self,
        cf_name: ColumnFamilyName,
        key: impl ToOwned<Owned = Vec<u8>>,
    ) -> Result<Option<DataContext<V>>, Error>
    where
        V: DeserializeOwned + Send + 'static,
    {
        self.reader
            .read::<DataContext<V>>(cf_name, key.to_owned())
            .await
    }

    /// Merges changes to the value in a given column family.
    async fn merge(
        &self,
        cf_name: ColumnFamilyName,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        self.db
            .merge_cf(&self.cf_handle(cf_name), key, value)
            .map_err(Into::into)
    }

    /// Compacts column family values.
    ///
    /// Todo: expose range parameters.
    fn compact(&self, cf_name: ColumnFamilyName) {
        self.db
            .compact_range_cf(&self.cf_handle(cf_name), None::<&[u8]>, None::<&[u8]>);
    }

    /// Returns database iterator within a given left/right bounds.
    fn range_iterator<C, K: Into<Vec<u8>>>(
        &self,
        left: Option<K>,
        right: Option<K>,
    ) -> rocksdb::DBIterator<'_>
    where
        C: cf::Column,
    {
        let mut ro = rocksdb::ReadOptions::default();
        if let Some(left) = left {
            ro.set_iterate_lower_bound(left);
        }
        if let Some(right) = right {
            ro.set_iterate_upper_bound(right);
        }

        // Without this if the cf has a prefix extractor and the range is outside a
        // single prefix the iterator will return nothing.
        //
        // https://github.com/facebook/rocksdb/issues/5973#issuecomment-1423795401
        // https://github.com/facebook/rocksdb/wiki/Prefix-Seek#how-to-ignore-prefix-bloom-filters-in-read
        ro.set_total_order_seek(true);

        self.db
            .iterator_cf_opt(&self.cf_handle(C::NAME), ro, rocksdb::IteratorMode::Start)
    }

    /// Returns database iterator for a given prefix.
    fn prefix_iterator<C, K: AsRef<[u8]>>(&self, prefix: K) -> rocksdb::DBIterator<'_>
    where
        C: cf::Column,
    {
        self.db.prefix_iterator_cf(&self.cf_handle(C::NAME), prefix)
    }

    fn prefix_iterator_with_cursor<C, K: AsRef<[u8]>>(&self, cursor: K) -> rocksdb::DBIterator<'_>
    where
        C: cf::Column,
    {
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_total_order_seek(false);
        ro.set_prefix_same_as_start(true);

        self.db.iterator_cf_opt(
            &self.cf_handle(C::NAME),
            ro,
            rocksdb::IteratorMode::From(cursor.as_ref(), rocksdb::Direction::Forward),
        )
    }

    /// Commits atomic batch.
    fn write_batch(&self, batch: batch::WriteBatch) -> Result<(), Error> {
        self.db.write(batch.write_batch).map_err(Into::into)
    }

    #[inline]
    fn cf_handle(&self, cf_name: ColumnFamilyName) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(cf_name.as_str())
            .expect("column family must be registered with database")
    }

    /// Returns the [`MemoryUsageStats`] of the RocksDB.
    pub fn memory_usage(&self) -> Result<MemoryUsageStats, Error> {
        rocksdb::perf::get_memory_usage_stats(Some(&[&self.db]), None).map_err(Into::into)
    }

    /// Returns the current [`Statistic`]s of the RocksDB.
    ///
    /// Returns `Ok(None)` if [`Statistic`]s are disabled (not enabled via
    /// [`Options::enable_statistics`]).
    pub fn statistics(&self) -> Result<Option<HashMap<String, Statistic>>, statistics::ParseError> {
        self.opts
            .get_statistics()
            .map(|s| Statistic::parse_multiple(&s))
            .transpose()
    }

    /// Returns backend-injected column family.
    #[inline]
    pub fn column<C: cf::Column>(&self) -> Option<cf::DbColumn<C>> {
        Some(cf::DbColumn::new(self.clone()))
    }

    /// Executes a blocking IO operation using [`tokio::task::spawn_blocking`].
    pub async fn exec_blocking<F, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(Self) -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let this = self.clone();
        self.reader.exec_raw(move |_| f(this)).await?
    }
}

/// Performance-related RocksDB parameters.
///
/// Most of these are directly translated into RocksDB initialization params.
/// See [`rocksdb::Options`] for details of each parameter.
///
/// `num_batch_threads` and `num_callback_threads` configure the custom thread
/// pools used for batching of read requests and iterators.
#[derive(Debug, Clone)]
pub struct RocksdbDatabaseConfig {
    /// Number of batch threads.
    pub num_batch_threads: usize,

    /// Number of callback threads.
    pub num_callback_threads: usize,

    /// Sets maximum number of threads that will concurrently perform a
    /// compaction job by breaking it into multiple, smaller ones that are
    /// run simultaneously.
    pub max_subcompactions: usize,

    /// Sets maximum number of concurrent background jobs (compactions and
    /// flushes).
    pub max_background_jobs: usize,

    /// Use to control write rate of flush and compaction. Flush has higher
    /// priority than compaction.
    pub ratelimiter: usize,

    /// Sets the number of background threads used for flush and compaction.
    pub increase_parallelism: usize,

    /// Sets the amount of data to build up in memory before converting to a
    /// sorted on-disk file.
    pub write_buffer_size: usize,

    // Set write buffer size of a single MemTable (how much to write into memory
    // before flushing). Each column family has its own MemTable.
    pub max_write_buffer_number: usize,

    /// Sets the minimum number of write buffers that will be merged together
    /// before writing to storage.
    pub min_write_buffer_number_to_merge: usize,

    /// Sets global cache for blocks (user data is stored in a set of blocks,
    /// and a block is the unit of reading from disk).
    pub block_cache_size: usize,

    /// Approximate size of user data packed per block.
    pub block_size: usize,

    /// Sets global cache for table-level rows.
    pub row_cache_size: usize,

    /// Whether to enable metrics. Note that this currently has some CPU
    /// cost, so it's disabled by default.
    pub enable_metrics: bool,
}

impl Default for RocksdbDatabaseConfig {
    fn default() -> Self {
        let num_cores = std::thread::available_parallelism()
            .map(|num| num.get())
            .unwrap_or(1);

        Self {
            num_batch_threads: num_cores,
            num_callback_threads: num_cores * 4,
            max_subcompactions: num_cores,
            max_background_jobs: num_cores * 4,
            ratelimiter: 64 * 1024 * 1024,
            increase_parallelism: num_cores * 2,
            write_buffer_size: 128 * 1024 * 1024,
            max_write_buffer_number: 8,
            min_write_buffer_number_to_merge: 2,
            block_cache_size: 4 * 1024 * 1024 * 1024,
            block_size: 4 * 1024,
            row_cache_size: 1024 * 1024 * 1024,
            enable_metrics: false,
        }
    }
}

/// Provides a way for flexible database configuration.
pub struct RocksDatabaseBuilder {
    path: PathBuf,
    cfs: Vec<rocksdb::ColumnFamilyDescriptor>,
    cfg: RocksdbDatabaseConfig,
}

impl RocksDatabaseBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            cfs: vec![],
            cfg: Default::default(),
        }
    }

    pub fn with_config(mut self, cfg: RocksdbDatabaseConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Allows specifying which columns are supported by created database.
    pub fn with_column_family<C: cf::Column>(mut self, _: C) -> Self {
        let cf = rocksdb::ColumnFamilyDescriptor::new(C::NAME, create_cf_opts::<C>());
        self.cfs.push(cf);
        self
    }

    /// Creates configured database.
    pub fn build(self) -> Result<RocksBackend, Error> {
        // Clean up existing log file, as it seems like RocksDB doesn't do it by
        // default. It has optional rotation, but we don't need it as we export
        // logs ouside anyway.
        let _ = std::fs::remove_file(self.path.join(LOG_FILE_NAME));

        let opts = create_db_opts(&self.cfg);
        let db = Arc::new(rocksdb::DB::open_cf_descriptors(
            &opts,
            &*self.path,
            self.cfs,
        )?);

        let log_consumer_handle = tokio::spawn(
            consume_logs(self.path)
                .map_err(|err| tracing::error!(?err, "Log consumer task failed"))
                .map(drop),
        );

        let reader = reader::Reader::new(db.clone(), reader::Config {
            num_batch_threads: self.cfg.num_batch_threads,
            num_callback_threads: self.cfg.num_callback_threads,
        })?;

        let inner = Arc::new(RocksBackendInner {
            db,
            opts,
            reader,
            log_consumer_handle: Some(log_consumer_handle),
        });
        Ok(RocksBackend { inner })
    }
}

fn create_db_opts(cfg: &RocksdbDatabaseConfig) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    if cfg.enable_metrics {
        opts.enable_statistics();
    }

    // Let RocksDB automatically adjust max bytes for each level. The goal is to
    // have a lower bound on size amplification.
    opts.set_level_compaction_dynamic_level_bytes(true);

    // Parallelize compactions to make them faster.
    opts.set_max_subcompactions(cfg.max_subcompactions as u32);

    // Cap the number of background compaction and flush jobs.
    opts.set_max_background_jobs(cfg.max_background_jobs as i32);

    // Unconstrained writes can result in compaction proceeding at hundreds of
    // MiB/s, on flash disks. This can result in stalls for other jobs. So, setting
    // the maximum bytes per second (`rate_bytes_per_sec`) to some sane value (20-40
    // MiB/s) allows to have a good balance between throughput and latency.
    //
    // The `rate_bytes_per_sec` is the only parameter you probably want to tweak.
    //
    // The `refill_period_us` is the time period in microseconds after which the
    // rate limiter will be refilled (the default 100000 should be good enough for
    // the most cases).
    //
    // The `fairness` should be good enough with the default value of 10.
    //
    // When rate limiting is enabled, `bytes_per_sync` is automatically adjusted to
    // 1MiB.
    //
    // See [docs](https://github.com/facebook/rocksdb/wiki/Rate-Limiter) for more details.
    opts.set_ratelimiter(cfg.ratelimiter as i64, 100 * 1000, 10);

    // By default a single background thread is used for flushing and compaction.
    // The good value for this is the number of cores.
    opts.increase_parallelism(cfg.increase_parallelism as i32);

    // Set write buffer size of a single MemTable (how much to write into memory
    // before flushing). Each column family has its own MemTable.
    //
    // Currently we don't have many column families, so we can set it to a high
    // value. If you want to control the total amount of memory used for all
    // MemTables, in all column families, use `set_db_write_buffer_size`.
    //
    // If you want to specify how many MemTables to keep in memory, use
    // `set_max_write_buffer_number`. The default value of 2 allows to have 1
    // MemTable in memory and 1 MemTable being flushed.
    opts.set_write_buffer_size(cfg.write_buffer_size);
    opts.set_max_write_buffer_number(cfg.max_write_buffer_number as i32);
    opts.set_min_write_buffer_number_to_merge(cfg.min_write_buffer_number_to_merge as i32);

    // Block cache (https://github.com/facebook/rocksdb/wiki/Block-Cache).
    //
    // By default RocksDB uses 32MiB (8MiB before 8.2) block cache (LRU Cache),
    // which is only suitable if the application doesn't depend heavily on read
    // performance. The upside of this is a very conservative memory footprint.
    // Since we depend on read performance, we need to increase the block cache size
    // substantially.
    //
    // TODO: Experiment with HyperClockCache (which is faster than LRU Cache,
    // see https://smalldatum.blogspot.com/2022/10/hyping-hyper-clock-cache-in-rocksdb.html).
    //
    // The hardware and OS page size is normally 4KiB, and by having a higher block
    // size in DB we can reduce the number of disk reads (for additional management
    // required when flushing the data -- as atomicity is guaranteed on page size
    // only). Use `set_block_size` to set the block size.
    //
    // When enabled, `set_cache_index_and_filter_blocks` setting gives higher
    // priority to index and filter blocks (they will be less likely to be evicted
    // than data blocks).
    //
    // With `set_bloom_filter(15, false)` Bloom filters are enabled to reduce disk
    // access. 15 bits per key are 99% as effective as 100 bits per key, and `false`
    // means we are using the new filter format (the previous format is block based
    // and memory non-aligned see: https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter).
    let cache = rocksdb::Cache::new_lru_cache(cfg.block_cache_size);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    block_opts.set_block_size(cfg.block_size);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_format_version(5);
    block_opts.set_index_block_restart_interval(16);
    block_opts.set_bloom_filter(15.0, false);
    opts.set_block_based_table_factory(&block_opts);

    // A block is logically divided into records. Whenever one sends a point lookup
    // query, block cache is checked, and then -- if record exists there -- the
    // record is read from the block cache. RocksDB allows to cache records
    // themselves, so that before querying the block cache, the record cache is
    // checked. This speeds up point lookups, but increases memory usage.
    let cache = rocksdb::Cache::new_lru_cache(cfg.row_cache_size);
    opts.set_row_cache(&cache);

    opts
}

fn create_cf_opts<C: 'static + cf::Column>() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();

    if C::PREFIX_SEEK_ENABLED {
        opts.set_prefix_extractor(rocksdb::SliceTransform::create(
            "dynamic_prefix_extractor",
            dynamic_prefix_extractor::<C>,
            None,
        ));
        // Prefix bloom filter's size will be of
        // `write_buffer_size * memtable_prefix_bloom_ratio`.
        opts.set_memtable_prefix_bloom_ratio(0.25);
        // TODO: consider setting set_memtable_whole_key_filtering(true)
    }

    // If compaction filter factory is defined for a given column family, load it.
    let name = &format!("compaction_filter_factory({})", C::NAME);
    if let Some(factory) = <C as cf::Column>::CompactionFilterFactory::new(name) {
        opts.set_compaction_filter_factory(factory);
    }

    C::MergeOperator::initialize(&format!("merge_operator({})", C::NAME), &mut opts);

    opts
}

fn dynamic_prefix_extractor<C: cf::Column>(data: &[u8]) -> &[u8] {
    if data.len() == schema::keys::KEYRING_POSITION_PREFIX_LENGTH {
        // This is the case of iterating over database keys using only the keyring
        // position as prefix, as being used by the data exporter.
        return data;
    }

    let prefix_len = match GenericKey::calculate_size(data) {
        Ok(len) => len,

        Err(err) => {
            tracing::warn!(
                ?err,
                cf = %C::NAME.as_str(),
                key_length = data.len(),
                "dynamic key prefix extraction failed to calculate len"
            );
            return data;
        }
    };

    if prefix_len > data.len() {
        tracing::warn!(cf = %C::NAME.as_str(), "invalid prefix length");
        return data;
    };

    &data[0..prefix_len]
}

/// Consumes logs from RocksDB LOG file, and prints them via [`tracing`].
async fn consume_logs(db_path: PathBuf) -> io::Result<()> {
    let mut lines = linemux::MuxedLines::new()?;
    lines.add_file(db_path.join(LOG_FILE_NAME)).await?;

    while let Some(line) = lines.next_line().await? {
        tracing::trace!(log = line.line());
    }

    tracing::warn!("Log consumer task finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            schema::{
                test_types::{TestKey, TestValue},
                StringColumn,
            },
            types::StringStorage,
        },
        util::{db_path::DBPath, timestamp_micros, timestamp_secs},
        Error,
        RocksDatabaseBuilder,
        UnixTimestampSecs,
    };

    fn timestamp(added: UnixTimestampSecs) -> UnixTimestampSecs {
        crate::util::timestamp_secs() + added
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn op_responses() {
        let expiration = timestamp_secs() + 600;
        let path = DBPath::new("op_responses");
        let rocks_db = RocksDatabaseBuilder::new(&path)
            .with_column_family(StringColumn)
            .build()
            .unwrap();
        let db = &rocks_db.column::<StringColumn>().unwrap();

        let key = &TestKey::new(42).into();
        let val = &TestValue::new("value1").into();

        {
            // Make sure that no data exists.
            assert_eq!(db.get(key).await.unwrap(), None);

            // Set expiration to non-existent data.
            db.set_exp(key, timestamp(30), timestamp_micros())
                .await
                .unwrap();
            assert_eq!(db.get(key).await.unwrap(), None);

            // Expiration from non-existent keys return an error.
            assert!(matches!(db.get_exp(key).await, Err(Error::EntryNotFound)));

            db.set(key, val, expiration, timestamp_micros())
                .await
                .unwrap();
            assert_eq!(db.get_exp(key).await, Ok(expiration));

            // Should return proper TTL if set for an existing key.
            let expiration = timestamp(30);
            db.set_exp(key, expiration, timestamp_micros())
                .await
                .unwrap();
            let result = db.get_exp(key).await.unwrap();
            assert_eq!(result, expiration);
        }
    }
}
