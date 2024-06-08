//! Compaction filters and filter factories.

use {
    crate::{db::cf, util::serde::deserialize, DataContext},
    rocksdb::{
        compaction_filter::{CompactionFilter, Decision},
        compaction_filter_factory::{self, CompactionFilterContext},
    },
    std::{
        ffi::{CStr, CString},
        marker::PhantomData,
        time::Instant,
    },
};

pub trait CompactionFilterFactory<C: cf::Column>:
    compaction_filter_factory::CompactionFilterFactory
{
    /// Constructor that allows empty constructed objects.
    ///
    /// Whenever compaction filter factory is not required, use the
    /// [`NoopCompactionFilterFactory`] when defining the
    /// [`cf::Column`] implementation. Noop variant returns
    /// `None` from the constructor, and no compaction filter factory is
    /// registered with the database.
    fn new(name: &str) -> Option<Self>
    where
        Self: Sized;
}

/// Compaction filter for purging records with expired TTL.
pub struct ExpiredDataCompactionFilter<C: cf::Column> {
    name: CString,
    started: Instant,
    num_remove: u64,
    num_change: u64,
    num_keep: u64,
    _column: PhantomData<C>,
}

impl<C: cf::Column> ExpiredDataCompactionFilter<C> {
    pub fn new(name: CString) -> Self {
        Self {
            name,
            started: Instant::now(),
            num_remove: 0,
            num_change: 0,
            num_keep: 0,
            _column: PhantomData,
        }
    }

    fn filter_internal(&self, _level: u32, _key: &[u8], value: &[u8]) -> Decision {
        if let Ok(value) = deserialize::<DataContext<C::ValueType>>(value) {
            // Data context may not have a payload if it was initialized from the merge
            // operator with defaults.
            if value.expired() || value.payload().is_none() {
                Decision::Remove
            } else {
                use wc::metrics::otel::KeyValue;

                if let Some(expiration) = value.expiration_timestamp() {
                    let expires_in = expiration.saturating_sub(crate::util::timestamp_secs());

                    // 30 days is max TTL we currently have in business logic.
                    const MAX_TTL_SECS: u64 = 86400 * 30 + 120;

                    if expires_in > MAX_TTL_SECS {
                        wc::metrics::counter!("rocksdb_invalid_ttl_entries", 1, &[KeyValue::new(
                            "cf",
                            C::NAME.as_str()
                        )]);

                        return Decision::Remove;
                    }
                } else {
                    wc::metrics::counter!("rocksdb_persistent_entries", 1, &[KeyValue::new(
                        "cf",
                        C::NAME.as_str()
                    )]);
                }

                let last_modified = value
                    .modification_timestamp()
                    .unwrap_or(value.creation_timestamp());

                // 120 seconds in microseconds.
                const UPDATE_TIMESTAMP_LEEWAY: u64 = 120 * 1000 * 1000;

                if last_modified > crate::util::timestamp_micros() + UPDATE_TIMESTAMP_LEEWAY {
                    wc::metrics::counter!("rocksdb_invalid_update_timestamp_entries", 1, &[
                        KeyValue::new("cf", C::NAME.as_str())
                    ]);

                    return Decision::Remove;
                }

                Decision::Keep
            }
        } else {
            tracing::warn!(
                name = %C::NAME, data_len = %value.len(),
                "compaction: failed to deserialize data"
            );

            // We couldn't deserialize data, so there's no point in keeping it.
            Decision::Remove
        }
    }
}

impl<C: cf::Column> CompactionFilter for ExpiredDataCompactionFilter<C> {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> Decision {
        let decision = self.filter_internal(level, key, value);

        match decision {
            Decision::Keep => self.num_keep += 1,
            Decision::Remove => self.num_remove += 1,
            Decision::Change(_) => self.num_change += 1,
        }

        decision
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

impl<C: cf::Column> Drop for ExpiredDataCompactionFilter<C> {
    fn drop(&mut self) {
        use wc::metrics::otel::KeyValue;

        #[inline]
        fn decision_kv(decision: &'static str) -> KeyValue {
            KeyValue::new("decision", decision)
        }

        let cf_name_kv = KeyValue::new("cf_name", C::NAME.as_str());
        let counter = wc::metrics::counter!("rocksdb_compaction_keys_processed");

        counter.add(self.num_remove, &[
            cf_name_kv.clone(),
            decision_kv("remove"),
        ]);
        counter.add(self.num_change, &[
            cf_name_kv.clone(),
            decision_kv("change"),
        ]);
        counter.add(self.num_keep, &[cf_name_kv.clone(), decision_kv("keep")]);

        let elapsed = self.started.elapsed();

        wc::metrics::histogram!("rocksdb_compaction_time", elapsed.as_millis() as f64, &[
            cf_name_kv
        ]);

        let keys_processed = self.num_remove + self.num_change + self.num_keep;

        tracing::info!(
            elapsed = elapsed.as_millis() as u64,
            cf_name = C::NAME.as_str(),
            keys_processed,
            "rocksdb compaction finished"
        );
    }
}

/// Compaction filter factory for purging records with expired TTL.
pub struct ExpiredDataCompactionFilterFactory<C: cf::Column> {
    name: CString,
    _column: PhantomData<C>,
}

impl<C: cf::Column> CompactionFilterFactory<C> for ExpiredDataCompactionFilterFactory<C> {
    fn new(name: &str) -> Option<Self> {
        let name = CString::new(name).unwrap();
        Some(Self {
            name,
            _column: PhantomData,
        })
    }
}

impl<C: cf::Column> compaction_filter_factory::CompactionFilterFactory
    for ExpiredDataCompactionFilterFactory<C>
{
    type Filter = ExpiredDataCompactionFilter<C>;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        let name = CString::new(format!("key_value_compaction_filter({})", C::NAME)).unwrap();
        ExpiredDataCompactionFilter::<C>::new(name)
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

/// Noop compaction filter, does nothing, used to define filters when not
/// filtering is required.
pub struct NoopCompactionFilter {
    name: CString,
}

impl CompactionFilter for NoopCompactionFilter {
    fn filter(&mut self, _level: u32, _key: &[u8], _value: &[u8]) -> Decision {
        Decision::Keep
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

/// Compaction factory for noop compaction filter.
pub struct NoopCompactionFilterFactory {
    name: CString,
}

impl<C: cf::Column> CompactionFilterFactory<C> for NoopCompactionFilterFactory {
    fn new(_name: &str) -> Option<Self> {
        None
    }
}

impl compaction_filter_factory::CompactionFilterFactory for NoopCompactionFilterFactory {
    type Filter = NoopCompactionFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        let name = CString::new("noop").unwrap();
        NoopCompactionFilter { name }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}
