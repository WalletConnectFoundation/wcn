use {
    crate::quic,
    governor::DefaultDirectRateLimiter,
    lru::LruCache,
    quinn::Incoming,
    std::{
        net::IpAddr,
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc,
    },
    tokio::sync::{OwnedSemaphorePermit, Semaphore},
    wc::metrics::{self, enum_ordinalize::Ordinalize},
};

const LOCAL_LIMITERS_LRU_CAPACITY: usize = 500;

/// Connection filter.
///
/// Takes into account the global number of connections, number of connection
/// from the same IP address, and number of incoming connections per second.
pub struct Filter {
    global_semaphore: Arc<Semaphore>,
    local_limiters: LruCache<IpAddr, LocalLimiters>,
    max_connection_rate: NonZeroU32,
    max_connections_per_ip: u32,
}

impl Filter {
    pub fn new(cfg: &super::Config) -> Result<Self, quic::Error> {
        let max_connection_rate: NonZeroU32 = cfg
            .max_connection_rate_per_ip
            .try_into()
            .map_err(|_| quic::Error::InvalidConnectionRate)?;

        Ok(Self {
            global_semaphore: Arc::new(Semaphore::new(cfg.max_connections as usize)),
            // Safe unwrap as long as `LOCAL_LIMITERS_LRU_CAPACITY` is >0.
            local_limiters: LruCache::new(NonZeroUsize::new(LOCAL_LIMITERS_LRU_CAPACITY).unwrap()),
            max_connection_rate,
            max_connections_per_ip: cfg.max_connections_per_ip,
        })
    }

    pub fn try_acquire_permit(&mut self, incoming: &Incoming) -> Result<Permit, RejectionReason> {
        if !incoming.remote_address_validated() {
            return Err(RejectionReason::AddressNotValidated);
        }

        let remote_addr = incoming.remote_address().ip();

        let limiters = self.local_limiters.get_or_insert(remote_addr, || {
            LocalLimiters::new(self.max_connections_per_ip, self.max_connection_rate)
        });

        if limiters.rate_limiter.check().is_err() {
            return Err(RejectionReason::RateLimit);
        }

        let Ok(_local) = limiters.semaphore.clone().try_acquire_owned() else {
            return Err(RejectionReason::LocalSemaphore);
        };

        let Ok(_global) = self.global_semaphore.clone().try_acquire_owned() else {
            return Err(RejectionReason::GlobalSemaphore);
        };

        Ok(Permit { _global, _local })
    }
}

/// Per client IP address limiters. Consists of a semaphore and a GCRA-based
/// rate limiter.
struct LocalLimiters {
    semaphore: Arc<Semaphore>,
    rate_limiter: DefaultDirectRateLimiter,
}

impl LocalLimiters {
    fn new(max_connections: u32, max_rate: NonZeroU32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_connections as usize)),
            rate_limiter: governor::RateLimiter::direct(governor::Quota::per_second(max_rate)),
        }
    }
}

pub struct Permit {
    _global: OwnedSemaphorePermit,
    _local: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Ordinalize, PartialEq, Eq)]
pub enum RejectionReason {
    GlobalSemaphore,
    LocalSemaphore,
    RateLimit,
    AddressNotValidated,
}

impl metrics::Enum for RejectionReason {
    fn as_str(&self) -> &'static str {
        match self {
            Self::GlobalSemaphore => "global_semaphore",
            Self::LocalSemaphore => "local_semaphore",
            Self::RateLimit => "rate_limit",
            Self::AddressNotValidated => "address_not_validated",
        }
    }
}
