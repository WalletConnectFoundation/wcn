use {
    derive_where::derive_where,
    futures::Stream,
    std::{ops::RangeInclusive, sync::Arc},
    wcn_cluster::{Cluster, PeerId},
    wcn_storage_api::{
        self as storage_api,
        operation,
        DataItem,
        Error,
        Operation,
        Result,
        StorageApi,
    },
};

#[cfg(test)]
pub mod test;

/// [`Replica`] config.
pub trait Config: wcn_cluster::Config {
    /// Type of the outbound connection to the WCN Database.
    type OutboundDatabaseConnection: StorageApi + Clone;
}

#[derive_where(Clone)]
pub struct Replica<C: Config> {
    _config: Arc<C>,
    cluster: Cluster<C>,
    database: C::OutboundDatabaseConnection,
}

impl<C: Config> Replica<C> {
    /// Creates a new [`Replica`].
    pub fn new(
        config: Arc<C>,
        cluster: Cluster<C>,
        database: C::OutboundDatabaseConnection,
    ) -> Self {
        Self {
            _config: config,
            cluster,
            database,
        }
    }

    /// Establishes a new [`InboundConnection`].
    ///
    /// Returns `None` if the peer is not authorized to use this [`Replica`].
    pub fn new_inbound_connection(&self, peer_id: PeerId) -> Option<InboundConnection<C>> {
        if !self.cluster.contains_node(&peer_id) {
            return None;
        }

        Some(InboundConnection {
            _peer_id: peer_id,
            replica: self.clone(),
        })
    }
}

/// Inbound connection to the local [`Replica`] from a remote peer.
#[derive_where(Clone)]
pub struct InboundConnection<C: Config> {
    _peer_id: PeerId,
    replica: Replica<C>,
}

impl<C: Config> storage_api::Factory<PeerId> for Replica<C> {
    type StorageApi = InboundConnection<C>;

    fn new_storage_api(&self, peer_id: PeerId) -> storage_api::Result<Self::StorageApi> {
        self.new_inbound_connection(peer_id)
            .ok_or_else(storage_api::Error::unauthorized)
    }
}

impl<C: Config> StorageApi for InboundConnection<C> {
    async fn execute_ref(&self, operation: &Operation<'_>) -> Result<operation::Output> {
        // TODO: once we add signatures to write operations check them here

        self.replica.cluster.using_view(|view| {
            operation
                .keyspace_version()
                .filter(|&version| view.validate_keyspace_version(version))
                .ok_or_else(Error::keyspace_version_mismatch)
        })?;

        self.replica
            .database
            .execute_ref(operation)
            .await
            .map_err(|_| Error::new(storage_api::ErrorKind::Internal))
    }

    async fn read_data(
        &self,
        keyrange: RangeInclusive<u64>,
        keyspace_version: u64,
    ) -> storage_api::Result<impl Stream<Item = storage_api::Result<DataItem>> + Send> {
        self.replica.cluster.using_view(|view| {
            if !view.validate_keyspace_version(keyspace_version) {
                return Err(Error::keyspace_version_mismatch());
            }

            Ok(())
        })?;

        self.replica
            .database
            .read_data(keyrange, keyspace_version)
            .await
    }
}
