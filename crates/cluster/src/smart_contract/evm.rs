//! EVM implementation of [`SmartContract`](super::SmartContract).

use {
    super::{
        AccountAddress,
        ConnectionError,
        Connector,
        Deployer,
        DeploymentError,
        ReadError,
        ReadResult,
        RpcUrl,
        Signer,
        SignerKind,
        WriteError,
        WriteResult,
    },
    crate::{
        self as cluster,
        maintenance,
        migration,
        node_operator,
        ownership,
        settings,
        smart_contract,
        Event,
        Keyspace,
        Maintenance,
        Migration,
        NodeOperator,
        NodeOperators,
        Ownership,
        SerializedEvent,
        Settings,
    },
    alloy::{
        contract::{CallBuilder, CallDecoder},
        network::EthereumWallet,
        primitives::Uint,
        providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
        rpc::types::{Filter, Log},
        sol_types::SolEvent,
    },
    futures::{Stream, StreamExt as _},
    std::{collections::HashSet, fmt, sync::Arc, time::Duration},
};

mod bindings {
    alloy::sol!(
        #[sol(rpc)]
        #[derive(Debug)]
        Cluster,
        "../../contracts/out/Cluster.sol/Cluster.json",
    );
}

pub(crate) type Address = alloy::primitives::Address;

type AlloyContract = bindings::Cluster::ClusterInstance<DynProvider, alloy::network::Ethereum>;

type U256 = Uint<256, 4>;

/// RPC provider for EVM chains.
pub struct RpcProvider<S = ()> {
    signer: S,
    alloy: DynProvider,
}

impl RpcProvider {
    /// Creates a new [`RpcProvider`].
    pub async fn new(
        url: RpcUrl,
        signer: Signer,
    ) -> Result<RpcProvider<Signer>, RpcProviderCreationError> {
        let wallet: EthereumWallet = match &signer.kind {
            SignerKind::PrivateKey(key) => key.clone().into(),
        };

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_ws(WsConnect::new(url.0))
            .await?;

        Ok(RpcProvider {
            signer,
            alloy: DynProvider::new(provider),
        })
    }

    /// Creates a new read-only [`RpcProvider`].
    pub async fn new_ro(self, url: RpcUrl) -> Result<RpcProvider<()>, RpcProviderCreationError> {
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url.0))
            .await?;

        Ok(RpcProvider {
            signer: (),
            alloy: DynProvider::new(provider),
        })
    }
}

/// EVM implementation of WCN Cluster smart contract.
#[derive(Clone)]
pub struct SmartContract<S = ()> {
    signer: S,
    alloy: AlloyContract,
}

impl Deployer<SmartContract<Signer>> for RpcProvider<Signer> {
    async fn deploy(
        &self,
        initial_settings: Settings,
        initial_operators: NodeOperators<node_operator::SerializedData>,
    ) -> Result<SmartContract<Signer>, DeploymentError> {
        let signer = self.signer.clone();

        let settings = initial_settings.into();
        let operators = initial_operators
            .into_slots()
            .into_iter()
            .filter_map(|op| op.map(Into::into))
            .collect();

        bindings::Cluster::deploy(self.alloy.clone(), settings, operators)
            .await
            .map(|alloy| SmartContract { signer, alloy })
            .map_err(Into::into)
    }
}

impl<S: Clone> Connector<SmartContract<S>> for RpcProvider<S> {
    async fn connect(
        &self,
        address: smart_contract::Address,
    ) -> Result<SmartContract<S>, ConnectionError> {
        let signer = self.signer.clone();

        let code = self
            .alloy
            .get_code_at(address.0)
            .await
            .map_err(|err| ConnectionError::Other(err.to_string()))?;

        if code.is_empty() {
            return Err(ConnectionError::UnknownContract);
        }

        // TODO: false positive
        // if code != bindings::Cluster::BYTECODE {
        //     return Err(ConnectionError::WrongContract);
        // }

        Ok(SmartContract {
            signer,
            alloy: bindings::Cluster::new(address.0, self.alloy.clone()),
        })
    }
}

impl smart_contract::Write for SmartContract<Signer> {
    fn signer(&self) -> &Signer {
        &self.signer
    }

    async fn start_migration(&self, new_keyspace: Keyspace) -> WriteResult<()> {
        check_receipt(self.alloy.startMigration(new_keyspace.into())).await
    }

    async fn complete_migration(&self, id: migration::Id) -> WriteResult<()> {
        check_receipt(self.alloy.completeMigration(id)).await
    }

    async fn abort_migration(&self, id: migration::Id) -> WriteResult<()> {
        check_receipt(self.alloy.abortMigration(id)).await
    }

    async fn start_maintenance(&self) -> WriteResult<()> {
        check_receipt(self.alloy.startMaintenance()).await
    }

    async fn finish_maintenance(&self) -> WriteResult<()> {
        check_receipt(self.alloy.finishMaintenance()).await
    }

    async fn add_node_operator(
        &self,
        idx: node_operator::Idx,
        operator: NodeOperator<node_operator::SerializedData>,
    ) -> WriteResult<()> {
        check_receipt(self.alloy.addNodeOperator(idx, operator.into())).await
    }

    async fn update_node_operator(&self, operator: node_operator::Serialized) -> WriteResult<()> {
        check_receipt(self.alloy.updateNodeOperator(operator.into())).await
    }

    async fn remove_node_operator(&self, id: node_operator::Id) -> WriteResult<()> {
        check_receipt(self.alloy.removeNodeOperator(id.0)).await
    }

    async fn update_settings(&self, new_settings: Settings) -> WriteResult<()> {
        check_receipt(self.alloy.updateSettings(new_settings.into())).await
    }

    async fn transfer_ownership(&self, new_owner: AccountAddress) -> WriteResult<()> {
        check_receipt(self.alloy.transferOwnership(new_owner.0)).await
    }
}

async fn check_receipt<D>(call: CallBuilder<&DynProvider, D>) -> WriteResult<()>
where
    D: CallDecoder,
{
    let receipt = call
        .send()
        .await?
        .with_timeout(Some(Duration::from_secs(30)))
        .get_receipt()
        .await?;

    if !receipt.status() {
        return Err(WriteError::Revert(format!("{}", receipt.transaction_hash)));
    }

    Ok(())
}

impl<S: Send + Sync + 'static> smart_contract::Read for SmartContract<S> {
    fn address(&self) -> smart_contract::Address {
        smart_contract::Address(*self.alloy.address())
    }

    async fn cluster_view(&self) -> ReadResult<cluster::View<(), node_operator::SerializedData>> {
        self.alloy.getView().call().await?.try_into()
    }

    async fn events(
        &self,
    ) -> ReadResult<impl Stream<Item = ReadResult<SerializedEvent>> + Send + 'static> {
        let filter = Filter::new().address(*self.alloy.address());

        Ok(self
            .alloy
            .provider()
            .subscribe_logs(&filter)
            .await?
            .into_stream()
            .map(TryFrom::try_from))
    }
}

impl TryFrom<Log> for Event<node_operator::SerializedData> {
    type Error = ReadError;

    fn try_from(log: Log) -> Result<Self, Self::Error> {
        use bindings::Cluster;

        let topics = log.topics();
        let data = &log.data().data;

        Ok(match log.topics().first() {
            Some(&Cluster::MigrationStarted::SIGNATURE_HASH) => {
                Cluster::MigrationStarted::decode_raw_log_validate(topics, data)?.try_into()?
            }
            Some(&Cluster::MigrationDataPullCompleted::SIGNATURE_HASH) => {
                Cluster::MigrationDataPullCompleted::decode_raw_log_validate(topics, data)?.into()
            }
            Some(&Cluster::MigrationCompleted::SIGNATURE_HASH) => {
                Cluster::MigrationCompleted::decode_raw_log_validate(topics, data)?.into()
            }
            Some(&Cluster::MigrationAborted::SIGNATURE_HASH) => {
                Cluster::MigrationAborted::decode_raw_log_validate(topics, data)?.into()
            }

            Some(&Cluster::MaintenanceStarted::SIGNATURE_HASH) => {
                Cluster::MaintenanceStarted::decode_raw_log_validate(topics, data)?.into()
            }
            Some(&Cluster::MaintenanceFinished::SIGNATURE_HASH) => {
                Cluster::MaintenanceFinished::decode_raw_log_validate(topics, data)?.into()
            }

            Some(&Cluster::NodeOperatorAdded::SIGNATURE_HASH) => {
                Cluster::NodeOperatorAdded::decode_raw_log_validate(topics, data)?.into()
            }
            Some(&Cluster::NodeOperatorUpdated::SIGNATURE_HASH) => {
                Cluster::NodeOperatorUpdated::decode_raw_log_validate(topics, data)?.into()
            }
            Some(&Cluster::NodeOperatorRemoved::SIGNATURE_HASH) => {
                Cluster::NodeOperatorRemoved::decode_raw_log_validate(topics, data)?.into()
            }

            Some(&Cluster::SettingsUpdated::SIGNATURE_HASH) => {
                Cluster::SettingsUpdated::decode_raw_log_validate(topics, data)?.into()
            }

            Some(&Cluster::OwnershipTransferred::SIGNATURE_HASH) => {
                Cluster::OwnershipTransferred::decode_raw_log_validate(topics, data)?.into()
            }

            other => {
                return Err(ReadError::InvalidData(format!(
                    "Unexpected event: {other:?}"
                )))
            }
        })
    }
}

impl<D> TryFrom<bindings::Cluster::MigrationStarted> for Event<D> {
    type Error = ReadError;

    fn try_from(evt: bindings::Cluster::MigrationStarted) -> ReadResult<Self> {
        Ok(Self::MigrationStarted(migration::Started {
            migration_id: evt.id,
            new_keyspace: Keyspace::try_from_alloy(&evt.newKeyspace, evt.newKeyspaceVersion)?,
            cluster_version: evt.clusterVersion,
        }))
    }
}

impl<D> From<bindings::Cluster::MigrationDataPullCompleted> for Event<D> {
    fn from(evt: bindings::Cluster::MigrationDataPullCompleted) -> Self {
        Self::MigrationDataPullCompleted(migration::DataPullCompleted {
            migration_id: evt.id,
            operator_id: evt.operatorAddress.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::MigrationCompleted> for Event<D> {
    fn from(evt: bindings::Cluster::MigrationCompleted) -> Self {
        Self::MigrationCompleted(migration::Completed {
            migration_id: evt.id,
            operator_id: evt.operatorAddress.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::MigrationAborted> for Event<D> {
    fn from(evt: bindings::Cluster::MigrationAborted) -> Self {
        Self::MigrationAborted(migration::Aborted {
            migration_id: evt.id,
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::MaintenanceStarted> for Event<D> {
    fn from(evt: bindings::Cluster::MaintenanceStarted) -> Self {
        Self::MaintenanceStarted(maintenance::Started {
            operator_id: evt.addr.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::MaintenanceFinished> for Event<D> {
    fn from(evt: bindings::Cluster::MaintenanceFinished) -> Self {
        Self::MaintenanceFinished(maintenance::Finished {
            cluster_version: evt.clusterVersion,
        })
    }
}

impl From<bindings::Cluster::NodeOperatorAdded> for Event<node_operator::SerializedData> {
    fn from(evt: bindings::Cluster::NodeOperatorAdded) -> Self {
        Self::NodeOperatorAdded(node_operator::Added {
            idx: evt.idx,
            operator: evt.operator.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl From<bindings::Cluster::NodeOperatorUpdated> for Event<node_operator::SerializedData> {
    fn from(evt: bindings::Cluster::NodeOperatorUpdated) -> Self {
        Self::NodeOperatorUpdated(node_operator::Updated {
            operator: evt.operator.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::NodeOperatorRemoved> for Event<D> {
    fn from(evt: bindings::Cluster::NodeOperatorRemoved) -> Self {
        Self::NodeOperatorRemoved(node_operator::Removed {
            id: evt.operatorAddress.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::SettingsUpdated> for Event<D> {
    fn from(evt: bindings::Cluster::SettingsUpdated) -> Self {
        Self::SettingsUpdated(settings::Updated {
            settings: evt.newSettings.into(),
            cluster_version: evt.clusterVersion,
        })
    }
}

impl<D> From<bindings::Cluster::OwnershipTransferred> for Event<D> {
    fn from(evt: bindings::Cluster::OwnershipTransferred) -> Self {
        Self::OwnershipTransferred(ownership::Transferred {
            new_owner: evt.newOwner.into(),
            cluster_version: evt.cluserVersion,
        })
    }
}

impl From<node_operator::Serialized> for bindings::Cluster::NodeOperator {
    fn from(op: node_operator::Serialized) -> Self {
        Self {
            addr: op.id.0,
            data: op.data.0.into(),
        }
    }
}

impl From<bindings::Cluster::NodeOperator> for node_operator::Serialized {
    fn from(op: bindings::Cluster::NodeOperator) -> Self {
        NodeOperator {
            id: smart_contract::AccountAddress(op.addr),
            data: node_operator::SerializedData(op.data.into()),
        }
    }
}

impl From<Settings> for bindings::Cluster::Settings {
    fn from(settings: Settings) -> Self {
        Self {
            maxOperatorDataBytes: settings.max_node_operator_data_bytes,
        }
    }
}

impl From<bindings::Cluster::Settings> for Settings {
    fn from(settings: bindings::Cluster::Settings) -> Self {
        Self {
            max_node_operator_data_bytes: settings.maxOperatorDataBytes,
        }
    }
}

impl From<Keyspace> for bindings::Cluster::Keyspace {
    fn from(keyspace: Keyspace) -> Self {
        let mut operators_bitmask = U256::ZERO;

        for idx in keyspace.operators() {
            operators_bitmask.set_bit(idx as usize, true);
        }

        Self {
            operatorsBitmask: operators_bitmask,
            replicationStrategy: keyspace.replication_strategy() as u8,
        }
    }
}

impl Keyspace {
    fn try_from_alloy(
        keyspace: &bindings::Cluster::Keyspace,
        version: u64,
    ) -> Result<Self, ReadError> {
        let mut operators = HashSet::new();

        // TODO: do less iterations
        for idx in 0..256 {
            if keyspace.operatorsBitmask.bit(idx) {
                operators.insert(idx as u8);
            }
        }

        let replication_strategy = keyspace.replicationStrategy.try_into().map_err(|err| {
            ReadError::InvalidData(format!("Invalid ReplicationStrategy: {err:?}"))
        })?;

        Self::new(operators, replication_strategy, version)
            .map_err(|err| ReadError::InvalidData(format!("Invalid Keyspace: {err:?}")))
    }
}

impl Migration {
    fn from_alloy(migration: bindings::Cluster::Migration, keyspace: Keyspace) -> Self {
        let mut pulling_operators = HashSet::new();

        // TODO: do less iterations
        for idx in 0..256 {
            if migration.pullingOperatorsBitmask.bit(idx) {
                pulling_operators.insert(idx as u8);
            }
        }

        Self::new(migration.id, keyspace, pulling_operators)
    }
}

impl From<bindings::Cluster::Maintenance> for Option<Maintenance> {
    fn from(maintenance: bindings::Cluster::Maintenance) -> Self {
        if maintenance.slot.is_zero() {
            None
        } else {
            Some(Maintenance::new(maintenance.slot.into()))
        }
    }
}

impl TryFrom<bindings::Cluster::ClusterView> for cluster::View<(), node_operator::SerializedData> {
    type Error = ReadError;

    fn try_from(view: bindings::Cluster::ClusterView) -> Result<Self, Self::Error> {
        let node_operators =
            NodeOperators::new(view.nodeOperatorSlots.into_iter().map(|operator| {
                if operator.addr.is_zero() {
                    None
                } else {
                    Some(operator.into())
                }
            }))
            .map_err(|err| ReadError::InvalidData(format!("Invalid NodeOperators: {err:?}")))?;

        // assert array length
        let _: &[_; 2] = &view.keyspaces;

        let try_keyspace =
            |version| Keyspace::try_from_alloy(&view.keyspaces[version as usize % 2], version);

        let (keyspace, migration) = if view.migration.pullingOperatorsBitmask == U256::ZERO {
            // migration is not in progress

            (try_keyspace(view.keyspaceVersion)?, None)
        } else {
            // migration is in progress

            let prev_keyspace_version = view
                .keyspaceVersion
                .checked_sub(1)
                .ok_or_else(|| ReadError::InvalidData("Invalid keyspace::Version".into()))?;

            let keyspace = try_keyspace(prev_keyspace_version)?;
            let migration_keyspace = try_keyspace(view.keyspaceVersion)?;
            let migration = Migration::from_alloy(view.migration, migration_keyspace);

            (keyspace, Some(migration))
        };

        Ok(Self {
            node_operators,
            ownership: Ownership::new(view.owner.into()),
            settings: view.settings.into(),
            keyspace: Arc::new(keyspace),
            migration,
            maintenance: view.maintenance.into(),
            cluster_version: view.version,
        })
    }
}

impl From<alloy::contract::Error> for DeploymentError {
    fn from(err: alloy::contract::Error) -> Self {
        Self(format!("{err:?}"))
    }
}

impl<E: fmt::Debug> From<alloy::transports::RpcError<E>> for WriteError {
    fn from(err: alloy::transports::RpcError<E>) -> Self {
        WriteError::Transport(format!("{err:?}"))
    }
}

impl<E: fmt::Debug> From<alloy::transports::RpcError<E>> for ReadError {
    fn from(err: alloy::transports::RpcError<E>) -> Self {
        ReadError::Transport(format!("{err:?}"))
    }
}

impl From<alloy::providers::PendingTransactionError> for WriteError {
    fn from(err: alloy::providers::PendingTransactionError) -> Self {
        Self::Other(format!("{err:?}"))
    }
}

impl From<alloy::contract::Error> for WriteError {
    fn from(err: alloy::contract::Error) -> Self {
        match err {
            alloy::contract::Error::TransportError(err) => Self::Transport(format!("{err:?}")),
            _ => Self::Other(format!("{err:?}")),
        }
    }
}

impl From<alloy::contract::Error> for ReadError {
    fn from(err: alloy::contract::Error) -> Self {
        match err {
            alloy::contract::Error::TransportError(err) => Self::Transport(format!("{err:?}")),
            _ => Self::Other(format!("{err:?}")),
        }
    }
}

impl From<alloy::sol_types::Error> for ReadError {
    fn from(err: alloy::sol_types::Error) -> Self {
        ReadError::InvalidData(format!("{err:?}"))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{_0}")]
pub struct RpcProviderCreationError(String);

impl From<alloy::transports::TransportError> for RpcProviderCreationError {
    fn from(err: alloy::transports::TransportError) -> Self {
        Self(format!("alloy transport: {err:?}"))
    }
}
