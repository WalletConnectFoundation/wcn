//! EVM implementation of [`SmartContract`](super::SmartContract).

use {
    super::{
        ClusterView,
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
        maintenance,
        migration,
        node_operator,
        settings,
        smart_contract,
        Event,
        Keyspace,
        Maintenance,
        Migration,
        Ownership,
        Settings,
    },
    alloy::{
        contract::{CallBuilder, CallDecoder},
        network::EthereumWallet,
        primitives::U256,
        providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
        rpc::types::{Filter, Log},
        sol_types::{SolCall, SolEventInterface, SolInterface},
    },
    futures::{Stream, StreamExt},
    std::{collections::HashSet, fmt, time::Duration},
};

mod bindings {
    alloy::sol!(
        #[sol(rpc)]
        #[derive(Debug)]
        Cluster,
        "../../contracts/out/Cluster.sol/Cluster.json",
    );

    alloy::sol!(
        #[sol(rpc)]
        #[derive(Debug)]
        ERC1967Proxy,
        "../../contracts/out/ERC1967Proxy.sol/ERC1967Proxy.json",
    );
}

pub(crate) type Address = alloy::primitives::Address;

type AlloyContract = bindings::Cluster::ClusterInstance<DynProvider, alloy::network::Ethereum>;

/// RPC provider for EVM chains.
pub struct RpcProvider {
    signer: Option<Signer>,
    alloy: DynProvider,
}

impl RpcProvider {
    /// Creates a new [`RpcProvider`].
    pub async fn new(url: RpcUrl, signer: Signer) -> Result<Self, RpcProviderCreationError> {
        let wallet: EthereumWallet = match &signer.kind {
            SignerKind::PrivateKey(key) => key.clone().into(),
        };

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_ws(WsConnect::new(url.0))
            .await?;

        Ok(RpcProvider {
            signer: Some(signer),
            alloy: DynProvider::new(provider),
        })
    }

    /// Creates a new read-only [`RpcProvider`].
    pub async fn new_ro(url: RpcUrl) -> Result<RpcProvider, RpcProviderCreationError> {
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url.0))
            .await?;

        Ok(RpcProvider {
            signer: None,
            alloy: DynProvider::new(provider),
        })
    }
}

/// EVM implementation of WCN Cluster smart contract.
#[derive(Clone)]
pub struct SmartContract {
    signer: Option<Signer>,
    alloy: AlloyContract,
}

impl Deployer<SmartContract> for RpcProvider {
    async fn deploy(
        &self,
        initial_settings: Settings,
        initial_operators: Vec<node_operator::Serialized>,
    ) -> Result<SmartContract, DeploymentError> {
        let settings: bindings::Cluster::Settings = initial_settings.into();
        let operators: Vec<bindings::Cluster::NodeOperator> =
            initial_operators.into_iter().map(Into::into).collect();

        let contract = bindings::Cluster::deploy(self.alloy.clone()).await?;

        let init_call = bindings::Cluster::initializeCall::new((settings, operators)).abi_encode();

        let proxy = bindings::ERC1967Proxy::deploy(
            self.alloy.clone(),
            *contract.address(),
            init_call.into(),
        )
        .await?;

        Ok(SmartContract {
            signer: self.signer.clone(),
            alloy: bindings::Cluster::new(*proxy.address(), self.alloy.clone()),
        })
    }
}

impl Connector<SmartContract> for RpcProvider {
    async fn connect(
        &self,
        address: smart_contract::Address,
    ) -> Result<SmartContract, ConnectionError> {
        let signer = self.signer.clone();

        let code = self
            .alloy
            .get_code_at(address.0)
            .await
            .map_err(|err| ConnectionError::Other(err.to_string()))?;

        if code.is_empty() {
            return Err(ConnectionError::UnknownContract);
        }

        // TODO: figure out how to check this with Proxy contract
        // if code != bindings::Cluster::BYTECODE {
        //     return Err(ConnectionError::WrongContract);
        // }

        Ok(SmartContract {
            signer,
            alloy: bindings::Cluster::new(address.0, self.alloy.clone()),
        })
    }
}

impl smart_contract::Write for SmartContract {
    fn signer(&self) -> Option<&Signer> {
        self.signer.as_ref()
    }

    async fn start_migration(&self, new_keyspace: Keyspace) -> WriteResult<()> {
        let new_keyspace: bindings::Cluster::Keyspace = new_keyspace.into();
        check_receipt(self.alloy.startMigration(new_keyspace)).await
    }

    async fn complete_migration(&self, id: migration::Id) -> WriteResult<()> {
        check_receipt(self.alloy.completeMigration(id)).await
    }

    async fn abort_migration(&self, id: migration::Id) -> WriteResult<()> {
        check_receipt(self.alloy.abortMigration(id)).await
    }

    async fn start_maintenance(&self) -> WriteResult<()> {
        check_receipt(self.alloy.setMaintenance(true)).await
    }

    async fn finish_maintenance(&self) -> WriteResult<()> {
        check_receipt(self.alloy.setMaintenance(false)).await
    }

    async fn add_node_operator(&self, operator: node_operator::Serialized) -> WriteResult<()> {
        check_receipt(self.alloy.addNodeOperator(operator.into())).await
    }

    async fn update_node_operator(&self, operator: node_operator::Serialized) -> WriteResult<()> {
        let operator: bindings::Cluster::NodeOperator = operator.into();
        check_receipt(
            self.alloy
                .updateNodeOperatorData(operator.addr, operator.data),
        )
        .await
    }

    async fn remove_node_operator(&self, id: node_operator::Id) -> WriteResult<()> {
        check_receipt(self.alloy.removeNodeOperator(id.0)).await
    }

    async fn update_settings(&self, new_settings: Settings) -> WriteResult<()> {
        check_receipt(self.alloy.updateSettings(new_settings.into())).await
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

impl smart_contract::Read for SmartContract {
    fn address(&self) -> ReadResult<smart_contract::Address> {
        Ok(smart_contract::Address(*self.alloy.address()))
    }

    async fn cluster_view(&self) -> ReadResult<ClusterView> {
        self.alloy.getView().call().await?.try_into()
    }

    async fn events(
        &self,
    ) -> ReadResult<impl Stream<Item = ReadResult<Event>> + Send + 'static + use<>> {
        let filter = Filter::new().address(*self.alloy.address());

        Ok(self
            .alloy
            .provider()
            .subscribe_logs(&filter)
            .await?
            .into_stream()
            .filter_map(|log| async move { Event::try_from(log).transpose() }))
    }
}

impl Event {
    fn try_from(log: Log) -> ReadResult<Option<Event>> {
        use bindings::Cluster::ClusterEvents as Event;

        let evt =
            Event::decode_log(&log.inner).map_err(|err| ReadError::InvalidData(err.to_string()))?;

        Ok(Some(match evt.data {
            Event::MaintenanceToggled(evt) => evt.into(),
            Event::MigrationAborted(evt) => evt.into(),
            Event::MigrationCompleted(evt) => evt.into(),
            Event::MigrationDataPullCompleted(evt) => evt.into(),
            Event::MigrationStarted(evt) => evt.try_into()?,
            Event::NodeOperatorAdded(evt) => evt.into(),
            Event::NodeOperatorRemoved(evt) => evt.into(),
            Event::NodeOperatorUpdated(evt) => evt.into(),
            Event::SettingsUpdated(evt) => evt.into(),
            Event::ClusterInitialized(_)
            | Event::Initialized(_)
            | Event::OwnershipTransferStarted(_)
            | Event::OwnershipTransferred(_)
            | Event::Upgraded(_) => return Ok(None),
        }))
    }
}

impl TryFrom<bindings::Cluster::MigrationStarted> for Event {
    type Error = ReadError;

    fn try_from(evt: bindings::Cluster::MigrationStarted) -> ReadResult<Self> {
        Ok(Self::MigrationStarted(migration::Started {
            migration_id: evt.id,
            new_keyspace: Keyspace::try_from_alloy(&evt.newKeyspace, evt.keyspaceVersion)?,
            cluster_version: evt.version,
        }))
    }
}

impl From<bindings::Cluster::MigrationDataPullCompleted> for Event {
    fn from(evt: bindings::Cluster::MigrationDataPullCompleted) -> Self {
        Self::MigrationDataPullCompleted(migration::DataPullCompleted {
            migration_id: evt.id,
            operator_id: evt.operator.into(),
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::MigrationCompleted> for Event {
    fn from(evt: bindings::Cluster::MigrationCompleted) -> Self {
        Self::MigrationCompleted(migration::Completed {
            migration_id: evt.id,
            operator_id: evt.operator.into(),
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::MigrationAborted> for Event {
    fn from(evt: bindings::Cluster::MigrationAborted) -> Self {
        Self::MigrationAborted(migration::Aborted {
            migration_id: evt.id,
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::MaintenanceToggled> for Event {
    fn from(evt: bindings::Cluster::MaintenanceToggled) -> Self {
        if evt.active {
            Self::MaintenanceStarted(maintenance::Started {
                by: evt.operator.into(),
                cluster_version: evt.version,
            })
        } else {
            Self::MaintenanceFinished(maintenance::Finished {
                cluster_version: evt.version,
            })
        }
    }
}

impl From<bindings::Cluster::NodeOperatorAdded> for Event {
    fn from(evt: bindings::Cluster::NodeOperatorAdded) -> Self {
        let operator = bindings::Cluster::NodeOperator {
            addr: evt.operator,
            data: evt.operatorData,
        };

        Self::NodeOperatorAdded(node_operator::Added {
            idx: evt.slot,
            operator: operator.into(),
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::NodeOperatorUpdated> for Event {
    fn from(evt: bindings::Cluster::NodeOperatorUpdated) -> Self {
        let operator = bindings::Cluster::NodeOperator {
            addr: evt.operator,
            data: evt.operatorData,
        };

        Self::NodeOperatorUpdated(node_operator::Updated {
            operator: operator.into(),
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::NodeOperatorRemoved> for Event {
    fn from(evt: bindings::Cluster::NodeOperatorRemoved) -> Self {
        Self::NodeOperatorRemoved(node_operator::Removed {
            id: evt.operator.into(),
            cluster_version: evt.version,
        })
    }
}

impl From<bindings::Cluster::SettingsUpdated> for Event {
    fn from(evt: bindings::Cluster::SettingsUpdated) -> Self {
        Self::SettingsUpdated(settings::Updated {
            settings: evt.newSettings.into(),
            cluster_version: evt.version,
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
        node_operator::Serialized {
            id: smart_contract::AccountAddress(op.addr),
            data: node_operator::Data(op.data.into()),
        }
    }
}

impl From<Settings> for bindings::Cluster::Settings {
    fn from(settings: Settings) -> Self {
        Self {
            maxOperatorDataBytes: settings.max_node_operator_data_bytes,
            minOperators: const { crate::MIN_OPERATORS },
            extra: alloy::primitives::Bytes::default(),
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
        let mut operator_bitmask = U256::default();
        for idx in keyspace.operators() {
            operator_bitmask.set_bit(idx as usize, true);
        }

        Self {
            operatorBitmask: operator_bitmask,
            replicationStrategy: keyspace.replication_strategy() as u8,
        }
    }
}

impl Keyspace {
    fn try_from_alloy(
        keyspace: &bindings::Cluster::Keyspace,
        version: u64,
    ) -> Result<Self, ReadError> {
        let operators = bitmask_to_hashset(keyspace.operatorBitmask);

        let replication_strategy = keyspace.replicationStrategy.try_into().map_err(|err| {
            ReadError::InvalidData(format!("Invalid ReplicationStrategy: {err:?}"))
        })?;

        Self::new(operators, replication_strategy, version)
            .map_err(|err| ReadError::InvalidData(format!("Invalid Keyspace: {err:?}")))
    }
}

impl TryFrom<bindings::Cluster::ClusterView> for ClusterView {
    type Error = ReadError;

    fn try_from(view: bindings::Cluster::ClusterView) -> Result<Self, Self::Error> {
        let node_operators = view
            .operatorSlots
            .into_iter()
            .map(|slot| {
                if slot.addr.is_zero() {
                    return None;
                }

                let operator = bindings::Cluster::NodeOperator {
                    addr: slot.addr,
                    data: slot.data,
                };

                Some(operator.into())
            })
            .collect();

        // assert array length
        let _: &[_; 2] = &view.keyspaces;

        let try_keyspace =
            |version| Keyspace::try_from_alloy(&view.keyspaces[version as usize % 2], version);

        let (keyspace, migration) = if view.migration.pullingOperatorBitmask.is_zero() {
            // migration is not in progress

            (try_keyspace(view.keyspaceVersion)?, None)
        } else {
            // migration is in progress

            let prev_keyspace_version = view
                .keyspaceVersion
                .checked_sub(1)
                .ok_or_else(|| ReadError::InvalidData("Invalid keyspace::Version".into()))?;

            let keyspace = try_keyspace(prev_keyspace_version)?;

            let migration = Migration::new(
                view.migration.id,
                try_keyspace(view.keyspaceVersion)?,
                bitmask_to_hashset(view.migration.pullingOperatorBitmask),
            );

            (keyspace, Some(migration))
        };

        let maintenance = if view.maintenanceSlot.is_zero() {
            None
        } else {
            Some(Maintenance::new(view.maintenanceSlot.into()))
        };

        Ok(Self {
            node_operators,
            ownership: Ownership::new(view.owner.into()),
            settings: view.settings.into(),
            keyspace,
            migration,
            maintenance,
            cluster_version: view.version,
        })
    }
}

impl From<alloy::contract::Error> for DeploymentError {
    fn from(err: alloy::contract::Error) -> Self {
        Self(format!("{err:?}"))
    }
}

impl<E: fmt::Debug> From<alloy::transports::RpcError<E>> for ReadError {
    fn from(err: alloy::transports::RpcError<E>) -> Self {
        ReadError::Transport(format!("{err:?}"))
    }
}

impl From<alloy::providers::PendingTransactionError> for WriteError {
    fn from(err: alloy::providers::PendingTransactionError) -> Self {
        if let alloy::providers::PendingTransactionError::TransportError(err) = &err {
            if let Some(err) = try_decode_error(err) {
                return WriteError::Revert(format!("{err:?}"));
            }
        }

        Self::Other(format!("{err:?}"))
    }
}

impl From<alloy::contract::Error> for WriteError {
    fn from(err: alloy::contract::Error) -> Self {
        match err {
            alloy::contract::Error::TransportError(err) => {
                if let Some(err) = try_decode_error(&err) {
                    Self::Revert(format!("{err:?}"))
                } else {
                    Self::Transport(format!("{err:?}"))
                }
            }
            _ => Self::Other(format!("{err:?}")),
        }
    }
}

fn try_decode_error(
    err: &alloy::transports::TransportError,
) -> Option<bindings::Cluster::ClusterErrors> {
    let data = match err {
        alloy::transports::RpcError::ErrorResp(resp) => resp.data.as_ref()?,
        _ => return None,
    };

    let data: String = serde_json::from_str(data.get()).ok()?;
    let data = data.strip_prefix("0x")?;
    let bytes = const_hex::decode(data).ok()?;

    bindings::Cluster::ClusterErrors::abi_decode_validate(&bytes).ok()
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

fn bitmask_to_hashset(bitmask: U256) -> HashSet<u8> {
    let mut set = HashSet::new();
    for idx in 0..=u8::MAX {
        if bitmask.bit(idx as usize) {
            set.insert(idx);
        }
    }
    set
}
