//! EVM implementation of [`SmartContract`](super::SmartContract).

use {
    super::{AccountAddress, ConnectError, Error, Result, RpcUrl, Signer, SignerInner},
    crate::{
        self as cluster,
        migration,
        node_operator,
        smart_contract,
        Keyspace,
        Maintenance,
        Migration,
        NodeOperator,
        NodeOperators,
        Ownership,
        Settings,
    },
    alloy::{
        contract::{CallBuilder, CallDecoder},
        network::EthereumWallet,
        primitives::Uint,
        providers::{DynProvider, Provider, ProviderBuilder},
    },
    std::{collections::HashSet, time::Duration},
};

mod bindings {
    alloy::sol!(
        #[sol(rpc)]
        #[derive(Debug)]
        Cluster,
        "../../contracts/out/Cluster.sol/Cluster.json",
    );
}

type AlloyContract = bindings::Cluster::ClusterInstance<DynProvider, alloy::network::Ethereum>;

type U256 = Uint<256, 4>;

/// EVM implementation of [`SmartContract`](super::SmartContract).
#[derive(Clone)]
pub struct SmartContract<S = AccountAddress> {
    signer_addr: S,
    alloy: AlloyContract,
}

/// EVM implementation of
/// [`ReadOnlySmartContract`](super::ReadOnlySmartContract).
pub type SmartContractRO = SmartContract<()>;

pub(crate) type Address = alloy::primitives::Address;

impl super::SmartContract for SmartContract {
    type ReadOnly = SmartContractRO;

    async fn deploy(
        signer: Signer,
        rpc_url: RpcUrl,
        initial_settings: Settings,
        initial_operators: NodeOperators<node_operator::SerializedData>,
    ) -> Result<Self> {
        let signer_addr = signer.address();

        let settings = initial_settings.into();
        let operators = initial_operators
            .into_slots()
            .into_iter()
            .filter_map(|op| op.map(Into::into))
            .collect();

        bindings::Cluster::deploy(new_provier(signer, rpc_url), settings, operators)
            .await
            .map(|alloy| Self { signer_addr, alloy })
            .map_err(Into::into)
    }

    async fn connect(
        address: smart_contract::Address,
        signer: Signer,
        rpc_url: RpcUrl,
    ) -> Result<Self, ConnectError> {
        let signer_addr = signer.address();

        connect(address, new_provier(signer, rpc_url))
            .await
            .map(|alloy| Self { signer_addr, alloy })
    }

    async fn connect_ro(
        address: smart_contract::Address,
        rpc_url: RpcUrl,
    ) -> Result<Self::ReadOnly, ConnectError> {
        let signer_addr = ();

        connect(address, new_ro_provier(rpc_url))
            .await
            .map(|alloy| Self::ReadOnly { signer_addr, alloy })
    }

    fn signer(&self) -> &AccountAddress {
        &self.signer_addr
    }

    async fn start_migration(&self, new_keyspace: Keyspace) -> Result<()> {
        check_receipt(self.alloy.startMigration(new_keyspace.into())).await
    }

    async fn complete_migration(&self, id: migration::Id) -> Result<()> {
        check_receipt(self.alloy.completeMigration(id)).await
    }

    async fn abort_migration(&self, id: migration::Id) -> Result<()> {
        check_receipt(self.alloy.abortMigration(id)).await
    }

    async fn start_maintenance(&self) -> Result<()> {
        check_receipt(self.alloy.startMaintenance()).await
    }

    async fn finish_maintenance(&self) -> Result<()> {
        check_receipt(self.alloy.finishMaintenance()).await
    }

    async fn add_node_operator(
        &self,
        idx: node_operator::Idx,
        operator: NodeOperator<node_operator::SerializedData>,
    ) -> Result<()> {
        check_receipt(self.alloy.addNodeOperator(idx, operator.into())).await
    }

    async fn update_node_operator(&self, operator: node_operator::Serialized) -> super::Result<()> {
        check_receipt(self.alloy.updateNodeOperator(operator.into())).await
    }

    async fn remove_node_operator(&self, id: node_operator::Id) -> Result<()> {
        check_receipt(self.alloy.removeNodeOperator(id.0)).await
    }

    async fn update_settings(&self, new_settings: Settings) -> Result<()> {
        check_receipt(self.alloy.updateSettings(new_settings.into())).await
    }

    async fn transfer_ownership(&self, new_owner: AccountAddress) -> Result<()> {
        check_receipt(self.alloy.transferOwnership(new_owner.0)).await
    }
}

async fn check_receipt<D>(call: CallBuilder<&DynProvider, D>) -> Result<(), Error>
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
        return Err(Error::Revert(format!("{}", receipt.transaction_hash)));
    }

    Ok(())
}

impl<S: Send + Sync + 'static> super::ReadOnlySmartContract for SmartContract<S> {
    async fn cluster_view(&self) -> Result<cluster::View<(), node_operator::SerializedData>> {
        self.alloy.getView().call().await?.try_into()
    }
}

async fn connect(
    address: smart_contract::Address,
    provider: DynProvider,
) -> Result<AlloyContract, ConnectError> {
    let code = provider
        .get_code_at(address.0)
        .await
        .map_err(|err| ConnectError::Other(err.to_string()))?;

    if code.is_empty() {
        return Err(ConnectError::UnknownContract);
    }

    if code != bindings::Cluster::BYTECODE {
        return Err(ConnectError::WrongContract);
    }

    Ok(bindings::Cluster::new(address.0, provider))
}

fn new_provier(signer: Signer, rpc_url: RpcUrl) -> DynProvider {
    let wallet: EthereumWallet = match signer.inner {
        SignerInner::PrivateKey(key) => key.into(),
    };

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        // .with_chain_id(10)
        .connect_http(rpc_url.0);

    DynProvider::new(provider)
}

fn new_ro_provier(rpc_url: RpcUrl) -> DynProvider {
    let provider = ProviderBuilder::new().connect_http(rpc_url.0);
    DynProvider::new(provider)
}

impl From<alloy::contract::Error> for Error {
    fn from(err: alloy::contract::Error) -> Self {
        Self::Other(format!("{err:?}"))
    }
}

impl From<alloy::providers::PendingTransactionError> for Error {
    fn from(err: alloy::providers::PendingTransactionError) -> Self {
        Self::Other(format!("{err:?}"))
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
    fn try_from_alloy(keyspace: &bindings::Cluster::Keyspace, version: u64) -> Result<Self, Error> {
        let mut operators = HashSet::new();

        // TODO: do less iterations
        for idx in 0..256 {
            if keyspace.operatorsBitmask.bit(idx) {
                operators.insert(idx as u8);
            }
        }

        let replication_strategy = keyspace
            .replicationStrategy
            .try_into()
            .map_err(|err| Error::Other(format!("Invalid ReplicationStrategy: {err:?}")))?;

        Self::new(operators, replication_strategy, version)
            .map_err(|err| Error::Other(format!("Invalid Keyspace: {err:?}")))
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
    type Error = Error;

    fn try_from(view: bindings::Cluster::ClusterView) -> Result<Self, Self::Error> {
        let node_operators =
            NodeOperators::new(view.nodeOperatorSlots.into_iter().map(|operator| {
                if operator.addr.is_zero() {
                    None
                } else {
                    Some(operator.into())
                }
            }))
            .map_err(|err| Error::Other(format!("Invalid NodeOperators: {err:?}")))?;

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
                .ok_or_else(|| Error::Other(format!("Invalid keyspace::Version")))?;

            let keyspace = try_keyspace(prev_keyspace_version)?;
            let migration_keyspace = try_keyspace(view.keyspaceVersion)?;
            let migration = Migration::from_alloy(view.migration, migration_keyspace);

            (keyspace, Some(migration))
        };

        Ok(Self {
            node_operators,
            ownership: Ownership::new(view.owner.into()),
            settings: view.settings.into(),
            keyspace,
            migration,
            maintenance: view.maintenance.into(),
            cluster_version: view.version,
        })
    }
}
