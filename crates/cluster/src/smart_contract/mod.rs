//! Smart-contract managing the state of a WCN cluster.

pub mod evm;

use {
    crate::{self as cluster, migration, node_operator, Keyspace, NodeOperators, Settings},
    alloy::{network::TxSigner as _, signers::local::PrivateKeySigner, transports::http::reqwest},
    derive_more::derive::{Display, From},
    serde::{Deserialize, Serialize},
    std::str::FromStr,
};

/// Smart-contract managing the state of a WCN cluster.
///
/// Logic invariants documented on the methods of this trait MUST be
/// implemented inside the on-chain implementation of the smart-contract itself.
pub trait SmartContract: ReadOnlySmartContract {
    type ReadOnly: ReadOnlySmartContract;

    /// Deploys a new smart-contract.
    async fn deploy(
        signer: Signer,
        rpc_url: RpcUrl,
        initial_settings: Settings,
        initial_operators: NodeOperators<node_operator::SerializedData>,
    ) -> Result<Self>;

    /// Connects to an existing smart-contract.
    async fn connect(
        address: Address,
        signer: Signer,
        rpc_url: RpcUrl,
    ) -> Result<Self, ConnectError>;

    /// Connects to an existing smart-contract in read-only mode.
    async fn connect_ro(address: Address, rpc_url: RpcUrl) -> Result<Self::ReadOnly, ConnectError>;

    /// Returns the [`AccountAddress`] of the [`Signer`] which is currently
    /// being used for signing transactions of [`SmartContract`] instance.
    fn signer(&self) -> &AccountAddress;

    /// Starts a new data [`migration`] process using the provided
    /// [`migration::Plan`].
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's no ongoing data migration
    /// - there's no ongoing maintenance
    /// - [`signer`](SmartContract::signer) is the owner of the
    ///   [`SmartContract`]
    ///
    /// The implementation MUST emit [`migration::Started`] event on success.
    async fn start_migration(&self, new_keyspace: Keyspace) -> Result<()>;

    /// Marks that the [`signer`](SmartContract::signer) has completed the data
    /// pull required for completion of the current [`migration`].
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's an ongoing data migration
    /// - the provided [`migration::Id`] matches the ID of the migration
    /// - [`signer`](Smart::signer) is a [`node::Operator`] under the provided
    ///   [`node::OperatorIdx`]
    ///
    /// If this [`node::Operator`] is the last remaining one left to complete
    /// the data pull then the migration MUST be completed and
    /// [`migration::Completed`] event MUST be emitted.
    /// Otherwise the data pull MUST be marked as completed for the
    /// [`node::Operator`] and [`migration::DataPullCompleted`] MUST be emitted.
    ///
    /// The implementation MAY be idempotent. In case of an idempotent
    /// execution the event MUST not be emitted.
    async fn complete_migration(&self, id: migration::Id) -> Result<()>;

    /// Aborts the ongoing data [`migration`] process restoring the WCN cluster
    /// to the original state it had before the migration had started.
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's an ongoing data migration
    /// - [`signer`](SmartContract::signer) is the owner of the
    ///   [`SmartContract`]
    ///
    /// The implementation MUST emit [`migration::Aborted`] event on success.
    async fn abort_migration(&self, id: migration::Id) -> Result<()>;

    /// Starts a [`maintenance`] process for the [`node::Operator`] being the
    /// current [`signer`](Manager::signer).
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's no ongoing data migration
    /// - there's no ongoing maintenance
    /// - [`signer`](SmartContract::signer) is a [`node::Operator`] under the
    ///   provided [`node::OperatorIdx`]
    ///
    /// The implementation MUST emit [`maintenance::Started`] event on success.
    async fn start_maintenance(&self) -> Result<()>;

    /// Completes the ongoing [`maintenance`] process.
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's an ongoing maintenance
    /// - [`signer`](SmartContract::signer) is the one who
    ///   [started](Manager::start_maintenance) the maintenance
    ///
    /// The implementation MUST emit [`maintenance::Completed`] event on
    /// success.
    async fn finish_maintenance(&self) -> Result<()>;

    async fn add_node_operator(
        &self,
        idx: node_operator::Idx,
        operator: node_operator::Serialized,
    ) -> Result<()>;

    /// Updates on-chain data of a [`node::Operator`].
    ///
    /// The implementation MUST validate the following invariants:
    /// - [`signer`](SmartContract::signer) is a [`node::Operator`] under the
    ///   provided [`node::OperatorIdx`]
    ///
    /// The implementation MUST emit [`node::OperatorUpdated`] event on success.
    async fn update_node_operator(&self, operator: node_operator::Serialized) -> Result<()>;

    async fn remove_node_operator(&self, id: node_operator::Id) -> Result<()>;

    async fn update_settings(&self, new_settings: Settings) -> Result<()>;

    async fn transfer_ownership(&self, new_owner: AccountAddress) -> Result<()>;
}

/// Read-only handle to a smart-contract managing the state of a WCN cluster.
pub trait ReadOnlySmartContract: Sized + Send + Sync + 'static {
    /// Returns the current [`ClusterView`].
    async fn cluster_view(&self) -> Result<cluster::View<(), node_operator::SerializedData>>;
}

/// [`SmartContract`] address.
#[derive(Clone, Copy, Debug, Display, From, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Address(evm::Address);

/// Account address on the chain hosting WCN cluster [`SmartContract`].
#[derive(Clone, Copy, Debug, Display, From, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountAddress(evm::Address);

/// Transaction signer.
pub struct Signer {
    inner: SignerInner,
}

/// RPC provider URL.
pub struct RpcUrl(reqwest::Url);

/// Error of [`SmartContract::connect`] and [`SmartContract::connect_ro`].
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("Smart-contract with the provided address doesn't exist")]
    UnknownContract,

    #[error("Smart-contract with the provided address is not a WCN cluster")]
    WrongContract,

    #[error("Other: {0}")]
    Other(String),
}

/// [`SmartContract`] error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Revert: {0}")]
    Revert(String),

    #[error("Other: {0}")]
    Other(String),
}

/// [`ReadOnlySmartContract`] error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReadError(String);

/// [`SmartContract`] result.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// [`ReadOnlySmartContract`] result.
pub type ReadResult<T, E = ReadError> = std::result::Result<T, E>;

impl Signer {
    pub fn try_from_private_key(hex: &str) -> Result<Self, InvalidPrivateKeyError> {
        PrivateKeySigner::from_str(hex)
            .map(SignerInner::PrivateKey)
            .map(|inner| Self { inner })
            .map_err(|err| InvalidPrivateKeyError(err.to_string()))
    }

    pub fn address(&self) -> AccountAddress {
        match &self.inner {
            SignerInner::PrivateKey(key) => AccountAddress(key.address()),
        }
    }
}

enum SignerInner {
    PrivateKey(PrivateKeySigner),
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid RPC URL: {0:?}")]
pub struct InvalidRpcUrlError(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid private key: {0:?}")]
pub struct InvalidPrivateKeyError(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid account address: {0:?}")]
pub struct InvalidAccountAddressError(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid smart-contract address: {0:?}")]
pub struct InvalidAddressError(String);

impl FromStr for RpcUrl {
    type Err = InvalidRpcUrlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        reqwest::Url::from_str(s)
            .map(Self)
            .map_err(|err| InvalidRpcUrlError(err.to_string()))
    }
}

impl FromStr for AccountAddress {
    type Err = InvalidAccountAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        alloy::primitives::Address::from_str(s)
            .map(AccountAddress)
            .map_err(|err| InvalidAccountAddressError(err.to_string()))
    }
}

impl FromStr for Address {
    type Err = InvalidAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        alloy::primitives::Address::from_str(s)
            .map(Address)
            .map_err(|err| InvalidAddressError(err.to_string()))
    }
}
