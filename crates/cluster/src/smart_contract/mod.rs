pub mod evm;

use {
    crate::{migration, node_operator, NewNodeOperator, Settings, View as ClusterView},
    alloy::{signers::local::PrivateKeySigner, transports::http::reqwest},
    derive_more::derive::Display,
    serde::{Deserialize, Serialize},
    std::str::FromStr,
};

/// Handle to a smart-contract managing the state of a WCN cluster.
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
        initial_operators: Vec<NewNodeOperator>,
    ) -> Result<Self>;

    /// Connects to an existing smart-contract.
    async fn connect(signer: Signer, rpc_url: RpcUrl) -> Result<Self>;

    /// Connects to an existing smart-contract in read-only mode.
    async fn connect_ro(rpc_url: RpcUrl) -> Result<Self::ReadOnly>;

    /// Returns the [`PublicKey`] of the [`Signer`] which is currently being
    /// used for this [`SmartContract`] handle.
    fn signer(&self) -> PublicKey;

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
    async fn start_migration(&self, plan: migration::Plan) -> Result<()>;

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
    async fn complete_migration(
        &self,
        id: migration::Id,
        operator_idx: node_operator::Idx,
    ) -> Result<()>;

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
    async fn start_maintenance(&self, operator_idx: node_operator::Idx) -> Result<()>;

    /// Completes the ongoing [`maintenance`] process.
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's an ongoing maintenance
    /// - [`signer`](SmartContract::signer) is the one who
    ///   [started](Manager::start_maintenance) the maintenance
    ///
    /// The implementation MUST emit [`maintenance::Completed`] event on
    /// success.
    async fn complete_maintenance(&self) -> Result<()>;

    /// Aborts the ongoing [`maintenance`] process.
    ///
    /// The implementation MUST validate the following invariants:
    /// - there's an ongoing maintenance
    /// - [`signer`](SmartContract::signer) is the owner of the
    ///   [`SmartContract`]
    ///
    /// The implementation MUST emit [`maintenance::Aborted`] event on success.
    async fn abort_maintenance(&self) -> Result<()>;

    /// Updates on-chain data of a [`node::Operator`].
    ///
    /// The implementation MUST validate the following invariants:
    /// - [`signer`](SmartContract::signer) is a [`node::Operator`] under the
    ///   provided [`node::OperatorIdx`]
    ///
    /// The implementation MUST emit [`node::OperatorUpdated`] event on success.
    async fn update_node_operator(
        &self,
        id: node_operator::Id,
        idx: node_operator::Idx,
        data: node_operator::SerializedData,
    ) -> Result<()>;
}

/// Read-only handle to a smart-contract managing the state of a WCN cluster.
pub trait ReadOnlySmartContract: Sized + Send + Sync + 'static {
    /// Returns the current [`ClusterView`].
    async fn cluster_view(&self) -> Result<ClusterView>;
}

// impl From<alloy::contract::Error> for Error {
//     fn from(err: alloy::contract::Error) -> Self {
//         Self(format!("{err:?}"))
//     }
// }

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

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type ReadResult<T, E = ReadError> = std::result::Result<T, E>;

#[derive(Clone, Debug, Display, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublicKey(evm::Address);

impl FromStr for PublicKey {
    type Err = InvalidPublicKey;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        alloy::primitives::Address::from_str(s)
            .map(PublicKey)
            .map_err(|err| InvalidPublicKey(err.to_string()))
    }
}

pub struct Signer {
    inner: SignerInner,
}

impl Signer {
    pub fn try_from_private_key(hex: &str) -> Result<Self, InvalidPrivateKey> {
        PrivateKeySigner::from_str(hex)
            .map(SignerInner::PrivateKey)
            .map(|inner| Self { inner })
            .map_err(|err| InvalidPrivateKey(err.to_string()))
    }
}

enum SignerInner {
    PrivateKey(PrivateKeySigner),
}

pub struct RpcUrl(reqwest::Url);

impl FromStr for RpcUrl {
    type Err = InvalidRpcUrl;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        reqwest::Url::from_str(s)
            .map(Self)
            .map_err(|err| InvalidRpcUrl(err.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid RPC URL: {0:?}")]
pub struct InvalidRpcUrl(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid private key: {0:?}")]
pub struct InvalidPrivateKey(String);

#[derive(Debug, thiserror::Error)]
#[error("Invalid public key: {0:?}")]
pub struct InvalidPublicKey(String);

// impl TryFrom<bindings::Cluster::KeyspaceView> for Keyspace {
//     type Error = Error;

//     fn try_from(view: bindings::Cluster::KeyspaceView) -> Result<Self> {
//         let replication_strategy = view
//             .replicationStrategy
//             .try_into()
//             .map_err(|err| Error(format!("Invalid ReplicationStrategy:
// {err}")))?;

//         Ok(Keyspace::new(replication_strategy))
//     }
// }

// #[cfg(test)]
// mod test {
//     #[test]
//     fn address_to_from_public_key_conversion() {
//         todo!()
//     }
// }
