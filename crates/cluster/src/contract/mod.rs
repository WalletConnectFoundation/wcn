use {crate::node, std::future::Future};

#[cfg(feature = "evm")]
pub mod evm;

/// Public key on a chain hosting the [`SmartContract`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PublicKey(String);

pub struct Settings {
    pub min_operators: u8,
    pub max_operator_data_bytes: u32,
}

pub enum Call {
    StartMigration {
        operators_to_remove: Vec<PublicKey>,
        operators_to_add: Vec<(PublicKey, node::Operator)>,
    },
    CompleteMigration,
    AbortMigration,

    StartMaintenance,
    CompleteMaintenance,
    AbortMaintenance,

    UpdateNodeOperator(node::Operator),

    UpdateSettings(Settings),

    TransferOwnership(PublicKey),
}

pub trait SmartContract<K = PublicKey> {
    fn call(&self, call: Call) -> impl Future<Output = Result<(), Error>> + Send;
}

pub struct Error(String);
