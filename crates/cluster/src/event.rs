use crate::{NodeOperator, PublicKey};

pub struct MigrationStarted<K = PublicKey> {
    pub operators_to_remove: Vec<K>,
    pub operators_to_add: Vec<(K, NodeOperator)>,
    pub version: u128,
}

pub struct MigrationDataPullCompleted<K = PublicKey> {
    pub operator: K,
    pub version: u128,
}

pub struct MigrationCompleted {
    pub version: u128,
}

pub struct MigrationAborted {
    pub version: u128,
}

pub struct MaintenanceStarted<K = PublicKey> {
    pub operator: K,
    pub version: u8,
}

pub struct MaintenanceCompleted<K = PublicKey> {
    pub operator: K,
    pub version: u8,
}

pub struct MaintenanceAborted<K = PublicKey> {
    pub operator: K,
    pub version: u8,
}

pub struct NodeOperatorUpdated<K = PublicKey> {
    pub key: K,
    pub operator: NodeOperator,
}
