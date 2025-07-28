//! WCN cluster settings.

use {
    crate::Version as ClusterVersion,
    serde::{Deserialize, Serialize},
};

#[allow(unused_imports)] // for doc comments
use crate::{Cluster, NodeOperator};

/// WCN [`Cluster`] settings.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    /// Maximum number of on-chain bytes stored for a single
    /// [`NodeOperator`].
    pub max_node_operator_data_bytes: u16,
}

/// Event of [`Settings`] being updated.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Updated {
    /// Updated [`Settings`].
    pub settings: Settings,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}
