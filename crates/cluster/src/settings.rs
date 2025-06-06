//! WCN cluster settings.
use crate::Version as ClusterVersion;

/// WCN cluster settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Settings {
    /// Maximum number of on-chain bytes stored for a single [`NodeOperator`].
    pub max_node_operator_data_bytes: u16,
}

/// Event of [`Settings`] being updated.
#[derive(Debug)]
pub struct Updated {
    /// Updated [`Settings`].
    pub settings: Settings,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}
