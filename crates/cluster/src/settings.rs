//! WCN cluster settings.
use crate::{LogicalError, Version as ClusterVersion, View as ClusterView};

/// WCN cluster settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Settings {
    /// Maximum number of on-chain bytes stored for a single [`NodeOperator`].
    pub max_node_operator_data_bytes: u16,
}

/// Event of [`Settings`] being updated.
pub struct Updated {
    /// Updated [`Settings`].
    pub settings: Settings,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Updated {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        if view.settings == self.settings {
            return Err(LogicalError::SettingsUnchanged);
        }

        view.settings = self.settings;

        Ok(())
    }
}
