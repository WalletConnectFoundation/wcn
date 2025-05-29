use crate::{node, node_operator, LogicalError, Version as ClusterVersion, View as ClusterView};

/// Maintenance process within a WCN cluster.
///
/// Only a single [`node_operator`] at a time is allowed to be under
/// maintenance.
pub struct Maintenance {
    operator_id: node_operator::Id,
}

impl Maintenance {
    /// Returns [`node_operator::Id`] of the node operator currently under
    /// [`Maintenance`].
    pub fn operator(&self) -> &node_operator::Id {
        &self.operator_id
    }
}

/// [`Maintenance`] has started.
pub struct Started {
    /// ID of the [`node_operator`] that started the [`Maintenance`].
    pub operator_id: node_operator::Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Started {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        view.no_migration()?;
        view.no_maintenance()?;

        view.maintenance = Some(Maintenance {
            operator_id: self.operator_id,
        });

        Ok(())
    }
}

/// [`Maintenance`] has been finished.
pub struct Finished {
    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Finished {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        view.has_maintenance()?;
        view.maintenance = None;

        Ok(())
    }
}
