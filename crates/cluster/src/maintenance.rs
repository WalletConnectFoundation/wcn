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

/// [`Maintenance`] has been completed.
pub struct Completed {
    /// ID of the [`node_operator`] that completed the [`Maintenance`].
    pub operator_id: node_operator::Id,

    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Completed {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        let maintenance = view.has_maintenance()?;

        if maintenance.operator_id != self.operator_id {
            return Err(LogicalError::MaintenanceNodeOperatorIdMismatch {
                event: self.operator_id,
                local: maintenance.operator_id,
            });
        }

        drop(maintenance);
        view.maintenance = None;

        Ok(())
    }
}

/// [`Maintenance`] has been aborted.
pub struct Aborted {
    /// Updated [`ClusterVersion`].
    pub cluster_version: ClusterVersion,
}

impl Aborted {
    pub(super) fn apply(self, view: &mut ClusterView) -> Result<(), LogicalError> {
        view.has_maintenance()?;
        view.maintenance = None;

        Ok(())
    }
}
