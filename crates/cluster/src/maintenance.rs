use crate::{node, node_operator, Version};

/// Maintenance process within a WCN cluster.
///
/// Only a single [`node_operator`] at a time is allowed to be under
/// maintenance.
pub struct Maintenance {
    operator: node_operator::Id,
}

impl Maintenance {
    /// Returns [`node_operator::Id`] of the node operator currently under
    /// [`Maintenance`].
    pub fn operator(&self) -> &node_operator::Id {
        &self.operator
    }
}

/// [`Maintenance`] has started.
pub struct Started {
    /// ID of the [`node_operator`] that started the [`Maintenance`].
    pub operator_id: node_operator::Id,

    /// Updated cluster [`Version`].
    pub version: Version,
}

/// [`Maintenance`] has been completed.
pub struct Completed {
    /// ID of the [`node_operator`] that completed the [`Maintenance`].
    pub operator_id: node_operator::Id,

    /// Updated cluster [`Version`].
    pub version: Version,
}

/// [`Maintenance`] has been aborted.
pub struct Aborted {
    /// Updated cluster [`Version`].
    pub version: Version,
}
