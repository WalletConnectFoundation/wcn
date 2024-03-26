#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Invalid cluster state")]
    InvalidState,

    #[error("Invalid node operation mode")]
    InvalidOperationMode,

    #[error("Node timestamp in future")]
    TimestampInFuture,

    #[error("Hash ring error: {0}")]
    RingError(#[from] HashRingError),

    #[error("{0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum HashRingError {
    #[error("out of ids")]
    OutOfIds,

    #[error("node not found")]
    NodeNotFound,

    #[error("node already exists")]
    NodeAlreadyExists,

    #[error("token already exists (hash collision)")]
    TokenAlreadyExists,
}
