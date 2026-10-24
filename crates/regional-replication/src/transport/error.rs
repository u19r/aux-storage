use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportNack {
    InvalidPage,
    SchemaUnavailable,
    Unavailable,
    UnresolvedCommit,
    Aborted,
    CapacityRejected,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("regional socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("regional protocol validation failed: {0}")]
    Protocol(replication_types::error::ReplicationProtocolError),
    #[error("native regional operation failed: {0}")]
    Native(#[from] storage_types::StorageError),
    #[error("invalid regional transport configuration")]
    Configuration,
    #[error("invalid or oversized regional frame")]
    Frame,
    #[error("regional frame exceeds its configured capacity")]
    FrameTooLarge,
    #[error("unexpected configured regional peer")]
    Peer,
    #[error("regional request deadline expired")]
    Timeout,
    #[error("regional frame worker failed")]
    Worker,
    #[error("regional peer deferred or rejected the page: {0:?}")]
    Nack(ImportNack),
}

pub(super) type TransportResult<T> = Result<T, TransportError>;

impl From<replication_types::error::ReplicationProtocolError> for TransportError {
    fn from(error: replication_types::error::ReplicationProtocolError) -> Self {
        match replication_provider::regional_application::error::RegionalApplicationError::from(error) {
            replication_provider::regional_application::error::RegionalApplicationError::Infrastructure(error) => Self::Native(error),
            _ => Self::Protocol(error),
        }
    }
}

impl infrastructure_provider::physical_attempt::PhysicalRecordFailure for TransportError {
    fn native_error(&self) -> Option<&storage_types::StorageError> {
        if let Self::Native(error) = self {
            Some(error)
        } else {
            None
        }
    }
}
