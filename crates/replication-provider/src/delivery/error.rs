#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error(transparent)]
    Infrastructure(#[from] storage_types::StorageError),
    #[error(transparent)]
    Protocol(replication_types::error::ReplicationProtocolError),
}

impl From<replication_types::error::ReplicationProtocolError> for DeliveryError {
    fn from(error: replication_types::error::ReplicationProtocolError) -> Self {
        match crate::regional_application::error::RegionalApplicationError::from(error) {
            crate::regional_application::error::RegionalApplicationError::Infrastructure(error) => {
                Self::Infrastructure(error)
            }
            _ => Self::Protocol(error),
        }
    }
}

impl infrastructure_provider::physical_attempt::PhysicalRecordFailure for DeliveryError {
    fn native_error(&self) -> Option<&storage_types::StorageError> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}

impl From<DeliveryError> for storage_types::StorageError {
    fn from(error: DeliveryError) -> Self {
        match error {
            DeliveryError::Infrastructure(error) => error,
            DeliveryError::Protocol(error) => {
                crate::regional_application::error::RegionalApplicationError::from(error).into()
            }
        }
    }
}
