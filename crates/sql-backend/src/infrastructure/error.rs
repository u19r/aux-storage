use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError};

pub(super) fn invalid_result() -> StorageError {
    StorageError::from(InfrastructureResultKind::Encoding {
        context: InfrastructureContext::RecordEncoding,
    })
}
