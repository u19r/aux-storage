use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError};

pub(super) fn capacity_rejected() -> StorageError {
    InfrastructureResultKind::NativeCapacityRejected.into()
}

pub(super) fn invalid_image() -> StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::RecordEncoding,
    }
    .into()
}

pub(super) fn invalid_observation() -> StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::Observation,
    }
    .into()
}
