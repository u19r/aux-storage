use replication_types::error::ReplicationProtocolError;
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReplicationDecisionError {
    #[error(transparent)]
    Protocol(#[from] ReplicationProtocolError),
    #[error("invalid source phase: {context}")]
    InvalidPhase { context: &'static str },
    #[error("required durable evidence missing: {context}")]
    MissingEvidence { context: &'static str },
    #[error("native outcome unknown: {context}")]
    UnknownCommit { context: &'static str },
    #[error("{context}: {required} origin bytes required, {available} available")]
    CapacityUnavailable {
        context: &'static str,
        required: u64,
        available: u64,
    },
    #[error("history still held: {context}")]
    HistoryHeld { context: &'static str },
    #[error("origination clock floor unknown: {context}")]
    ClockFloorUnknown { context: &'static str },
    #[error("protected authority lost: {context}")]
    AuthorityLost { context: &'static str },
}

pub type ReplicationResult<T> = Result<T, ReplicationDecisionError>;

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("record preparation capacity rejected")]
    CapacityRejected,
    #[error("record exceeds byte bound")]
    TooLarge,
    #[error("record envelope corrupt")]
    Corrupt,
    #[error("record version unsupported")]
    IncompatibleVersion,
    #[error("record payload encoding invalid")]
    Encoding,
}
impl From<RecordError> for StorageError {
    fn from(error: RecordError) -> Self {
        let failure = match error {
            RecordError::CapacityRejected => InfrastructureResultKind::NativeCapacityRejected,
            RecordError::TooLarge => InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Bounds,
            },
            RecordError::IncompatibleVersion => InfrastructureResultKind::UnsupportedVersion {
                context: InfrastructureContext::RecordEncoding,
            },
            RecordError::Corrupt | RecordError::Encoding => InfrastructureResultKind::Encoding {
                context: InfrastructureContext::RecordEncoding,
            },
        };
        failure.into()
    }
}
