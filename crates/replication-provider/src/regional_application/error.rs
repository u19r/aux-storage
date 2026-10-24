use storage_types::StorageError;
use thiserror::Error;

use crate::error::ReplicationDecisionError;
#[derive(Debug, Error)]
pub enum RegionalApplicationError {
    #[error(transparent)]
    Peer(#[from] replication_types::error::PeerError),
    #[error(transparent)]
    Infrastructure(#[from] StorageError),
    #[error(transparent)]
    Decision(ReplicationDecisionError),
    #[error(transparent)]
    Protocol(replication_types::error::ReplicationProtocolError),
}
pub type RegionalApplicationResult<T> = Result<T, RegionalApplicationError>;

impl RegionalApplicationError {
    /// Encoding accepted local values exhausts resources, unlike rejecting an
    /// oversized untrusted peer message. The caller still owns native abort.
    pub fn from_local_encoding(error: replication_types::error::ReplicationProtocolError) -> Self {
        match error {
            replication_types::error::ReplicationProtocolError::PayloadTooLarge { .. } => {
                Self::Infrastructure(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
                )
            }
            other => other.into(),
        }
    }
}

impl From<replication_types::error::ReplicationProtocolError> for RegionalApplicationError {
    fn from(error: replication_types::error::ReplicationProtocolError) -> Self {
        match error {
            replication_types::error::ReplicationProtocolError::CapacityRejected => {
                Self::Infrastructure(
                    storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
                )
            }
            other => Self::Protocol(other),
        }
    }
}

impl From<ReplicationDecisionError> for RegionalApplicationError {
    fn from(error: ReplicationDecisionError) -> Self {
        match error {
            ReplicationDecisionError::Protocol(
                replication_types::error::ReplicationProtocolError::CapacityRejected,
            ) => replication_types::error::ReplicationProtocolError::CapacityRejected.into(),
            other => Self::Decision(other),
        }
    }
}

impl infrastructure_provider::physical_attempt::PhysicalRecordFailure for RegionalApplicationError {
    fn native_error(&self) -> Option<&StorageError> {
        match self {
            Self::Infrastructure(error) => Some(error),
            Self::Peer(_) | Self::Decision(_) | Self::Protocol(_) => None,
        }
    }
}

/// Preserve a typed rejection while aborting the caller's native attempt.
/// Returning Ok(Err(...)) would commit partially staged import effects.
impl From<RegionalApplicationError> for StorageError {
    fn from(error: RegionalApplicationError) -> Self {
        use replication_types::error::PeerError;
        use storage_types::{InfrastructureContext as C, InfrastructureResultKind as K};
        match error {
            RegionalApplicationError::Infrastructure(error) => error,
            RegionalApplicationError::Protocol(
                replication_types::error::ReplicationProtocolError::CapacityRejected,
            )
            | RegionalApplicationError::Decision(ReplicationDecisionError::Protocol(
                replication_types::error::ReplicationProtocolError::CapacityRejected,
            )) => K::NativeCapacityRejected.into(),
            RegionalApplicationError::Peer(PeerError::SchemaUnavailable { .. }) => K::Unavailable {
                context: C::RegionalSchema,
            }
            .into(),
            RegionalApplicationError::Peer(PeerError::HistoryUnavailable { .. }) => {
                K::Unavailable {
                    context: C::PublicationHistory,
                }
                .into()
            }
            RegionalApplicationError::Peer(PeerError::CapacityUnavailable { .. }) => {
                K::Unavailable { context: C::Bounds }.into()
            }
            RegionalApplicationError::Peer(PeerError::UnknownCommit { .. }) => K::Unavailable {
                context: C::AttemptSettlement,
            }
            .into(),
            RegionalApplicationError::Peer(PeerError::ProtocolIncompatible { .. }) => {
                K::UnsupportedVersion {
                    context: C::RecordEncoding,
                }
                .into()
            }
            RegionalApplicationError::Protocol(
                replication_types::error::ReplicationProtocolError::ProtocolIncompatible { .. },
            ) => K::UnsupportedVersion {
                context: C::RecordEncoding,
            }
            .into(),
            RegionalApplicationError::Peer(_)
            | RegionalApplicationError::Protocol(_)
            | RegionalApplicationError::Decision(_) => K::Encoding {
                context: C::RegionalSource,
            }
            .into(),
        }
    }
}
