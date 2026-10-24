use std::{error::Error, fmt};

use storage_types::StorageError;

#[derive(Debug, PartialEq, Eq)]
pub enum GsiLifecycleError {
    DuplicateIncarnation,
    DuplicateIndex,
    InvalidBoundary,
    InvalidPageLimit,
    InvalidPhase,
    ObservationMismatch,
    ProgressConflict,
}

impl fmt::Display for GsiLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateIncarnation => {
                formatter.write_str("GSI lifecycle incarnation is duplicated")
            }
            Self::DuplicateIndex => formatter.write_str("GSI lifecycle index is duplicated"),
            Self::InvalidBoundary => formatter.write_str("GSI lifecycle boundary is invalid"),
            Self::InvalidPageLimit => formatter.write_str("GSI page limit must be positive"),
            Self::InvalidPhase => {
                formatter.write_str("GSI lifecycle phase cannot perform this transition")
            }
            Self::ObservationMismatch => {
                formatter.write_str("GSI observation does not match the requested lifecycle phase")
            }
            Self::ProgressConflict => {
                formatter.write_str("GSI lifecycle progress changed before commit")
            }
        }
    }
}

impl Error for GsiLifecycleError {}

impl From<GsiLifecycleError> for StorageError {
    fn from(error: GsiLifecycleError) -> Self {
        Self::internal(&error.to_string())
    }
}

pub enum GsiRecordError {
    Incarnation,
    Phase,
    Position,
    TableName,
}

impl fmt::Display for GsiRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Incarnation => "GSI build incarnation is malformed",
            Self::Phase => "GSI build phase is malformed",
            Self::Position => "GSI build stream position is malformed",
            Self::TableName => "GSI build table name is malformed",
        })
    }
}

impl fmt::Debug for GsiRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for GsiRecordError {}

impl From<GsiRecordError> for StorageError {
    fn from(error: GsiRecordError) -> Self {
        Self::internal(&error.to_string())
    }
}

impl From<super::GsiFailureReason> for StorageError {
    fn from(reason: super::GsiFailureReason) -> Self {
        match reason {
            super::GsiFailureReason::HistoryUnavailable => {
                storage_types::StorageEnum::HistoryUnavailable.into()
            }
            super::GsiFailureReason::ProjectionConflict => {
                Self::internal("GSI projection conflicts with its source revision")
            }
        }
    }
}
