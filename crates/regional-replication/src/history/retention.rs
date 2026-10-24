//! Immutable reader retention policy and terminal expiry settlement.
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis, context::WrappedError as _};

use super::checkpoint::CanonicalCheckpointState;

/// Native observation of both retention conflict domains, without grant
/// authority.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionRevision {
    pub(crate) lineage: Option<storage_types::Revision>,
    pub(crate) journal: storage_types::Revision,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointRetention {
    ExplicitRelease,
    Until(TimestampMillis),
}

impl CheckpointRetention {
    pub(crate) fn require_creation(self, observed_at: TimestampMillis) -> StorageResult<()> {
        if matches!(self, Self::Until(deadline) if deadline <= observed_at) {
            return Err(StorageError::validation(
                "Checkpoint retention deadline must be in the future",
            ));
        }
        Ok(())
    }

    pub(crate) fn expire_state(
        self,
        state: &mut CanonicalCheckpointState,
        observed_at: TimestampMillis,
    ) -> bool {
        if *state == CanonicalCheckpointState::Held
            && matches!(self, Self::Until(deadline) if deadline <= observed_at)
        {
            *state = CanonicalCheckpointState::Expired;
            return true;
        }
        false
    }
}

pub(crate) fn settle_expiry<T>(result: StorageResult<T>) -> StorageResult<StorageResult<T>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(error) if matches!(error.to_enum(), storage_types::StorageEnum::HistoryExpired) => {
            Ok(Err(error))
        }
        Err(error) => Err(error),
    }
}
