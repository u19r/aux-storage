//! The one classifier of a native COMMIT answer. Drivers translate their
//! native error into `NativeCommitError`; this table decides known abort or
//! unknown outcome.

use storage_types::{AttemptFailure, CapacityKind, NativeCause, StorageError};

/// What a driver knows about a failed COMMIT.
#[derive(Clone, Debug)]
pub enum NativeCommitError {
    /// The engine proved that nothing committed because of a concurrent
    /// writer (busy, serialization failure, not_committed).
    Conflict(NativeCause),
    /// The engine proved that nothing committed: a native limit.
    Capacity {
        kind: CapacityKind,
        cause: NativeCause,
    },
    /// The engine proved that nothing committed, for another reason. Only
    /// FoundationDB states the commit state of every error.
    NotCommitted(NativeCause),
    /// The engine gave no proof either way. `reason` is fixed text.
    Unproven {
        reason: &'static str,
        cause: Option<NativeCause>,
    },
    /// A local error on the commit path, already classified (no client, a
    /// test probe, a lost reply already judged).
    Classified(StorageError),
}

impl From<StorageError> for NativeCommitError {
    fn from(error: StorageError) -> Self {
        Self::Classified(error)
    }
}

/// `submitted` is true when a write's COMMIT was issued under a
/// `CommitTicket`. A read commits nothing, so nothing it answers is unknown.
#[must_use]
pub fn classify_commit(submitted: bool, error: NativeCommitError) -> StorageError {
    match error {
        NativeCommitError::Conflict(cause) => StorageError::native_conflict(cause),
        NativeCommitError::Capacity { kind, cause } => AttemptFailure::Capacity {
            kind,
            code: cause.code,
        }
        .into(),
        NativeCommitError::NotCommitted(cause) => {
            StorageError::internal(&format!("native commit failed without effect ({cause})"))
        }
        NativeCommitError::Unproven {
            reason,
            cause: Some(cause),
        } if submitted => StorageError::native_commit_unknown(reason, cause),
        NativeCommitError::Unproven {
            reason,
            cause: None,
        } if submitted => StorageError::commit_unknown(reason),
        NativeCommitError::Unproven {
            reason,
            cause: Some(cause),
        } => StorageError::internal(&format!("read commit failed: {reason} ({cause})")),
        NativeCommitError::Unproven {
            reason,
            cause: None,
        } => StorageError::internal(&format!("read commit failed: {reason}")),
        NativeCommitError::Classified(error) if !submitted && error.is_commit_unknown() => {
            StorageError::internal(&format!("read commit failed: {error}"))
        }
        NativeCommitError::Classified(error) => error,
    }
}
