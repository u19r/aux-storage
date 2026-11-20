//! The typed outcome of a failed native attempt: what the retry owner reads,
//! what a log line prints, and the class a wire mapping starts from.

use crate::{AttemptPhase, NativeUnavailable, StorageEnum, StorageError};

/// The engine that answered a native call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeEngine {
    Sqlite,
    Turso,
    Postgres,
    RocksDb,
    FoundationDb,
}

impl NativeEngine {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Turso => "turso",
            Self::Postgres => "postgres",
            Self::RocksDb => "rocksdb",
            Self::FoundationDb => "foundationdb",
        }
    }
}

/// The native call that answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCallPhase {
    Begin,
    Statement,
    Commit,
    Rollback,
}

/// A native result code. Never the native message: messages can quote values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCode {
    /// FoundationDB error code.
    Fdb(i32),
    /// Postgres SQLSTATE, five ASCII bytes.
    SqlState([u8; 5]),
    /// SQLite extended result code.
    Sqlite(i32),
    /// A fixed engine name: a RocksDB `ErrorKind`, a Turso error variant.
    Named(&'static str),
    /// The driver decided without a native code (a lost reply, a stopped task).
    None,
}

impl NativeCode {
    /// SQLSTATE from the text `tokio_postgres` returns; any other length is
    /// `None`.
    #[must_use]
    pub fn sql_state(code: &str) -> Self {
        <[u8; 5]>::try_from(code.as_bytes()).map_or(Self::None, Self::SqlState)
    }
}

impl std::fmt::Display for NativeCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fdb(code) => write!(f, "code {code}"),
            Self::SqlState(code) => write!(f, "SQLSTATE {}", String::from_utf8_lossy(code)),
            Self::Sqlite(code) => write!(f, "extended code {code}"),
            Self::Named(name) => f.write_str(name),
            Self::None => f.write_str("no native code"),
        }
    }
}

/// Which engine said what, and when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeCause {
    pub engine: NativeEngine,
    pub code: NativeCode,
    pub phase: NativeCallPhase,
}

impl std::fmt::Display for NativeCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} at {:?}",
            self.engine.as_str(),
            self.code,
            self.phase
        )
    }
}

/// What a conditional physical write expected and did not find.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissExpectation {
    /// An insert found the row or key present.
    Absent,
    /// An update or delete found no row.
    Present,
    /// A compare-and-set found another revision.
    Revision,
    /// A statement changed `found` rows where `expected` were required.
    Rows { expected: u64, found: u64 },
}

/// A conditional physical write missed: another writer changed the record
/// after this attempt observed it. `record` is a fixed name of the record
/// family (`"ttl_policy"`, `"stream_trim_receipt"`), never a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalWriteMiss {
    pub record: &'static str,
    pub expected: MissExpectation,
}

/// Why a fresh try may succeed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictCause {
    /// The engine aborted the attempt for a concurrent writer.
    Native(NativeCause),
    /// A conditional physical write missed (SQL row count, unique insert).
    WriteMiss(PhysicalWriteMiss),
    /// A shared domain rule asks for a fresh decision; the text is fixed.
    Logical(&'static str),
    /// A remote store answered TransactionConflictException.
    Remote,
}

/// Why cached observations need one fresh decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshCause {
    /// A cached read version or catalog observation was stale.
    StaleObservation,
    /// A read named a relation or column that no longer exists: the SQL was
    /// rendered from stale table metadata (Postgres 42P01, 42703).
    MissingRelation(NativeCause),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityKind {
    /// A native or local limit (size, memory, allocation, work budget).
    NativeLimit,
    /// The lowered native transaction exceeded its byte ceiling.
    TransactionBytes,
}

/// Why a commit outcome is unknown. `reason` is fixed text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownCause {
    pub reason: &'static str,
    pub native: Option<NativeCause>,
}

/// The failure of one native attempt. Every variant except `CommitUnknown`
/// is a known abort: nothing of the attempt committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptFailure {
    /// Retried by `retry_tries` with fresh observations.
    Conflict(ConflictCause),
    /// Retried once with fresh observations.
    FreshValidation(FreshCause),
    /// Not retried: the same attempt would exceed the limit again.
    Capacity {
        kind: CapacityKind,
        code: NativeCode,
    },
    /// Not retried by the shared owner; a client may retry.
    Unavailable(NativeUnavailable),
    /// COMMIT was submitted and not answered. Never retried, never known.
    CommitUnknown(UnknownCause),
}

impl AttemptFailure {
    pub const CAPACITY: Self = Self::Capacity {
        kind: CapacityKind::NativeLimit,
        code: NativeCode::None,
    };
    pub const TRANSACTION_BYTES: Self = Self::Capacity {
        kind: CapacityKind::TransactionBytes,
        code: NativeCode::None,
    };
    pub const FRESH_VALIDATION: Self = Self::FreshValidation(FreshCause::StaleObservation);

    #[must_use]
    pub const fn kind(&self) -> AttemptKind {
        match self {
            Self::Conflict(_) => AttemptKind::Conflict,
            Self::FreshValidation(_) => AttemptKind::FreshValidation,
            Self::Capacity {
                kind: CapacityKind::NativeLimit,
                ..
            } => AttemptKind::Capacity,
            Self::Capacity {
                kind: CapacityKind::TransactionBytes,
                ..
            } => AttemptKind::TransactionBytes,
            Self::Unavailable(NativeUnavailable::Deadline(_)) => AttemptKind::Deadline,
            Self::Unavailable(_) => AttemptKind::Unavailable,
            Self::CommitUnknown(_) => AttemptKind::CommitUnknown,
        }
    }

    /// Fixed text for structured logs that take `&'static str`.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Conflict(ConflictCause::Native(_)) => "native conflict",
            Self::Conflict(ConflictCause::WriteMiss(miss)) => miss.record,
            Self::Conflict(ConflictCause::Logical(reason)) => reason,
            Self::Conflict(ConflictCause::Remote) => "remote conflict",
            Self::FreshValidation(_) => "fresh validation required",
            Self::Capacity { .. } => "native capacity exceeded",
            Self::Unavailable(_) => "native attempt unavailable",
            Self::CommitUnknown(cause) => cause.reason,
        }
    }
}

impl std::fmt::Display for AttemptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(ConflictCause::Native(cause)) => write!(f, "native conflict: {cause}"),
            Self::Conflict(ConflictCause::WriteMiss(miss)) => {
                write!(
                    f,
                    "physical write miss on {}: expected {:?}",
                    miss.record, miss.expected
                )
            }
            Self::Conflict(ConflictCause::Logical(reason)) => {
                write!(f, "logical conflict: {reason}")
            }
            Self::Conflict(ConflictCause::Remote) => f.write_str("remote transaction conflict"),
            Self::FreshValidation(cause) => write!(f, "fresh validation required: {cause:?}"),
            Self::Capacity { kind, code } => {
                write!(f, "native capacity exceeded: {kind:?} ({code})")
            }
            Self::Unavailable(unavailable) => {
                write!(f, "native attempt unavailable: {unavailable:?}")
            }
            Self::CommitUnknown(UnknownCause {
                reason,
                native: Some(cause),
            }) => {
                write!(f, "commit outcome unknown: {reason} ({cause})")
            }
            Self::CommitUnknown(UnknownCause {
                reason,
                native: None,
            }) => {
                write!(f, "commit outcome unknown: {reason}")
            }
        }
    }
}

impl std::error::Error for AttemptFailure {}

/// The attempt class without its cause: what a retry owner, a metric label
/// or a test compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptKind {
    Conflict,
    FreshValidation,
    Capacity,
    TransactionBytes,
    Unavailable,
    Deadline,
    CommitUnknown,
}

impl AttemptKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Conflict => "conflict",
            Self::FreshValidation => "fresh_validation",
            Self::Capacity => "capacity",
            Self::TransactionBytes => "transaction_bytes",
            Self::Unavailable => "unavailable",
            Self::Deadline => "deadline",
            Self::CommitUnknown => "commit_unknown",
        }
    }

    /// Known not committed and a later try may succeed: what a wire mapping
    /// answers with 503 and Retry-After.
    #[must_use]
    pub const fn is_unavailable(self) -> bool {
        matches!(self, Self::Unavailable | Self::Deadline)
    }
}

/// The class every caller and wire mapping starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// Known not committed; repeating the request can succeed.
    Retryable,
    /// Known not committed; the same request exceeds a limit again.
    Capacity,
    /// The commit outcome is unknown.
    CommitUnknown,
    /// The request or the state it names is wrong.
    Client,
    /// A failure of this service.
    Internal,
}

impl ErrorClass {
    /// Fixed label for logs and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Capacity => "capacity",
            Self::CommitUnknown => "commit_unknown",
            Self::Client => "client",
            Self::Internal => "internal",
        }
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }
}

impl StorageError {
    /// The one constructor of an unknown commit outcome without a native code.
    #[must_use]
    pub fn commit_unknown(reason: &'static str) -> Self {
        AttemptFailure::CommitUnknown(UnknownCause {
            reason,
            native: None,
        })
        .into()
    }

    #[must_use]
    pub fn native_commit_unknown(reason: &'static str, cause: NativeCause) -> Self {
        AttemptFailure::CommitUnknown(UnknownCause {
            reason,
            native: Some(cause),
        })
        .into()
    }

    #[must_use]
    pub fn native_conflict(cause: NativeCause) -> Self {
        AttemptFailure::Conflict(ConflictCause::Native(cause)).into()
    }

    /// A shared domain rule that wants a fresh try says so with fixed text.
    #[must_use]
    pub fn logical_conflict(reason: &'static str) -> Self {
        AttemptFailure::Conflict(ConflictCause::Logical(reason)).into()
    }

    /// The known abort of an attempt whose call failed without a classified
    /// error or was abandoned.
    #[must_use]
    pub fn attempt_abandoned() -> Self {
        AttemptFailure::Unavailable(NativeUnavailable::Abandoned).into()
    }

    #[must_use]
    pub fn is_attempt_abandoned(&self) -> bool {
        matches!(
            self.as_ref(),
            StorageEnum::Attempt(AttemptFailure::Unavailable(NativeUnavailable::Abandoned))
        )
    }

    #[must_use]
    pub fn write_miss(record: &'static str, expected: MissExpectation) -> Self {
        AttemptFailure::Conflict(ConflictCause::WriteMiss(PhysicalWriteMiss {
            record,
            expected,
        }))
        .into()
    }

    #[must_use]
    pub fn attempt_failure(&self) -> Option<&AttemptFailure> {
        match self.as_ref() {
            StorageEnum::Attempt(failure) => Some(failure),
            _ => None,
        }
    }

    /// The attempt class of this error.
    #[must_use]
    pub fn attempt_kind(&self) -> Option<AttemptKind> {
        self.attempt_failure().map(AttemptFailure::kind)
    }

    /// The deadline phase of a deadline error.
    #[must_use]
    pub fn deadline_phase(&self) -> Option<AttemptPhase> {
        match self.as_ref() {
            StorageEnum::Attempt(AttemptFailure::Unavailable(NativeUnavailable::Deadline(
                phase,
            ))) => Some(*phase),
            _ => None,
        }
    }
}
