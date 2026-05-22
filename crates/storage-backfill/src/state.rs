use serde::{Deserialize, Serialize};
use storage_types::{StorageError, TimestampMillis};
use thiserror::Error;

/// Identifier for a backfill target composed of table and index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GsiBackfillDescriptor {
    pub table_name: String,
    pub index_name: String,
}

impl GsiBackfillDescriptor {
    #[must_use]
    pub fn new(table_name: impl Into<String>, index_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            index_name: index_name.into(),
        }
    }
}

/// Status of an individual GSI backfill.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum BackfillStatus {
    #[default]
    Pending,
    Backfilling,
    CatchingUp,
    Done,
}

/// Lightweight lock metadata to coordinate across workers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackfillLock {
    pub owner_id: String,
    pub expires_at: TimestampMillis,
}

impl BackfillLock {
    #[must_use]
    pub fn is_expired(&self, now: TimestampMillis) -> bool {
        *self.expires_at <= *now
    }
}

/// Persisted state for a backfill job including progress and locking info.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackfillState {
    pub status: BackfillStatus,
    pub scan_lek: Option<String>,
    pub captured_stream_tail: Option<String>,
    pub lock: Option<BackfillLock>,
    pub checkpoint: Option<String>,
    pub created_at: TimestampMillis,
    pub updated_at: TimestampMillis,
}

impl BackfillState {
    #[must_use]
    pub fn new(now: TimestampMillis) -> Self {
        Self {
            status: BackfillStatus::Pending,
            scan_lek: None,
            captured_stream_tail: None,
            lock: None,
            checkpoint: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[must_use]
    pub fn with_status(mut self, status: BackfillStatus) -> Self {
        self.status = status;
        self
    }

    pub fn refresh_updated_at(&mut self, now: TimestampMillis) {
        self.updated_at = now;
    }
}

/// Result of executing a single batch of work for a backfill target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct BackfillBatchOutcome {
    pub items_processed: usize,
    pub next_token: Option<String>,
    pub done: bool,
}

impl BackfillBatchOutcome {
    #[must_use]
    pub fn did_work(&self) -> bool {
        self.items_processed > 0 || self.done
    }
}

/// Top-level result returned by the coordinator after running once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillResult {
    DidWork,
    Idle,
}

impl From<bool> for BackfillResult {
    fn from(value: bool) -> Self {
        if value { Self::DidWork } else { Self::Idle }
    }
}

/// Errors that can occur during backfill orchestration.
#[derive(Debug, Error)]
pub enum BackfillError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub type BackfillResultType = Result<BackfillResult, BackfillError>;

/// Context provided to the coordinator for tracing and metrics.
#[derive(Debug, Clone)]
pub struct WorkerContext {
    pub worker_id: String,
    pub now: TimestampMillis,
}

impl WorkerContext {
    #[must_use]
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            now: TimestampMillis::now(),
        }
    }

    pub fn refresh_now(&mut self) {
        self.now = TimestampMillis::now();
    }
}
