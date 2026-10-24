//! Storage-owned scheduled maintenance job identities and cadence settings.
use std::{fmt, str::FromStr, time::Duration};

use scheduled_bg_jobs::{JobKind, QueueName};

use crate::{JobIntervalMillis, constants::DEFAULT_JOB_INTERVALS, errors::StorageJobParseError};

/// One bounded storage maintenance operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageJob {
    GsiBackfill,
    PartitionReconciliation,
    QueueMessageCleanup,
    StreamTrim,
    TableReclaim,
    TtlSweep,
}

impl StorageJob {
    pub const ALL: [Self; 6] = [
        Self::GsiBackfill,
        Self::PartitionReconciliation,
        Self::QueueMessageCleanup,
        Self::StreamTrim,
        Self::TableReclaim,
        Self::TtlSweep,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GsiBackfill => "gsi-backfill",
            Self::PartitionReconciliation => "partition-reconciliation",
            Self::QueueMessageCleanup => "queue-message-cleanup",
            Self::StreamTrim => "stream-trim",
            Self::TableReclaim => "table-reclaim",
            Self::TtlSweep => "ttl-sweep",
        }
    }

    #[must_use]
    pub fn kind(self) -> JobKind {
        // All literals above satisfy JobKind's bounded identifier contract.
        JobKind::try_new(self.as_str()).unwrap_or_else(|_| unreachable!("static job kind"))
    }

    #[must_use]
    pub fn queue(self) -> QueueName {
        let queue = match self {
            Self::GsiBackfill => "storage-gsi-backfill",
            Self::PartitionReconciliation => "storage-partition-reconciliation",
            Self::QueueMessageCleanup => "storage-queue-message-cleanup",
            Self::StreamTrim => "storage-stream-trim",
            Self::TableReclaim => "storage-table-reclaim",
            Self::TtlSweep => "storage-ttl-sweep",
        };
        QueueName::try_new(queue).unwrap_or_else(|_| unreachable!("static queue name"))
    }

    /// A scheduler fence belongs only to the queue carrying this job.
    #[must_use]
    pub fn owns_queue(self, queue: &QueueName) -> bool {
        self.queue() == *queue
    }

    #[must_use]
    pub const fn interval(self, intervals: DatabaseJobIntervals) -> Duration {
        let millis = match self {
            Self::GsiBackfill => intervals.gsi_backfill_interval_ms.0,
            Self::PartitionReconciliation => intervals.partition_reconciliation_interval_ms.0,
            Self::QueueMessageCleanup => intervals.queue_message_cleanup_interval_ms.0,
            Self::StreamTrim => intervals.stream_trim_interval_ms.0,
            Self::TableReclaim => intervals.table_reclaim_interval_ms.0,
            Self::TtlSweep => intervals.ttl_sweep_interval_ms.0,
        };
        Duration::from_millis(millis)
    }
}

impl fmt::Display for StorageJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for StorageJob {
    type Err = StorageJobParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "gsi-backfill" => Ok(Self::GsiBackfill),
            "partition-reconciliation" => Ok(Self::PartitionReconciliation),
            "queue-message-cleanup" => Ok(Self::QueueMessageCleanup),
            "stream-trim" => Ok(Self::StreamTrim),
            "table-reclaim" => Ok(Self::TableReclaim),
            "ttl-sweep" => Ok(Self::TtlSweep),
            _ => Err(StorageJobParseError::Unsupported),
        }
    }
}

/// Cadences for storage-owned scheduled maintenance jobs.
#[derive(Debug, Clone, Copy)]
pub struct DatabaseJobIntervals {
    pub partition_reconciliation_interval_ms: JobIntervalMillis,
    pub gsi_backfill_interval_ms: JobIntervalMillis,
    pub ttl_sweep_interval_ms: JobIntervalMillis,
    pub stream_trim_interval_ms: JobIntervalMillis,
    pub table_reclaim_interval_ms: JobIntervalMillis,
    pub queue_message_cleanup_interval_ms: JobIntervalMillis,
}

impl Default for DatabaseJobIntervals {
    fn default() -> Self {
        DEFAULT_JOB_INTERVALS
    }
}

/// Unified scheduled-job row state (plan §7.5, exhibit 8).
///
/// SQL and KV lowerers use this shared decision for row size and presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRowState {
    Present,
    KeyTooLarge,
    ValueTooLarge,
    Missing,
}

/// Compute one row's state from raw bytes and the canonical caps.
#[must_use]
pub fn job_row_state(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    max_key_bytes: usize,
    max_value_bytes: usize,
) -> JobRowState {
    let Some(key) = key else {
        return JobRowState::Missing;
    };
    if key.len() > max_key_bytes {
        return JobRowState::KeyTooLarge;
    }
    match value {
        None => JobRowState::Missing,
        Some(value) if value.len() > max_value_bytes => JobRowState::ValueTooLarge,
        Some(_) => JobRowState::Present,
    }
}
