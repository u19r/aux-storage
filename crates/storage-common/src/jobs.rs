//! Storage-owned scheduled maintenance job identities and cadence settings.
use std::{fmt, str::FromStr, time::Duration};

use scheduled_bg_jobs::{JobId, JobKind, QueueName};

use crate::{JobIntervalMillis, constants::DEFAULT_JOB_INTERVALS, errors::StorageJobParseError};

/// One bounded storage maintenance operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageJob {
    GsiBackfill,
    PartitionReconciliation,
    PubsubStaleSubscriptionSweep,
    QueueMessageCleanup,
    StreamTrim,
    TableReclaim,
    TransactReceiptTrim,
    TtlSweep,
}

impl StorageJob {
    pub const ALL: [Self; 8] = [
        Self::GsiBackfill,
        Self::PartitionReconciliation,
        Self::PubsubStaleSubscriptionSweep,
        Self::QueueMessageCleanup,
        Self::StreamTrim,
        Self::TableReclaim,
        Self::TransactReceiptTrim,
        Self::TtlSweep,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GsiBackfill => "gsi-backfill",
            Self::PartitionReconciliation => "partition-reconciliation",
            Self::PubsubStaleSubscriptionSweep => "pubsub-stale-subscription-sweep",
            Self::QueueMessageCleanup => "queue-message-cleanup",
            Self::StreamTrim => "stream-trim",
            Self::TableReclaim => "table-reclaim",
            Self::TransactReceiptTrim => "transact-receipt-trim",
            Self::TtlSweep => "ttl-sweep",
        }
    }

    /// Stable persisted identity of this namespace's maintenance controller.
    #[must_use]
    pub fn job_id(self) -> JobId {
        let identity = match self {
            Self::GsiBackfill => 1,
            Self::PartitionReconciliation => 2,
            Self::PubsubStaleSubscriptionSweep => 8,
            Self::QueueMessageCleanup => 3,
            Self::StreamTrim => 4,
            Self::TableReclaim => 5,
            Self::TtlSweep => 6,
            Self::TransactReceiptTrim => 7,
        };
        JobId::from([identity; 16])
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
            Self::PubsubStaleSubscriptionSweep => "storage-pubsub-stale-subscription-sweep",
            Self::QueueMessageCleanup => "storage-queue-message-cleanup",
            Self::StreamTrim => "storage-stream-trim",
            Self::TableReclaim => "storage-table-reclaim",
            Self::TransactReceiptTrim => "storage-transact-receipt-trim",
            Self::TtlSweep => "storage-ttl-sweep",
        };
        QueueName::try_new(queue).unwrap_or_else(|_| unreachable!("static queue name"))
    }

    #[must_use]
    pub const fn interval(self, intervals: DatabaseJobIntervals) -> Duration {
        let millis = match self {
            Self::GsiBackfill => intervals.gsi_backfill_interval_ms.0,
            Self::PartitionReconciliation => intervals.partition_reconciliation_interval_ms.0,
            Self::PubsubStaleSubscriptionSweep => {
                intervals.pubsub_stale_subscription_sweep_interval_ms.0
            }
            Self::QueueMessageCleanup => intervals.queue_message_cleanup_interval_ms.0,
            Self::StreamTrim => intervals.stream_trim_interval_ms.0,
            Self::TableReclaim => intervals.table_reclaim_interval_ms.0,
            Self::TransactReceiptTrim => intervals.transact_receipt_trim_interval_ms.0,
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
            "pubsub-stale-subscription-sweep" => Ok(Self::PubsubStaleSubscriptionSweep),
            "queue-message-cleanup" => Ok(Self::QueueMessageCleanup),
            "stream-trim" => Ok(Self::StreamTrim),
            "table-reclaim" => Ok(Self::TableReclaim),
            "transact-receipt-trim" => Ok(Self::TransactReceiptTrim),
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
    pub pubsub_stale_subscription_sweep_interval_ms: JobIntervalMillis,
    pub transact_receipt_trim_interval_ms: JobIntervalMillis,
}

impl Default for DatabaseJobIntervals {
    fn default() -> Self {
        DEFAULT_JOB_INTERVALS
    }
}
