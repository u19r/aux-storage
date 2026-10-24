use std::num::{NonZeroU64, NonZeroUsize};

use scheduled_bg_jobs::{JobId, Namespace};
use serde::{Deserialize, Serialize};
use storage_common::{
    TtlConfigRecord, TtlMaintenanceCursor, TtlPolicyGeneration, TtlSweepPageResult,
};
use storage_types::{
    KeyAttributes, Revision, StorageError, StorageResult, TableName, TimeToLiveStatus,
    TimestampMillis, WireItem,
    maintenance::{StepResult, TargetIncarnation, TaskAuthority, TaskProgress},
};

const EXPIRATION_CURSOR_PREFIX: &str = "ttl-maintenance-v1:";

#[derive(PartialEq, Eq)]
pub struct ExpirationCandidateCursor {
    /// TTL value from the last physical index row returned by the provider.
    pub expiration_seconds: i64,
    /// Opaque encoded item key used as the second half of the physical index
    /// ordering.
    pub ttl_item_key: String,
}

impl TryFrom<(i64, String)> for ExpirationCandidateCursor {
    type Error = StorageError;

    fn try_from((expiration_seconds, ttl_item_key): (i64, String)) -> StorageResult<Self> {
        if ttl_item_key.is_empty() {
            return Err(StorageError::validation(
                "expiration cursor TTL item key must not be empty",
            ));
        }
        Ok(Self {
            expiration_seconds,
            ttl_item_key,
        })
    }
}

impl TryFrom<&TtlMaintenanceCursor> for ExpirationCandidateCursor {
    type Error = StorageError;

    fn try_from(cursor: &TtlMaintenanceCursor) -> StorageResult<Self> {
        let value = cursor
            .token
            .strip_prefix(EXPIRATION_CURSOR_PREFIX)
            .ok_or_else(|| StorageError::validation("invalid expiration cursor version"))?;
        let (expiration_seconds, ttl_item_key) = value
            .split_once(':')
            .ok_or_else(|| StorageError::validation("invalid expiration cursor shape"))?;
        let expiration_seconds = expiration_seconds
            .parse::<i64>()
            .map_err(|_| StorageError::validation("invalid expiration cursor timestamp"))?;
        Self::try_from((expiration_seconds, ttl_item_key.to_owned()))
    }
}

impl From<ExpirationCandidateCursor> for String {
    fn from(cursor: ExpirationCandidateCursor) -> Self {
        format!(
            "{EXPIRATION_CURSOR_PREFIX}{}:{}",
            cursor.expiration_seconds, cursor.ttl_item_key
        )
    }
}

impl From<ExpirationCandidateCursor> for TtlMaintenanceCursor {
    fn from(cursor: ExpirationCandidateCursor) -> Self {
        Self {
            token: String::from(cursor),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ExpirationRequest {
    pub destination: Namespace,
    pub job: JobId,
    pub fence: NonZeroU64,
    pub requested_cutoff: TimestampMillis,
    pub candidate_limit: NonZeroUsize,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpirationProgress {
    pub cutoff: TimestampMillis,
    pub position: ExpirationPosition,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpirationPosition {
    Start,
    Active {
        table_name: TableName,
        table_incarnation: TargetIncarnation,
        policy_generation: TtlPolicyGeneration,
        cursor: TtlMaintenanceCursor,
    },
    After {
        table_name: TableName,
    },
}

#[derive(PartialEq, Eq)]
pub(super) enum ExpirationStart {
    Evaluate(ExpirationEvaluation),
    Superseded,
    BindingMismatch,
}

#[derive(PartialEq, Eq)]
pub(super) struct ExpirationEvaluation {
    pub(super) authority: TaskAuthority,
    pub(super) expected_revision: Option<Revision>,
    pub(super) progress: ExpirationProgress,
}

#[derive(PartialEq, Eq)]
pub enum ExpirationTargetRead {
    First,
    Exact {
        table_name: TableName,
        table_incarnation: TargetIncarnation,
    },
    After {
        table_name: TableName,
    },
}

#[derive(Clone)]
pub struct ExpirationTarget {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub ttl_attribute: String,
    pub status: TimeToLiveStatus,
    pub policy_generation: TtlPolicyGeneration,
}

#[derive(PartialEq, Eq)]
pub(super) struct ExpirationPolicy {
    pub(super) ttl_attribute: String,
    pub(super) status: TimeToLiveStatus,
    pub(super) policy_generation: TtlPolicyGeneration,
    pub(super) cursor: Option<TtlMaintenanceCursor>,
}

#[derive(PartialEq, Eq)]
pub(super) struct ExpectedExpirationPolicy {
    pub(super) ttl_attribute: String,
    pub(super) policy_generation: TtlPolicyGeneration,
    pub(super) cursor: Option<TtlMaintenanceCursor>,
}

pub(super) enum ExpirationTargetDecision {
    Read(ExpirationScanRead),
    Persist(ExpirationChange),
}

#[derive(PartialEq, Eq)]
pub struct ExpirationScanRead {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub ttl_attribute: String,
    pub policy_generation: TtlPolicyGeneration,
    pub cutoff_seconds: i64,
    pub cursor: Option<TtlMaintenanceCursor>,
    pub limit: NonZeroUsize,
}

pub struct ExpirationScanPage {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub policy_generation: TtlPolicyGeneration,
    pub table_info: storage_types::BoundTableInfo,
    pub rows: Vec<ExpirationIndexLocator>,
    pub more: bool,
}

/// The composite key of one physical TTL-index row.
///
/// This is retained through evaluation so persistence can remove the exact row
/// that was scanned, even when the item's current TTL value no longer matches
/// it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpirationIndexLocator {
    /// TTL value stored in the index, which is also its first ordering column.
    pub indexed_expiration_seconds: i64,
    /// Opaque encoded item key stored as the index's second ordering column.
    pub ttl_item_key: String,
}

pub(super) struct ExpirationPagePlan {
    pub(super) candidates: Vec<ExpirationScannedCandidate>,
    pub(super) next_cursor: Option<TtlMaintenanceCursor>,
}

pub(super) enum ExpirationScanDecision {
    Read(ExpirationCurrentRead),
    Persist(ExpirationChange),
}

pub struct ExpirationCurrentRead {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub table_info: storage_types::BoundTableInfo,
    pub policy_generation: TtlPolicyGeneration,
    pub batch: ExpirationCandidateBatch,
    pub next_cursor: Option<TtlMaintenanceCursor>,
}

pub struct ExpirationCandidateBatch {
    pub ttl_attribute: String,
    pub cutoff_seconds: i64,
    pub candidates: Vec<ExpirationScannedCandidate>,
}

pub struct ExpirationScannedCandidate {
    /// Physical index row consumed by the page and removed even when the item
    /// is stale or absent.
    pub index_locator: ExpirationIndexLocator,
    /// Decoded logical key used to observe the current item and request its
    /// native deletion.
    pub key: KeyAttributes,
}

pub struct ExpirationCurrentObservation {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub policy_generation: TtlPolicyGeneration,
    pub batch: ExpirationCandidateObservation,
}

pub struct ExpirationCandidateObservation {
    pub candidate_count: usize,
    pub present: Vec<ExpirationObservedItem>,
}

pub struct ExpirationObservedItem {
    /// Position in the requested candidate batch; observations remain dense and
    /// ordered.
    pub candidate_index: usize,
    pub item: WireItem,
}

pub struct ExpirationDelete {
    /// Decoded logical key selected for deletion by the shared expiration
    /// policy.
    pub key: KeyAttributes,
}

/// Atomic persistence input for one evaluated expiration page.
#[derive(Default)]
pub struct ExpirationDeletionBatch {
    /// Every physical index row consumed by the page, including stale entries.
    pub index_entries: Vec<ExpirationIndexLocator>,
    /// Logical keys whose current TTL still matches the scanned index value.
    pub items: Vec<ExpirationDelete>,
}

pub(super) struct ExpirationChange {
    pub(super) expected_revision: Option<Revision>,
    pub(super) next_progress: TaskProgress<ExpirationProgress>,
    pub(super) deletions: ExpirationDeletionBatch,
    pub(super) result: StepResult<()>,
}

pub struct ExpirationProgressWrite {
    pub current_target: TargetIncarnation,
    pub expected_revision: Option<Revision>,
    pub next_progress: TaskProgress<ExpirationProgress>,
    pub result: StepResult<()>,
}

pub struct ExpirationPageWrite {
    pub progress: ExpirationProgressWrite,
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub deletions: ExpirationDeletionBatch,
    pub captured_at: TimestampMillis,
}

pub struct DirectExpirationScanRead {
    pub config: TtlConfigRecord,
    pub table_info: storage_types::BoundTableInfo,
    pub table_name: TableName,
    pub cutoff_seconds: i64,
    pub cursor: Option<ExpirationCandidateCursor>,
    pub limit: NonZeroUsize,
    pub captured_at: TimestampMillis,
}

pub struct DirectExpirationCandidateRead {
    pub config: TtlConfigRecord,
    pub table_info: storage_types::BoundTableInfo,
    pub table_name: TableName,
    pub batch: ExpirationCandidateBatch,
    pub next_cursor: Option<TtlMaintenanceCursor>,
    pub captured_at: TimestampMillis,
}

pub struct DirectExpirationPageWrite {
    pub config: TtlConfigRecord,
    pub table_info: storage_types::BoundTableInfo,
    pub table_name: TableName,
    pub deletions: ExpirationDeletionBatch,
    pub captured_at: TimestampMillis,
    pub result: TtlSweepPageResult,
}
