use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use storage_types::{
    ItemStreamVersion, KeyAttributes, StorageError, StorageResult, StreamItemId, StreamName,
    StreamRetentionDuration, TableName, TimestampMillis, maintenance::TargetIncarnation,
};

const MAX_STREAM_TRIM_PAGE_LIMIT: u32 = 10_000;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StreamTrimPageLimit(NonZeroU32);

impl TryFrom<usize> for StreamTrimPageLimit {
    type Error = StorageError;

    fn try_from(limit: usize) -> StorageResult<Self> {
        let limit = u32::try_from(limit)
            .map_err(|_| StorageError::validation("stream trim page limit exceeds its maximum"))?;
        Self::try_from(limit)
    }
}

impl TryFrom<u32> for StreamTrimPageLimit {
    type Error = StorageError;

    fn try_from(limit: u32) -> StorageResult<Self> {
        let limit = NonZeroU32::new(limit)
            .ok_or_else(|| StorageError::validation("stream trim page limit must be positive"))?;
        if limit.get() > MAX_STREAM_TRIM_PAGE_LIMIT {
            return Err(StorageError::validation(
                "stream trim page limit exceeds its maximum",
            ));
        }
        Ok(Self(limit))
    }
}

impl From<StreamTrimPageLimit> for u32 {
    fn from(limit: StreamTrimPageLimit) -> Self {
        limit.0.get()
    }
}

impl From<StreamTrimPageLimit> for usize {
    fn from(limit: StreamTrimPageLimit) -> Self {
        limit.0.get() as usize
    }
}

impl From<StreamTrimPageLimit> for i64 {
    fn from(limit: StreamTrimPageLimit) -> Self {
        Self::from(limit.0.get())
    }
}

pub struct TableStreamDurationPlan {
    pub table_name: TableName,
    pub policy_version: u64,
    pub retention: StreamRetentionDuration,
    pub default_item_retention: StreamRetentionDuration,
    pub trim_state: StreamTrimState,
    pub due_marker: Option<StreamTrimDueMarker>,
}

/// Facts needed to plan an item retention policy. The shared planner derives
/// effective duration and policy identity; callers cannot supply either.
pub struct ItemStreamDurationInput {
    pub table_name: TableName,
    pub scope_id: String,
    pub key_hash: String,
    pub requested_retention: StreamRetentionDuration,
    pub table_retention: StreamRetentionDuration,
    pub updated_at: TimestampMillis,
}

pub struct ItemStreamTtlIntent {
    pub table_name: TableName,
    pub item_key: KeyAttributes,
    pub retention: StreamRetentionDuration,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTrimScopeKind {
    Table,
    Item,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimScope {
    pub kind: StreamTrimScopeKind,
    pub scope_id: String,
    pub table_name: TableName,
    pub item_key_hash: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimState {
    pub scope: StreamTrimScope,
    pub policy_version: u64,
    pub retention: StreamRetentionDuration,
    pub effective_retention: StreamRetentionDuration,
    pub next_due_at: Option<TimestampMillis>,
    pub oldest_retained_version: Option<ItemStreamVersion>,
    pub oldest_retained_timestamp: Option<TimestampMillis>,
    pub latest_version: Option<ItemStreamVersion>,
    pub latest_timestamp: Option<TimestampMillis>,
    pub updated_at: TimestampMillis,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimDueMarker {
    pub due_bucket: TimestampMillis,
    pub scope: StreamTrimScope,
    pub policy_version: u64,
}

pub enum StreamTrimMarkerOutcome {
    Current,
    Stale,
    Forever,
}

#[derive(Clone, Copy)]
pub enum StreamTrimDeletionBound {
    Unbounded,
    Inclusive(StreamItemId),
    DeleteNone,
}

/// Bounded observations from the transaction that will delete item history.
/// `at_reference` contains the exact referenced image and its predecessor.
pub struct ItemHistoryRetention {
    pub latest: Option<StreamItemId>,
    pub referenced: Option<StreamItemId>,
    pub at_reference: [Option<StreamItemId>; 2],
}

#[derive(Clone, Copy)]
pub struct StreamDurationTrimConfig {
    pub marker_page_size: StreamTrimPageLimit,
    pub stream_page_size: StreamTrimPageLimit,
}

impl StreamTrimPageLimit {
    // Literal defaults stay `const`-constructible without a fallible
    // conversion: both values are manifestly inside `1..=10_000`, so the
    // saturating addition cannot clamp.
    const DEFAULT_MARKER_PAGE_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(249);
    const DEFAULT_STREAM_PAGE_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);
}

impl Default for StreamDurationTrimConfig {
    fn default() -> Self {
        Self {
            marker_page_size: StreamTrimPageLimit(StreamTrimPageLimit::DEFAULT_MARKER_PAGE_LIMIT),
            stream_page_size: StreamTrimPageLimit(StreamTrimPageLimit::DEFAULT_STREAM_PAGE_LIMIT),
        }
    }
}

#[derive(Clone, Copy)]
pub struct StreamTrimBoundary {
    pub item_id: StreamItemId,
}

/// Native stream row that must be observed before a persisted retention hold
/// can participate in trimming.
pub enum StreamRetentionHold {
    /// The persisted zero boundary means the first row that is still retained.
    StreamHead,
    /// A non-zero boundary must still name this exact retained stream row.
    Position(StreamItemId),
}

pub struct StreamTrimScopeBoundaries {
    pub latest_item_id: Option<StreamItemId>,
    pub protected_boundary: Option<StreamTrimBoundary>,
    pub retained_table_pointer_boundary: Option<StreamTrimBoundary>,
}

impl StreamTrimScopeBoundaries {
    pub fn for_item_history(
        observation: ItemHistoryRetention,
    ) -> storage_types::StorageResult<Self> {
        Ok(Self {
            latest_item_id: observation.latest,
            protected_boundary: None,
            retained_table_pointer_boundary: observation
                .protected_predecessor()?
                .map(|item_id| StreamTrimBoundary { item_id }),
        })
    }

    pub fn unbounded() -> Self {
        Self {
            latest_item_id: None,
            protected_boundary: None,
            retained_table_pointer_boundary: None,
        }
    }
}

#[derive(Clone)]
pub struct StreamDurationTrimPageRequest {
    pub scope: StreamTrimScope,
    pub cutoff_timestamp: TimestampMillis,
    pub max_deleted_item_id: Option<StreamItemId>,
    pub page_limit: StreamTrimPageLimit,
}

pub struct StreamDurationTrimPageResult {
    pub deleted_rows: usize,
    pub first_remaining_version: Option<ItemStreamVersion>,
    pub first_remaining_timestamp: Option<TimestampMillis>,
}

/// One ordered physical stream row selected by a retention scan.
pub struct StreamTrimRow {
    pub item_id: StreamItemId,
    pub created_at: TimestampMillis,
}

/// Serialized retention state ready for one provider persistence operation.
pub struct StreamTrimStateRecord {
    pub scope_id: String,
    pub state_blob: Vec<u8>,
    pub updated_at: i64,
    pub next_marker: Option<StreamTrimDueRecord>,
}

impl TryFrom<StreamTrimStateWrite> for StreamTrimStateRecord {
    type Error = StorageError;

    fn try_from(write: StreamTrimStateWrite) -> StorageResult<Self> {
        let state = write.state;
        Ok(Self {
            scope_id: state.scope.scope_id.clone(),
            state_blob: storage_types::storage_serde::to_bytes(&state)?,
            updated_at: *state.updated_at,
            next_marker: write.next_marker.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Serialized retention due marker ready for one provider persistence
/// operation.
pub struct StreamTrimDueRecord {
    pub due_bucket: i64,
    pub scope_id: String,
    pub policy_version: i64,
    pub marker_blob: Vec<u8>,
}

impl TryFrom<StreamTrimDueRecord> for StreamTrimDueMarker {
    type Error = StorageError;

    fn try_from(record: StreamTrimDueRecord) -> StorageResult<Self> {
        let marker: Self = storage_types::storage_serde::from_bytes(&record.marker_blob)?;
        if i64::try_from(marker.policy_version).ok() != Some(record.policy_version)
            || *marker.due_bucket != record.due_bucket
            || marker.scope.scope_id != record.scope_id
        {
            return Err(StorageError::internal(
                "stream trim due marker row identity does not match its payload",
            ));
        }
        Ok(marker)
    }
}

impl TryFrom<StreamTrimDueMarker> for StreamTrimDueRecord {
    type Error = StorageError;

    fn try_from(marker: StreamTrimDueMarker) -> StorageResult<Self> {
        let policy_version = i64::try_from(marker.policy_version).map_err(|error| {
            StorageError::validation(format!("stream trim policy version is too large: {error}"))
        })?;
        Ok(Self {
            due_bucket: *marker.due_bucket,
            scope_id: marker.scope.scope_id.clone(),
            policy_version,
            marker_blob: storage_types::storage_serde::to_bytes(&marker)?,
        })
    }
}

/// Highest detached table-stream pointer observed for one item stream.
///
/// The item row may be reclaimed only when its own retention has elapsed and
/// no latest row, retained table pointer, or other live boundary protects it.
pub struct DetachedItemStreamRange {
    pub table_name: TableName,
    pub table_incarnation: TargetIncarnation,
    pub item_stream: StreamName,
    pub max_item_id: StreamItemId,
}

#[derive(Default)]
pub struct DetachedItemStreamRanges {
    pub(super) ranges: Vec<DetachedItemStreamRange>,
}

pub struct DetachedItemStreamObservation {
    pub range: DetachedItemStreamRange,
    pub scope: StreamTrimScope,
    pub custom_state: Option<StreamTrimState>,
    pub table_retention: StreamRetentionDuration,
    pub default_item_retention: StreamRetentionDuration,
    pub boundaries: StreamTrimScopeBoundaries,
}

pub struct DetachedItemStreamReclaim {
    pub request: StreamDurationTrimPageRequest,
    pub expected_table_incarnation: TargetIncarnation,
    pub expected_custom_state: Option<StreamTrimState>,
    pub expected_table_retention: StreamRetentionDuration,
    pub expected_default_item_retention: StreamRetentionDuration,
}

/// Exact physical stream family named by a decoded SystemStream pointer.
///
/// The pointer id identifies the SystemStream row, while the item id identifies
/// the target item-stream row named by that pointer.
pub struct PhysicalStreamPointer {
    pub pointer_item_id: StreamItemId,
    pub item_stream_item_id: StreamItemId,
    pub table_name: TableName,
    pub item_stream: StreamName,
}

/// Retention facts attached to the current incarnation of a detached item
/// stream.
pub struct DetachedItemStreamPolicy {
    pub table_incarnation: TargetIncarnation,
    pub table_retention: StreamRetentionDuration,
    pub default_item_retention: StreamRetentionDuration,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTrimStateWrite {
    pub state: StreamTrimState,
    pub next_marker: Option<StreamTrimDueMarker>,
}

#[derive(Clone)]
pub struct StreamTrimStateUpdate {
    pub expected_state: StreamTrimState,
    pub write: StreamTrimStateWrite,
}

#[derive(Default)]
pub struct StreamDurationTrimStats {
    pub runtime_ms: u64,
    pub due_markers_scanned: usize,
    pub marker_page_full: bool,
    pub stale_markers: usize,
    pub forever_markers: usize,
    pub missing_states: usize,
    pub protected_markers: usize,
    pub scopes_trimmed: usize,
    pub rows_deleted: usize,
    pub state_writes: usize,
}

pub enum StreamRetentionStateDecision {
    Finish(StreamRetentionFinish),
    ReadBoundaries {
        scope: StreamTrimScope,
        evaluation: StreamRetentionEvaluation,
    },
}

pub enum StreamRetentionBoundaryDecision {
    Finish(StreamRetentionFinish),
    Reclaim(StreamRetentionReclaim),
}

pub struct StreamRetentionEvaluation {
    pub(super) marker: StreamTrimDueMarker,
    pub(super) state: StreamTrimState,
    pub(super) due_before: TimestampMillis,
    pub(super) observed_at: TimestampMillis,
    pub(super) page_limit: StreamTrimPageLimit,
}

pub struct StreamRetentionReclaim {
    pub request: StreamDurationTrimPageRequest,
    pub(super) marker: StreamTrimDueMarker,
    pub(super) state: StreamTrimState,
    pub(super) deletion_bound: StreamTrimDeletionBound,
    pub(super) due_before: TimestampMillis,
    pub(super) observed_at: TimestampMillis,
}

pub struct StreamRetentionFinish {
    pub marker: StreamTrimDueMarker,
    pub update: Option<StreamTrimStateUpdate>,
    pub reason: StreamRetentionFinishReason,
}

/// Serialized comparison and replacement facts for one retention-marker finish.
pub struct StreamRetentionFinishRecord {
    pub due_bucket: i64,
    pub scope_id: String,
    pub policy_version: i64,
    pub expected_state_blob: Option<Vec<u8>>,
    pub valid_update: bool,
    pub state_write: Option<StreamTrimStateRecord>,
}

impl TryFrom<StreamRetentionFinish> for StreamRetentionFinishRecord {
    type Error = StorageError;

    fn try_from(finish: StreamRetentionFinish) -> StorageResult<Self> {
        let marker = finish.marker;
        let policy_version = i64::try_from(marker.policy_version).map_err(|error| {
            StorageError::validation(format!("stream trim policy version is too large: {error}"))
        })?;
        let (expected_state_blob, valid_update, state_write) = match finish.update {
            Some(update) => {
                let valid = update.expected_state.scope == marker.scope
                    && update.write.state.scope == marker.scope
                    && update.expected_state.policy_version == marker.policy_version
                    && update.write.state.policy_version == marker.policy_version
                    && update.write.next_marker.as_ref().is_none_or(|next| {
                        next.scope == marker.scope && next.policy_version == marker.policy_version
                    });
                let expected = storage_types::storage_serde::to_bytes(&update.expected_state)?;
                (Some(expected), valid, Some(update.write.try_into()?))
            }
            None => (None, true, None),
        };
        Ok(Self {
            due_bucket: *marker.due_bucket,
            scope_id: marker.scope.scope_id,
            policy_version,
            expected_state_blob,
            valid_update,
            state_write,
        })
    }
}

pub enum StreamRetentionFinishReason {
    MissingState,
    StaleMarker,
    Forever,
    Protected,
    Page { deleted_rows: usize },
}
