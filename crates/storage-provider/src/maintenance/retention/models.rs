use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use storage_types::{
    StorageError, StorageResult, StreamRetentionDuration, TableName, TimestampMillis,
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimScope {
    pub scope_id: String,
    pub table_name: TableName,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimState {
    pub scope: StreamTrimScope,
    pub policy_version: u64,
    pub frontier: super::TableHistoryTrimFrontier,
    pub retention: StreamRetentionDuration,
    pub next_due_at: Option<TimestampMillis>,
    /// Last policy change or completed cleanup turn. Image-budget deferral
    /// keeps this age so the next selected wave prioritizes waiting tables.
    pub updated_at: TimestampMillis,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimDueMarker {
    pub due_at: TimestampMillis,
    pub scope: StreamTrimScope,
    pub policy_version: u64,
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

/// Serialized retention state ready for one provider persistence operation.
pub struct StreamTrimStateRecord {
    pub scope_id: String,
    pub state_blob: Vec<u8>,
    pub updated_at: i64,
    pub next_marker: Option<StreamTrimDueRecord>,
    pub replaced_marker: Option<StreamTrimDueRecord>,
}

impl TryFrom<StreamTrimStateWrite> for StreamTrimStateRecord {
    type Error = StorageError;

    fn try_from(write: StreamTrimStateWrite) -> StorageResult<Self> {
        let state = write.state;
        state.require_valid_frontier()?;
        Ok(Self {
            scope_id: state.scope.scope_id.clone(),
            state_blob: storage_types::storage_serde::to_bytes(&state)?,
            updated_at: *state.updated_at,
            next_marker: write.next_marker.map(TryInto::try_into).transpose()?,
            replaced_marker: write.replaced_marker.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Serialized retention due marker ready for one provider persistence
/// operation.
pub struct StreamTrimDueRecord {
    pub due_at: i64,
    pub scope_id: String,
    pub policy_version: i64,
    pub marker_blob: Vec<u8>,
}

impl TryFrom<StreamTrimDueRecord> for StreamTrimDueMarker {
    type Error = StorageError;

    fn try_from(record: StreamTrimDueRecord) -> StorageResult<Self> {
        let marker: Self = storage_types::storage_serde::from_bytes(&record.marker_blob)?;
        if i64::try_from(marker.policy_version).ok() != Some(record.policy_version)
            || *marker.due_at != record.due_at
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
            due_at: *marker.due_at,
            scope_id: marker.scope.scope_id.clone(),
            policy_version,
            marker_blob: storage_types::storage_serde::to_bytes(&marker)?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTrimStateWrite {
    pub state: StreamTrimState,
    pub next_marker: Option<StreamTrimDueMarker>,
    pub replaced_marker: Option<StreamTrimDueMarker>,
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
    pub scopes_trimmed: usize,
    pub rows_deleted: usize,
    pub state_writes: usize,
}

pub struct StreamRetentionFinish {
    pub marker: StreamTrimDueMarker,
    pub update: Option<StreamTrimStateUpdate>,
    pub reason: StreamRetentionFinishReason,
}

/// Serialized comparison and replacement facts for one retention-marker finish.
pub struct StreamRetentionFinishRecord {
    pub due_at: i64,
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
            due_at: *marker.due_at,
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
    Page { deleted_rows: usize },
}
