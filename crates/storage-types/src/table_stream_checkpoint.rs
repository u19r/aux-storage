//! An incarnation-qualified continuation carries independent lineage positions.

use std::{collections::BTreeSet, fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::{
    StorageError, StorageResult, SystemStreamPosition, SystemStreamScope, TableHistoryId,
    TimestampMillis,
};

/// Fresh horizon scans skip only rows already expired at their initial read.
/// Later expiry is a gap, even while that initial scan is still paginating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableStreamReadProgress {
    pub next_lane: usize,
    pub initial_visibility_cutoff: Option<TimestampMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CheckpointWire", into = "CheckpointWire")]
pub struct TableStreamCheckpoint {
    history_id: TableHistoryId,
    source_scope: SystemStreamScope,
    positions: Vec<SystemStreamPosition>,
    progress: TableStreamReadProgress,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointWire {
    history_id: TableHistoryId,
    source_scope: SystemStreamScope,
    positions: Vec<SystemStreamPosition>,
    progress: TableStreamReadProgress,
}

impl TableStreamCheckpoint {
    pub fn try_new(
        history_id: TableHistoryId,
        source_scope: SystemStreamScope,
        positions: Vec<SystemStreamPosition>,
        progress: TableStreamReadProgress,
    ) -> StorageResult<Self> {
        let mut identities = BTreeSet::new();
        if history_id.as_ref() == &[0; 16]
            || positions.is_empty()
            || positions.len() > crate::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES
            || progress.next_lane >= positions.len()
        {
            return Err(invalid_checkpoint());
        }
        for position in &positions {
            position
                .offset()
                .order()
                .map_err(|_| invalid_checkpoint())?;
            if !identities.insert((position.shard_id(), position.lineage_id())) {
                return Err(invalid_checkpoint());
            }
        }
        Ok(Self {
            history_id,
            source_scope,
            positions,
            progress,
        })
    }

    pub fn history_id(&self) -> TableHistoryId {
        self.history_id
    }
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }
    pub fn positions(&self) -> &[SystemStreamPosition] {
        &self.positions
    }
    pub fn next_lane(&self) -> usize {
        self.progress.next_lane
    }
    pub fn initial_visibility_cutoff(&self) -> Option<TimestampMillis> {
        self.progress.initial_visibility_cutoff
    }
}

impl TryFrom<CheckpointWire> for TableStreamCheckpoint {
    type Error = StorageError;
    fn try_from(wire: CheckpointWire) -> StorageResult<Self> {
        Self::try_new(
            wire.history_id,
            wire.source_scope,
            wire.positions,
            wire.progress,
        )
    }
}
impl From<TableStreamCheckpoint> for CheckpointWire {
    fn from(checkpoint: TableStreamCheckpoint) -> Self {
        Self {
            history_id: checkpoint.history_id,
            source_scope: checkpoint.source_scope,
            positions: checkpoint.positions,
            progress: checkpoint.progress,
        }
    }
}

impl fmt::Display for TableStreamCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = serde_json::to_vec(self).map_err(|_| fmt::Error)?;
        write!(formatter, "table1.{}", URL_SAFE_NO_PAD.encode(bytes))
    }
}

impl FromStr for TableStreamCheckpoint {
    type Err = StorageError;
    fn from_str(value: &str) -> StorageResult<Self> {
        if value.len() > crate::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES * 1024 {
            return Err(invalid_checkpoint());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(
                value
                    .strip_prefix("table1.")
                    .ok_or_else(invalid_checkpoint)?,
            )
            .map_err(|_| invalid_checkpoint())?;
        let checkpoint: Self = serde_json::from_slice(&bytes).map_err(|_| invalid_checkpoint())?;
        if checkpoint.to_string() != value {
            return Err(invalid_checkpoint());
        }
        Ok(checkpoint)
    }
}

fn invalid_checkpoint() -> StorageError {
    StorageError::raw_validation("Invalid table stream checkpoint")
}
