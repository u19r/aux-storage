use serde::{Deserialize, Serialize};
use storage_types::{ItemStreamVersion, StreamRetentionDuration, TableName, TimestampMillis};

pub const STREAM_TRIM_DUE_BUCKET_MILLIS: i64 = 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTrimScopeKind {
    Table,
    Item,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimScope {
    pub kind: StreamTrimScopeKind,
    pub scope_id: String,
    pub table_name: TableName,
    pub item_key_hash: Option<String>,
}

impl StreamTrimScope {
    pub fn table(scope_id: impl Into<String>, table_name: TableName) -> Self {
        Self {
            kind: StreamTrimScopeKind::Table,
            scope_id: scope_id.into(),
            table_name,
            item_key_hash: None,
        }
    }

    pub fn item(
        scope_id: impl Into<String>,
        table_name: TableName,
        item_key_hash: impl Into<String>,
    ) -> Self {
        Self {
            kind: StreamTrimScopeKind::Item,
            scope_id: scope_id.into(),
            table_name,
            item_key_hash: Some(item_key_hash.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl StreamTrimState {
    pub fn has_finite_due_work(&self) -> bool {
        self.effective_retention != StreamRetentionDuration::Forever && self.next_due_at.is_some()
    }

    pub fn marker_matches(&self, marker: &StreamTrimDueMarker) -> bool {
        self.scope.kind == marker.scope.kind
            && self.scope.scope_id == marker.scope.scope_id
            && self.policy_version == marker.policy_version
    }

    pub fn validated_marker_outcome(
        &self,
        marker: &StreamTrimDueMarker,
    ) -> StreamTrimMarkerOutcome {
        if !self.marker_matches(marker) {
            return StreamTrimMarkerOutcome::Stale;
        }
        if self.effective_retention == StreamRetentionDuration::Forever {
            return StreamTrimMarkerOutcome::Forever;
        }
        StreamTrimMarkerOutcome::Current
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTrimDueMarker {
    pub due_bucket: TimestampMillis,
    pub scope: StreamTrimScope,
    pub policy_version: u64,
}

impl StreamTrimDueMarker {
    pub fn new(due_at: TimestampMillis, scope: StreamTrimScope, policy_version: u64) -> Self {
        Self {
            due_bucket: due_bucket_for(due_at, STREAM_TRIM_DUE_BUCKET_MILLIS),
            scope,
            policy_version,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimMarkerOutcome {
    Current,
    Stale,
    Forever,
}

pub fn due_bucket_for(due_at: TimestampMillis, bucket_millis: i64) -> TimestampMillis {
    debug_assert!(bucket_millis > 0);
    let due_ms = due_at.timestamp_millis();
    TimestampMillis::from_timestamp(due_ms.div_euclid(bucket_millis) * bucket_millis)
}

pub fn next_due_from_first_remaining(
    first_remaining_timestamp: Option<TimestampMillis>,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    let first_remaining_timestamp = first_remaining_timestamp?;
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(first_remaining_timestamp + (i64::from(hours) * 60 * 60 * 1000))
        }
    }
}
