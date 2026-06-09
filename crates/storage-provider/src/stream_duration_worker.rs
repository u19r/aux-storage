use std::time::Instant;

use async_trait::async_trait;
use storage_types::{
    ItemStreamVersion, StorageResult, StreamItemId, StreamRetentionDuration, TimestampMillis,
};

use crate::{
    StreamTrimDueMarker, StreamTrimMarkerOutcome, StreamTrimScope, StreamTrimScopeKind,
    StreamTrimState, next_due_from_first_remaining,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDurationTrimConfig {
    pub marker_page_size: usize,
    pub stream_page_size: usize,
}

impl Default for StreamDurationTrimConfig {
    fn default() -> Self {
        Self {
            marker_page_size: 250,
            stream_page_size: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTrimBoundary {
    pub item_id: StreamItemId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTrimScopeBoundaries {
    pub latest_item_id: Option<StreamItemId>,
    pub protected_boundary: Option<StreamTrimBoundary>,
    pub retained_table_pointer_boundary: Option<StreamTrimBoundary>,
}

impl StreamTrimScopeBoundaries {
    pub fn unbounded() -> Self {
        Self {
            latest_item_id: None,
            protected_boundary: None,
            retained_table_pointer_boundary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDurationTrimPageRequest {
    pub scope: StreamTrimScope,
    pub cutoff_timestamp: TimestampMillis,
    pub max_deleted_item_id: Option<StreamItemId>,
    pub page_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDurationTrimPageResult {
    pub deleted_rows: usize,
    pub first_remaining_version: Option<ItemStreamVersion>,
    pub first_remaining_timestamp: Option<TimestampMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTrimStateWrite {
    pub state: StreamTrimState,
    pub next_marker: Option<StreamTrimDueMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

impl StreamDurationTrimStats {
    pub fn did_work(self) -> bool {
        self.rows_deleted > 0
            || self.stale_markers > 0
            || self.forever_markers > 0
            || self.missing_states > 0
    }
}

#[async_trait]
pub trait StreamDurationTrimBackend {
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>>;

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>>;

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries>;

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult>;

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult>;

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()>;
}

pub struct StreamDurationTrimWorker<B> {
    backend: B,
    config: StreamDurationTrimConfig,
}

impl<B> StreamDurationTrimWorker<B> {
    pub fn new(backend: B, config: StreamDurationTrimConfig) -> Self {
        Self { backend, config }
    }
}

impl<B> StreamDurationTrimWorker<B>
where B: StreamDurationTrimBackend + Send + Sync
{
    pub async fn run_due_page(
        &self,
        due_before: TimestampMillis,
        now: TimestampMillis,
    ) -> StorageResult<StreamDurationTrimStats> {
        let started_at = Instant::now();
        let mut stats = StreamDurationTrimStats::default();
        let markers = self
            .backend
            .list_due_stream_trim_markers(due_before, self.config.marker_page_size)
            .await?;
        stats.marker_page_full = markers.len() == self.config.marker_page_size;

        for marker in markers {
            stats.due_markers_scanned = stats.due_markers_scanned.saturating_add(1);
            self.process_marker(marker, now, &mut stats).await?;
        }

        stats.runtime_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(stats)
    }

    async fn process_marker(
        &self,
        marker: StreamTrimDueMarker,
        now: TimestampMillis,
        stats: &mut StreamDurationTrimStats,
    ) -> StorageResult<()> {
        let Some(mut state) = self.backend.load_stream_trim_state(&marker.scope).await? else {
            stats.missing_states = stats.missing_states.saturating_add(1);
            self.backend.finish_stream_trim_marker(marker, None).await?;
            return Ok(());
        };

        match state.validated_marker_outcome(&marker) {
            StreamTrimMarkerOutcome::Stale => {
                stats.stale_markers = stats.stale_markers.saturating_add(1);
                self.backend.finish_stream_trim_marker(marker, None).await?;
                return Ok(());
            }
            StreamTrimMarkerOutcome::Forever => {
                stats.forever_markers = stats.forever_markers.saturating_add(1);
                state.next_due_at = None;
                self.backend
                    .finish_stream_trim_marker(
                        marker,
                        Some(StreamTrimStateWrite {
                            state,
                            next_marker: None,
                        }),
                    )
                    .await?;
                stats.state_writes = stats.state_writes.saturating_add(1);
                return Ok(());
            }
            StreamTrimMarkerOutcome::Current => {}
        }

        let Some(cutoff_timestamp) = cutoff_timestamp(now, state.effective_retention) else {
            stats.forever_markers = stats.forever_markers.saturating_add(1);
            self.backend.finish_stream_trim_marker(marker, None).await?;
            return Ok(());
        };

        let boundaries = self
            .backend
            .load_stream_trim_boundaries(&state.scope)
            .await?;
        let max_deleted_item_id = max_deleted_item_id(&boundaries);
        if max_deleted_item_id.is_none() && has_any_boundary(&boundaries) {
            stats.protected_markers = stats.protected_markers.saturating_add(1);
        }

        let request = StreamDurationTrimPageRequest {
            scope: state.scope.clone(),
            cutoff_timestamp,
            max_deleted_item_id,
            page_limit: self.config.stream_page_size,
        };
        let page = match state.scope.kind {
            StreamTrimScopeKind::Table => self.backend.trim_table_stream_page(request).await?,
            StreamTrimScopeKind::Item => self.backend.trim_item_stream_page(request).await?,
        };

        stats.rows_deleted = stats.rows_deleted.saturating_add(page.deleted_rows);
        if page.deleted_rows > 0 {
            stats.scopes_trimmed = stats.scopes_trimmed.saturating_add(1);
        }

        state.oldest_retained_version = page.first_remaining_version;
        state.oldest_retained_timestamp = page.first_remaining_timestamp;
        state.next_due_at = next_due_from_first_remaining(
            page.first_remaining_timestamp,
            state.effective_retention,
        );
        state.updated_at = now;

        let next_marker = state.next_due_at.map(|due_at| {
            StreamTrimDueMarker::new(due_at, state.scope.clone(), state.policy_version)
        });
        self.backend
            .finish_stream_trim_marker(marker, Some(StreamTrimStateWrite { state, next_marker }))
            .await?;
        stats.state_writes = stats.state_writes.saturating_add(1);

        Ok(())
    }
}

fn cutoff_timestamp(
    now: TimestampMillis,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(now - (i64::from(hours) * 60 * 60 * 1000))
        }
    }
}

fn max_deleted_item_id(boundaries: &StreamTrimScopeBoundaries) -> Option<StreamItemId> {
    [
        boundaries.latest_item_id,
        boundaries
            .protected_boundary
            .as_ref()
            .map(|boundary| boundary.item_id),
        boundaries
            .retained_table_pointer_boundary
            .as_ref()
            .map(|boundary| boundary.item_id),
    ]
    .into_iter()
    .flatten()
    .filter_map(previous_item_id)
    .min()
}

fn previous_item_id(item_id: StreamItemId) -> Option<StreamItemId> {
    let mut bytes = *item_id.as_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            return Some(StreamItemId::from(bytes));
        }
        *byte = u8::MAX;
    }
    None
}

fn has_any_boundary(boundaries: &StreamTrimScopeBoundaries) -> bool {
    boundaries.latest_item_id.is_some()
        || boundaries.protected_boundary.is_some()
        || boundaries.retained_table_pointer_boundary.is_some()
}
