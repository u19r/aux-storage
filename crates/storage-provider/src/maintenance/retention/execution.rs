use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};

use storage_types::{StorageResult, SystemStreamScope, TimestampMillis};
use stream_provider::{
    SystemStreamTrimContinuation, SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
};

use super::{
    StreamDurationTrimConfig, StreamDurationTrimStats, StreamRetentionRepository,
    StreamRetentionWave, StreamTrimDueMarker, SystemStreamTrimRepository,
};

pub struct SystemStreamTrimExecutor<'a> {
    repository: &'a dyn SystemStreamTrimRepository,
    page_limit: usize,
}

impl<'a> SystemStreamTrimExecutor<'a> {
    pub fn new(repository: &'a dyn SystemStreamTrimRepository, page_limit: usize) -> Self {
        Self {
            repository,
            page_limit,
        }
    }

    #[tracing::instrument(
        name = "system_stream_operation",
        skip_all,
        fields(operation = "TrimSystemStreamPage", stream_name = "system")
    )]
    pub async fn run_page(
        &self,
        scope: SystemStreamScope,
        cutoff_timestamp: TimestampMillis,
        continuation: Option<SystemStreamTrimContinuation>,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        let request = SystemStreamTrimPageRequest::try_new(
            scope,
            cutoff_timestamp,
            self.page_limit,
            continuation,
        )?;
        let page = self
            .repository
            .trim_system_stream_page(request.clone())
            .await?;
        page.validate_for(&request)?;
        Ok(page)
    }
}

pub struct StreamDurationTrimWorker<'a> {
    backend: &'a (dyn StreamRetentionRepository + Send + Sync),
    config: StreamDurationTrimConfig,
}

impl<'a> StreamDurationTrimWorker<'a> {
    pub fn new(
        backend: &'a (dyn StreamRetentionRepository + Send + Sync),
        config: StreamDurationTrimConfig,
    ) -> Self {
        Self { backend, config }
    }

    pub async fn run_due_page(
        &self,
        due_before: TimestampMillis,
        now: TimestampMillis,
    ) -> StorageResult<StreamDurationTrimStats> {
        let started_at = Instant::now();
        let mut stats = StreamDurationTrimStats::default();
        // Respect the caller's due window: markers due in the future must not
        // be eagerly finished. Eager finishing deletes future markers and
        // nulls fresh tables' next-due state, which breaks bootstrap
        // admission's pristine-baseline check and disables future trim
        // scheduling. Tests that need trim now must backdate trim state.
        let markers = self
            .backend
            .list_due_stream_trim_markers(due_before, usize::from(self.config.marker_page_size))
            .await?;
        stats.marker_page_full = markers.len() == usize::from(self.config.marker_page_size);
        let mut scopes = BTreeMap::<String, VecDeque<StreamTrimDueMarker>>::new();
        for marker in markers {
            scopes
                .entry(marker.scope.scope_id.clone())
                .or_default()
                .push_back(marker);
        }
        while !scopes.is_empty() {
            let markers = Self::take_marker_wave(&mut scopes);
            stats += self
                .backend
                .trim_stream_marker_wave(StreamRetentionWave {
                    markers,
                    due_before,
                    observed_at: now,
                    page_limit: self.config.stream_page_size,
                })
                .await?;
            scopes.retain(|_, markers| !markers.is_empty());
        }
        stats.runtime_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(stats)
    }

    fn take_marker_wave(
        scopes: &mut BTreeMap<String, VecDeque<StreamTrimDueMarker>>,
    ) -> Vec<StreamTrimDueMarker> {
        scopes
            .values_mut()
            .filter_map(VecDeque::pop_front)
            .collect()
    }
}
