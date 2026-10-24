use std::future::Future;

use async_trait::async_trait;
use storage_backfill::{
    GsiActivationObservation, GsiActivationState, GsiBuild, GsiBuildCandidates,
    GsiBuildProjectionObservation, GsiBuildTarget, GsiPageLimit, GsiProjection, GsiProjectionKey,
    GsiProjectionStateRead, GsiProjectionStateReadBatch, GsiReferencedHistory,
    GsiReferencedHistoryItem, GsiReferencedHistoryRead, GsiScanObservation, GsiScanSourcePage,
    GsiSourceProjection, GsiStreamObservation, GsiStreamSourcePage,
};
use storage_common::{TtlConfigRecord, retry::retry_native_conflicts};
use storage_types::{
    IndexName, StorageResult, SystemStreamCheckpoint, TableName,
    maintenance::{StepResult, TargetIncarnation, TaskProgress},
};

use super::{
    GsiBuildAdvance, GsiBuildRequest, GsiPort, GsiTransition, models::GsiBuildStart,
    transaction::GsiAttempt,
};
use crate::{
    DYNAMO_WRITE_RETRY, DynamoPort, GsiIndexReadiness, GsiReadinessRecord, GsiReadinessSnapshot,
};

/// The scheduler's wraparound decision is shared. Physical stores issue the
/// two independent bounded range reads together when a cursor is present.
pub trait GsiCandidateRead<Cursor> {
    fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> impl Future<Output = StorageResult<[Option<GsiBuild<Cursor>>; 2]>>;

    fn admits_gsi_candidate(
        &mut self,
        _build: &GsiBuild<Cursor>,
    ) -> impl Future<Output = StorageResult<bool>> {
        async { Ok(true) }
    }

    fn validate_gsi_candidate(
        &mut self,
        build: &GsiBuild<Cursor>,
    ) -> impl Future<Output = StorageResult<()>>;

    fn read_gsi_build_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> impl Future<Output = StorageResult<GsiBuildCandidates<Cursor>>>
    where
        Self: Sized,
    {
        async move {
            let [after_candidate, first_candidate] = self.scan_gsi_candidates(after).await?;
            let build = after_candidate.or(first_candidate);
            let mut candidates = GsiBuildCandidates::new();
            if let Some(build) = build {
                if !self.admits_gsi_candidate(&build).await? {
                    candidates.scanned_through = Some(build.target);
                    return Ok(candidates);
                }
                self.validate_gsi_candidate(&build).await?;
                candidates.select_next_build_candidate(build);
            }
            Ok(candidates)
        }
    }
}

/// Native transaction boundary for one GSI build step.
///
/// The default methods own observation assembly and lifecycle advancement.
/// Implementors supply only the typed database reads and writes required by
/// those shared operations.
pub trait GsiBuildTransaction {
    type Cursor: PartialEq + Send + Sync;

    fn advance_gsi_build(
        &mut self,
        request: &GsiBuildRequest,
    ) -> impl Future<Output = StorageResult<GsiBuildAdvance>>
    where
        Self: Sized,
    {
        async move {
            let (current_target, durable) = self.read_gsi_admission().await?;
            GsiBuildStart::new(current_target, durable, request)
                .advance(self)
                .await
        }
    }

    /// Evaluate one protected build using the same decisions for every caller.
    fn evaluate_gsi_build(
        &mut self,
        build: &storage_backfill::GsiBuild<Self::Cursor>,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<storage_backfill::GsiChange<Self::Cursor>>>
    where
        Self: Sized,
    {
        super::observation::evaluate(self, build, limit)
    }

    fn read_maintenance_identity(
        &mut self,
    ) -> impl Future<Output = StorageResult<TargetIncarnation>>;

    fn read_gsi_progress(
        &mut self,
    ) -> impl Future<
        Output = StorageResult<Option<TaskProgress<Option<storage_backfill::GsiBuildTarget>>>>,
    >;

    /// Observe independent authority and progress in one native read wave.
    /// The default keeps existing in-memory transaction fixtures usable.
    fn read_gsi_admission(
        &mut self,
    ) -> impl Future<
        Output = StorageResult<(
            TargetIncarnation,
            Option<TaskProgress<Option<storage_backfill::GsiBuildTarget>>>,
        )>,
    > {
        async move {
            let identity = self.read_maintenance_identity().await?;
            let progress = self.read_gsi_progress().await?;
            Ok((identity, progress))
        }
    }

    fn read_gsi_build_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> impl Future<Output = StorageResult<GsiBuildCandidates<Self::Cursor>>>;

    fn read_gsi_scan(
        &mut self,
        target: &GsiBuildTarget,
        after: Option<&Self::Cursor>,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiScanObservation<Self::Cursor>>>
    where
        Self: Sized,
    {
        async move {
            let page = self.read_gsi_scan_source_page(target, after, limit).await?;
            let sources = GsiSourceProjection::project_scanned_items(
                &page.table,
                &target.index_name,
                page.items,
            )?;
            let reads = GsiProjectionStateRead::plan(&sources);
            let current = self
                .read_gsi_projected_states(GsiProjectionStateReadBatch {
                    table: &page.table,
                    target,
                    sources: &sources,
                    reads: &reads,
                })
                .await?;
            Ok(GsiScanObservation {
                expected_after: page.expected_after,
                end: page.end,
                projections: GsiBuildProjectionObservation::attach_all(sources, current)?,
            })
        }
    }

    fn read_gsi_catch_up(
        &mut self,
        target: &GsiBuildTarget,
        after: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiStreamObservation>>
    where
        Self: Sized,
    {
        self.read_gsi_stream_observation(target, after, through, limit)
    }

    fn read_gsi_continuation(
        &mut self,
        target: &GsiBuildTarget,
        after: SystemStreamCheckpoint,
        pending_high: Option<SystemStreamCheckpoint>,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiStreamObservation>>
    where
        Self: Sized,
    {
        async move {
            let through = match pending_high {
                Some(high) => high,
                None => self.read_gsi_stream_tail().await?,
            };
            self.read_gsi_stream_observation(target, after, through, limit)
                .await
        }
    }

    fn read_gsi_activation(
        &mut self,
        target: &GsiBuildTarget,
        required_from: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<GsiActivationObservation>>
    where
        Self: Sized,
    {
        async move {
            self.validate_gsi_frontier(&required_from, &through).await?;
            let state = self.read_gsi_activation_state(target, through).await?;
            Ok(GsiActivationObservation {
                target_incarnation: state.target_incarnation,
                projected_through: state.projected_through,
            })
        }
    }

    fn read_gsi_stream_observation(
        &mut self,
        target: &GsiBuildTarget,
        after: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiStreamObservation>>
    where
        Self: Sized,
    {
        async move {
            let mut page = self
                .read_gsi_stream_source_page(target, &after, &through, limit)
                .await?;
            let history_reads = GsiReferencedHistoryRead::plan(&page.table, &page.candidates);
            let histories = GsiReferencedHistory::group(if history_reads.is_empty() {
                Vec::new()
            } else {
                self.read_gsi_referenced_histories(&history_reads).await?
            })?;
            GsiReferencedHistory::attach_all(&mut page.candidates, histories)?;
            let mut sources = Vec::new();
            for candidate in page.candidates {
                sources.extend(candidate.into_projections(&page.table, &target.index_name)?);
            }
            let reads = GsiProjectionStateRead::plan(&sources);
            let current = self
                .read_gsi_projected_states(GsiProjectionStateReadBatch {
                    table: &page.table,
                    target,
                    sources: &sources,
                    reads: &reads,
                })
                .await?;
            Ok(GsiStreamObservation {
                expected_cursor: after,
                through,
                end: page.end,
                projections: GsiBuildProjectionObservation::attach_all(sources, current)?,
            })
        }
    }

    fn read_gsi_scan_source_page(
        &mut self,
        target: &GsiBuildTarget,
        after: Option<&Self::Cursor>,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiScanSourcePage<Self::Cursor>>>;

    fn read_gsi_stream_source_page(
        &mut self,
        target: &GsiBuildTarget,
        after: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiStreamSourcePage>>;

    fn read_gsi_stream_tail(
        &mut self,
    ) -> impl Future<Output = StorageResult<SystemStreamCheckpoint>>;

    fn read_gsi_projection_states(
        &mut self,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>>>;

    fn read_gsi_projected_states(
        &mut self,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> impl Future<Output = StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>>>
    where
        Self: Sized,
    {
        async move {
            if batch.reads.is_empty() {
                Ok((0..batch.sources.len()).map(|_| None).collect())
            } else {
                self.read_gsi_projection_states(batch).await
            }
        }
    }

    fn read_gsi_referenced_histories(
        &mut self,
        reads: &[GsiReferencedHistoryRead<'_>],
    ) -> impl Future<Output = StorageResult<Vec<GsiReferencedHistoryItem>>>;

    fn validate_gsi_frontier(
        &mut self,
        current: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<()>>;

    fn read_gsi_activation_state(
        &mut self,
        target: &GsiBuildTarget,
        through: SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<GsiActivationState>>;

    fn read_gsi_ttl_config(
        &mut self,
        table_name: &TableName,
    ) -> impl Future<Output = StorageResult<Option<TtlConfigRecord>>>;

    fn write_gsi_progress(
        &mut self,
        expected_revision: Option<storage_types::Revision>,
        next_progress: TaskProgress<Option<storage_backfill::GsiBuildTarget>>,
    ) -> impl Future<Output = StorageResult<()>>;

    fn write_gsi_transition(
        &mut self,
        transition: GsiTransition<'_, Self::Cursor>,
    ) -> impl Future<Output = StorageResult<()>>;
}

#[async_trait]
pub trait GsiBuildRepository: Send + Sync {
    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>>;
}

#[async_trait]
impl<P: DynamoPort> GsiBuildRepository for P
where for<'native> P::Attempt<'native>: GsiPort
{
    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>> {
        let advance = retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { GsiAttempt::advance(attempt, &request).await })
            })
        })
        .await?;
        if let Some(name) = advance.ttl_activation.map(|policy| policy.table_name) {
            self.metadata_cache().invalidate(&name);
        }
        Ok(advance.result)
    }
}

#[async_trait]
pub trait GsiLifecycleRepository: Send + Sync {
    async fn read_gsi_readiness_records(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<GsiReadinessRecord>>;

    async fn read_gsi_index_readiness(
        &self,
        table_name: &TableName,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<(IndexName, GsiIndexReadiness)>> {
        GsiReadinessSnapshot::from(
            self.read_gsi_readiness_records(table_name, index_names)
                .await?,
        )
        .select(index_names)
    }
}
