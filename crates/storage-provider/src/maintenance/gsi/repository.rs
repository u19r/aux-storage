use std::future::Future;

use storage_backfill::{
    GsiBuild, GsiBuildCandidates, GsiBuildTarget, GsiPageLimit, GsiScanObservation,
    GsiScanSourcePage,
};
use storage_common::TtlConfigRecord;
use storage_types::{
    StorageResult, TableName,
    maintenance::{TargetIncarnation, TaskProgress},
};

use super::{
    GsiBuildAdvance, GsiBuildRequest, GsiPort, GsiTransition, models::GsiBuildStart,
    transaction::GsiAttempt,
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

    fn advance_native_gsi(
        &mut self,
        _build: &GsiBuild<Self::Cursor>,
        _progress: super::NativeGsiProgress,
    ) -> impl Future<Output = StorageResult<Option<GsiBuildAdvance>>> {
        async { Ok(None) }
    }

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
            let projections = storage_backfill::GsiProjectedRow::project_scanned_items(
                &page.table,
                &target.index_name,
                page.items,
            )?;
            Ok(GsiScanObservation {
                expected_after: page.expected_after,
                end: page.end,
                projections,
            })
        }
    }

    fn read_gsi_scan_source_page(
        &mut self,
        target: &GsiBuildTarget,
        after: Option<&Self::Cursor>,
        limit: GsiPageLimit,
    ) -> impl Future<Output = StorageResult<GsiScanSourcePage<Self::Cursor>>>;

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

/// One try. Each try after the first halves the page of the try before it.
pub(crate) async fn gsi_build_in_attempt<P: GsiPort>(
    attempt: &mut P,
    request: &GsiBuildRequest,
    attempt_try: storage_types::AttemptTry,
) -> StorageResult<GsiBuildAdvance> {
    let mut request = request.clone();
    for _ in 1..attempt_try.number {
        request.page_limit = request.page_limit.halved();
    }
    GsiAttempt::advance(attempt, &request).await
}
