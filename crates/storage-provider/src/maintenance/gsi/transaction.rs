//! One admitted GSI workflow and catalog cache over physical ports.

use std::sync::Arc;

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildCandidates, GsiBuildTarget, GsiPageLimit, GsiProjection,
    GsiProjectionKey, GsiProjectionStateReadBatch, GsiReferencedHistoryItem,
    GsiReferencedHistoryRead, GsiScanSourcePage, GsiStreamSourcePage,
};
use storage_common::TtlConfigRecord;
use storage_types::{
    BoundTableInfo, Revision, StorageResult, SystemStreamCheckpoint, TableName,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{
    GsiBuildAdvance, GsiBuildRequest, GsiBuildTransaction, GsiCandidateRead, GsiPort,
    GsiScanSource, GsiStreamSource, GsiTargetAdmission, GsiTransition, OrdinaryGsiBuild,
    models::GsiBuildStart,
};

pub struct GsiAttempt<'a, P: GsiPort, S = OrdinaryGsiBuild> {
    pub(super) port: &'a mut P,
    admission: S,
    pub(super) table: Option<Arc<BoundTableInfo>>,
    pub(super) ttl: Option<Option<TtlConfigRecord>>,
    progress_fence: P::ProgressFence,
}

impl<P: GsiPort> GsiAttempt<'_, P> {
    pub(super) async fn advance(
        port: &mut P,
        request: &GsiBuildRequest,
    ) -> StorageResult<GsiBuildAdvance> {
        Self::advance_with_admission(port, request, OrdinaryGsiBuild).await
    }
}

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiAttempt<'_, P, S> {
    pub async fn advance_with_admission(
        port: &mut P,
        request: &GsiBuildRequest,
        admission: S,
    ) -> StorageResult<GsiBuildAdvance> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(port);
        let result = async {
            let (identity, progress, progress_fence) = settlement
                .as_mut()
                .load_task_admission(MaintenanceKind::GsiBackfill)
                .await?;
            let start = GsiBuildStart::new(identity, progress, request);
            let mut attempt = GsiAttempt {
                port: settlement.as_mut(),
                admission,
                table: None,
                ttl: None,
                progress_fence,
            };
            start.advance(&mut attempt).await
        }
        .await;
        settlement.complete(result)
    }
}

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiCandidateRead<Vec<u8>> for GsiAttempt<'_, P, S> {
    async fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]> {
        self.port.scan_gsi_candidates(after).await
    }
    async fn admits_gsi_candidate(&mut self, build: &GsiBuild<Vec<u8>>) -> StorageResult<bool> {
        self.admission.admits_target(self.port, &build.target).await
    }
    async fn validate_gsi_candidate(&mut self, build: &GsiBuild<Vec<u8>>) -> StorageResult<()> {
        let observed = self.port.load_gsi_candidate_holds(build).await?;
        build.validate_candidate_holds(observed.build.as_ref(), &observed.holds)
    }
}

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiBuildTransaction for GsiAttempt<'_, P, S> {
    type Cursor = Vec<u8>;

    async fn read_gsi_admission(
        &mut self,
    ) -> StorageResult<(
        TargetIncarnation,
        Option<TaskProgress<Option<GsiBuildTarget>>>,
    )> {
        let (identity, progress, fence) = self
            .port
            .load_task_admission(MaintenanceKind::GsiBackfill)
            .await?;
        self.progress_fence = fence;
        Ok((identity, progress))
    }

    async fn read_maintenance_identity(&mut self) -> StorageResult<TargetIncarnation> {
        self.read_gsi_admission()
            .await
            .map(|(identity, _)| identity)
    }

    async fn read_gsi_progress(
        &mut self,
    ) -> StorageResult<Option<TaskProgress<Option<GsiBuildTarget>>>> {
        self.read_gsi_admission()
            .await
            .map(|(_, progress)| progress)
    }

    async fn read_gsi_build_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> StorageResult<GsiBuildCandidates<Self::Cursor>> {
        GsiCandidateRead::read_gsi_build_candidates(self, after).await
    }

    async fn read_gsi_scan_source_page(
        &mut self,
        target: &GsiBuildTarget,
        after: Option<&Self::Cursor>,
        limit: GsiPageLimit,
    ) -> StorageResult<GsiScanSourcePage<Self::Cursor>> {
        let table = self.load_table(&target.table_name).await?;
        target.validate_table(&table)?;
        self.port
            .load_gsi_scan_source_page(GsiScanSource {
                table: &table,
                target,
                after,
                limit,
            })
            .await
    }

    async fn read_gsi_stream_source_page(
        &mut self,
        target: &GsiBuildTarget,
        after: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
        limit: GsiPageLimit,
    ) -> StorageResult<GsiStreamSourcePage> {
        let table = self.load_table(&target.table_name).await?;
        target.validate_table(&table)?;
        self.port
            .load_gsi_stream_source_page(GsiStreamSource {
                table: &table,
                target,
                after,
                through,
                limit,
            })
            .await
    }

    async fn read_gsi_stream_tail(&mut self) -> StorageResult<SystemStreamCheckpoint> {
        self.port.load_gsi_stream_tail().await
    }

    async fn read_gsi_projection_states(
        &mut self,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
        let table = self.load_table(&batch.table.table_name).await?;
        batch.target.validate_table(&table)?;
        self.port.load_gsi_projection_states(&table, batch).await
    }

    async fn read_gsi_referenced_histories(
        &mut self,
        reads: &[GsiReferencedHistoryRead<'_>],
    ) -> StorageResult<Vec<GsiReferencedHistoryItem>> {
        self.port.load_gsi_referenced_histories(reads).await
    }

    async fn validate_gsi_frontier(
        &mut self,
        current: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
    ) -> StorageResult<()> {
        self.port.load_gsi_frontier(current, through).await
    }

    async fn read_gsi_activation_state(
        &mut self,
        target: &GsiBuildTarget,
        _through: SystemStreamCheckpoint,
    ) -> StorageResult<GsiActivationState> {
        let table = self.load_table(&target.table_name).await?;
        target.validate_table(&table)?;
        self.port.load_gsi_activation_state(&table, target).await
    }

    async fn read_gsi_ttl_config(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        self.load_ttl(name).await
    }

    async fn write_gsi_progress(
        &mut self,
        expected: Option<Revision>,
        next: TaskProgress<Option<GsiBuildTarget>>,
    ) -> StorageResult<()> {
        self.port
            .store_task_progress(
                MaintenanceKind::GsiBackfill,
                &self.progress_fence,
                expected,
                next,
            )
            .await
    }

    async fn write_gsi_transition(
        &mut self,
        transition: GsiTransition<'_, Self::Cursor>,
    ) -> StorageResult<()> {
        let table = self
            .load_table(&transition.expected.target.table_name)
            .await?;
        transition.validate_table(&table)?;
        // The shared executor derives TTL activation from this attempt's
        // protected policy observation. A concurrent policy change is rejected
        // by the native conflict fence (KV) or conditional update (SQL), never
        // a cached-value check.
        self.port
            .store_gsi_transition(&table, transition, &self.progress_fence)
            .await
    }
}
