//! One admitted GSI workflow and catalog cache over physical ports.

use std::sync::Arc;

use storage_backfill::{
    GsiBuild, GsiBuildCandidates, GsiBuildTarget, GsiPageLimit, GsiScanSourcePage,
};
use storage_common::TtlConfigRecord;
use storage_types::{
    BoundTableInfo, Revision, StorageResult, TableName,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskProgress},
};

use super::{
    GsiBuildAdvance, GsiBuildRequest, GsiBuildTransaction, GsiCandidateRead, GsiPort,
    GsiScanSource, GsiTargetAdmission, GsiTransition, OrdinaryGsiBuild, models::GsiBuildStart,
};

pub struct GsiAttempt<'a, P: GsiPort, S = OrdinaryGsiBuild> {
    pub(super) port: &'a mut P,
    admission: S,
    pub(super) table: Option<Arc<BoundTableInfo>>,
    pub(super) ttl: Option<Option<TtlConfigRecord>>,
    pub(super) progress_fence: crate::maintenance::ProgressFence,
    primary_ttl_metadata: Vec<super::PrimaryTtlMetadata>,
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
                primary_ttl_metadata: Vec::new(),
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
        if !self
            .admission
            .admits_target(self.port, &build.target)
            .await?
        {
            return Ok(false);
        }
        if !build.state.is_runnable() {
            return Ok(true);
        }
        // Only the empty Building page activates the TTL policy and changes the
        // binding. Fill pages keep the shared catalog lock, so ordinary writers
        // of the table do not wait for each page; their rows are read FOR
        // UPDATE.
        if build.target.index_name.is_ttl_index()
            && matches!(build.state, storage_backfill::GsiState::Building(_))
        {
            self.port
                .lock_catalog_for_change(&build.target.table_name)
                .await?;
        }
        let table = self.load_table(&build.target.table_name).await?;
        Ok(table.table_status == storage_types::TableStatus::Active)
    }
    async fn validate_gsi_candidate(&mut self, build: &GsiBuild<Vec<u8>>) -> StorageResult<()> {
        let observed = self.port.load_gsi_candidate(build).await?;
        build.validate_candidate(observed.as_ref())
    }
}

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiBuildTransaction for GsiAttempt<'_, P, S> {
    type Cursor = Vec<u8>;

    async fn advance_native_gsi(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
        progress: super::NativeGsiProgress,
    ) -> StorageResult<Option<GsiBuildAdvance>> {
        if P::NATIVE_INDEX
            && !build.target.index_name.is_ttl_index()
            && matches!(
                build.state,
                storage_backfill::GsiState::Filling(_) | storage_backfill::GsiState::Building(_)
            )
        {
            self.advance_native_index(build.clone(), progress)
                .await
                .map(Some)
        } else {
            Ok(None)
        }
    }

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
        let page = self
            .port
            .load_gsi_scan_source_page(GsiScanSource {
                table: &table,
                target,
                after,
                limit,
            })
            .await?;
        if P::NATIVE_INDEX && target.index_name.is_ttl_index() {
            let config = table.binding.ttl.as_ref().ok_or_else(|| {
                storage_types::StorageError::internal("TTL fill policy is absent")
            })?;
            // The page read its source rows FOR UPDATE; these current images
            // remain protected through checkpoint commit.
            let rows = page
                .items
                .iter()
                .map(|source| {
                    Ok(super::PrimaryTtlMetadata {
                        key: table.info.table_key_from_wire(&source.item)?,
                        ttl: storage_common::TtlIndexKey::for_wire_item(
                            &table.info,
                            &config.attribute,
                            &source.item,
                        )?,
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            self.primary_ttl_metadata = rows;
        }
        Ok(page)
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
        mut transition: GsiTransition<'_, Self::Cursor>,
    ) -> StorageResult<()> {
        let table = self
            .load_table(&transition.expected.target.table_name)
            .await?;
        transition.validate_table(&table)?;
        transition.primary_ttl_metadata = std::mem::take(&mut self.primary_ttl_metadata);
        let progress = crate::maintenance::ProgressWrite::new(
            MaintenanceKind::GsiBackfill,
            &self.progress_fence,
            transition.expected_revision,
            &transition.next_progress,
        )?;
        self.port
            .store_gsi_transition(&table, transition, progress)
            .await
    }
}
