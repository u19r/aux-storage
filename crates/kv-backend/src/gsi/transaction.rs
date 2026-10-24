//! Physical GSI I/O joined to the initialized catalog attempt.

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildTarget, GsiProjection, GsiProjectionKey,
    GsiProjectionStateReadBatch, GsiReferencedHistoryItem, GsiReferencedHistoryRead,
    GsiScanSourcePage, GsiStreamSourcePage,
};
use storage_provider::maintenance::{GsiPort, GsiScanSource, GsiStreamSource, GsiTransition};
use storage_types::{BoundTableInfo, StorageResult, SystemStreamCheckpoint};

use super::{
    apply_gsi_transition, read_activation_state, read_candidate_holds, read_projection_states,
    read_referenced_histories, read_scan_source_page, read_stream_source_page,
    scan_build_candidates,
};
use crate::{KvAttempt, catalog::KvCatalogAttempt, stream_history};

impl<A: KvAttempt> GsiPort for KvCatalogAttempt<'_, A> {
    async fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { scan_build_candidates(catalog.attempt, after).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_candidate_holds(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
    ) -> StorageResult<storage_provider::maintenance::GsiCandidateObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { read_candidate_holds(catalog.attempt, build).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_scan_source_page(
        &mut self,
        source: GsiScanSource<'_>,
    ) -> StorageResult<GsiScanSourcePage<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&source.target.table_name)?.clone();
            read_scan_source_page(catalog, &identity, source).await
        }
        .await;
        settlement.complete(result)
    }
    async fn load_gsi_stream_source_page(
        &mut self,
        source: GsiStreamSource<'_>,
    ) -> StorageResult<GsiStreamSourcePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&source.target.table_name)?.clone();
            read_stream_source_page(catalog.attempt, &identity, source).await
        }
        .await;
        settlement.complete(result)
    }
    async fn load_gsi_stream_tail(&mut self) -> StorageResult<SystemStreamCheckpoint> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { stream_history::capture_high(catalog.attempt).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_projection_states(
        &mut self,
        table: &BoundTableInfo,
        batch: GsiProjectionStateReadBatch<'_>,
    ) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&table.table_name)?.clone();
            read_projection_states(catalog, &identity, table, batch).await
        }
        .await;
        settlement.complete(result)
    }
    async fn load_gsi_referenced_histories(
        &mut self,
        reads: &[GsiReferencedHistoryRead<'_>],
    ) -> StorageResult<Vec<GsiReferencedHistoryItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { read_referenced_histories(catalog.attempt, reads).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_frontier(
        &mut self,
        current: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            async { stream_history::validate_frontier(catalog.attempt, current, through).await }
                .await;
        settlement.complete(result)
    }
    async fn load_gsi_activation_state(
        &mut self,
        _table: &BoundTableInfo,
        target: &GsiBuildTarget,
    ) -> StorageResult<GsiActivationState> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&target.table_name)?.clone();
            read_activation_state(catalog.attempt, &identity, target).await
        }
        .await;
        settlement.complete(result)
    }
    async fn store_gsi_transition(
        &mut self,
        table: &BoundTableInfo,
        transition: GsiTransition<'_, Vec<u8>>,
        fence: &Self::ProgressFence,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { apply_gsi_transition(catalog, table, transition, fence).await }.await;
        settlement.complete(result)
    }
}
