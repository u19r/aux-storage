//! Physical GSI I/O joined to the initialized catalog attempt.

use storage_backfill::{GsiBuild, GsiBuildTarget, GsiScanSourcePage};
use storage_provider::maintenance::{GsiPort, GsiScanSource, GsiTransition};
use storage_types::{BoundTableInfo, StorageResult};

use super::{
    apply_gsi_transition, read_candidate_binding, read_scan_source_page, scan_build_candidates,
};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

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
    async fn load_gsi_candidate(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
    ) -> StorageResult<Option<GsiBuild<Vec<u8>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { read_candidate_binding(catalog.attempt, build).await }.await;
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
            let identity = catalog.load_table_identity(source.table).await?;
            read_scan_source_page(catalog, &identity, source).await
        }
        .await;
        settlement.complete(result)
    }

    async fn store_gsi_transition(
        &mut self,
        table: &BoundTableInfo,
        transition: GsiTransition<'_, Vec<u8>>,
        progress: storage_provider::maintenance::ProgressWrite,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            async { apply_gsi_transition(catalog, table, transition, progress).await }.await;
        settlement.complete(result)
    }
}
