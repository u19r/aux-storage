//! Physical GSI I/O joined to the initialized catalog attempt.

use storage_backfill::{GsiBuild, GsiBuildTarget, GsiScanSourcePage};
use storage_provider::maintenance::{GsiPort, GsiScanSource, GsiTransition};
use storage_types::{BoundTableInfo, StorageResult};

use super::{
    apply_gsi_transition, read_candidate_binding, read_scan_source_page, scan_build_candidates,
};
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> GsiPort for SqlCatalogAttempt<'_, A, L> {
    const NATIVE_INDEX: bool = true;
    fn stage_native_index(
        &mut self,
        work: storage_provider::maintenance::NativeIndexWork,
    ) -> StorageResult<()> {
        self.deferred_ddl
            .push(crate::ddl::SqlDdl::for_index(self.engine, work)?);
        Ok(())
    }
    async fn load_native_gsi_page(
        &mut self,
        table: &BoundTableInfo,
        target: &GsiBuildTarget,
        after: Option<&Vec<u8>>,
        limit: storage_backfill::GsiPageLimit,
    ) -> StorageResult<storage_provider::maintenance::NativeGsiPage> {
        super::fill::load_fill_page(self, table, (target, after, limit)).await
    }
    async fn store_native_gsi_fills(
        &mut self,
        table: &BoundTableInfo,
        target: &GsiBuildTarget,
        fills: Vec<storage_provider::maintenance::NativeGsiFill>,
    ) -> StorageResult<Vec<bool>> {
        super::fill::store_fills(self, table, target, fills).await
    }
    async fn load_native_index_state(
        &mut self,
        table: &BoundTableInfo,
        index: &storage_types::BoundGsi,
    ) -> StorageResult<storage_provider::maintenance::NativeIndexState> {
        use storage_provider::maintenance::NativeIndexState;
        let page = self
            .attempt
            .query(
                super::sql_statements::index_state_statement(self.engine, table.history_id, index),
                crate::ReadProtection::Snapshot,
                storage_provider::CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() > 1 {
            return Err(storage_types::StorageError::internal(
                "Native index validity page differs",
            ));
        }
        match page
            .rows
            .first()
            .map(|row| row.value(crate::ColumnIndex::new(0)))
            .transpose()?
        {
            None => Ok(NativeIndexState::Missing),
            Some(crate::SqlValue::Integer(0)) => Ok(NativeIndexState::Invalid),
            Some(crate::SqlValue::Integer(1)) => Ok(NativeIndexState::Valid),
            Some(crate::SqlValue::Integer(2)) => Ok(NativeIndexState::Building),
            _ => Err(storage_types::StorageError::internal(
                "Native index validity is invalid",
            )),
        }
    }
    async fn store_native_gsi_transition(
        &mut self,
        table: &BoundTableInfo,
        transition: storage_provider::maintenance::NativeGsiTransition,
        progress: storage_provider::maintenance::ProgressWrite,
    ) -> StorageResult<()> {
        let binding = transition.next_binding(&table.binding)?;
        let mut statements = vec![
            super::sql_statements::native_transition_statement(self.engine, &transition)?,
            crate::maintenance::progress_statement(self.engine, progress)?,
        ];
        let changed = if binding != table.binding {
            statements.push(super::transition::binding_statement(
                self.engine,
                table,
                &binding,
            )?);
            self.execute_catalog_change(&table.table_name, statements)
                .await?
        } else {
            self.attempt.execute_batch_exact(statements).await?
        };
        if changed.iter().any(|count| *count != 1) {
            return Err(storage_backfill::GsiActivationState::conflict_error());
        }
        Ok(())
    }
    async fn scan_gsi_candidates(
        &mut self,
        after: Option<&GsiBuildTarget>,
    ) -> StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            async { scan_build_candidates(catalog.attempt, catalog.engine, after).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_candidate(
        &mut self,
        build: &GsiBuild<Vec<u8>>,
    ) -> StorageResult<Option<GsiBuild<Vec<u8>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            async { read_candidate_binding(catalog.attempt, catalog.engine, build).await }.await;
        settlement.complete(result)
    }
    async fn load_gsi_scan_source_page(
        &mut self,
        source: GsiScanSource<'_>,
    ) -> StorageResult<GsiScanSourcePage<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async { read_scan_source_page(catalog, source).await }.await;
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
