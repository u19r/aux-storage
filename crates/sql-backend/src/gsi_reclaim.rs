//! Protected native index observations and exact retired relation disposal.

use storage_backfill::GsiBuildTarget;
use storage_provider::{CATALOG_READ_BYTES, GsiReclaimAttempt, TableReclaimStep};
use storage_types::{
    ReadProtection, StorageError, StorageResult, TableHistoryId, maintenance::TargetIncarnation,
};

use crate::{
    ColumnIndex, SqlAttempt, SqlLayout, SqlValue, catalog::SqlCatalogAttempt,
    gsi_reclaim_sql_statements,
};

impl<A: SqlAttempt, L: SqlLayout> GsiReclaimAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_index_incarnation(
        &mut self,
        _history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<Option<TargetIncarnation>> {
        let page = self
            .attempt
            .query(
                gsi_reclaim_sql_statements::load_incarnation(self.engine, target),
                ReadProtection::Conflict,
                CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal("SQL index binding page is invalid"));
        }
        page.rows
            .first()
            .map(|row| {
                let SqlValue::Blob(bytes) = row.value(ColumnIndex::new(0))? else {
                    return Err(StorageError::internal("SQL index binding type is invalid"));
                };
                TargetIncarnation::try_from(bytes.clone())
                    .map_err(|_| StorageError::internal("SQL index binding incarnation is invalid"))
            })
            .transpose()
    }

    async fn delete_index_page(
        &mut self,
        _history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<TableReclaimStep> {
        self.attempt
            .execute(gsi_reclaim_sql_statements::drop_relation(
                self.engine,
                target,
            ))
            .await?;
        Ok(TableReclaimStep::Complete)
    }
}
