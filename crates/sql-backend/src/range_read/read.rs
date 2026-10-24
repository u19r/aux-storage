//! One SQL range lowerer for SQLite, Turso, and Postgres.

use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, DynamoRangeReadAttempt, GsiReadinessSnapshot,
    PrimaryStateRow, PrimaryStateScan, PrimaryStateScanAttempt, RangeReadItem, RangeReadPage,
};
use storage_types::{BoundTableInfo, IndexName, StorageError, StorageResult};

use super::{
    decode::{RangeDecode, decode_range_rows},
    projection::{GsiState, PrimaryState, PublicImage, RangeProjection},
    sql_statements::{RangeStatementInput, ordered_columns, range_statement},
};
use crate::{
    SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt, constants::RANGE_PAGE_READ_BYTES,
    gsi::read_gsi_readiness,
};

impl<A: SqlAttempt, L: SqlLayout> DynamoRangeReadAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_gsi_readiness(
        &mut self,
        table: &BoundTableInfo,
        index: Option<&IndexName>,
    ) -> StorageResult<GsiReadinessSnapshot> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let (snapshot, targets) = read_gsi_readiness(
                catalog.attempt,
                catalog.engine,
                table,
                crate::ReadProtection::Snapshot,
                index,
            )
            .await?;
            catalog.bind_gsi_targets(targets)?;
            Ok(snapshot)
        }
        .await;
        settlement.complete(result)
    }

    async fn read_range_page(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
    ) -> StorageResult<RangeReadPage<RangeReadItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .read_projected_range::<PublicImage>(table, bound, page)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> PrimaryStateScanAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn scan_primary_state(
        &mut self,
        scan: &PrimaryStateScan,
    ) -> StorageResult<RangeReadPage<PrimaryStateRow>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let bound = scan.bind()?;
            let page = scan.page(&bound)?;
            catalog
                .read_projected_range::<PrimaryState>(scan.table(), &bound, &page)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    pub(super) async fn read_projected_range<P: RangeProjection>(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
    ) -> StorageResult<RangeReadPage<P::Item>> {
        let columns = ordered_columns(table, bound);
        let incarnation = bound
            .index_name
            .as_ref()
            .map(|index| {
                self.gsi_target(&table.table_name, index)
                    .map(|target| target.incarnation.clone())
            })
            .transpose()?;
        let mut result = RangeReadPage::<P::Item>::default();
        let mut start = page.start.clone();
        loop {
            let physical_page = BoundRangeReadPage {
                condition: page.condition,
                start,
                budget: page.budget,
                forward: page.forward,
            };
            let input = RangeStatementInput {
                table,
                bound,
                page: &physical_page,
                columns: &columns,
                incarnation: incarnation.as_ref(),
            };
            let decoded = self.read_range_batch::<P>(&input, &mut result).await?;
            if matches!(decoded, RangeDecode::Exhausted) {
                return result.finish();
            }
            if matches!(decoded, RangeDecode::BudgetStop)
                || result.progress.evaluated_count >= page.budget.max_items
            {
                return result.finish_with_more();
            }
            start = Some(result.progress.last_evaluated_key.clone().ok_or_else(|| {
                storage_types::StorageError::internal(
                    "SQL range continuation lacks an admitted row",
                )
            })?);
        }
    }

    async fn read_range_batch<P: RangeProjection>(
        &mut self,
        input: &RangeStatementInput<'_, '_>,
        result: &mut RangeReadPage<P::Item>,
    ) -> StorageResult<RangeDecode> {
        let statement = range_statement::<P>(self.engine, input)?;
        let mut fetched = self
            .attempt
            .query(statement, self.protection, RANGE_PAGE_READ_BYTES)
            .await?;
        let hydrated = if let Some(image_column) = P::IMAGE_COLUMN {
            let offset = input.columns.len() + 1 + image_column;
            if let Some(index) = input.bound.index.as_ref() {
                let incarnation = input.incarnation.ok_or_else(|| {
                    StorageError::internal("SQL projection page has no index incarnation")
                })?;
                crate::gsi::hydrate_projection_page(
                    self,
                    &mut fetched,
                    crate::gsi::ProjectionPageScope {
                        table: input.table,
                        index: &index.index_name,
                        incarnation,
                        image_column: offset,
                    },
                )
                .await?
            } else {
                crate::item_read::select_image_prefix(
                    input.table,
                    &mut fetched,
                    offset,
                    RANGE_PAGE_READ_BYTES.get(),
                )?;
                let requests = fetched
                    .rows
                    .iter()
                    .map(|row| {
                        Ok((
                            input.table,
                            Some(row.value(crate::ColumnIndex::new(offset))?),
                            input.table.max_indexers,
                        ))
                    })
                    .collect::<StorageResult<Vec<_>>>()?;
                crate::item_read::hydrate_item_images(self, requests, RANGE_PAGE_READ_BYTES.get())
                    .await?
            }
        } else {
            fetched.rows.iter().map(|_| None).collect()
        };
        decode_range_rows::<P>(self.engine, input, (fetched, hydrated), result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> storage_provider::GsiProjectionScanAttempt
    for SqlCatalogAttempt<'_, A, L>
{
    async fn scan_gsi_projections(
        &mut self,
        scan: &storage_provider::GsiProjectionScan,
    ) -> StorageResult<RangeReadPage<storage_provider::GsiProjectionRow>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let bound = scan.bind()?;
            catalog
                .read_gsi_readiness(scan.table(), Some(scan.index()))
                .await?
                .require_queryable(scan.index())?;
            let bound = bound.mark_gsi_published()?;
            let page = scan.page(&bound)?;
            catalog
                .read_projected_range::<GsiState>(scan.table(), &bound, &page)
                .await
        }
        .await;
        settlement.complete(result)
    }
}
