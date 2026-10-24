use storage_provider::{ItemTombstonePage, ItemTombstoneScan, ItemTombstoneScanAttempt};
use storage_types::StorageResult;

use super::{decode::decode_scan_row, sql_statements::scan_statement};
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> ItemTombstoneScanAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn scan_item_tombstones(
        &mut self,
        scan: &ItemTombstoneScan,
    ) -> StorageResult<ItemTombstonePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let page = catalog
                .attempt
                .query(
                    scan_statement(catalog.engine, scan)?,
                    catalog.protection,
                    scan.max_bytes(),
                )
                .await?;
            let mut items = Vec::new();
            items
                .try_reserve_exact(page.rows.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for row in page.rows {
                items.push(decode_scan_row(scan, row)?);
            }
            ItemTombstonePage::try_new(scan, items, page.more)
        }
        .await;
        settlement.complete(result)
    }
}
