use storage_provider::{ItemTombstonePage, ItemTombstoneScan, ItemTombstoneScanAttempt};
use storage_types::StorageResult;

use super::{
    decode::decode_scan_entry,
    key_layout::{tombstone_json_key, tombstone_prefix},
};
use crate::{KvAttempt, KvRange, catalog::KvCatalogAttempt};

impl<A: KvAttempt> ItemTombstoneScanAttempt for KvCatalogAttempt<'_, A> {
    async fn scan_item_tombstones(
        &mut self,
        scan: &ItemTombstoneScan,
    ) -> StorageResult<ItemTombstonePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut range = KvRange::try_for_prefix(
                tombstone_prefix(scan.table())?,
                scan.probe_limit(),
                scan.max_bytes(),
            )?;
            if let Some(after) = scan.after() {
                range.begin = tombstone_json_key(scan.table(), after)?;
                range
                    .begin
                    .try_reserve_exact(1)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                range.begin.push(0);
            }
            let page = catalog.attempt.scan(range, catalog.protection).await?;
            let mut items = Vec::new();
            items
                .try_reserve_exact(page.entries.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for entry in page.entries {
                items.push(decode_scan_entry(scan, entry)?);
            }
            ItemTombstonePage::try_new(scan, items, page.more)
        }
        .await;
        settlement.complete(result)
    }
}
