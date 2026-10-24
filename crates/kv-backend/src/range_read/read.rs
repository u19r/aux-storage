//! One KV range lowerer for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, DynamoRangeReadAttempt, GsiReadinessSnapshot,
    PrimaryStateRow, PrimaryStateScan, PrimaryStateScanAttempt, RangeAdmission, RangeReadItem,
    RangeReadPage,
};
use storage_types::{
    BoundTableInfo, IndexName, ItemKey, LogicalItemSize, StorageError, StorageResult,
};

use super::{
    key_layout::{after_key, physical_item_key, physical_span},
    projection::{GsiState, PrimaryState, PublicImage, RangeContent, RangeProjection},
};
use crate::{
    KvAttempt, KvRange, catalog::KvCatalogAttempt, constants::PAGE_READ_BYTES,
    gsi::read_gsi_readiness,
};

impl<A: KvAttempt> DynamoRangeReadAttempt for KvCatalogAttempt<'_, A> {
    async fn read_gsi_readiness(
        &mut self,
        table: &BoundTableInfo,
        index: Option<&IndexName>,
    ) -> StorageResult<GsiReadinessSnapshot> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&table.info.table_name)?.clone();
            read_gsi_readiness(catalog.attempt, &identity, catalog.protection, index).await
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
                .read_projected_range::<PublicImage>(RangeReadInput { table, bound, page })
                .await
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> PrimaryStateScanAttempt for KvCatalogAttempt<'_, A> {
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
                .read_projected_range::<PrimaryState>(RangeReadInput {
                    table: scan.table(),
                    bound: &bound,
                    page: &page,
                })
                .await
        }
        .await;
        settlement.complete(result)
    }
}

pub(super) struct RangeReadInput<'a, 'b> {
    pub(super) table: &'a BoundTableInfo,
    pub(super) bound: &'a BoundRangeReadPlan<'b>,
    pub(super) page: &'a BoundRangeReadPage<'b>,
}

impl<A: KvAttempt> KvCatalogAttempt<'_, A> {
    async fn read_projected_range<P: RangeProjection>(
        &mut self,
        input: RangeReadInput<'_, '_>,
    ) -> StorageResult<RangeReadPage<P::Item>> {
        let RangeReadInput { table, bound, page } = input;
        let identity = self.identity_for(&table.info.table_name)?.clone();
        let span = physical_span(&identity, table, bound, page)?;
        let mut begin = span.begin;
        let mut end = span.end;
        let mut result = RangeReadPage::<P::Item>::default();
        loop {
            if begin >= end {
                break;
            }
            let range = input.physical_range(&begin, &end)?;
            let (batch, projections) = if let Some(index) = bound.index_name.as_ref() {
                let (batch, images) = crate::gsi::read_tree_page(self, table, index, range).await?;
                (batch, Some(images))
            } else {
                let mut batch = self.read_range_batch(range).await?;
                crate::item_read::select_primary_prefix(table, &mut batch)?;
                (batch, None)
            };
            let has_more = batch.more;
            let (last_physical, stopped) = input
                .decode_batch::<P, A>(self, (batch, projections), &mut result, &identity)
                .await?;
            if stopped {
                return result.finish_with_more();
            }
            if !has_more {
                break;
            }
            if result.progress.evaluated_count >= page.budget.max_items {
                return result.finish_with_more();
            }
            let last = last_physical.ok_or_else(|| {
                StorageError::internal("KV range reports more without a physical cursor")
            })?;
            if page.forward {
                begin = after_key(last);
            } else {
                end = last;
            }
        }
        result.finish()
    }

    async fn read_range_batch(&mut self, range: KvRange) -> StorageResult<crate::KvRangePage> {
        let _call = metrics_facade::begin_database_call("dynamo.range_read");
        self.attempt.scan(range, self.protection).await
    }
}

impl RangeReadInput<'_, '_> {
    pub(super) fn decode_entry<P: RangeProjection>(
        &self,
        identity: &crate::catalog::KvTableIdentity,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(P::Item, ItemKey, LogicalItemSize)> {
        let decoded = P::decode(self.table, self.bound, physical, content)?;
        if physical_item_key(identity, self.table, self.bound, &decoded.1)? != physical {
            return Err(StorageError::internal(
                "KV range image differs from physical key",
            ));
        }
        Ok(decoded)
    }

    fn physical_range(&self, begin: &[u8], end: &[u8]) -> StorageResult<KvRange> {
        Ok(KvRange {
            entry_bounds: None,
            begin: begin.to_vec(),
            end: end.to_vec(),
            reverse: !self.page.forward,
            limit: NonZeroU32::new(self.page.budget.max_items.saturating_add(1))
                .ok_or_else(|| StorageError::internal("range page limit is zero"))?,
            max_bytes: PAGE_READ_BYTES,
        })
    }

    async fn decode_batch<P: RangeProjection, A: KvAttempt>(
        &self,
        attempt: &mut KvCatalogAttempt<'_, A>,
        input: (
            crate::KvRangePage,
            Option<Vec<crate::gsi::HydratedProjection>>,
        ),
        result: &mut RangeReadPage<P::Item>,
        identity: &crate::catalog::KvTableIdentity,
    ) -> StorageResult<(Option<Vec<u8>>, bool)> {
        let (batch, projections) = input;
        let mut content = if self.bound.index.is_none() {
            crate::item_read::hydrate_primary_items(
                attempt,
                batch
                    .entries
                    .iter()
                    .map(|entry| (self.table, Some(entry.value.as_slice())))
                    .collect(),
                PAGE_READ_BYTES.get(),
            )
            .await?
            .into_iter()
            .map(|item| {
                item.map(|item| RangeContent::Primary(Box::new(item)))
                    .ok_or_else(|| StorageError::internal("Missing primary range content"))
            })
            .collect::<StorageResult<Vec<_>>>()?
        } else {
            projections
                .ok_or_else(|| StorageError::internal("Missing verified tree images"))?
                .into_iter()
                .map(|projection| RangeContent::Projection(Box::new(projection)))
                .collect()
        }
        .into_iter();
        let mut last = None;
        for entry in batch.entries {
            let hydrated = content
                .next()
                .ok_or_else(|| StorageError::internal("Missing range hydration slot"))?;
            let (item, key, size) = self.decode_entry::<P>(identity, &entry.key, hydrated)?;
            last = Some(entry.key);
            if self.page.budget.admit(&mut result.progress, key, size)? == RangeAdmission::Stop {
                return Ok((last, true));
            }
            result.items.push(item);
        }
        Ok((last, false))
    }
}

impl<A: KvAttempt> storage_provider::GsiProjectionScanAttempt for KvCatalogAttempt<'_, A> {
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
                .read_projected_range::<GsiState>(RangeReadInput {
                    table: scan.table(),
                    bound: &bound,
                    page: &page,
                })
                .await
        }
        .await;
        settlement.complete(result)
    }
}
