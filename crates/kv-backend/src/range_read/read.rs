//! One KV range lowerer for RocksDB and FoundationDB.

use std::num::NonZeroU32;

use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, ItemReadAttempt, PrimaryStateRow, PrimaryStateScan,
    PrimaryStateScanAttempt, RangeAdmission, RangeReadPage,
};
use storage_types::{BoundTableInfo, ItemKey, LogicalItemSize, StorageError, StorageResult};

use super::{
    key_layout::{after_key, physical_item_key, physical_span},
    projection::{GsiState, PrimaryState, RangeContent, RangeProjection},
};
use crate::{KvAttempt, KvRange, catalog::KvCatalogAttempt, constants::PAGE_READ_BYTES};

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
            let page = catalog
                .read_projected_range::<PrimaryState>(RangeReadInput {
                    table: scan.table(),
                    bound: &bound,
                    page: &page,
                })
                .await?;
            page.ok_or_else(|| StorageError::internal("unrequested pipelined catalog version"))
        }
        .await;
        settlement.complete(result)
    }
}

pub(crate) struct RangeReadInput<'a, 'b> {
    pub(crate) table: &'a BoundTableInfo,
    pub(crate) bound: &'a BoundRangeReadPlan<'b>,
    pub(crate) page: &'a BoundRangeReadPage<'b>,
}

impl<A: KvAttempt> KvCatalogAttempt<'_, A> {
    /// `None`: the pipelined catalog version moved before the first decode
    /// (PERF-FDB-1).
    pub(crate) async fn read_projected_range<P: RangeProjection>(
        &mut self,
        input: RangeReadInput<'_, '_>,
    ) -> StorageResult<Option<RangeReadPage<P::Item>>> {
        let RangeReadInput { table, bound, page } = input;
        let span = physical_span(table, bound, page)?;
        let mut begin = span.begin;
        let mut end = span.end;
        let mut result = RangeReadPage::<P::Item>::default();
        loop {
            if begin >= end {
                break;
            }
            let range = input.physical_range(&begin, &end)?;
            let (batch, projections) = if bound.index_name.is_some() {
                // GSI reads stay version-first (no pipelined version), so this
                // gate holds only if a GSI pipeline is ever added.
                let (batch, images) = crate::gsi::read_projection_page(self, table, range).await?;
                if self.attempt.root_attempt().native_pipelined_version_moved() {
                    return Ok(None);
                }
                (batch, Some(images))
            } else {
                let mut batch = self.read_range_batch(range).await?;
                if self.attempt.root_attempt().native_pipelined_version_moved() {
                    return Ok(None);
                }
                crate::item_read::select_primary_prefix(table, &mut batch)?;
                (batch, None)
            };
            let has_more = batch.more;
            let (last_physical, stopped) =
                input.decode_batch::<P>((batch, projections), &mut result)?;
            if stopped {
                return result.finish_with_more().map(Some);
            }
            if !has_more {
                break;
            }
            if result.progress.evaluated_count >= page.budget.max_items {
                return result.finish_with_more().map(Some);
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
        result.finish().map(Some)
    }

    async fn read_range_batch(&mut self, range: KvRange) -> StorageResult<crate::KvRangePage> {
        let _call = metrics_facade::begin_database_call("dynamo.range_read");
        self.attempt.scan(range, self.protection).await
    }
}

impl RangeReadInput<'_, '_> {
    pub(super) fn decode_entry<P: RangeProjection>(
        &self,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(P::Item, ItemKey, LogicalItemSize)> {
        let decoded = P::decode(self.table, self.bound, physical, content)?;
        if physical_item_key(self.table, self.bound, &decoded.1)? != physical {
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

    fn decode_batch<P: RangeProjection>(
        &self,
        input: (
            crate::KvRangePage,
            Option<Vec<crate::gsi::DecodedProjection>>,
        ),
        result: &mut RangeReadPage<P::Item>,
    ) -> StorageResult<(Option<Vec<u8>>, bool)> {
        let (batch, projections) = input;
        let mut content = if self.bound.index.is_none() {
            crate::item_read::decode_primary_items(
                batch
                    .entries
                    .iter()
                    .map(|entry| (self.table, Some(entry.value.as_slice())))
                    .collect(),
                PAGE_READ_BYTES.get(),
            )?
            .into_iter()
            .map(|item| {
                item.map(|item| RangeContent::Primary(Box::new(item)))
                    .ok_or_else(|| StorageError::internal("Missing primary range content"))
            })
            .collect::<StorageResult<Vec<_>>>()?
        } else {
            projections
                .ok_or_else(|| StorageError::internal("Missing materialized GSI images"))?
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
            let (item, key, size) = self.decode_entry::<P>(&entry.key, hydrated)?;
            last = Some(entry.key);
            if self.page.budget.admit(&mut result.progress, key, size)? == RangeAdmission::Stop {
                return Ok((last, true));
            }
            result.items.push(item);
        }
        Ok((last, false))
    }
}

impl<A: KvAttempt> storage_provider::ProjectionScanAttempt for KvCatalogAttempt<'_, A> {
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
            let page = catalog
                .read_projected_range::<GsiState>(RangeReadInput {
                    table: scan.table(),
                    bound: &bound,
                    page: &page,
                })
                .await?;
            page.ok_or_else(|| StorageError::internal("unrequested pipelined catalog version"))
        }
        .await;
        settlement.complete(result)
    }

    async fn encode_gsi_projection_addresses(
        &mut self,
        table: &BoundTableInfo,
        keys: &[storage_backfill::GsiProjectionKey],
    ) -> StorageResult<Vec<storage_provider::NativeProjectionAddress>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut addresses = Vec::with_capacity(keys.len());
            for key in keys {
                catalog
                    .read_gsi_readiness(table, Some(&key.index_name))
                    .await?
                    .require_queryable(&key.index_name)?;
                let incarnation = catalog.projection_incarnation(table, &key.index_name)?;
                addresses.push(storage_provider::NativeProjectionAddress::try_new(
                    table,
                    &key.index_name,
                    incarnation,
                    crate::gsi::ordered_projection_suffix(&key.item_key(&table.info)?)?,
                )?);
            }
            Ok(addresses)
        }
        .await;
        settlement.complete(result)
    }
}
