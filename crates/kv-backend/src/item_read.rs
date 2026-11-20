//! Primary item reads over the shared ordered KV key/value format.

use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, ItemReadAttempt, RangeReadItem, RangeReadPage,
};
use storage_types::{
    BoundTableInfo, ItemKey, ItemProvenance, KeyAttributes, StorageError, StorageResult,
    StoredWireItem, WireItem,
};

use crate::{
    KvAttempt,
    catalog::{KvCatalogAttempt, table_id_from_history},
    item_codec::PrimaryItemEnvelope,
    range_read::{PublicImage, RangeReadInput},
    table_key_layout::{TableSubFamily, table_key},
};

impl<A: KvAttempt> ItemReadAttempt for KvCatalogAttempt<'_, A> {
    async fn read_primary_item(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<WireItem>> {
        self.read_primary_pairs(&[(table, key)])
            .await?
            .into_single()
            .ok_or_else(|| StorageError::internal("Missing primary point-read result"))
    }

    async fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> StorageResult<storage_types::Exact<Option<WireItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut physical = Vec::new();
            let mut requests = Vec::new();
            let mut results = Vec::new();
            physical
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            results
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            for (table, key) in reads {
                physical.push(primary_item_key(
                    table_id_from_history(table.history_id)?,
                    table,
                    key,
                )?);
            }
            let values = catalog
                .attempt
                .get_batch_exact(physical, catalog.protection)
                .await?;
            if catalog
                .attempt
                .root_attempt()
                .native_pipelined_version_moved()
            {
                return Ok(None);
            }
            requests.extend(
                reads
                    .iter()
                    .zip(&values)
                    .map(|((table, _), value)| (*table, value.as_deref())),
            );
            let bound = reads
                .len()
                .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
                .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
            let hydrated = decode_primary_items(requests, bound)?;
            for (item, (_, key)) in hydrated.into_iter().zip(reads) {
                if let Some(item) = &item
                    && !item.stored.item.has_verified_keys(key)?
                {
                    return Err(StorageError::internal(
                        "KV primary content differs from physical key",
                    ));
                }
                results.push(item.map(|item| item.stored.item));
            }
            storage_types::Exact::try_new(reads.len(), results).map(Some)
        }
        .await;
        match settlement.complete(result)? {
            Some(items) => Ok(items),
            None => Err(crate::catalog::pipelined_version_moved()),
        }
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
        match settlement.complete(result)? {
            Some(page) => Ok(page),
            None => Err(crate::catalog::pipelined_version_moved()),
        }
    }
}

pub(crate) fn primary_item_key(
    table_id: u32,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<Vec<u8>> {
    let item_key =
        ItemKey::from_key_schema(table.info.table_name.clone(), &table.info.key_schema, key)?;
    item_key_from_suffix(table_id, &item_key.sorted_storage_suffix()?)
}

pub(crate) fn item_key_from_suffix(table_id: u32, suffix: &[u8]) -> StorageResult<Vec<u8>> {
    table_key(table_id, TableSubFamily::Item, &[suffix])
}

/// Verified primary content and live row metadata.
pub(crate) struct HydratedPrimaryItem {
    pub stored: StoredWireItem,
    pub provenance: ItemProvenance,
    pub ttl_epoch: Option<i64>,
}

pub(crate) fn decode_primary_items(
    reads: Vec<(&storage_types::BoundTableInfo, Option<&[u8]>)>,
    maximum: usize,
) -> StorageResult<Vec<Option<HydratedPrimaryItem>>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let mut budget = storage_types::PageByteBudget::try_new(maximum)?;
    reads
        .into_iter()
        .map(|(table, value)| {
            value
                .map(|value| {
                    let envelope = PrimaryItemEnvelope::decode(value)?;
                    let record = storage_types::MaterializedItemRecord::decode(
                        envelope.image,
                        table.history_id,
                        storage_types::MaterializedItemRecord::encoded_length_bound(),
                    )?;
                    if !budget.admit(record.decoded_bytes()) {
                        return Err(storage_types::AttemptFailure::CAPACITY.into());
                    }
                    Ok(HydratedPrimaryItem {
                        stored: record.into_stored(table.info.max_indexers)?,
                        provenance: envelope.decode_provenance()?,
                        ttl_epoch: envelope.ttl_epoch,
                    })
                })
                .transpose()
        })
        .collect()
}

pub(crate) fn select_primary_prefix(
    table: &storage_types::BoundTableInfo,
    page: &mut crate::KvRangePage,
) -> StorageResult<()> {
    let maximum = crate::constants::PAGE_READ_BYTES.get();
    let mut budget = storage_types::PageByteBudget::try_new(maximum)?;
    let mut count = 0;
    for entry in &page.entries {
        let envelope = PrimaryItemEnvelope::decode(&entry.value)?;
        let record = storage_types::MaterializedItemRecord::decode(
            envelope.image,
            table.history_id,
            storage_types::MaterializedItemRecord::encoded_length_bound(),
        )?;
        let work = record.decoded_bytes();
        if !budget.admit(work) {
            break;
        }
        count += 1;
    }
    if count < page.entries.len() {
        page.entries.truncate(count);
        page.more = true;
    }
    Ok(())
}
