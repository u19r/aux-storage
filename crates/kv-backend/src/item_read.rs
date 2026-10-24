//! Primary item reads over the shared ordered KV key/value format.

use storage_provider::GetItemAttempt;
use storage_types::{
    BoundTableInfo, ItemKey, ItemProvenance, ItemRevision, KeyAttributes, StorageError,
    StorageResult, StoredWireItem, WireItem,
};

use crate::{
    KvAttempt,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    constants::PRIMARY_ITEM_FAMILY,
    item_codec::PrimaryItemEnvelope,
};

impl<A: KvAttempt> GetItemAttempt for KvCatalogAttempt<'_, A> {
    async fn read_primary_item(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<WireItem>> {
        self.read_primary_pairs(&[(table, key)])
            .await?
            .pop()
            .ok_or_else(|| StorageError::internal("Missing primary point-read result"))
    }

    async fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> StorageResult<Vec<Option<WireItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut physical = Vec::new();
            let mut requests = Vec::new();
            let mut results = Vec::new();
            physical
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            results
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (table, key) in reads {
                let identity = catalog.identity_for(&table.info.table_name)?;
                physical.push(primary_item_key(identity, table, key)?);
            }
            let values = catalog
                .attempt
                .get_batch(physical, catalog.protection)
                .await?;
            if values.len() != reads.len() {
                return Err(StorageError::internal(
                    "KV point-read batch returned the wrong count",
                ));
            }
            requests.extend(
                reads
                    .iter()
                    .zip(&values)
                    .map(|((table, _), value)| (*table, value.as_deref())),
            );
            let bound = reads
                .len()
                .checked_mul(crate::constants::PAGE_READ_BYTES.get())
                .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
            let hydrated = hydrate_primary_items(catalog, requests, bound).await?;
            if hydrated.len() != reads.len() {
                return Err(StorageError::internal("KV primary hydration count differs"));
            }
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
            Ok(results)
        }
        .await;
        settlement.complete(result)
    }
}

pub(crate) fn primary_item_key(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<Vec<u8>> {
    let item_key =
        ItemKey::from_key_schema(table.info.table_name.clone(), &table.info.key_schema, key)?;
    let suffix = item_key.sorted_storage_suffix()?;
    let count = 5usize
        .checked_add(suffix.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut physical = Vec::new();
    physical
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    physical.push(PRIMARY_ITEM_FAMILY);
    physical.extend_from_slice(&identity.table_id.to_be_bytes());
    physical.extend_from_slice(&suffix);
    Ok(physical)
}

/// Verified primary content and native revision authority remain distinct.
pub(crate) struct HydratedPrimaryItem {
    pub stored: StoredWireItem,
    pub revision: ItemRevision,
    pub provenance: ItemProvenance,
    pub ttl_epoch: Option<i64>,
}

pub(crate) async fn hydrate_primary_items<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    attempt: &mut A,
    reads: Vec<(&storage_types::BoundTableInfo, Option<&[u8]>)>,
    bound: usize,
) -> StorageResult<Vec<Option<HydratedPrimaryItem>>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let maximum = crate::constants::PAGE_READ_BYTES.get();
        let present = reads.iter().filter(|(_, value)| value.is_some()).count();
        let mut headers = Vec::new();
        let mut records = Vec::new();
        let mut results = Vec::new();
        headers
            .try_reserve_exact(reads.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        records
            .try_reserve_exact(present)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        results
            .try_reserve_exact(reads.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (table, value) in reads {
            let header = value
                .map(|value| -> StorageResult<_> {
                    let envelope = PrimaryItemEnvelope::decode(value)?;
                    let record = stream_provider::retained_image::RetainedItemRecord::decode(
                        envelope.image,
                        table.history_id,
                        maximum,
                    )?;
                    records.push((table, record, table.info.max_indexers));
                    Ok((
                        envelope.revision,
                        envelope.decode_provenance()?,
                        envelope.ttl_epoch,
                    ))
                })
                .transpose()?;
            headers.push(header);
        }
        if records.is_empty() {
            results.extend(headers.into_iter().map(|_| None));
            return Ok(results);
        }
        let hydrated =
            stream_provider::retained_image::RetainedItemRecord::hydrate(attempt, records, bound)
                .await?;
        let mut items = hydrated.into_iter();
        for header in headers {
            results.push(
                header
                    .map(|(revision, provenance, ttl_epoch)| -> StorageResult<_> {
                        let stored = items.next().ok_or_else(|| {
                            StorageError::internal("Missing hydrated primary item")
                        })?;
                        Ok(HydratedPrimaryItem {
                            stored,
                            revision,
                            provenance,
                            ttl_epoch,
                        })
                    })
                    .transpose()?,
            );
        }
        if items.next().is_some() {
            return Err(StorageError::internal("Extra hydrated primary item"));
        }
        Ok(results)
    }
    .await;
    settlement.complete(result)
}

pub(crate) fn select_primary_prefix(
    table: &storage_types::BoundTableInfo,
    page: &mut crate::KvRangePage,
) -> StorageResult<()> {
    let maximum = crate::constants::PAGE_READ_BYTES.get();
    let mut budget = stream_provider::retained_image::RetainedImagePageBudget::try_new(maximum)?;
    let mut count = 0;
    for entry in &page.entries {
        let envelope = PrimaryItemEnvelope::decode(&entry.value)?;
        let record = stream_provider::retained_image::RetainedItemRecord::decode(
            envelope.image,
            table.history_id,
            maximum,
        )?;
        let work = record.read_work_bytes(maximum)?;
        if !budget.admit(work)? {
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
