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
    async fn read_primary_live_revisions(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> StorageResult<Vec<Option<ItemRevision>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let keys = reads
                .iter()
                .map(|(table, key)| {
                    primary_item_key(catalog.identity_for(&table.table_name)?, table, key)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            load_live_revisions(catalog.attempt, &keys, catalog.protection).await
        }
        .await;
        operation.complete(result)
    }

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
                .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
                .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
            let hydrated = decode_primary_items(requests, bound)?;
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

async fn load_live_revisions<A: KvAttempt>(
    attempt: &A,
    keys: &[Vec<u8>],
    protection: crate::ReadProtection,
) -> StorageResult<Vec<Option<ItemRevision>>> {
    let bound = crate::constants::KV_UNQUALIFIED_VALUE_BYTES;
    let maximum = crate::constants::KV_MAX_VALUE_BYTES;
    let per_head = attempt.native_value_limit().min(maximum).max(1);
    let group_size = (bound / per_head).max(1);
    let mut revisions = Vec::with_capacity(keys.len());
    for keys in keys.chunks(group_size) {
        let call = attempt.settlement().begin_call()?;
        let prefixes = call.complete(
            crate::value_prefix::load_value_prefixes(
                attempt,
                keys,
                crate::value_prefix::KvPrefixRead {
                    protection,
                    prefix_bytes: crate::constants::PRIMARY_ITEM_HEADER_BYTES,
                    allowance: crate::KvValueAllowance::new(maximum, bound.max(per_head)),
                },
            )
            .await,
        )?;
        revisions.extend(
            prefixes
                .into_iter()
                .map(|prefix| {
                    prefix
                        .map(|prefix| crate::item_codec::decode_primary_revision(&prefix.bytes))
                        .transpose()
                })
                .collect::<StorageResult<Vec<_>>>()?,
        );
    }
    Ok(revisions)
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
                        return Err(
                            storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
                        );
                    }
                    Ok(HydratedPrimaryItem {
                        stored: record.into_stored(table.info.max_indexers)?,
                        revision: envelope.revision,
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
