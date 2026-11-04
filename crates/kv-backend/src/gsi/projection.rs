//! Protected ordered projection rows and revision tombstones share one attempt.
use std::num::NonZeroUsize;

use storage_backfill::{GsiProjection, GsiProjectionKey, GsiProjectionStateReadBatch};
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use super::{
    key_layout::{ProjectionKeys, projection_keys},
    read::target_index,
    write::{DecodedProjection, PhysicalProjection, decode_projection, decode_projection_state},
};
use crate::{
    KvAttempt, ReadProtection,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    expiration::key_layout::encode_index_key,
};

struct ReadSlot {
    source_ordinal: usize,
    live_index: usize,
    projection: Option<ProjectionKeys>,
}

pub(crate) async fn read_projection_states<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    batch: GsiProjectionStateReadBatch<'_>,
) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
    if identity.deleted
        || identity.table_name != batch.table.table_name
        || table.info.table_name != batch.table.table_name
    {
        return Err(StorageError::internal(
            "KV GSI destination table has changed",
        ));
    }
    if !batch.target.index_name.is_ttl_index() {
        target_index(identity, batch.target)?;
    }
    let (keys, slots) = plan_projection_reads(identity, table, &batch)?;
    let values = ReadSlot::load_values(catalog, keys, &slots).await?;
    ReadSlot::bind_values(table, &batch, slots, values)
}

impl ReadSlot {
    async fn load_values<A: KvAttempt>(
        catalog: &KvCatalogAttempt<'_, A>,
        keys: Vec<Vec<u8>>,
        slots: &[Self],
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let descriptor = storage_types::MaterializedItemRecord::encoded_length_bound();
        let maximum = (descriptor + 10 + crate::constants::GSI_REVISION_BYTES)
            .checked_mul(slots.len())
            .ok_or_else(|| StorageError::internal("Invalid KV GSI projection observation"))?;
        let expected = keys.len();
        let mut values = catalog
            .attempt
            .get_batch_bounded(
                keys,
                ReadProtection::Conflict,
                NonZeroUsize::new(maximum.max(1)).ok_or_else(|| {
                    StorageError::internal("Invalid KV GSI projection observation")
                })?,
            )
            .await?;
        if values.len() != expected {
            return Err(StorageError::internal(
                "Invalid KV GSI projection observation",
            ));
        }
        for slot in slots {
            if let Some(projection) = &slot.projection {
                for (offset, key) in [&projection.live, &projection.tombstone]
                    .into_iter()
                    .enumerate()
                {
                    if let Some(pending) = catalog.projection_changes.rows.get(key) {
                        values[slot.live_index + offset] = pending
                            .as_deref()
                            .map(super::write::copy_projection_bytes)
                            .transpose()?;
                    }
                }
            }
        }
        Ok(values)
    }

    fn bind_values(
        table: &BoundTableInfo,
        batch: &GsiProjectionStateReadBatch<'_>,
        slots: Vec<Self>,
        values: Vec<Option<Vec<u8>>>,
    ) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
        let mut current = Vec::new();
        current
            .try_reserve_exact(batch.sources.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        current.resize_with(batch.sources.len(), || None);
        for slot in slots {
            let observation = slot.decode_value(table, batch, &values)?;
            current[slot.source_ordinal] = observation;
        }
        Ok(current)
    }

    fn decode_value(
        &self,
        table: &BoundTableInfo,
        batch: &GsiProjectionStateReadBatch<'_>,
        values: &[Option<Vec<u8>>],
    ) -> StorageResult<Option<GsiProjection<GsiProjectionKey>>> {
        let source = batch
            .sources
            .get(self.source_ordinal)
            .ok_or_else(|| StorageError::internal("Invalid KV GSI source ordinal"))?;
        let live = values[self.live_index].as_deref();
        let state = if let Some(projection) = &self.projection {
            let state =
                decode_projection_state(table, live, values[self.live_index + 1].as_deref())?;
            if let Some(bytes) = live {
                let image = decode_projection(table, bytes)?;
                super::key_layout::require_projection_key(
                    table,
                    &batch.target.index_name,
                    &projection.live,
                    &image.image,
                )?;
            }
            match state {
                Some(PhysicalProjection::Live(revision)) => Some((revision, true)),
                Some(PhysicalProjection::Tombstone(revision)) => Some((revision, false)),
                None => None,
            }
        } else {
            live.map(|_| (source.item_version(), true))
        };
        state
            .map(|(version, live)| GsiProjection::from_stored_state(source, version, live))
            .transpose()
    }
}

fn plan_projection_reads(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    batch: &GsiProjectionStateReadBatch<'_>,
) -> StorageResult<(Vec<Vec<u8>>, Vec<ReadSlot>)> {
    let count = batch
        .reads
        .len()
        .checked_mul(2)
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut keys = Vec::new();
    let mut slots = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    slots
        .try_reserve_exact(batch.reads.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for read in batch.reads {
        if read.key.index_name != batch.target.index_name {
            return Err(StorageError::internal(
                "KV GSI destination index has changed",
            ));
        }
        let live_index = keys.len();
        let projection = if let Some(ttl) = &read.key.ttl_index_key {
            keys.push(encode_index_key(identity.table_id, ttl)?);
            None
        } else {
            let pair = projection_keys(identity, table, read.key)?;
            keys.push(super::write::copy_projection_bytes(&pair.live)?);
            keys.push(super::write::copy_projection_bytes(&pair.tombstone)?);
            Some(pair)
        };
        slots.push(ReadSlot {
            source_ordinal: read.source_ordinal,
            live_index,
            projection,
        });
    }
    Ok((keys, slots))
}

pub(crate) async fn read_projection_page<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    table: &BoundTableInfo,
    range: crate::KvRange,
) -> StorageResult<(crate::KvRangePage, Vec<DecodedProjection>)> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = settlement.as_mut();
    let result = async {
        let page = catalog.attempt.scan(range, catalog.protection).await?;
        let images = page
            .entries
            .iter()
            .map(|entry| decode_projection(table, &entry.value))
            .collect::<StorageResult<Vec<_>>>()?;
        Ok((page, images))
    }
    .await;
    settlement.complete(result)
}
