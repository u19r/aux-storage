//! Protected KV item observations for a native write.

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlIndexWrite, TtlIndexWriteBatch};
use storage_provider::{
    AcceptedDeleteItem, AcceptedUpsert, BoundPutItem, ItemWriteAttempt, ItemWriteObservation,
    ItemWriteObservationRead, LiveItemMetadata, combine_write_observations,
    lifecycle_tables_for_writes,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use crate::{
    KvAttempt, KvMutation,
    catalog::{KvCatalogAttempt, table_id_from_history},
    expiration::key_layout::encode_index_key,
    item_read::{decode_primary_items, primary_item_key},
    write_preparation::KvPreparedWrites,
};

impl<A: KvAttempt> ItemWriteAttempt for KvCatalogAttempt<'_, A> {
    async fn read_item_write_prerequisites(
        &mut self,
        _table: &BoundTableInfo,
        prerequisites: storage_provider::ItemWritePrerequisites,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            if prerequisites.needs_history
                && catalog.attempt.root_attempt().writer_routes().is_none()
            {
                return Err(StorageError::internal("KV writer routes were not prepared"));
            }
            Ok(())
        }
        .await;
        operation.complete(result)
    }

    async fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> StorageResult<storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let lifecycle_tables = lifecycle_tables_for_writes(reads)?;
            // Paired write targets read their marker in the same native call
            // (no new wave).
            let mut marker_reads = Vec::new();
            for (index, read) in reads.iter().enumerate() {
                if read.reads_marker() {
                    marker_reads.push(index);
                }
            }
            let mut keys = Vec::new();
            keys.try_reserve_exact(reads.len() + marker_reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            for read in reads {
                let (table, key) = read.target();
                keys.push(primary_item_key(
                    table_id_from_history(table.history_id)?,
                    table,
                    key,
                )?);
            }
            for &index in &marker_reads {
                let (table, key) = reads[index].target();
                keys.push(crate::regional::regional_marker_key(
                    table_id_from_history(table.history_id)?,
                    table,
                    key,
                )?);
            }
            let expected = keys.len();
            let mut values = catalog
                .attempt
                .get_batch_primary_layouts(keys, &mut catalog.primary_layouts)
                .await?;
            if values.len() != expected {
                return Err(StorageError::internal(
                    "KV write observation returned the wrong count",
                ));
            }
            if catalog
                .attempt
                .root_attempt()
                .native_pipelined_version_moved()
            {
                return Ok(None);
            }
            let markers = values.split_off(reads.len());
            let mut requests = Vec::new();
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            requests.extend(
                reads
                    .iter()
                    .enumerate()
                    .map(|(index, read)| (read.target().0, values[index].as_deref())),
            );
            let hydrated = decode_primary_items(
                requests,
                reads
                    .len()
                    .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
                    .ok_or_else(|| StorageError::internal("Primary observation bound overflow"))?,
            )?;
            for (read, item) in reads.iter().zip(&hydrated) {
                if let Some(item) = item
                    && !item.stored.item.has_verified_keys(read.target().1)?
                {
                    return Err(StorageError::internal(
                        "KV observation content differs from physical key",
                    ));
                }
            }
            let mut items: Vec<ItemWriteObservation> = hydrated
                .into_iter()
                .map(|item| match item {
                    None => ItemWriteObservation::Absent,
                    Some(item) => ItemWriteObservation::Live {
                        stored: Box::new(item.stored),
                        provenance: item.provenance,
                        ttl_epoch: item.ttl_epoch,
                    },
                })
                .collect();
            for (index, value) in marker_reads.into_iter().zip(markers) {
                let marker = value
                    .as_deref()
                    .map(storage_types::RegionalMarker::decode)
                    .transpose()?;
                let observed = std::mem::replace(&mut items[index], ItemWriteObservation::Absent);
                items[index] = observed.with_marker(marker)?;
            }
            // GSI phases come from the confirmed catalog binding.
            let lifecycles = lifecycle_tables
                .into_iter()
                .map(|table| {
                    (
                        table.info.table_name.clone(),
                        GsiProjectionLifecycle::from_binding(table),
                    )
                })
                .collect();
            combine_write_observations(
                reads,
                storage_types::Exact::try_new(reads.len(), items)?,
                lifecycles,
            )
            .map(Some)
        }
        .await;
        match operation.complete(result)? {
            Some(observations) => Ok(observations),
            None => Err(crate::catalog::pipelined_version_moved()),
        }
    }

    async fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &LiveItemMetadata<'_>,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let mut prepared =
                std::mem::replace(&mut catalog.prepared_writes, Ok(Default::default()))?;
            let image = match prepared
                .primary
                .iter()
                .position(|write| write.matches(item))
            {
                Some(position) => prepared.primary.swap_remove(position),
                None => crate::write_preparation::PreparedPrimaryWrite::try_new(item.into())?,
            };
            catalog.prepared_writes = Ok(prepared);
            let table_id = table_id_from_history(item.table.history_id)?;
            let mut changes = image.prepare(table_id, effects)?;
            let ttl = ttl_index_mutations(table_id, writes)?;
            changes
                .try_reserve(ttl.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            changes.extend(ttl);
            catalog.append_dynamo_mutations(changes)?;
            Ok(())
        }
        .await;
        operation.complete(result)
    }

    fn stage_primary_delete(
        &mut self,
        item: &AcceptedDeleteItem,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            let table_id = table_id_from_history(item.table.history_id)?;
            let ttl = ttl_index_mutations(table_id, writes)?;
            let mut changes = Vec::new();
            changes
                .try_reserve(ttl.len() + 1)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            changes.push(crate::KvMutation::Delete {
                key: crate::item_read::primary_item_key(table_id, &item.table, &item.key)?,
            });
            changes.extend(ttl);
            catalog.append_dynamo_mutations(changes)?;
            Ok(())
        })();
        operation.complete(result)
    }

    type PreparedWrites = KvPreparedWrites;
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>) {
        if let Err(error) = &writes {
            self.attempt.root_attempt().settlement().reject_error(error);
        }
        self.prepared_writes = writes;
    }

    fn prepare_put_items(&mut self, items: &[&BoundPutItem]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        catalog.prepared_writes = Ok(Default::default());
        let result = KvPreparedWrites::try_new(items).map(|writes| {
            catalog.prepared_writes = Ok(writes);
        });
        operation.complete(result)
    }
}

pub(crate) async fn apply_ttl_index_writes<A: KvAttempt>(
    attempt: &mut A,
    table_id: u32,
    writes: impl IntoIterator<Item = TtlIndexWrite>,
) -> StorageResult<()> {
    attempt.stage(ttl_index_mutations(table_id, writes)?)
}

pub(crate) fn ttl_index_mutations(
    table_id: u32,
    writes: impl IntoIterator<Item = TtlIndexWrite>,
) -> StorageResult<Vec<KvMutation>> {
    let mut mutations = Vec::new();
    for write in writes {
        let (upsert, key) = match write {
            TtlIndexWrite::Delete(key) => (false, key),
            TtlIndexWrite::Upsert(key) => (true, key),
        };
        let key = encode_index_key(table_id, &key)?;
        mutations
            .try_reserve(1)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        // Empty markers never hold chunks.
        mutations.push(if upsert {
            KvMutation::PutUnchunked {
                key,
                value: Vec::new(),
            }
        } else {
            KvMutation::DeleteUnchunked { key }
        });
    }
    Ok(mutations)
}
