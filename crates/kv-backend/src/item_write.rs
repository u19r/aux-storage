//! Protected KV item and revision observations for a native write.

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlConfigRecord, TtlIndexWrite, TtlIndexWriteBatch};
use storage_provider::{
    AcceptedUpsert, ItemPrimaryEffects, ItemWriteObservation, ItemWriteObservationAttempt,
    ItemWriteObservationRead, StreamTrimScope, StreamTrimState, UpsertAttempt,
    combine_write_observations, lifecycle_tables_for_writes,
};
use storage_types::{BoundTableInfo, KeyAttributes, StorageError, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvMutation, ReadProtection,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    expiration::key_layout::encode_index_key,
    gsi::{decode_gsi_lifecycle, gsi_build_keys},
    item_read::{HydratedPrimaryItem, hydrate_primary_items, primary_item_key},
    item_tombstone::{decode::decode_deleted, primary_tombstone_key},
    retention, stream_history, ttl_policy,
};

impl<A: KvAttempt> ItemWriteObservationAttempt for KvCatalogAttempt<'_, A> {
    fn item_write_cache(&self) -> Option<&storage_provider::TableMetadataCache> {
        self.metadata_cache.as_deref()
    }

    async fn read_item_write_prerequisites(
        &mut self,
        table: &BoundTableInfo,
        retention_scope: Option<&StreamTrimScope>,
        needs_ttl: bool,
    ) -> StorageResult<(Option<TtlConfigRecord>, Option<StreamTrimState>)> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let mut keys = Vec::new();
            keys.try_reserve_exact(usize::from(needs_ttl) + usize::from(retention_scope.is_some()))
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            if needs_ttl {
                let identity = catalog.identity_for(&table.info.table_name)?;
                keys.push(ttl_policy::ttl_config_key(identity)?);
            }
            if let Some(scope) = retention_scope {
                keys.push(retention::state_key(scope));
            }
            let needs_history = catalog.dynamo_history.is_none();
            let attempt = &*catalog.attempt;
            let protection = catalog.protection;
            let values = async {
                if keys.is_empty() {
                    Ok(Vec::new())
                } else {
                    attempt.get_batch(keys, protection).await
                }
            };
            let history = async {
                if needs_history {
                    stream_history::read_history(attempt).await.map(Some)
                } else {
                    Ok(None)
                }
            };
            let (values, history) = futures::try_join!(values, history)?;
            if let Some(history) = history {
                catalog.dynamo_history = Some(stream_history::KvSystemWriteState::new(history));
            }
            if values.len() != usize::from(needs_ttl) + usize::from(retention_scope.is_some()) {
                return Err(StorageError::internal(
                    "KV item write prerequisite count differs",
                ));
            }
            let mut values = values.into_iter();
            let ttl = if needs_ttl {
                values
                    .next()
                    .ok_or_else(|| StorageError::internal("KV TTL prerequisite is missing"))?
                    .map(|bytes| storage_serde::from_bytes(&bytes))
                    .transpose()?
            } else {
                None
            };
            let retention = retention_scope
                .zip(values.next())
                .map(|(scope, value)| {
                    value
                        .map(|bytes| retention::decode_trim_state(&bytes, scope))
                        .transpose()
                })
                .transpose()?
                .flatten();
            Ok((ttl, retention))
        }
        .await;
        operation.complete(result)
    }

    async fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> StorageResult<Vec<(ItemWriteObservation, GsiProjectionLifecycle)>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let count = reads
                .len()
                .checked_mul(2)
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut keys = Vec::new();
            let mut key_jsons = Vec::new();
            keys.try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            key_jsons
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let lifecycle_tables = lifecycle_tables_for_writes(reads)?;
            for read in reads {
                let (table, key) = read.target();
                let identity = catalog.identity_for(&table.info.table_name)?;
                let (primary, key_json) = write_observation_keys(identity, table, key)?;
                keys.extend(primary);
                key_jsons.push(key_json);
            }
            let primary_count = keys.len();
            let mut identities = Vec::new();
            let mut lifecycle_counts = Vec::new();
            identities
                .try_reserve_exact(lifecycle_tables.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            lifecycle_counts
                .try_reserve_exact(lifecycle_tables.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for table in lifecycle_tables {
                identities.push(catalog.identity_for(&table.info.table_name)?.clone());
            }
            for identity in &identities {
                let builds = gsi_build_keys(identity)?;
                keys.try_reserve_exact(builds.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                lifecycle_counts.push(builds.len());
                keys.extend(builds);
            }
            let expected = keys.len();
            let values = catalog
                .attempt
                .get_batch(keys, ReadProtection::Conflict)
                .await?;
            if values.len() != expected {
                return Err(StorageError::internal(
                    "KV write observation returned the wrong count",
                ));
            }
            let mut requests = Vec::new();
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            requests.extend(
                reads
                    .iter()
                    .enumerate()
                    .map(|(index, read)| (read.target().0, values[index * 2].as_deref())),
            );
            let hydrated = hydrate_primary_items(
                catalog,
                requests,
                reads
                    .len()
                    .checked_mul(crate::constants::PAGE_READ_BYTES.get())
                    .ok_or_else(|| StorageError::internal("Primary observation bound overflow"))?,
            )
            .await?;
            for (read, item) in reads.iter().zip(&hydrated) {
                if let Some(item) = item
                    && !item.stored.item.has_verified_keys(read.target().1)?
                {
                    return Err(StorageError::internal(
                        "KV observation content differs from physical key",
                    ));
                }
            }
            let mut hydrated = hydrated.into_iter();
            let mut values = values.into_iter();
            let mut items = Vec::new();
            items
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (read, key_json) in reads.iter().zip(key_jsons) {
                let (table, _) = read.target();
                values
                    .next()
                    .ok_or_else(|| StorageError::internal("KV item observation is missing"))?;
                let revision = values
                    .next()
                    .ok_or_else(|| StorageError::internal("KV revision observation is missing"))?;
                items.push(decode_primary_observation(
                    table,
                    &key_json,
                    hydrated
                        .next()
                        .ok_or_else(|| StorageError::internal("Missing primary observation"))?,
                    revision,
                )?);
            }
            debug_assert_eq!(primary_count, reads.len() * 2);
            let mut lifecycles = Vec::new();
            lifecycles
                .try_reserve_exact(identities.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (identity, count) in identities.into_iter().zip(lifecycle_counts) {
                let mut builds = Vec::new();
                builds
                    .try_reserve_exact(count)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                builds.extend(values.by_ref().take(count));
                lifecycles.push((
                    identity.table_name.clone(),
                    decode_gsi_lifecycle(&identity, builds)?,
                ));
            }
            combine_write_observations(reads, items, lifecycles)
        }
        .await;
        operation.complete(result)
    }
}

impl<A: KvAttempt> UpsertAttempt for KvCatalogAttempt<'_, A> {
    async fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &ItemPrimaryEffects,
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
            let identity = catalog.identity_for(&item.table.info.table_name)?.clone();
            let mut changes = image
                .prepare(catalog, &identity, effects, item.old_item.as_ref())
                .await?;
            let ttl = ttl_index_mutations(identity.table_id, writes)?;
            changes
                .try_reserve(ttl.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes.extend(ttl);
            catalog.append_dynamo_mutations(changes)?;
            Ok(())
        }
        .await;
        operation.complete(result)
    }
}

fn write_observation_keys(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<([Vec<u8>; 2], String)> {
    let primary = primary_item_key(identity, table, key)?;
    let (tombstone, key_json) = primary_tombstone_key(table, key)?;
    Ok(([primary, tombstone], key_json))
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
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        mutations.push(if upsert {
            KvMutation::Put {
                key,
                value: Vec::new(),
            }
        } else {
            KvMutation::Delete { key }
        });
    }
    Ok(mutations)
}

fn decode_primary_observation(
    table: &BoundTableInfo,
    key_json: &str,
    item: Option<HydratedPrimaryItem>,
    revision: Option<Vec<u8>>,
) -> StorageResult<ItemWriteObservation> {
    match (item, revision) {
        (None, None) => Ok(ItemWriteObservation::Absent),
        (Some(item), None) => Ok(ItemWriteObservation::Live {
            stored: Box::new(item.stored),
            revision: item.revision,
            provenance: item.provenance,
            ttl_epoch: item.ttl_epoch,
        }),
        (None, Some(bytes)) => decode_deleted(table, key_json, &bytes),
        (Some(_), Some(_)) => Err(StorageError::internal(
            "KV key has both a live item and a tombstone",
        )),
    }
}
