//! KV persistence for the shared UpdateTable decision.

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_common::GsiLifecycleAction;
use storage_provider::{
    CatalogAttempt, StreamTrimScope, TableUpdateChange, TableUpdateRetention,
    TableUpdateTransaction, table_retention_scope_id,
};
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, TableName, TimestampMillis,
    maintenance::TargetIncarnation,
};

use crate::{
    KvAttempt, KvMutation,
    catalog::{KvCatalogAttempt, KvIndexKind, KvTableIdentity},
    gsi::{delete_gsi_builds, gsi_name_lock_key, register_gsi_builds},
    retention::{load_table_retention, write_stream_retention},
    stream_history::capture_high,
};

impl<A: KvAttempt> TableUpdateTransaction for KvCatalogAttempt<'_, A> {
    async fn load_stream_source(&mut self) -> StorageResult<stream_provider::SystemStreamHistory> {
        stream_provider::StreamAttempt::read_system_page_start_snapshot(self).await
    }

    async fn observe_table_update(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<BoundTableInfo>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let Some(stamp) = catalog.read_catalog_stamp(name).await? else {
                return Ok(None);
            };
            Ok(Some(
                catalog
                    .read_table_at_stamp(name, stamp)
                    .await?
                    .as_ref()
                    .clone(),
            ))
        }
        .await;
        settlement.complete(result)
    }

    async fn observe_table_retention(
        &mut self,
        table: &BoundTableInfo,
    ) -> StorageResult<TableUpdateRetention> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.load_table_identity(table).await?;
            let scope = StreamTrimScope::table(
                table_retention_scope_id(identity.table_id.into()),
                table.info.table_name.clone(),
            );
            let state = load_table_retention(catalog.attempt, &identity, &scope).await?;
            Ok(TableUpdateRetention { scope, state })
        }
        .await;
        settlement.complete(result)
    }

    async fn load_index_name_lock(
        &mut self,
        table: &BoundTableInfo,
        name: &IndexName,
    ) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let identity = catalog.identity_for(&table.info.table_name)?;
            let lock = catalog
                .attempt
                .get(
                    &gsi_name_lock_key(identity.table_id, name)?,
                    crate::ReadProtection::Conflict,
                )
                .await?;
            lock.map(|bytes| {
                let raw: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| StorageError::internal("invalid KV GSI name reservation"))?;
                Ok(TimestampMillis::from_timestamp(i64::from_be_bytes(raw)))
            })
            .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn persist_table_update(
        &mut self,
        current: BoundTableInfo,
        change: TableUpdateChange,
    ) -> StorageResult<BoundTableInfo> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let (plan, retention, name_locked_until) = change.into_parts();
            let old_identity = catalog.load_table_identity(&current).await?;
            let next_identity = old_identity.reconcile_user_indexes(&plan.next_table)?;
            let current_binding = catalog.current_binding(&current);
            let binding = match plan.lifecycle.as_ref() {
                Some(GsiLifecycleAction::Create(create)) => {
                    let phase =
                        register_created_index(catalog.attempt, &next_identity, &create.index_name)
                            .await?;
                    next_identity.with_added_gsi(
                        &current_binding,
                        &create.clone().into(),
                        &plan.next_table.attribute_definitions,
                        phase,
                    )?
                }
                Some(GsiLifecycleAction::Delete(delete)) => {
                    // KV keeps no native index; reclaim deletes key ranges
                    // only.
                    let retirement = storage_provider::deleted_index_retirement(
                        &current,
                        &plan,
                        <Self as storage_provider::maintenance::GsiPort>::NATIVE_INDEX,
                    )?
                    .ok_or_else(|| StorageError::internal("KV deleted index has no retirement"))?;
                    storage_provider::register_index_retirement(
                        catalog,
                        current.history_id,
                        retirement.target,
                        retirement.native_index,
                    )
                    .await?;
                    delete_gsi_builds(catalog.attempt, &old_identity, Some(&delete.index_name))
                        .await?;
                    catalog.attempt.stage(vec![KvMutation::Put {
                        key: gsi_name_lock_key(old_identity.table_id, &delete.index_name)?,
                        value: name_locked_until.timestamp_millis().to_be_bytes().to_vec(),
                    }])?;
                    current_binding.without_gsi(&delete.index_name)
                }
                None => current_binding,
            };
            if let Some(write) = retention {
                write_stream_retention(catalog.attempt, &next_identity, write).await?;
            }
            catalog
                .replace_table_layout(&current, next_identity, plan.next_table, binding)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

async fn register_created_index<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    name: &storage_types::IndexName,
) -> StorageResult<storage_types::GsiPhase> {
    let index = identity
        .indexes
        .iter()
        .find(|index| index.index_name == *name && index.index_kind == KvIndexKind::UserGsi)
        .ok_or_else(|| StorageError::internal("KV created GSI has no index identity"))?;
    let frontier = capture_high(attempt).await?;
    let incarnation = crate::gsi::index_incarnation(identity.table_id, index.index_id);
    let build = GsiBuild::<()>::new(
        GsiBuildTarget {
            history_id: identity.table_id.into(),
            table_name: identity.table_name.clone(),
            index_name: name.clone(),
            incarnation: TargetIncarnation::from(incarnation),
        },
        frontier,
    );
    let phase = storage_backfill::GsiBuildPhase::from(&build.state).into();
    register_gsi_builds(attempt, identity, vec![build]).await?;
    Ok(phase)
}
