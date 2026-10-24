//! One protected native attempt for ordinary PutItem and DeleteItem effects.

use storage_types::{
    BoundTableInfo, ItemProvenance, ItemRevision, KeyAttributes, StorageResult,
    StreamRetentionDuration, TimestampMillis,
};
use stream_provider::{
    DynamoStreamIds, LogicalSystemStreamMutation, LogicalSystemStreamMutationInput,
    dynamo_item_history_address,
};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, DeleteItemAttempt, DynamoStreamWriteAttempt,
    ItemStreamDurationInput, StreamTrimState, StreamTrimStateWrite, UpsertAttempt,
    item_retention_scope_id, plan_item_stream_duration,
};

/// Only response images survive effect staging and native settlement.
pub struct UpsertResponseImages {
    pub new_item: storage_types::WireItem,
    pub old_item: Option<storage_types::WireItem>,
}

/// Remaining stream and response ownership after the GSI plan is consumed.
struct AcceptedUpsertStream {
    table: std::sync::Arc<BoundTableInfo>,
    route: stream_provider::DynamoMutationRoute,
    stream_image: stream_provider::PreparedStreamImage,
    old_item: Option<storage_types::VerifiedWireItem>,
    declaration: storage_types::IndexerDeclaration,
    old_indexers: Vec<String>,
    new_item: storage_types::WireItem,
}

pub async fn prepare_accepted_upsert<A: UpsertAttempt>(
    attempt: &mut A,
    item: AcceptedUpsert,
    created_at: TimestampMillis,
    provenance: ItemProvenance,
) -> StorageResult<UpsertResponseImages> {
    let mut settlement_guard =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement_guard.as_mut();
    let result = async {
        let retention = plan_custom_item_retention(
            &item.table,
            &item.key,
            item.item_stream_ttl_hours,
            created_at,
        )?;
        let (ttl_config, current_retention) = attempt
            .prepare_item_write(
                &item.table,
                retention.as_ref().map(|write| &write.state.scope),
            )
            .await?;
        let ttl = item.ttl_effects(ttl_config.as_ref())?;
        let effects =
            item.primary_effects_at_least(ItemRevision::initial(), provenance, ttl.epoch())?;
        attempt
            .prepare_primary_upsert(&item, &effects, ttl.into_writes())
            .await?;
        let (gsi, stream) = item.finish_gsi(effects.revision())?;
        let position = attempt
            .prepare_gsi_and_stream(&stream.table, &effects, gsi, stream.route.routing_key())
            .await?;
        let ids = DynamoStreamIds::for_write(effects.revision(), &position)?;
        let (table, mutation, response) = stream.into_stream_response(ids, created_at);
        attempt
            .prepare_logical_stream_write(&table, mutation, position)
            .await?;
        stage_custom_item_retention(attempt, &table, retention, current_retention)?;
        Ok(response)
    }
    .await;
    settlement_guard.complete(result)
}

pub async fn prepare_accepted_delete<A: DeleteItemAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
    created_at: TimestampMillis,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let mut settlement_guard =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement_guard.as_mut();
    let result = async {
        let retention = plan_custom_item_retention(
            &item.table,
            &item.key,
            item.item_stream_ttl_hours,
            created_at,
        )?;
        let (ttl_config, current_retention) = attempt
            .prepare_item_write(
                &item.table,
                retention.as_ref().map(|write| &write.state.scope),
            )
            .await?;
        let ttl = item.ttl_writes(ttl_config.as_ref())?;
        let effects = item.primary_effects_at_least(ItemRevision::initial(), provenance)?;
        stream_provider::retained_image::PrimaryImageOwner::try_new(&item.table, &item.key)?
            .release(attempt, item.old_item.as_ref())
            .await?;
        let Some(old_item) = item.old_item.as_ref() else {
            attempt.stage_primary_delete(item, &effects, ttl)?;
            return stage_custom_item_retention(attempt, &item.table, retention, current_retention);
        };
        let gsi = item.gsi_writes(effects.revision())?;
        let position = attempt
            .prepare_gsi_and_stream(&item.table, &effects, gsi, item.route.routing_key())
            .await?;
        let ids = DynamoStreamIds::for_write(effects.revision(), &position)?;
        let mutation = item.prepare_stream_mutation(old_item, ids, created_at)?;
        attempt.stage_primary_delete(item, &effects, ttl)?;
        attempt
            .prepare_logical_stream_write(&item.table, mutation, position)
            .await?;
        stage_custom_item_retention(attempt, &item.table, retention, current_retention)
    }
    .await;
    settlement_guard.complete(result)
}

impl AcceptedUpsert {
    fn finish_gsi(
        self,
        revision: ItemRevision,
    ) -> StorageResult<(
        storage_backfill::GsiProjectionWriteBatch,
        AcceptedUpsertStream,
    )> {
        let gsi = self.prepared_gsi.finish(
            &self.gsi_lifecycle,
            &self.table.info,
            self.old_item.as_deref(),
            revision,
        )?;
        let stream = AcceptedUpsertStream {
            table: self.table,
            route: self.route,
            stream_image: self.stream_image,
            old_item: self.old_item,
            declaration: self.declaration,
            old_indexers: self.old_indexers,
            new_item: self.new_item,
        };
        Ok((gsi, stream))
    }
}

impl AcceptedUpsertStream {
    fn into_stream_response(
        self,
        ids: DynamoStreamIds,
        created_at: TimestampMillis,
    ) -> (
        std::sync::Arc<BoundTableInfo>,
        LogicalSystemStreamMutation,
        UpsertResponseImages,
    ) {
        let mutation = LogicalSystemStreamMutation::from(LogicalSystemStreamMutationInput {
            history_id: self.table.history_id,
            route: &self.route,
            item: self.stream_image,
            old_item: self.old_item.clone(),
            indexers: self.declaration.names(),
            old_indexers: self.old_item.as_ref().map(|_| self.old_indexers.as_slice()),
            stream_item_id: ids.system_pointer_id(),
            item_stream_version: ids.item_stream_version(),
            is_deleted: false,
            created_at,
        });
        let response = UpsertResponseImages {
            new_item: self.new_item,
            old_item: self.old_item.map(storage_types::WireItem::from),
        };
        (self.table, mutation, response)
    }
}

impl AcceptedDeleteItem {
    fn prepare_stream_mutation(
        &self,
        old_item: &storage_types::VerifiedWireItem,
        ids: DynamoStreamIds,
        created_at: TimestampMillis,
    ) -> StorageResult<LogicalSystemStreamMutation> {
        Ok(LogicalSystemStreamMutation::from(
            LogicalSystemStreamMutationInput {
                history_id: self.table.history_id,
                route: &self.route,
                item: stream_provider::PreparedStreamImage::try_from(old_item.clone())?,
                old_item: Some(old_item.clone()),
                indexers: &[],
                old_indexers: Some(&self.old_indexers),
                stream_item_id: ids.system_pointer_id(),
                item_stream_version: ids.item_stream_version(),
                is_deleted: true,
                created_at,
            },
        ))
    }
}

fn plan_custom_item_retention(
    table: &BoundTableInfo,
    key: &KeyAttributes,
    requested: Option<StreamRetentionDuration>,
    updated_at: TimestampMillis,
) -> StorageResult<Option<StreamTrimStateWrite>> {
    let Some(requested_retention) = requested else {
        return Ok(None);
    };
    let address = dynamo_item_history_address(table, key)?;
    let scope_id = item_retention_scope_id(&address);
    Ok(Some(plan_item_stream_duration(ItemStreamDurationInput {
        table_name: table.info.table_name.clone(),
        key_hash: scope_id.clone(),
        scope_id,
        requested_retention,
        table_retention: table.info.table_stream_duration,
        updated_at,
    })))
}

fn stage_custom_item_retention<A: DynamoStreamWriteAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    write: Option<StreamTrimStateWrite>,
    current: Option<StreamTrimState>,
) -> StorageResult<()> {
    let Some(write) = write else {
        return Ok(());
    };
    let write = write.replace_item_policy(current.as_ref())?;
    attempt.stage_item_stream_retention(table, write)
}
