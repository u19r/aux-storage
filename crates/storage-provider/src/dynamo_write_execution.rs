//! One protected native attempt for primary, index, TTL and eligible history
//! effects.

use storage_types::{BoundTableInfo, ItemProvenance, ItemRevision, StorageResult, TimestampMillis};
use stream_provider::{
    DynamoMutationRoute, LogicalSystemStreamMutation, LogicalSystemStreamMutationInput,
};

use crate::{AcceptedDeleteItem, AcceptedUpsert, DeleteItemAttempt, UpsertAttempt};

pub(crate) struct PreparedItemHistory {
    route: DynamoMutationRoute,
    image: storage_types::VerifiedWireItem,
}

impl PreparedItemHistory {
    pub(crate) fn try_new(
        table: &BoundTableInfo,
        key: &storage_types::KeyAttributes,
        item: &storage_types::WireItem,
    ) -> StorageResult<Option<Self>> {
        prepare_history_route(table, key)?
            .map(|route| {
                let image = route.verify_item(item.clone())?;
                Ok(Self { route, image })
            })
            .transpose()
    }
}

pub(crate) fn prepare_history_route(
    table: &BoundTableInfo,
    key: &storage_types::KeyAttributes,
) -> StorageResult<Option<DynamoMutationRoute>> {
    table
        .info
        .requires_stream_entry()
        .then(|| DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, key))
        .transpose()
}

pub struct UpsertResponseImages {
    pub new_item: storage_types::WireItem,
    pub old_item: Option<storage_types::WireItem>,
}

pub async fn prepare_accepted_upsert<A: UpsertAttempt>(
    attempt: &mut A,
    mut item: AcceptedUpsert,
    created_at: TimestampMillis,
    provenance: ItemProvenance,
) -> StorageResult<UpsertResponseImages> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let policy = attempt.prepare_item_write(&item.table).await?;
        let ttl = item.ttl_effects(policy.as_ref())?;
        let effects =
            item.primary_effects_at_least(ItemRevision::initial(), provenance, ttl.epoch())?;
        attempt
            .prepare_primary_upsert(&item, &effects, ttl.into_writes())
            .await?;
        if let Some(history) = item.history.take() {
            item.capture_history(attempt, history, effects.revision(), created_at)
                .await?;
        }
        let gsi = item.prepared_gsi.finish(
            &item.gsi_lifecycle,
            &item.table.info,
            item.old_item.as_deref(),
            effects.revision(),
        )?;
        attempt
            .prepare_gsi_writes(&item.table, &effects, gsi)
            .await?;
        Ok(UpsertResponseImages {
            new_item: item.new_item,
            old_item: item.old_item.map(storage_types::WireItem::from),
        })
    }
    .await;
    settlement.complete(result)
}

pub async fn prepare_accepted_delete<A: DeleteItemAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
    created_at: TimestampMillis,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let policy = attempt.prepare_item_write(&item.table).await?;
        let ttl = item.ttl_writes(policy.as_ref())?;
        let effects = item.primary_effects_at_least(ItemRevision::initial(), provenance)?;
        if item.old_item.is_some() {
            attempt
                .prepare_gsi_writes(&item.table, &effects, item.gsi_writes(effects.revision())?)
                .await?;
            if let Some(route) = &item.history_route {
                item.capture_history(attempt, route, effects.revision(), created_at)
                    .await?;
            }
        }
        attempt.stage_primary_delete(item, &effects, ttl)
    }
    .await;
    settlement.complete(result)
}

impl AcceptedUpsert {
    async fn capture_history(
        &self,
        attempt: &mut impl crate::DynamoStreamWriteAttempt,
        history: PreparedItemHistory,
        revision: ItemRevision,
        created_at: TimestampMillis,
    ) -> StorageResult<()> {
        let PreparedItemHistory { route, image } = history;
        crate::AcceptedMutationRef::Upsert(self)
            .capture_initial_predecessor(attempt)
            .await?;
        let mutation = LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
            history_id: self.table.history_id,
            route: &route,
            item: Some(image),
            old_item: self.old_item.as_ref(),
            previous_version: self
                .old_item
                .as_ref()
                .and(self.old_revision)
                .map(|revision| storage_types::ItemStreamVersion::new(revision.get())),
            indexers: self.declaration.names(),
            item_stream_version: storage_types::ItemStreamVersion::new(revision.get()),
            created_at,
        })?;
        let publication = attempt.reserve_stream_position(route.routing_key()).await?;
        attempt
            .prepare_logical_stream_write(&self.table, mutation, publication)
            .await
    }
}

impl AcceptedDeleteItem {
    async fn capture_history(
        &self,
        attempt: &mut impl crate::DynamoStreamWriteAttempt,
        route: &DynamoMutationRoute,
        revision: ItemRevision,
        created_at: TimestampMillis,
    ) -> StorageResult<()> {
        crate::AcceptedMutationRef::Delete(self)
            .capture_initial_predecessor(attempt)
            .await?;
        let mutation = LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
            history_id: self.table.history_id,
            route,
            item: None,
            old_item: self.old_item.as_ref(),
            previous_version: self
                .old_item
                .as_ref()
                .and(self.old_revision)
                .map(|revision| storage_types::ItemStreamVersion::new(revision.get())),
            indexers: &[],
            item_stream_version: storage_types::ItemStreamVersion::new(revision.get()),
            created_at,
        })?;
        let publication = attempt.reserve_stream_position(route.routing_key()).await?;
        attempt
            .prepare_logical_stream_write(&self.table, mutation, publication)
            .await
    }
}

impl crate::AcceptedMutationRef<'_> {
    async fn capture_initial_predecessor(
        &self,
        attempt: &mut impl crate::DynamoStreamWriteAttempt,
    ) -> StorageResult<()> {
        let table = self.table();
        if table.info.table_status != storage_types::TableStatus::Updating
            && table.info.has_public_stream_request()
        {
            return Ok(());
        }
        let (key, old, revision, indexers) = match self {
            Self::Upsert(item) => (
                &item.key,
                &item.old_item,
                item.old_revision,
                &item.old_indexers,
            ),
            Self::Delete(item) => (
                &item.key,
                &item.old_item,
                item.old_revision,
                &item.old_indexers,
            ),
        };
        let Some((item, revision)) = old.as_ref().zip(revision) else {
            return Ok(());
        };
        let anchor = crate::table_stream_initialization::capture::ItemHistoryAnchor {
            key,
            item,
            revision,
            indexers,
        };
        crate::table_stream_initialization::capture::ensure_item_history_anchors(
            attempt,
            table,
            &[anchor],
        )
        .await
    }
}
