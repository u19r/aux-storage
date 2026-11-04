//! Baseline materialization has native primary/projection/TTL effects but no
//! original customer mutation or invented pre-enable stream history.

use storage_types::{
    BoundTableInfo, ItemHistoryChange, ItemHistoryEntry, ItemHistoryMetadata,
    ItemHistoryPublication, ItemHistoryReference, ItemProvenance, ItemRevision, ItemStreamVersion,
    KeyAttributes, ReadProtection, StorageError, StorageResult,
};

use crate::{AcceptedDeleteItem, AcceptedUpsert, DeleteItemAttempt, UpsertAttempt};

pub async fn prepare_baseline_upsert<A: UpsertAttempt>(
    attempt: &mut A,
    item: AcceptedUpsert,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let delete = release_former_current(
            attempt,
            &item.table,
            &item.key,
            item.old_item.as_ref().and(item.old_revision),
        )
        .await?;
        let policy = attempt.prepare_item_write(&item.table).await?;
        let ttl = item.ttl_effects(policy.as_ref())?;
        let effects =
            item.primary_effects_at_least(ItemRevision::initial(), provenance, ttl.epoch())?;
        attempt
            .prepare_primary_upsert(&item, &effects, ttl.into_writes())
            .await?;
        let (table, gsi, anchor) = item.finish_baseline(effects.revision())?;
        attempt.stage_item_history(Some(anchor), delete)?;
        attempt.prepare_gsi_writes(&table, &effects, gsi).await
    }
    .await;
    settlement.complete(result)
}

pub async fn prepare_baseline_delete<A: DeleteItemAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let delete = release_former_current(
            attempt,
            &item.table,
            &item.key,
            item.old_item.as_ref().and(item.old_revision),
        )
        .await?;
        attempt.stage_item_history(None, delete)?;
        let policy = attempt.prepare_item_write(&item.table).await?;
        let ttl = item.ttl_writes(policy.as_ref())?;
        let effects = item.primary_effects_at_least(ItemRevision::initial(), provenance)?;
        attempt.stage_primary_delete(item, &effects, ttl)?;
        let gsi = item.gsi_writes(effects.revision())?;
        attempt.prepare_gsi_writes(&item.table, &effects, gsi).await
    }
    .await;
    settlement.complete(result)
}

impl AcceptedUpsert {
    fn finish_baseline(
        self,
        revision: ItemRevision,
    ) -> StorageResult<(
        std::sync::Arc<BoundTableInfo>,
        storage_backfill::GsiProjectionWriteBatch,
        ItemHistoryEntry,
    )> {
        let gsi = self.prepared_gsi.finish(
            &self.gsi_lifecycle,
            &self.table.info,
            self.old_item.as_deref(),
            revision,
        )?;
        let metadata = ItemHistoryMetadata::try_new(
            stream_provider::dynamo_item_history_address(&self.table, &self.key)?,
            ItemStreamVersion::new(revision.get()),
            None,
            ItemHistoryPublication::PredecessorAnchor,
        )?;
        let anchor = ItemHistoryEntry::try_new(
            metadata,
            ItemHistoryChange::Image {
                item: self.new_item,
                indexers: self.declaration,
            },
        )?;
        Ok((self.table, gsi, anchor))
    }
}

async fn release_former_current<A: crate::DynamoStreamWriteAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    revision: Option<ItemRevision>,
) -> StorageResult<Option<ItemHistoryReference>> {
    if attempt.read_protection() != ReadProtection::Conflict {
        return Err(StorageError::internal(
            "Baseline history requires protected current state",
        ));
    }
    let Some(revision) = revision else {
        return Ok(None);
    };
    let reference = ItemHistoryReference::try_new(
        stream_provider::dynamo_item_history_address(table, key)?,
        ItemStreamVersion::new(revision.get()),
    )?;
    let metadata = load_current_history_metadata(attempt, &reference).await?;
    if matches!(
        metadata.publication(),
        ItemHistoryPublication::PredecessorAnchor
    ) {
        return Ok(Some(reference));
    }
    let state = attempt
        .load_table_history_retention(table, ReadProtection::Conflict)
        .await?
        .ok_or_else(|| StorageError::internal("Table retention state is missing"))?;
    state.require_valid_frontier()?;
    if state.frontier.table_history_id != table.history_id {
        return Err(StorageError::internal(
            "Baseline history has another table authority",
        ));
    }
    Ok(state.frontier.is_retired(&metadata)?.then_some(reference))
}

async fn load_current_history_metadata(
    attempt: &mut impl stream_provider::ItemHistoryReadAttempt,
    reference: &ItemHistoryReference,
) -> StorageResult<ItemHistoryMetadata> {
    let headers = attempt
        .load_item_history_headers(stream_provider::ItemHistoryReadBatch {
            references: std::slice::from_ref(reference),
            maximum_bytes: std::num::NonZeroUsize::new(
                storage_types::ItemHistoryRecord::encoded_length_bound()
                    + stream_provider::StreamRowValue::history_envelope_bytes(reference.address())?,
            )
            .ok_or_else(|| StorageError::internal("Invalid history admission bound"))?,
        })
        .await?;
    let [header]: [_; 1] = headers
        .try_into()
        .map_err(|_| StorageError::internal("History header count differs"))?;
    let header =
        header.ok_or_else(|| StorageError::internal("Current item history is unavailable"))?;
    if header.metadata().kind() != storage_types::ItemHistoryKind::Image {
        return Err(StorageError::internal(
            "Current item history is not an image",
        ));
    }
    Ok(header.into_metadata())
}
