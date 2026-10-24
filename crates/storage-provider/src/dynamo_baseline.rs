//! Baseline materialization has native primary/projection/TTL effects but no
//! original customer mutation or invented pre-enable stream history.

use storage_types::{ItemProvenance, ItemRevision, StorageResult};

use crate::{AcceptedDeleteItem, AcceptedUpsert, DeleteItemAttempt, UpsertAttempt};

pub async fn prepare_baseline_upsert<A: UpsertAttempt>(
    attempt: &mut A,
    item: AcceptedUpsert,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let (policy, _) = attempt.prepare_item_write(&item.table, None).await?;
    let ttl = item.ttl_effects(policy.as_ref())?;
    let effects =
        item.primary_effects_at_least(ItemRevision::initial(), provenance, ttl.epoch())?;
    attempt
        .prepare_primary_upsert(&item, &effects, ttl.into_writes())
        .await?;
    let gsi = item.prepared_gsi.finish(
        &item.gsi_lifecycle,
        &item.table.info,
        item.old_item.as_deref(),
        effects.revision(),
    )?;
    attempt.prepare_gsi_writes(&item.table, &effects, gsi).await
}

pub async fn prepare_baseline_delete<A: DeleteItemAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let (policy, _) = attempt.prepare_item_write(&item.table, None).await?;
    let ttl = item.ttl_writes(policy.as_ref())?;
    let effects = item.primary_effects_at_least(ItemRevision::initial(), provenance)?;
    stream_provider::retained_image::PrimaryImageOwner::try_new(&item.table, &item.key)?
        .release(attempt, item.old_item.as_ref())
        .await?;
    attempt.stage_primary_delete(item, &effects, ttl)?;
    let gsi = item.gsi_writes(effects.revision())?;
    attempt.prepare_gsi_writes(&item.table, &effects, gsi).await
}
