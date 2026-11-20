//! Baseline materialization has native primary/projection/TTL effects but no
//! original customer mutation or invented pre-enable stream history.

use storage_types::{ItemProvenance, StorageResult};

use crate::{AcceptedDeleteItem, AcceptedUpsert, ItemWriteAttempt};

pub async fn prepare_baseline_upsert<A: ItemWriteAttempt>(
    attempt: &mut A,
    item: AcceptedUpsert,
    provenance: ItemProvenance,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let mut ttl = item.ttl_effects()?;
        let writes = ttl.take_writes();
        let effects = ttl.metadata(provenance);
        attempt
            .prepare_primary_upsert(&item, &effects, writes)
            .await?;
        let gsi = item.prepared_gsi.finish(
            &item.gsi_lifecycle,
            &item.table.info,
            item.old_item.as_deref(),
        )?;
        attempt.prepare_gsi_writes(&item.table, gsi).await
    }
    .await;
    settlement.complete(result)
}

pub async fn prepare_baseline_delete<A: ItemWriteAttempt>(
    attempt: &mut A,
    item: &AcceptedDeleteItem,
) -> StorageResult<()> {
    if item.old_item.is_none() {
        return Ok(());
    }
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let ttl = item.ttl_writes()?;
        attempt.stage_primary_delete(item, ttl)?;
        let gsi = item.gsi_writes()?;
        attempt.prepare_gsi_writes(&item.table, gsi).await
    }
    .await;
    settlement.complete(result)
}
