//! Reuse the Dynamo delete decision and all of its item, index, and stream
//! effects.

use std::sync::Arc;

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlIndexKey, TtlIndexWrite, ttl::TtlIndexKeyToken};
use storage_types::{BoundTableInfo, ItemProvenance, StorageResult, TimestampMillis};

use super::{ExpirationDelete, ExpirationIndexLocator};
use crate::{
    AcceptedDeleteItem, BoundDeleteItem, DeleteItemAttempt, ItemWriteObservation,
    ItemWriteObservationRead, prepare_accepted_delete,
};

pub fn expiration_index_deletes(
    entries: Vec<ExpirationIndexLocator>,
) -> impl Iterator<Item = TtlIndexWrite> {
    entries.into_iter().map(|entry| {
        TtlIndexWrite::Delete(TtlIndexKey::new(
            entry.indexed_expiration_seconds,
            TtlIndexKeyToken::from(entry.ttl_item_key),
        ))
    })
}

pub async fn prepare_expiration_item_deletes<A: DeleteItemAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    items: Vec<ExpirationDelete>,
    captured_at: TimestampMillis,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        for accepted in accept_expiration_item_deletes(attempt, table, items).await? {
            let accepted = accepted?;
            prepare_accepted_delete(attempt, &accepted, captured_at, ItemProvenance::default())
                .await?;
        }
        attempt.flush_dynamo_write().await
    }
    .await;
    settlement.complete(result)
}

pub async fn accept_expiration_item_deletes<A: DeleteItemAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    items: Vec<ExpirationDelete>,
) -> StorageResult<impl Iterator<Item = StorageResult<AcceptedDeleteItem>> + Send + use<A>> {
    let (bound, observations) = observe_expiration_item_deletes(attempt, table, items).await?;
    Ok(bound
        .into_iter()
        .zip(observations)
        .map(|(item, (observed, lifecycle))| item.accept_old(observed, lifecycle)?.into_result()))
}

pub(super) async fn observe_expiration_item_deletes<A: DeleteItemAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    items: Vec<ExpirationDelete>,
) -> StorageResult<(
    Vec<BoundDeleteItem>,
    Vec<(ItemWriteObservation, GsiProjectionLifecycle)>,
)> {
    let bound = items
        .into_iter()
        .map(|item| BoundDeleteItem::for_maintenance(Arc::clone(&table), item.key))
        .collect::<StorageResult<Vec<_>>>()?;
    let reads = bound
        .iter()
        .map(|item| ItemWriteObservationRead::Write(&item.table, &item.key))
        .collect::<Vec<_>>();
    let observations = attempt.observe_for_writes(&reads).await?;
    if observations.len() != bound.len() {
        return Err(storage_types::StorageError::internal(
            "expiration observation count differs",
        ));
    }
    Ok((bound, observations))
}

impl super::ExpirationDeletionBatch {
    pub(super) async fn prepare_deletions<
        A: super::ExpirationPort,
        S: super::ExpirationEffectPolicy<A>,
    >(
        self,
        attempt: &mut A,
        admission: &S,
        table: Arc<BoundTableInfo>,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        admission
            .delete_items(attempt, Arc::clone(&table), self.items, captured_at)
            .await?;
        attempt
            .delete_expiration_index(&table, expiration_index_deletes(self.index_entries))
            .await
    }
}
