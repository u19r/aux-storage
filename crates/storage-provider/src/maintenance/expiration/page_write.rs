//! Reuse the Dynamo delete decision and all of its item, index, and stream
//! effects.

use std::sync::Arc;

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlIndexKey, TtlIndexWrite, ttl::TtlIndexKeyToken};
use storage_types::{BoundTableInfo, StorageResult, TimestampMillis};

use super::{ExpirationDelete, ExpirationIndexLocator};
use crate::{
    AcceptedDeleteItem, ItemWriteAttempt, ItemWriteObservation, ItemWriteObservationRead,
    prepare_accepted_delete,
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

pub async fn prepare_expiration_item_deletes<A: ItemWriteAttempt + crate::CatalogAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    items: Vec<ExpirationDelete>,
    captured_at: TimestampMillis,
) -> StorageResult<()> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let mut routes_ready = false;
        for accepted in accept_expiration_item_deletes(attempt, table, items).await? {
            let mut accepted = accepted?;
            if !routes_ready && accepted.old_item.is_some() && accepted.history_route.is_some() {
                crate::prepare_writer_routes_in_attempt(attempt, None).await?;
                routes_ready = true;
            }
            accepted.user_identity = Some(storage_types::StreamUserIdentity::service_ttl());
            prepare_accepted_delete(attempt, &mut accepted, captured_at).await?;
        }
        attempt.flush_dynamo_write().await
    }
    .await;
    settlement.complete(result)
}

pub async fn accept_expiration_item_deletes<A: ItemWriteAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    items: Vec<ExpirationDelete>,
) -> StorageResult<impl Iterator<Item = StorageResult<AcceptedDeleteItem>> + Send + use<A>> {
    let (items, observations) =
        observe_expiration_item_deletes(attempt, Arc::clone(&table), items).await?;
    Ok(items
        .into_iter()
        .zip(observations)
        .map(move |(item, (observed, lifecycle))| {
            let old = crate::dynamo_put_item::ObservedWrite::try_new(observed, &item.key)?;
            AcceptedDeleteItem::try_from_observed(Arc::clone(&table), item.key, old, lifecycle)
        }))
}

pub(super) async fn observe_expiration_item_deletes<A: ItemWriteAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    mut items: Vec<ExpirationDelete>,
) -> StorageResult<(
    Vec<ExpirationDelete>,
    Vec<(ItemWriteObservation, GsiProjectionLifecycle)>,
)> {
    for item in &mut items {
        storage_types::validate_transact_key(&table.info, &item.key)?;
        item.key.normalize_numbers_for_write();
    }
    let reads = items
        .iter()
        .map(|item| ItemWriteObservationRead::Write(&table, &item.key))
        .collect::<Vec<_>>();
    let observations = attempt.observe_for_writes(&reads).await?;
    Ok((items, observations.into_vec()))
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
