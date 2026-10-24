use std::{collections::HashSet, hash::BuildHasher, sync::Arc, time::Duration};

use tokio::sync::Semaphore;

use super::{
    PointReadClient, PointReadEntry, PointReadReservation,
    association::ResponseSources,
    constants::{
        POINT_READ_BATCH_ATTEMPTS, POINT_READ_ITEM_OVERHEAD_BYTES, POINT_READ_RESPONSE_BYTES,
    },
    key_fingerprint,
    source::SourceKey,
};
use crate::types::{
    BatchGetItemRequest, BatchGetItemResponse, KeysAndAttributes, StorageError, StorageResult,
    VecMap,
};

pub(super) struct BatchExecutor<'a> {
    db: &'a dyn PointReadClient,
    budget: &'a Arc<Semaphore>,
    reservation: PointReadReservation,
}

impl<'a> BatchExecutor<'a> {
    pub(super) fn new(
        db: &'a dyn PointReadClient,
        budget: &'a Arc<Semaphore>,
        reservation: PointReadReservation,
    ) -> Self {
        Self {
            db,
            budget,
            reservation,
        }
    }

    pub(super) async fn execute(&self, sources: &[SourceKey]) {
        if let Err(error) = self.read_batch(sources).await {
            for source in sources {
                source.publish_result(|| Err(error.clone()));
            }
        }
    }

    async fn read_batch(&self, sources: &[SourceKey]) -> StorageResult<()> {
        let mut pending: Vec<_> = sources.iter().collect();
        for attempt in 0..POINT_READ_BATCH_ATTEMPTS {
            Self::retain_pending(&mut pending);
            if pending.is_empty() {
                return Ok(());
            }
            let response = self.fetch_batch(&pending).await?;
            pending = self.apply_response(response, &pending)?;
            if pending.is_empty() {
                return Ok(());
            }
            if attempt + 1 < POINT_READ_BATCH_ATTEMPTS {
                let jitter = std::collections::hash_map::RandomState::new().hash_one(attempt) % 3;
                tokio::time::sleep(Duration::from_millis((2 << attempt) + jitter)).await;
            }
        }
        Err(StorageError::service_unavailable(1))
    }

    fn retain_pending(pending: &mut Vec<&SourceKey>) {
        pending.retain(|source| {
            if source.is_obsolete() {
                source.publish_result(|| Err(StorageError::service_unavailable(1)));
            }
            !source.has_completed()
        });
    }

    async fn fetch_batch(&self, pending: &[&SourceKey]) -> StorageResult<BatchGetItemResponse> {
        let Some(first) = pending.first() else {
            return Err(StorageError::service_unavailable(1));
        };
        first.observe(super::PointReadEvent::Attempt);
        first
            .cost_context
            .scope(async {
                self.db
                    .batch_get_item(Self::build_batch_request(pending), self.reservation.clone())
                    .await
            })
            .await
    }

    fn build_batch_request(sources: &[&SourceKey]) -> BatchGetItemRequest {
        let mut request_items = VecMap::new();
        for source in sources {
            let request = &source.request;
            let table = request_items.get_or_insert_with(request.table_name.clone(), || {
                KeysAndAttributes {
                    keys: Vec::new(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                }
            });
            table.keys.push(request.key.clone());
        }
        BatchGetItemRequest {
            request_items,
            return_consumed_capacity: None,
        }
    }

    fn apply_response<'r>(
        &self,
        response: BatchGetItemResponse,
        sources: &[&'r SourceKey],
    ) -> StorageResult<Vec<&'r SourceKey>> {
        let unprocessed = response
            .unprocessed_keys
            .iter()
            .flat_map(|tables| tables.values())
            .map(|table| table.keys.len() as u64)
            .sum();
        if let Some(first) = sources.first() {
            first.observe(super::PointReadEvent::Unprocessed(unprocessed));
        }
        Self::validate_response_budget(&response)?;
        self.apply_hits(response.responses, sources)?;
        let unprocessed = response
            .unprocessed_keys
            .into_iter()
            .flat_map(|tables| tables.into_iter())
            .flat_map(|(table, attributes)| {
                attributes
                    .keys
                    .into_iter()
                    .map(move |key| key_fingerprint(&table, &key))
            })
            .collect::<StorageResult<HashSet<_>>>()?;
        let mut pending = Vec::new();
        for source in sources {
            if source.has_completed() {
                continue;
            }
            if unprocessed.contains(&source.request.fingerprint) {
                pending.push(*source);
            } else {
                source.publish_result(|| Ok(PointReadEntry::Miss));
            }
        }
        Ok(pending)
    }

    fn validate_response_budget(response: &BatchGetItemResponse) -> StorageResult<()> {
        let bytes = response
            .responses
            .iter()
            .flat_map(|tables| tables.values())
            .flatten()
            .try_fold(0usize, |total, item| {
                total.checked_add(
                    item.payload_len()
                        .saturating_add(POINT_READ_ITEM_OVERHEAD_BYTES),
                )
            });
        if bytes.is_none_or(|bytes| bytes > POINT_READ_RESPONSE_BYTES) {
            return Err(StorageError::service_unavailable(1));
        }
        Ok(())
    }

    fn apply_hits(
        &self,
        responses: Option<VecMap<crate::types::TableName, Vec<crate::types::WireItem>>>,
        sources: &[&SourceKey],
    ) -> StorageResult<()> {
        let index = ResponseSources::try_new(sources)?;
        for (table, items) in responses.into_iter().flat_map(|tables| tables.into_iter()) {
            for item in items {
                let Some(source) = index.find_source(&table, &item)? else {
                    continue;
                };
                source.publish_result(|| PointReadEntry::try_hit(item, self.budget));
            }
        }
        Ok(())
    }
}
