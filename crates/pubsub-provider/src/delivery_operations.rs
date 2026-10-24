//! Shared delivery idempotency, claim, and fenced outcome decisions.

use async_trait::async_trait;
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordUpdate, PubsubAttempt, PubsubDeliveryRepository, PubsubError, PubsubPort,
    PubsubResult,
    constants::{PUBSUB_CLAIM_BATCH_SIZE, PUBSUB_WRITE_RETRY},
};

#[async_trait]
impl<P: PubsubPort> PubsubDeliveryRepository for P {
    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()> {
        self.put_delivery_records(vec![record]).await
    }

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        for (index, record) in records.iter().enumerate() {
            record.validate_storage_range()?;
            if records[..index]
                .iter()
                .any(|previous| previous.id == record.id)
            {
                return Err(PubsubError::storage("delivery batch repeats an identity"));
            }
        }
        retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let records = records.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    let ids = records.iter().map(|record| record.id.clone()).collect();
                    let existing = attempt.read_deliveries(ids).await?;
                    if existing.len() != records.len() {
                        return Err(StorageError::internal("delivery lookup count differs"));
                    }
                    let mut missing = Vec::new();
                    for (proposed, stored) in records.into_iter().zip(existing) {
                        // Creation never rewinds an existing delivery's claim
                        // or outcome.
                        if stored.is_none() {
                            missing.push(proposed);
                        }
                    }
                    attempt.write_delivery_records(missing).await
                })
            })
        })
        .await
        .map_err(PubsubError::from)
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse> {
        let limit = PUBSUB_CLAIM_BATCH_SIZE.get().min(u32::from(request.limit));
        let limit = std::num::NonZeroU32::new(limit)
            .ok_or_else(|| PubsubError::storage("delivery claim limit is zero"))?;
        let records = retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let request = request.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { claim_in_attempt(attempt, &request, limit).await })
            })
        })
        .await
        .map_err(PubsubError::from)?;
        Ok(ClaimDeliveryRecordsResponse { records })
    }

    async fn update_delivery_record(&self, update: DeliveryRecordUpdate) -> PubsubResult<()> {
        let id = update.record_id.clone();
        let changed = retry_native_conflicts(self, PUBSUB_WRITE_RETRY, || {
            let update = update.clone();
            self.with_pubsub_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { crate::settle_delivery_in_attempt(attempt, update).await })
            })
        })
        .await
        .map_err(PubsubError::from)?;
        if changed {
            Ok(())
        } else {
            Err(PubsubError::delivery_claim_lost(id.0))
        }
    }

    async fn get_delivery_record(
        &self,
        id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>> {
        let id = id.clone();
        self.with_pubsub_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { one_delivery(attempt.read_deliveries(vec![id]).await?) })
        })
        .await
        .map_err(PubsubError::from)
    }
}

async fn claim_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    request: &ClaimDeliveryRecordsRequest,
    limit: std::num::NonZeroU32,
) -> StorageResult<Vec<DeliveryRecord>> {
    let candidates = attempt.read_due_deliveries(request.now, limit).await?;
    if candidates.len() > limit.get() as usize {
        return Err(StorageError::internal(
            "delivery claim page exceeded its limit",
        ));
    }
    let mut claimed = Vec::with_capacity(candidates.len());
    let mut replacements = Vec::with_capacity(candidates.len());
    for previous in candidates {
        let current = previous
            .claim(request)?
            .ok_or_else(|| StorageError::internal("delivery due index contains unclaimable row"))?;
        replacements.push((previous, current.clone()));
        claimed.push(current);
    }
    attempt.replace_deliveries(replacements).await?;
    Ok(claimed)
}

pub(crate) fn one_delivery(
    rows: Vec<Option<DeliveryRecord>>,
) -> StorageResult<Option<DeliveryRecord>> {
    if rows.len() != 1 {
        return Err(StorageError::internal(
            "delivery point read returned another count",
        ));
    }
    Ok(rows.into_iter().next().flatten())
}
