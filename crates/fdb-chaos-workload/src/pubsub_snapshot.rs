//! Complete bounded durable snapshots for the simulator's publish oracle.

use kv_backend::{AttemptMode, KvAttempt, KvBackend, KvRange, ReadProtection};
use pubsub_provider::{
    DeliveryPayload, DeliveryRecordId, PublishCandidate, PublishIntent, Subscription,
};
use storage_types::{AttemptPlan, DeadlineClass, StorageError, StorageResult, storage_serde};

pub(crate) async fn read_pubsub_snapshot(
    driver: kv_backend::foundationdb::FoundationDbBackend,
) -> Result<(Vec<PublishCandidate>, Vec<DeliveryPayload>), String> {
    driver
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(driver.attempt_deadlines()),
            |attempt| Box::pin(async move { read_snapshot_rows(attempt).await }),
        )
        .await
        .map_err(|error| crate::common::storage_error_detail(&error))
}

async fn read_snapshot_rows(
    attempt: &impl KvAttempt,
) -> StorageResult<(Vec<PublishCandidate>, Vec<DeliveryPayload>)> {
    let ranges = [
        kv_backend::PUBSUB_PUBLISH_INTENT_PREFIX,
        kv_backend::PUBSUB_DELIVERY_PAYLOAD_PREFIX,
    ]
    .into_iter()
    .map(|prefix| {
        KvRange::try_for_prefix(
            prefix.to_vec(),
            std::num::NonZeroU32::new(1024).expect("nonzero audit rows"),
            std::num::NonZeroUsize::new(16 * 1024 * 1024).expect("nonzero audit bytes"),
        )
    })
    .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt.scan_batch(ranges, ReadProtection::Snapshot).await?;
    let [intents, deliveries]: [_; 2] = pages
        .try_into()
        .map_err(|_| StorageError::internal("pubsub audit page count differs"))?;
    if intents.more || deliveries.more {
        return Err(StorageError::internal(
            "pubsub audit exceeded complete-scan bound",
        ));
    }
    let intents = intents
        .entries
        .into_iter()
        .map(decode_intent)
        .collect::<StorageResult<Vec<_>>>()?;
    let keys = intents
        .iter()
        .map(|intent| kv_backend::pubsub_publication::chunk_key(intent.snapshot.generation, 0))
        .collect();
    let chunks = attempt.get_batch(keys, ReadProtection::Snapshot).await?;
    if chunks.len() != intents.len() {
        return Err(StorageError::internal(
            "publish audit snapshot count differs",
        ));
    }
    let candidates = intents
        .into_iter()
        .zip(chunks)
        .map(|(intent, chunk)| {
            let chunk =
                chunk.ok_or_else(|| StorageError::internal("publish audit snapshot missing"))?;
            let subscriptions: Vec<Subscription> = storage_serde::from_bytes(&chunk)?;
            Ok(PublishCandidate {
                due_at: intent.due_at(),
                intent,
                subscriptions,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let records = deliveries
        .entries
        .into_iter()
        .map(decode_delivery)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok((candidates, records))
}

fn decode_intent(entry: kv_backend::KvEntry) -> StorageResult<PublishIntent> {
    let intent: PublishIntent = storage_serde::from_bytes(&entry.value)?;
    if entry
        .key
        .strip_prefix(kv_backend::PUBSUB_PUBLISH_INTENT_PREFIX)
        != Some(intent.acceptance.message_id.as_str().as_bytes())
    {
        return Err(StorageError::internal(
            "publish audit identity differs from key",
        ));
    }
    if intent.snapshot.chunk_count != 1 || intent.next_chunk != 0 {
        return Err(StorageError::internal(
            "publish audit fixture chunk progress differs",
        ));
    }
    Ok(intent)
}

/// The payloads: written once by the chunk commit (W3S R1).
fn decode_delivery(entry: kv_backend::KvEntry) -> StorageResult<DeliveryPayload> {
    let id = entry
        .key
        .strip_prefix(kv_backend::PUBSUB_DELIVERY_PAYLOAD_PREFIX)
        .and_then(|id| std::str::from_utf8(id).ok())
        .ok_or_else(|| StorageError::internal("delivery audit key is invalid"))?;
    DeliveryPayload::decode(&entry.value, &DeliveryRecordId(id.to_owned()))
}
