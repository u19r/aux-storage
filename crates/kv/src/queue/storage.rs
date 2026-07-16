#[cfg(any(test, feature = "rocksdb-backend"))]
use queue_provider::MessageId;
use serde::{Deserialize, Serialize};
#[cfg(any(test, feature = "rocksdb-backend"))]
use storage_types::ItemKey;
use storage_types::{DurationSeconds, StorageError, StorageResult, TimestampMillis};
#[cfg(any(test, feature = "rocksdb-backend"))]
use uuid::Uuid;

use crate::{
    helpers::increment_bytes,
    keyspace::compact::QueueStorageId,
    partition_family::PartitionFamilyKvStore,
    queue::constants::QUEUE_PAYLOAD_CHUNK_BYTES,
    sorted_kv_store::{DirectWriteOperation, SortedKvStore},
};
#[cfg(any(test, feature = "rocksdb-backend"))]
use crate::{
    newtypes::MessageVisibilityKey,
    partition_family::{
        queue_payload_key_with_slot, queue_ready_hint_bytes, queue_ready_key_with_slot,
        queue_state_key_with_slot,
    },
};

#[cfg(any(test, feature = "rocksdb-backend"))]
type QueueReadyEntry = (Box<[u8]>, Box<[u8]>);

#[derive(Clone, Debug)]
pub struct QueueClaimRange {
    pub(crate) queue_id: QueueStorageId,
    pub(crate) placement_slot: u16,
    pub(crate) partition_id: u16,
    pub(crate) ready_start: Vec<u8>,
    pub(crate) ready_end: Vec<u8>,
    pub(crate) ready_hint_key: Vec<u8>,
    pub(crate) limit: u32,
    pub(crate) scan_limit: u32,
    pub(crate) claim_limit: u32,
    pub(crate) candidate_seed: u64,
}

#[derive(Clone, Debug)]
pub struct PartitionedQueueMessageWrite {
    pub(crate) state_key: Vec<u8>,
    pub(crate) state_bytes: Arc<[u8]>,
    pub(crate) payload_key: Vec<u8>,
    pub(crate) payload_bytes: Arc<[u8]>,
    pub(crate) payload_record_bytes: Option<Arc<[u8]>>,
    pub(crate) ready_key: Vec<u8>,
    pub(crate) ready_hint_key: Vec<u8>,
    pub(crate) ready_hint_bytes: Vec<u8>,
    pub(crate) wake_key: Vec<u8>,
    pub(crate) wake_bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct QueuePrewarmPartition {
    pub(crate) placement_slot: u16,
    pub(crate) partition_id: u16,
    pub(crate) marker_key: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct QueueClaimedMessage {
    pub(crate) partition_id: u16,
    pub(crate) message_id_hex: String,
    pub(crate) body_bytes: Vec<u8>,
    pub(crate) visibility_timestamp: TimestampMillis,
    pub(crate) delivery_attempt: u32,
    pub(crate) claim_nonce: String,
}

#[derive(Clone, Debug, Default)]
pub struct QueueClaimBatch {
    pub(crate) messages: Vec<QueueClaimedMessage>,
    pub(crate) ready_entries_seen: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum QueuePayloadRecord {
    Chunked { chunks: u16, total_len: usize },
}

#[async_trait::async_trait]
pub trait QueueKvStore: SortedKvStore + PartitionFamilyKvStore {
    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch>;

    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()>;

    async fn write_partitioned_queue_messages(
        &self,
        messages: Vec<PartitionedQueueMessageWrite>,
    ) -> StorageResult<()> {
        for message in messages {
            self.write_partitioned_queue_message(message).await?;
        }
        Ok(())
    }

    async fn prewarm_partitioned_queue(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()>;
}

#[cfg(any(test, feature = "rocksdb-backend"))]
pub(crate) async fn claim_queue_messages_from_ranges_generic<S>(
    store: &S,
    ranges: Vec<QueueClaimRange>,
    now: TimestampMillis,
    visibility_timeout: DurationSeconds,
    max_claims: usize,
) -> StorageResult<QueueClaimBatch>
where
    S: SortedKvStore + Sync,
{
    let mut batch = QueueClaimBatch::default();
    if max_claims == 0 {
        return Ok(batch);
    }

    for range in ranges {
        if batch.messages.len() >= max_claims {
            break;
        }
        claim_queue_messages_from_range(
            store,
            range,
            now,
            visibility_timeout,
            max_claims,
            &mut batch,
        )
        .await?;
    }

    Ok(batch)
}

#[cfg(any(test, feature = "rocksdb-backend"))]
pub(crate) async fn write_partitioned_queue_message_generic<S>(
    store: &S,
    message: PartitionedQueueMessageWrite,
) -> StorageResult<()>
where
    S: SortedKvStore + Sync,
{
    let mut operations = vec![
        DirectWriteOperation::Put {
            key: message.state_key,
            value: message.state_bytes.as_ref().to_vec(),
        },
        DirectWriteOperation::Put {
            key: message.ready_key,
            value: Vec::new(),
        },
        DirectWriteOperation::Put {
            key: message.ready_hint_key,
            value: message.ready_hint_bytes,
        },
        DirectWriteOperation::Put {
            key: message.wake_key,
            value: message.wake_bytes,
        },
    ];
    operations.extend(queue_payload_write_operations(
        message.payload_key,
        message.payload_bytes.as_ref().to_vec(),
        message
            .payload_record_bytes
            .map(|bytes| bytes.as_ref().to_vec()),
    )?);
    store.transact_write_unchecked(operations).await
}

pub(crate) fn queue_payload_write_operations(
    payload_key: Vec<u8>,
    payload_bytes: Vec<u8>,
    payload_record_bytes: Option<Vec<u8>>,
) -> StorageResult<Vec<DirectWriteOperation>> {
    if payload_bytes.len() <= QUEUE_PAYLOAD_CHUNK_BYTES {
        return Ok(vec![DirectWriteOperation::Put {
            key: payload_key,
            value: payload_bytes,
        }]);
    }

    let chunks = payload_bytes
        .chunks(QUEUE_PAYLOAD_CHUNK_BYTES)
        .collect::<Vec<_>>();
    let mut operations = Vec::with_capacity(chunks.len().saturating_add(1));
    let record_bytes = payload_record_bytes.ok_or_else(|| {
        StorageError::internal("chunked queue payload is missing its prepared record")
    })?;
    operations.push(DirectWriteOperation::Put {
        key: payload_key.clone(),
        value: record_bytes,
    });
    for (index, chunk) in chunks.into_iter().enumerate() {
        operations.push(DirectWriteOperation::Put {
            key: queue_payload_chunk_key(&payload_key, u16::try_from(index).unwrap_or(u16::MAX)),
            value: chunk.to_vec(),
        });
    }
    Ok(operations)
}

pub(crate) async fn read_partitioned_queue_payload<S>(
    store: &S,
    payload_key: &[u8],
    payload_record_bytes: Vec<u8>,
) -> StorageResult<Vec<u8>>
where
    S: SortedKvStore + Sync,
{
    let Ok(QueuePayloadRecord::Chunked { chunks, total_len }) =
        storage_types::storage_serde::from_bytes::<QueuePayloadRecord>(&payload_record_bytes)
    else {
        return Ok(payload_record_bytes);
    };
    let chunk_keys = (0..chunks)
        .map(|index| queue_payload_chunk_key(payload_key, index))
        .collect::<Vec<_>>();
    let chunk_values = store.multi_get(chunk_keys, false).await?;
    let mut payload = Vec::with_capacity(total_len);
    for (index, chunk) in chunk_values.into_iter().enumerate() {
        let Some(chunk) = chunk else {
            return Err(StorageError::internal(&format!(
                "missing queue payload chunk index={index}"
            )));
        };
        payload.extend_from_slice(&chunk);
    }
    if payload.len() != total_len {
        return Err(StorageError::internal(&format!(
            "queue payload chunk length mismatch expected={total_len} actual={}",
            payload.len()
        )));
    }
    Ok(payload)
}

pub(crate) fn queue_payload_delete_range(payload_key: Vec<u8>) -> DirectWriteOperation {
    DirectWriteOperation::DeleteRange {
        exclusive_end: increment_bytes(payload_key.clone()),
        start: payload_key,
    }
}

pub(crate) fn queue_payload_is_chunk_key(payload_key: &[u8]) -> bool {
    payload_key
        .windows(QUEUE_PAYLOAD_CHUNK_KEY_SEGMENT.len())
        .any(|window| window == QUEUE_PAYLOAD_CHUNK_KEY_SEGMENT)
}

pub(crate) fn queue_payload_chunk_key(payload_key: &[u8], index: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        payload_key
            .len()
            .saturating_add(QUEUE_PAYLOAD_CHUNK_KEY_SEGMENT.len())
            .saturating_add(4),
    );
    key.extend_from_slice(payload_key);
    key.extend_from_slice(QUEUE_PAYLOAD_CHUNK_KEY_SEGMENT);
    key.extend_from_slice(format!("{index:04x}").as_bytes());
    key
}

pub(crate) fn queue_payload_record_bytes(
    payload_len: usize,
) -> StorageResult<Option<Vec<u8>>> {
    if payload_len <= QUEUE_PAYLOAD_CHUNK_BYTES {
        return Ok(None);
    }
    let chunks = payload_len.div_ceil(QUEUE_PAYLOAD_CHUNK_BYTES);
    let chunks = u16::try_from(chunks)
        .map_err(|_| StorageError::internal("queue payload chunk count exceeds u16"))?;
    storage_types::storage_serde::to_bytes(&QueuePayloadRecord::Chunked {
        chunks,
        total_len: payload_len,
    })
    .map(Some)
}

const QUEUE_PAYLOAD_CHUNK_KEY_SEGMENT: &[u8] = b"/chunk/";

#[cfg(any(test, feature = "rocksdb-backend"))]
pub(crate) async fn prewarm_partitioned_queue_generic(
    partitions: Vec<QueuePrewarmPartition>,
) -> StorageResult<()> {
    for partition in partitions {
        let QueuePrewarmPartition {
            placement_slot,
            partition_id,
            marker_key,
        } = partition;
        drop((placement_slot, partition_id, marker_key));
    }
    Ok(())
}

#[cfg(any(test, feature = "rocksdb-backend"))]
async fn claim_queue_messages_from_range<S>(
    store: &S,
    range: QueueClaimRange,
    now: TimestampMillis,
    visibility_timeout: DurationSeconds,
    max_claims: usize,
    batch: &mut QueueClaimBatch,
) -> StorageResult<()>
where
    S: SortedKvStore + Sync,
{
    // Scan a bounded visible range once, then batch-read body and state records
    // for the rotated candidate set. The remaining awaits are only conditional
    // writes for candidates that can actually produce a message.
    let ready_entries = store
        .get_range(
            &range.ready_start,
            &range.ready_end,
            Some(range.scan_limit.max(range.limit)),
            None::<ItemKey>,
            true,
        )
        .await?;
    batch.ready_entries_seen = batch
        .ready_entries_seen
        .saturating_add(ready_entries.items.len());
    if ready_entries.items.is_empty() {
        return Ok(());
    }

    let scan_limit = usize::try_from(range.scan_limit.max(range.limit)).unwrap_or(usize::MAX);
    let ready_entries_len = ready_entries.items.len();
    let mut ready_items = ready_entries.items;
    rotate_claim_candidates(&mut ready_items, range.candidate_seed);
    let range_claim_limit = usize::try_from(range.claim_limit.max(1)).unwrap_or(usize::MAX);
    let claim_budget = max_claims
        .saturating_sub(batch.messages.len())
        .min(range_claim_limit);
    let candidates = queue_claim_candidates(&range, ready_items, claim_budget)?;
    if candidates.is_empty() {
        return Ok(());
    }

    let payload_keys = candidates
        .iter()
        .map(|candidate| candidate.state_key.clone())
        .collect::<Vec<_>>();
    let delivery_states = store.multi_get(payload_keys, false).await?;
    let mut range_claims = 0usize;

    for (candidate, state_bytes) in candidates.into_iter().zip(delivery_states) {
        if batch.messages.len() >= max_claims || range_claims >= range_claim_limit {
            break;
        }

        let Some(state_bytes) = state_bytes else {
            let _ = store.delete(&candidate.ready_key).await;
            continue;
        };

        let Some(claimed) = claim_queue_candidate(
            store,
            &range,
            candidate,
            &state_bytes,
            now,
            visibility_timeout,
        )
        .await?
        else {
            continue;
        };
        range_claims = range_claims.saturating_add(1);
        batch.messages.push(claimed);
    }
    if range_claims > 0 && range_claims == ready_entries_len && ready_entries_len < scan_limit {
        let _ = store
            .put(
                &range.ready_hint_key,
                &queue_ready_hint_bytes(range.partition_id, now),
                None,
            )
            .await;
    }

    Ok(())
}

#[cfg(any(test, feature = "rocksdb-backend"))]
async fn claim_queue_candidate<S>(
    store: &S,
    range: &QueueClaimRange,
    candidate: QueueClaimCandidate,
    state_bytes: &[u8],
    now: TimestampMillis,
    visibility_timeout: DurationSeconds,
) -> StorageResult<Option<QueueClaimedMessage>>
where
    S: SortedKvStore + Sync,
{
    let mut state: crate::queue_provider::PartitionedQueueState =
        storage_types::storage_serde::from_bytes(state_bytes).map_err(|error| {
            StorageError::internal(&format!(
                "deserialize partitioned queue state key={} error={:?}",
                String::from_utf8_lossy(&candidate.state_key),
                error.as_ref()
            ))
        })?;
    if state.visibility_timestamp > now {
        return Ok(None);
    }

    let expected_ready_value = state
        .claim_nonce
        .as_ref()
        .map_or_else(Vec::new, |nonce| nonce.as_bytes().to_vec());
    state.delivery_attempt = state.delivery_attempt.saturating_add(1);
    state.visibility_timestamp = now + visibility_timeout;
    state.claim_nonce = Some(Uuid::now_v7().to_string());
    let new_visibility_key = MessageVisibilityKey(crate::queue_provider::visibility_key(
        state.visibility_timestamp,
        &candidate.message_id,
    ));
    let new_ready_key = queue_ready_key_with_slot(
        range.queue_id,
        range.placement_slot,
        range.partition_id,
        &new_visibility_key,
    );
    let claim_nonce = state.claim_nonce.clone().unwrap_or_default();
    let delivery_attempt = state.delivery_attempt;
    let visibility_timestamp = state.visibility_timestamp;
    let state_value = storage_types::storage_serde::to_bytes(&state)?;

    // The ready marker check turns racing receivers into a cheap conditional
    // miss instead of duplicate delivery.
    let claim_result = store
        .transact_write_unchecked(vec![
            DirectWriteOperation::CheckValue {
                key: candidate.ready_key.clone(),
                expected_value: Some(expected_ready_value),
            },
            DirectWriteOperation::Delete {
                key: candidate.ready_key,
            },
            DirectWriteOperation::Put {
                key: new_ready_key,
                value: claim_nonce.as_bytes().to_vec(),
            },
            DirectWriteOperation::Put {
                key: candidate.state_key,
                value: state_value,
            },
        ])
        .await;

    match claim_result {
        Ok(()) => {
            let Some(body_bytes) = store.get(&candidate.payload_key, false).await? else {
                return Ok(None);
            };
            let body_bytes =
                read_partitioned_queue_payload(store, &candidate.payload_key, body_bytes).await?;
            Ok(Some(QueueClaimedMessage {
                partition_id: range.partition_id,
                message_id_hex: candidate.message_id_hex,
                body_bytes,
                visibility_timestamp,
                delivery_attempt,
                claim_nonce,
            }))
        }
        Err(error)
            if matches!(
                error.as_ref(),
                storage_types::StorageEnum::ConditionalCheckFailed
                    | storage_types::StorageEnum::TransactionConflict { .. }
                    | storage_types::StorageEnum::TransactionCanceled { .. }
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(test, feature = "rocksdb-backend"))]
fn queue_claim_candidates(
    range: &QueueClaimRange,
    ready_items: Vec<QueueReadyEntry>,
    claim_budget: usize,
) -> StorageResult<Vec<QueueClaimCandidate>> {
    let mut candidates = Vec::with_capacity(claim_budget.min(ready_items.len()));
    for (ready_key, _) in ready_items {
        if candidates.len() >= claim_budget {
            break;
        }
        let ready_visibility_key =
            crate::queue_provider::partitioned_ready_visibility_key(&ready_key)
                .map_err(|error| StorageError::internal(&error.to_string()))?;
        let message_id = ready_visibility_key
            .get_message_id()
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let message_id_hex = message_id.to_string();
        candidates.push(QueueClaimCandidate {
            ready_key: ready_key.into_vec(),
            state_key: queue_state_key_with_slot(
                range.queue_id,
                range.placement_slot,
                range.partition_id,
                &message_id_hex,
            ),
            payload_key: queue_payload_key_with_slot(
                range.queue_id,
                range.placement_slot,
                range.partition_id,
                &message_id_hex,
            ),
            message_id,
            message_id_hex,
        });
    }
    Ok(candidates)
}

#[cfg(any(test, feature = "rocksdb-backend"))]
struct QueueClaimCandidate {
    ready_key: Vec<u8>,
    state_key: Vec<u8>,
    payload_key: Vec<u8>,
    message_id: MessageId,
    message_id_hex: String,
}

#[cfg(any(test, feature = "rocksdb-backend"))]
fn rotate_claim_candidates(items: &mut [QueueReadyEntry], seed: u64) {
    if items.len() <= 1 {
        return;
    }
    let offset = usize::try_from(seed % u64::try_from(items.len()).unwrap_or(1)).unwrap_or(0);
    items.rotate_left(offset);
}
use std::sync::Arc;
