use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use bg_jobs::{BackgroundJob, BackgroundJobName, JobConfig};
use futures::future::try_join_all;
use queue_provider::{
    MessageAttributeValue, MessageId, MessageResponse, Queue, QueueError, QueueInternalKind,
    QueueMessage, QueueMessageCounts, QueueProvider, QueueResult, QueueValidationKind,
    ReceiptHandle,
};
use serde::{Deserialize, Serialize};
use storage_types::{DurationSeconds, StorageEnum, StorageError, StorageResult, TimestampMillis};
use uuid::Uuid;

use crate::{
    SortedKvDbStorageProvider,
    constants::PARTITION_ROUTING_RETRIES_TOTAL_METRIC,
    helpers::increment_bytes,
    keyspace::compact::{self, QueueStorageId, U48},
    newtypes::MessageVisibilityKey,
    partition_family::{
        DEFAULT_STANDARD_QUEUE_PARTITION_COUNT, PartitionFamilyKind, QueueReceiptHandleData,
        ResolvedPartitionFamily, default_partition_family_config, find_partition_by_id,
        initial_partition_infos, ordered_log_hash, parse_partition_family_config,
        parse_partition_info, partition_family_config_key, partition_info_prefix,
        queue_body_prefix_with_slot, queue_checkpoint_key_with_slot, queue_family_component,
        queue_partition_marker_bytes, queue_partition_marker_key, queue_payload_key_with_slot,
        queue_ready_hint_bytes, queue_ready_hint_key, queue_ready_key_with_slot,
        queue_ready_prefix_with_slot, queue_state_key_with_slot, queue_wake_key, wake_value_bytes,
    },
    queue::{
        PartitionedQueueMessageWrite, QueueClaimRange, QueueKvStore, QueuePrewarmPartition,
        constants::{
            PARTITIONED_QUEUE_EMPTY_RECEIVE_POLL_MS,
            PARTITIONED_QUEUE_RECEIVE_COALESCE_CLAIM_ROUNDS,
            PARTITIONED_QUEUE_RECEIVE_SCAN_OVERFETCH_MULTIPLIER,
            PARTITIONED_QUEUE_RECEIVE_SCAN_SHARDS, PARTITIONED_QUEUE_SEND_MAX_ATTEMPTS,
            QUEUE_PREWARM_MESSAGE_ID, RECEIVE_SCAN_MAX_LIMIT, RECEIVE_SCAN_MIN_LIMIT,
        },
        record_queue_storage_operation, set_queue_storage_gauge,
        storage::{
            is_queue_prewarm_marker_bytes, queue_payload_delete_range, queue_payload_is_chunk_key,
        },
    },
    sorted_kv_store::{DirectWriteOperation, RawKey},
};

#[inline]
#[must_use]
pub(crate) fn visibility_key(timestamp: TimestampMillis, message_id: &MessageId) -> String {
    format!("{:013}:{message_id}", *timestamp)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PartitionedQueueBody {
    body: String,
    message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    created_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PartitionedQueueState {
    pub(crate) queue_url: String,
    pub(crate) visibility_timestamp: TimestampMillis,
    pub(crate) delivery_attempt: u32,
    pub(crate) claim_nonce: Option<String>,
    pub(crate) checkpoint_data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueDeleteLedgerEntry {
    payload_key: Vec<u8>,
    state_key: Vec<u8>,
    created_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredQueueIdentity {
    queue_id: QueueStorageId,
    queue: Queue,
}

fn partitioned_body_bytes(
    body: String,
    message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    created_at: TimestampMillis,
) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&PartitionedQueueBody {
        body,
        message_attributes,
        created_at,
    })
}

fn record_deferred_payload_cleanup(count: u64) {
    record_queue_storage_operation("queue_payload_cleanup", "deferred", count);
    record_queue_storage_operation("queue_payload_orphan", "created", count);
}

fn queue_delete_ledger_prefix_for_id(queue_id: QueueStorageId) -> Vec<u8> {
    compact::queue_record_key(
        crate::partition_family::queue_data_bucket(0),
        queue_id,
        0,
        compact::QueueRecordKind::DeleteLedger,
        b"",
    )
}

pub(crate) fn queue_delete_ledger_key(
    queue_id: QueueStorageId,
    placement_slot: u16,
    partition_id: u16,
    message_id_hex: &str,
) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(4 + message_id_hex.len());
    suffix.extend_from_slice(&placement_slot.to_be_bytes());
    suffix.extend_from_slice(&partition_id.to_be_bytes());
    suffix.extend_from_slice(message_id_hex.as_bytes());
    compact::queue_record_key(
        crate::partition_family::queue_data_bucket(0),
        queue_id,
        0,
        compact::QueueRecordKind::DeleteLedger,
        &suffix,
    )
}

fn queue_delete_ledger_entry_bytes(
    queue_id: QueueStorageId,
    route: QueuePartitionRoute,
    message_id_hex: &str,
) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&QueueDeleteLedgerEntry {
        payload_key: queue_payload_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            message_id_hex,
        ),
        state_key: queue_state_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            message_id_hex,
        ),
        created_at: TimestampMillis::now(),
    })
}

fn encode_queue_storage_id(queue_id: QueueStorageId) -> Vec<u8> {
    let bytes = queue_id.get().to_be_bytes();
    bytes[2..].to_vec()
}

fn decode_queue_storage_id(bytes: &[u8]) -> StorageResult<QueueStorageId> {
    if bytes.len() != 6 {
        return Err(StorageError::internal(&format!(
            "invalid queue storage id width: expected 6 bytes, got {}",
            bytes.len()
        )));
    }
    let mut padded = [0u8; 8];
    padded[2..].copy_from_slice(bytes);
    U48::new(u64::from_be_bytes(padded))
        .map(QueueStorageId::from)
        .map_err(|err| StorageError::internal(&format!("invalid queue storage id: {err}")))
}

fn queue_storage_id(value: u64) -> StorageResult<QueueStorageId> {
    QueueStorageId::new(value)
        .map_err(|err| StorageError::internal(&format!("invalid queue storage id: {err}")))
}

fn queue_url_with_storage_id(queue_url: &str, queue_id: QueueStorageId) -> String {
    let encoded_id = format!("{:012x}", queue_id.get());
    queue_url.rsplit_once('/').map_or_else(
        || format!("{encoded_id}/{queue_url}"),
        |(prefix, queue_name)| format!("{prefix}/{encoded_id}/{queue_name}"),
    )
}

pub(crate) fn queue_storage_id_from_url(queue_url: &str) -> QueueResult<QueueStorageId> {
    let mut segments = queue_url.rsplit('/');
    let _queue_name = segments.next();
    let encoded_id = segments
        .next()
        .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?;
    if encoded_id.len() != 12 {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat,
        ));
    }
    let value = u64::from_str_radix(encoded_id, 16)
        .map_err(|_| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))?;
    queue_storage_id(value).map_err(QueueError::from)
}

fn queue_url_without_storage_id(queue_url: &str) -> Option<String> {
    let (prefix_with_id, queue_name) = queue_url.rsplit_once('/')?;
    let (prefix, encoded_id) = prefix_with_id
        .rsplit_once('/')
        .map_or(("", prefix_with_id), |parts| parts);
    if encoded_id.len() != 12 || !encoded_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(if prefix.is_empty() {
        queue_name.to_string()
    } else {
        format!("{prefix}/{queue_name}")
    })
}

fn encode_queue_identity(identity: &StoredQueueIdentity) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(identity)
}

fn decode_queue_identity(bytes: &[u8]) -> QueueResult<StoredQueueIdentity> {
    storage_types::storage_serde::from_bytes(bytes).map_err(QueueError::from)
}

fn parse_ready_hint_value(value: &[u8]) -> Option<(u16, TimestampMillis)> {
    let partition_id_bytes = value.get(..2)?;
    let timestamp_bytes = value.get(2..10)?;
    let partition_id = u16::from_be_bytes([partition_id_bytes[0], partition_id_bytes[1]]);
    let next_visible_at = TimestampMillis::from(i64::from_be_bytes([
        timestamp_bytes[0],
        timestamp_bytes[1],
        timestamp_bytes[2],
        timestamp_bytes[3],
        timestamp_bytes[4],
        timestamp_bytes[5],
        timestamp_bytes[6],
        timestamp_bytes[7],
    ]));
    Some((partition_id, next_visible_at))
}

const QUEUE_PAYLOAD_CLEANUP_JOB_ID: BackgroundJobName = BackgroundJobName::Database {
    kind: bg_jobs::DatabaseJobKind::QueuePayloadCleanup,
};
const QUEUE_PAYLOAD_CLEANUP_INTERVAL_SECONDS: u64 = 60;
const QUEUE_PAYLOAD_CLEANUP_BATCH_LIMIT: u32 = 256;

pub struct QueuePayloadCleanupJob<S: QueueKvStore + 'static> {
    provider: Arc<SortedKvDbStorageProvider<S>>,
}

impl<S: QueueKvStore + 'static> QueuePayloadCleanupJob<S> {
    pub fn new(provider: Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: QueueKvStore + 'static> BackgroundJob for QueuePayloadCleanupJob<S> {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let cleaned = self
            .provider
            .cleanup_queue_payload_orphans(QUEUE_PAYLOAD_CLEANUP_BATCH_LIMIT)
            .await?;
        Ok(cleaned > 0)
    }
}

#[derive(Clone)]
pub(crate) enum QueueRoutingState {
    Control(ResolvedPartitionFamily),
}

pub(crate) struct QueueExecutionContext {
    queue_id: QueueStorageId,
    routing_state: QueueRoutingState,
}

#[derive(Clone, Copy)]
pub(crate) struct QueuePartitionRoute {
    partition_id: u16,
    placement_slot: u16,
}

pub(crate) struct PreparedPartitionedQueueMessage {
    message_id: MessageId,
    message_id_hex: String,
    queue_url: String,
    visibility_timestamp: TimestampMillis,
    state_bytes: Arc<[u8]>,
    payload_bytes: Arc<[u8]>,
    payload_record_bytes: Option<Arc<[u8]>>,
}

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    pub(crate) async fn initialize_operation(&self) -> QueueResult<()> {
        self.start_partition_reconcile_task().await?;
        self.start_queue_payload_cleanup_task().await?;
        Ok(())
    }

    pub(crate) async fn get_queue_operation(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        Ok(self
            .queue_identity_by_url(queue_url)
            .await?
            .map(|identity| identity.queue))
    }

    pub(crate) async fn send_message_operation(
        &self,
        message: QueueMessage,
    ) -> QueueResult<MessageId> {
        self.send_partitioned_message(message).await
    }
}

mod execution;
mod identity;
pub(crate) use identity::partitioned_ready_visibility_key;
use identity::{queue_partition_hash, queue_partition_route_for_id, queue_partition_routes};
mod message_operations;
mod message_state;
mod provider_trait;
mod receive;
mod visibility_operations;
