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
        queue_body_prefix_with_slot,
        queue_checkpoint_key_with_slot, queue_family_component, queue_partition_marker_bytes,
        queue_partition_marker_key, queue_payload_key_with_slot, queue_ready_hint_bytes,
        queue_ready_hint_key, queue_ready_key_with_slot, queue_ready_prefix_with_slot,
        queue_state_key_with_slot, queue_wake_key, wake_value_bytes,
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
        storage::{queue_payload_delete_range, queue_payload_is_chunk_key},
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
struct StoredQueueIdentity {
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
    let encoded_id = segments.next().ok_or_else(|| {
        QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat,
        )
    })?;
    if encoded_id.len() != 12 {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat,
        ));
    }
    let value = u64::from_str_radix(encoded_id, 16).map_err(|_| {
        QueueError::validation(
            QueueValidationKind::InvalidQueueUrlFormat,
        )
    })?;
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
enum QueueRoutingState {
    Control(ResolvedPartitionFamily),
}

struct QueueExecutionContext {
    queue_id: QueueStorageId,
    routing_state: QueueRoutingState,
}

#[derive(Clone, Copy)]
struct QueuePartitionRoute {
    partition_id: u16,
    placement_slot: u16,
}

struct PreparedPartitionedQueueMessage {
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
    async fn ensure_queue_execution_context(
        &self,
        queue_url: &str,
    ) -> QueueResult<QueueExecutionContext> {
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        let routing_state = QueueRoutingState::Control(
            self.ensure_queue_family_state(queue_url, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)
                .await?,
        );
        Ok(QueueExecutionContext {
            queue_id,
            routing_state,
        })
    }

    async fn queue_execution_context(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<QueueExecutionContext>> {
        let Some(routing_state) = self.queue_routing_state(queue_url).await? else {
            return Ok(None);
        };
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        Ok(Some(QueueExecutionContext {
            queue_id,
            routing_state,
        }))
    }

    fn record_queue_partition_load(
        &self,
        queue_url: &str,
        partition_id: u16,
        sample: crate::partition_family::PartitionLoadSample,
    ) {
        self.runtime_partition_load_tracker.record(
            crate::partition_family::RuntimePartitionLoadSample {
                family_kind: PartitionFamilyKind::StandardQueue,
                family_component: queue_family_component(queue_url),
                partition_id,
                sample,
            },
        );
    }

    fn local_queue_partition_load_hint(&self, queue_url: &str, partition_id: u16) -> u64 {
        let family_component = queue_family_component(queue_url);
        self.runtime_partition_load_tracker.load_hint(
            PartitionFamilyKind::StandardQueue,
            &family_component,
            partition_id,
        ) + self.kv_store.partition_runtime_load_hint(
            PartitionFamilyKind::StandardQueue,
            &family_component,
            partition_id,
        )
    }

    fn queue_partition_route_for_send(
        &self,
        routing_state: &QueueRoutingState,
        queue_url: &str,
        message_id: &MessageId,
    ) -> Option<QueuePartitionRoute> {
        match routing_state {
            QueueRoutingState::Control(family) => {
                let writable: Vec<_> = family
                    .partitions
                    .iter()
                    .filter(|partition| partition.is_writable())
                    .collect();
                let first = writable.first()?;
                if writable.len() == 1 {
                    return Some(QueuePartitionRoute {
                        partition_id: first.partition_id,
                        placement_slot: first.placement_slot,
                    });
                }

                let primary_index = usize::try_from(queue_partition_hash(queue_url, message_id))
                    .unwrap_or(0)
                    % writable.len();
                // Power-of-two choices: compare the primary hash route with a
                // second deterministic route so hot queues can avoid a locally
                // overloaded partition without making send placement random.
                let mut secondary_route_key = queue_url.as_bytes().to_vec();
                secondary_route_key.extend_from_slice(message_id.as_bytes());
                secondary_route_key.extend_from_slice(b"/secondary-route");
                let secondary_index = usize::try_from(ordered_log_hash(&secondary_route_key))
                    .unwrap_or(0)
                    % writable.len();

                let primary = writable[primary_index];
                let secondary = writable[secondary_index];
                let primary_hint =
                    self.local_queue_partition_load_hint(queue_url, primary.partition_id);
                let secondary_hint =
                    self.local_queue_partition_load_hint(queue_url, secondary.partition_id);
                let chosen = match primary_hint.cmp(&secondary_hint) {
                    std::cmp::Ordering::Less => primary,
                    std::cmp::Ordering::Greater => secondary,
                    std::cmp::Ordering::Equal => primary,
                };

                Some(QueuePartitionRoute {
                    partition_id: chosen.partition_id,
                    placement_slot: chosen.placement_slot,
                })
            }
        }
    }

    async fn queue_roots_for_payload_cleanup(&self) -> StorageResult<Vec<StoredQueueIdentity>> {
        self.list_queue_identities()
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))
    }

    pub(crate) async fn cleanup_queue_payload_orphans(&self, limit: u32) -> StorageResult<u64> {
        if limit == 0 {
            return Ok(0);
        }
        let now = TimestampMillis::now();
        let mut remaining = limit;
        let mut scanned = 0u64;
        let mut deleted = 0u64;
        let mut oldest_age_ms = 0i64;
        let mut pending_deletes = Vec::new();
        let mut ledger_scanned = 0u64;

        for identity in self.queue_roots_for_payload_cleanup().await? {
            if remaining == 0 {
                break;
            }
            let queue_url = identity.queue.queue_url;
            let queue_id = identity.queue_id;
            let ledger_entries = self
                .kv_store
                .get_prefix(
                    &queue_delete_ledger_prefix_for_id(queue_id),
                    true,
                    Some(remaining),
                    false,
                )
                .await?;
            let ledger_entries_found = !ledger_entries.items.is_empty();
            if ledger_entries_found {
                ledger_scanned = ledger_scanned
                    .saturating_add(u64::try_from(ledger_entries.items.len()).unwrap_or(u64::MAX));
                remaining = remaining
                    .saturating_sub(u32::try_from(ledger_entries.items.len()).unwrap_or(u32::MAX));
                for (ledger_key, ledger_value) in ledger_entries.items {
                    let Ok(entry) = storage_types::storage_serde::from_bytes::<
                        QueueDeleteLedgerEntry,
                    >(&ledger_value) else {
                        pending_deletes.push(DirectWriteOperation::Delete {
                            key: ledger_key.into_vec(),
                        });
                        continue;
                    };
                    oldest_age_ms = oldest_age_ms.max(
                        now.timestamp_millis()
                            .saturating_sub(entry.created_at.timestamp_millis()),
                    );
                    pending_deletes.extend([
                        queue_payload_delete_range(entry.payload_key),
                        DirectWriteOperation::Delete {
                            key: entry.state_key,
                        },
                        DirectWriteOperation::Delete {
                            key: ledger_key.into_vec(),
                        },
                    ]);
                    deleted = deleted.saturating_add(1);
                }
            }
            if remaining == 0 {
                break;
            }
            if ledger_entries_found {
                continue;
            }
            let Some(family) = self
                .load_queue_family_state(&queue_url)
                .await
                .map_err(|error| StorageError::internal(&error.to_string()))?
            else {
                continue;
            };
            for route in queue_partition_routes(&QueueRoutingState::Control(family.clone())) {
                if remaining == 0 {
                    break;
                }
                let payload_prefix =
                    queue_body_prefix_with_slot(queue_id, route.placement_slot, route.partition_id);
                let payloads = self
                    .kv_store
                    .get_prefix(&payload_prefix, true, Some(remaining), false)
                    .await?;
                if payloads.items.is_empty() {
                    continue;
                }

                scanned =
                    scanned.saturating_add(u64::try_from(payloads.items.len()).unwrap_or(u64::MAX));
                remaining = remaining
                    .saturating_sub(u32::try_from(payloads.items.len()).unwrap_or(u32::MAX));
                let payload_infos = payloads
                    .items
                    .into_iter()
                    .filter_map(|(payload_key, payload_value)| {
                        if queue_payload_is_chunk_key(&payload_key) {
                            return None;
                        }
                        let message_id_hex =
                            std::str::from_utf8(payload_key.get(payload_prefix.len()..)?)
                                .ok()?
                                .to_string();
                        if message_id_hex.contains('/') {
                            return None;
                        }
                        let state_key = queue_state_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &message_id_hex,
                        );
                        Some((payload_key, payload_value, message_id_hex, state_key))
                    })
                    .collect::<Vec<_>>();
                let states = self
                    .kv_store
                    .multi_get(
                        payload_infos
                            .iter()
                            .map(|(_, _, _, state_key)| state_key.clone())
                            .collect(),
                        false,
                    )
                    .await?;
                let mut payload_deletes = 0u64;
                for ((payload_key, payload_value, message_id_hex, state_key), state) in
                    payload_infos.into_iter().zip(states)
                {
                    let mut delete_state = false;
                    if let Some(state_bytes) = state {
                        let Ok(state) = storage_types::storage_serde::from_bytes::<
                            PartitionedQueueState,
                        >(&state_bytes) else {
                            continue;
                        };
                        let Ok(message_id) = message_id_hex.parse::<MessageId>() else {
                            continue;
                        };
                        let visibility_key = MessageVisibilityKey(visibility_key(
                            state.visibility_timestamp,
                            &message_id,
                        ));
                        let ready_key = queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &visibility_key,
                        );
                        if self.kv_store.get(&ready_key, false).await?.is_some() {
                            continue;
                        }
                        delete_state = true;
                    }
                    if let Ok(body) = storage_types::storage_serde::from_bytes::<PartitionedQueueBody>(
                        &payload_value,
                    ) {
                        oldest_age_ms = oldest_age_ms.max(
                            now.timestamp_millis()
                                .saturating_sub(body.created_at.timestamp_millis()),
                        );
                    }
                    pending_deletes.push(queue_payload_delete_range(payload_key.into_vec()));
                    payload_deletes = payload_deletes.saturating_add(1);
                    if delete_state {
                        pending_deletes.push(DirectWriteOperation::Delete { key: state_key });
                    }
                }
                if payload_deletes == 0 {
                    continue;
                }
                deleted = deleted.saturating_add(payload_deletes);
            }
        }
        if !pending_deletes.is_empty() {
            self.kv_store
                .transact_write_unchecked(pending_deletes)
                .await?;
        }

        record_queue_storage_operation("queue_payload_cleanup", "orphan_scanned", scanned);
        record_queue_storage_operation("queue_payload_cleanup", "ledger_scanned", ledger_scanned);
        record_queue_storage_operation("queue_payload_cleanup", "orphan_deleted", deleted);
        set_queue_storage_gauge(
            "queue_payload_cleanup",
            "oldest_orphan_age_ms",
            oldest_age_ms as f64,
        );
        set_queue_storage_gauge(
            "queue_payload_cleanup",
            "last_orphan_scan_count",
            scanned as f64,
        );
        Ok(deleted)
    }

    async fn start_queue_payload_cleanup_task(&self) -> StorageResult<()> {
        if !self.database_jobs_enabled || !self.kv_store.supports_partition_families() {
            return Ok(());
        }
        if self
            .job_manager
            .is_job_running(QUEUE_PAYLOAD_CLEANUP_JOB_ID)
            .await
        {
            return Ok(());
        }
        let job = QueuePayloadCleanupJob::new(Arc::new(self.clone()));
        let config = JobConfig {
            start_immediately: false,
            sleep_duration: std::time::Duration::from_secs(QUEUE_PAYLOAD_CLEANUP_INTERVAL_SECONDS),
            jitter_percent: 10,
        };
        self.job_manager
            .register_job(QUEUE_PAYLOAD_CLEANUP_JOB_ID, job, config)
            .await
            .map_err(|error| {
                StorageError::internal(&format!(
                    "register queue payload cleanup job failed: {error}"
                ))
            })?;
        Ok(())
    }

    async fn queue_receive_hinted_route_window(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        routes: &[QueuePartitionRoute],
        scan_shards: usize,
        now: TimestampMillis,
    ) -> StorageResult<Vec<QueuePartitionRoute>> {
        if routes.is_empty() || scan_shards == 0 {
            return Ok(Vec::new());
        }
        let ready_limit = routes.len();
        let start = {
            let mut cursors = match self.queue_receive_hint_cursors.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let cursor = cursors.entry(queue_url.to_string()).or_insert(0);
            let start = *cursor % routes.len();
            *cursor = (start + ready_limit.max(1)) % routes.len();
            start
        };

        let sampled_routes = (0..routes.len())
            .map(|offset| routes[(start + offset) % routes.len()])
            .collect::<Vec<_>>();
        let hint_keys = sampled_routes
            .iter()
            .map(|route| queue_ready_hint_key(queue_id, route.placement_slot, route.partition_id))
            .collect::<Vec<_>>();
        let hints = self.kv_store.multi_get(hint_keys, false).await?;
        let mut ready_routes = Vec::with_capacity(ready_limit);
        for (route, hint) in sampled_routes.into_iter().zip(hints) {
            let Some(value) = hint else {
                continue;
            };
            let Some((partition_id, next_visible_at)) = parse_ready_hint_value(&value) else {
                continue;
            };
            if partition_id == route.partition_id && next_visible_at <= now {
                ready_routes.push(route);
                if ready_routes.len() >= ready_limit {
                    break;
                }
            }
        }
        Ok(ready_routes)
    }

    fn queue_claim_seed(queue_url: &str, partition_id: u16) -> u64 {
        // Claims rotate visible candidates per receive call so repeated scans
        // do not keep contending on the same oldest ready entries.
        let nonce = Uuid::now_v7();
        let bytes = nonce.as_bytes();
        let mut seed_bytes = [0u8; 8];
        seed_bytes.copy_from_slice(&bytes[..8]);
        let nonce_seed = u64::from_be_bytes(seed_bytes);
        let route_seed =
            ordered_log_hash(&[queue_url.as_bytes(), &partition_id.to_be_bytes()].concat());
        nonce_seed ^ route_seed
    }

    async fn receive_partitioned_messages_once_coalesced(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        self.receive_partitioned_messages_with_claim_rounds(
            queue_url,
            queue_id,
            max_messages,
            visibility_timeout,
            routing_state,
        )
        .await
    }

    async fn receive_partitioned_messages_with_claim_rounds(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let mut claimed_messages = Vec::new();
        for _ in 0..PARTITIONED_QUEUE_RECEIVE_COALESCE_CLAIM_ROUNDS {
            let remaining = max_messages
                .saturating_sub(u32::try_from(claimed_messages.len()).unwrap_or(u32::MAX));
            if remaining == 0 {
                break;
            }
            match self
                .receive_partitioned_messages_once(
                    queue_url,
                    queue_id,
                    remaining,
                    visibility_timeout,
                    routing_state,
                )
                .await
            {
                Ok(messages) if messages.is_empty() => break,
                Ok(mut messages) => claimed_messages.append(&mut messages),
                Err(error) => return Err(error),
            }
        }
        Ok(claimed_messages)
    }

    fn is_partition_routing_retryable(storage_error: &storage_types::StorageError) -> bool {
        matches!(
            storage_error.as_ref(),
            StorageEnum::ConditionalCheckFailed
                | StorageEnum::TransactionConflict { .. }
                | StorageEnum::TransactionCanceled { .. }
        )
    }

    fn invalid_receipt_handle(receipt_handle: &ReceiptHandle) -> QueueError {
        QueueError::validation_with_detail(
            QueueValidationKind::MessageNotFound,
            format!("receipt handle is invalid or expired: {receipt_handle}"),
        )
    }

    fn decode_receipt_handle(
        receipt_handle: &ReceiptHandle,
    ) -> QueueResult<QueueReceiptHandleData> {
        QueueReceiptHandleData::decode(receipt_handle.as_str())
            .map_err(|_| Self::invalid_receipt_handle(receipt_handle))
    }

    async fn load_queue_family_state(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<ResolvedPartitionFamily>> {
        Ok(self
            .load_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &queue_family_component(queue_url),
            )
            .await?)
    }

    async fn ensure_queue_family_state(
        &self,
        queue_url: &str,
        initial_partition_count: u16,
    ) -> QueueResult<ResolvedPartitionFamily> {
        if let Some(existing) = self.load_queue_family_state(queue_url).await? {
            return Ok(existing);
        }

        let partitions = initial_partition_infos(initial_partition_count);
        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::StandardQueue,
                initial_partition_count,
            ),
            partitions,
        };
        self.save_partition_family_state(
            PartitionFamilyKind::StandardQueue,
            &queue_family_component(queue_url),
            &family,
        )
        .await?;
        Ok(family)
    }

    async fn queue_routing_state(&self, queue_url: &str) -> QueueResult<Option<QueueRoutingState>> {
        Ok(self
            .load_queue_family_state(queue_url)
            .await?
            .map(QueueRoutingState::Control))
    }

    fn prepare_partitioned_queue_message(
        mut message: QueueMessage,
    ) -> QueueResult<PreparedPartitionedQueueMessage> {
        if message.message_id == MessageId::default() {
            message.message_id = MessageId::random();
        }

        let visibility_timestamp = message
            .visibility_timestamp
            .unwrap_or_else(TimestampMillis::now);
        let state = PartitionedQueueState {
            queue_url: message.queue_url.clone(),
            visibility_timestamp,
            delivery_attempt: 0,
            claim_nonce: None,
            checkpoint_data: None,
        };
        let state_bytes = storage_types::storage_serde::to_bytes(&state)?;
        let payload_bytes =
            partitioned_body_bytes(message.body, message.message_attributes, message.created_at)?;
        let payload_record_bytes = crate::queue::storage::queue_payload_record_bytes(
            payload_bytes.len(),
        )?
        .map(Arc::from);
        Ok(PreparedPartitionedQueueMessage {
            message_id: message.message_id,
            message_id_hex: message.message_id.to_string(),
            queue_url: message.queue_url,
            visibility_timestamp,
            state_bytes: state_bytes.into(),
            payload_bytes: payload_bytes.into(),
            payload_record_bytes,
        })
    }

    fn partitioned_queue_write_for_route(
        prepared: &PreparedPartitionedQueueMessage,
        queue_id: QueueStorageId,
        route: QueuePartitionRoute,
        wake_key: &[u8],
        wake_bytes: &[u8],
    ) -> PartitionedQueueMessageWrite {
        let visibility_key = MessageVisibilityKey(visibility_key(
            prepared.visibility_timestamp,
            &prepared.message_id,
        ));
        PartitionedQueueMessageWrite {
            state_key: queue_state_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &prepared.message_id_hex,
            ),
            state_bytes: prepared.state_bytes.clone(),
            payload_key: queue_payload_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &prepared.message_id_hex,
            ),
            payload_bytes: prepared.payload_bytes.clone(),
            payload_record_bytes: prepared.payload_record_bytes.clone(),
            ready_key: queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            ),
            ready_hint_key: queue_ready_hint_key(
                queue_id,
                route.placement_slot,
                route.partition_id,
            ),
            ready_hint_bytes: queue_ready_hint_bytes(
                route.partition_id,
                prepared.visibility_timestamp,
            ),
            wake_key: wake_key.to_vec(),
            wake_bytes: wake_bytes.to_vec(),
        }
    }

    async fn send_partitioned_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<MessageId>> {
        let started_at = Instant::now();
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut prepared_messages = Vec::with_capacity(messages.len());
        for message in messages {
            prepared_messages.push(Self::prepare_partitioned_queue_message(message)?);
        }
        let queue_url = prepared_messages
            .first()
            .map(|message| message.queue_url.as_str())
            .ok_or_else(|| QueueError::internal(QueueInternalKind::NoWritableQueuePartition))?;
        if prepared_messages
            .iter()
            .any(|message| message.queue_url.as_str() != queue_url)
        {
            return Err(QueueError::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                "send_messages requires all messages to target the same queue_url",
            ));
        }
        let context = self.ensure_queue_execution_context(queue_url).await?;
        let queue_id = context.queue_id;
        let mut initial_routing_state = Some(context.routing_state);
        let cache_key = crate::sorted_kv::PartitionFamilyCacheKey::new(
            PartitionFamilyKind::StandardQueue,
            &queue_family_component(queue_url),
        );
        let wake_key = queue_wake_key(queue_id);
        let wake_bytes = wake_value_bytes()?;
        for attempt in 0..PARTITIONED_QUEUE_SEND_MAX_ATTEMPTS {
            let routing_state = match initial_routing_state.take() {
                Some(routing_state) => routing_state,
                None => match self.queue_routing_state(queue_url).await? {
                    Some(routing_state) => routing_state,
                    None => QueueRoutingState::Control(
                        self.ensure_queue_family_state(
                            queue_url,
                            DEFAULT_STANDARD_QUEUE_PARTITION_COUNT,
                        )
                        .await?,
                    ),
                },
            };
            let mut send_messages = Vec::with_capacity(prepared_messages.len());
            let mut partition_load = HashMap::<u16, (u64, u64)>::new();
            for prepared in &prepared_messages {
                let route = self
                    .queue_partition_route_for_send(
                        &routing_state,
                        &prepared.queue_url,
                        &prepared.message_id,
                    )
                    .ok_or_else(|| {
                        QueueError::internal(QueueInternalKind::NoWritableQueuePartition)
                    })?;
                let entry = partition_load.entry(route.partition_id).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry
                    .1
                    .saturating_add(u64::try_from(prepared.state_bytes.len()).unwrap_or(u64::MAX));
                send_messages.push(Self::partitioned_queue_write_for_route(
                    prepared,
                    queue_id,
                    route,
                    &wake_key,
                    &wake_bytes,
                ));
            }
            let QueueRoutingState::Control(_) = &routing_state;

            match self
                .kv_store
                .write_partitioned_queue_messages(send_messages)
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        queue_url = %queue_url,
                        message_count = prepared_messages.len(),
                        attempt,
                        elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        "sent partitioned queue messages"
                    );
                    for (partition_id, (writes, bytes)) in partition_load {
                        self.record_queue_partition_load(
                            queue_url,
                            partition_id,
                            crate::partition_family::PartitionLoadSample {
                                writes,
                                bytes,
                                conflicts: u64::try_from(attempt).unwrap_or(u64::MAX),
                                routing_key_bucket_bitmap: 0,
                                queue_scan_work: 0,
                                queue_claim_conflicts: 0,
                                oldest_visible_age_ms: 0,
                                visible_count: 0,
                                invisible_count: 0,
                            },
                        );
                    }
                    return Ok(prepared_messages
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect());
                }
                Err(error) if Self::is_partition_routing_retryable(&error) => {
                    metrics_facade::counter!(
                        PARTITION_ROUTING_RETRIES_TOTAL_METRIC,
                        "family_kind" => "standard_queue",
                        "operation" => "send",
                        "reason" => "stale_topology"
                    )
                    .increment(1);
                    self.invalidate_partition_family_cache(&cache_key);
                }
                Err(error) => return Err(QueueError::TransactWrite(error)),
            }
        }
        Err(QueueError::internal(
            QueueInternalKind::PartitionedQueueSendRetriesExhausted,
        ))
    }

    async fn send_partitioned_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        let mut sent_ids = self.send_partitioned_messages(vec![message]).await?;
        sent_ids
            .pop()
            .ok_or_else(|| QueueError::internal(QueueInternalKind::NoWritableQueuePartition))
    }

    async fn receive_partitioned_messages_once(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        routing_state: &QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let started_at = Instant::now();
        let mut messages = Vec::new();
        let per_partition_claim_limit = max_messages.max(1);
        let per_partition_scan_limit = max_messages
            .saturating_mul(PARTITIONED_QUEUE_RECEIVE_SCAN_OVERFETCH_MULTIPLIER)
            .clamp(RECEIVE_SCAN_MIN_LIMIT, RECEIVE_SCAN_MAX_LIMIT);
        let now = TimestampMillis::now();
        let mut scanned_partitions = 0usize;
        let mut claim_ranges = Vec::new();
        let routes = queue_partition_routes(routing_state);
        let receive_scan_shards = max_messages.max(1);
        let scan_shards = usize::try_from(receive_scan_shards)
            .unwrap_or(1)
            .clamp(1, PARTITIONED_QUEUE_RECEIVE_SCAN_SHARDS)
            .min(routes.len());
        let route_window = self
            .queue_receive_hinted_route_window(queue_url, queue_id, &routes, scan_shards, now)
            .await?;

        for route in route_window {
            if messages.len() >= usize::try_from(max_messages).unwrap_or(10) {
                break;
            }
            scanned_partitions = scanned_partitions.saturating_add(1);

            let ready_start = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &MessageVisibilityKey(visibility_key(
                    TimestampMillis::from(0),
                    &MessageId::default(),
                )),
            );
            let ready_end = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &MessageVisibilityKey(visibility_key(now + 1, &MessageId::default())),
            );
            claim_ranges.push(QueueClaimRange {
                queue_id,
                placement_slot: route.placement_slot,
                partition_id: route.partition_id,
                ready_start,
                ready_end,
                ready_hint_key: queue_ready_hint_key(
                    queue_id,
                    route.placement_slot,
                    route.partition_id,
                ),
                limit: per_partition_scan_limit,
                scan_limit: per_partition_scan_limit,
                claim_limit: per_partition_claim_limit,
                candidate_seed: Self::queue_claim_seed(queue_url, route.partition_id),
            });
        }

        let mut claimed_messages = self
            .kv_store
            .claim_queue_messages_from_ranges(
                claim_ranges,
                now,
                visibility_timeout,
                usize::try_from(max_messages).unwrap_or(10),
            )
            .await
            .map_err(QueueError::TransactWrite)?;
        let ready_entries_seen = claimed_messages.ready_entries_seen;
        claimed_messages.messages.sort_by(|left, right| {
            left.visibility_timestamp
                .cmp(&right.visibility_timestamp)
                .then_with(|| left.message_id_hex.cmp(&right.message_id_hex))
        });

        for claimed in claimed_messages.messages {
            let message_id = claimed
                .message_id_hex
                .parse::<MessageId>()
                .map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )
                })?;
            let body: PartitionedQueueBody =
                storage_types::storage_serde::from_bytes(&claimed.body_bytes).map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::PartitionedQueueBodyDecode,
                        format!(
                            "deserialize claimed partitioned queue body message_id={} error={:?}",
                            claimed.message_id_hex,
                            error.as_ref()
                        ),
                    )
                })?;
            let receipt_handle = QueueReceiptHandleData {
                partition_id: claimed.partition_id,
                message_id_hex: claimed.message_id_hex,
                visibility_timestamp_ms: claimed.visibility_timestamp.timestamp_millis(),
                delivery_attempt: claimed.delivery_attempt,
                claim_nonce: claimed.claim_nonce,
            }
            .encode()?;
            messages.push(MessageResponse::from_message(
                QueueMessage {
                    message_id,
                    queue_url: queue_url.to_string(),
                    body: body.body,
                    message_attributes: body.message_attributes,
                    receipt_handle: Some(ReceiptHandle(receipt_handle.clone())),
                    created_at: body.created_at,
                    visibility_timestamp: Some(claimed.visibility_timestamp),
                },
                &ReceiptHandle(receipt_handle),
            ));
        }

        tracing::debug!(
            queue_url,
            max_messages,
            scanned_partitions,
            ready_entries_seen,
            claimed_messages = messages.len(),
            elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            "received partitioned queue messages"
        );

        Ok(messages)
    }

    async fn receive_partitioned_messages(
        &self,
        queue_url: &str,
        queue_id: QueueStorageId,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
        routing_state: QueueRoutingState,
    ) -> QueueResult<Vec<MessageResponse>> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(u64::from(*wait_time_seconds));
        let mut messages = Vec::new();
        loop {
            let remaining_max_messages =
                max_messages.saturating_sub(u32::try_from(messages.len()).unwrap_or(u32::MAX));
            if remaining_max_messages == 0 {
                return Ok(messages);
            }

            let mut received = self
                .receive_partitioned_messages_once_coalesced(
                    queue_url,
                    queue_id,
                    remaining_max_messages,
                    visibility_timeout,
                    &routing_state,
                )
                .await?;
            let received_messages = !received.is_empty();
            messages.append(&mut received);

            if messages.len() >= usize::try_from(max_messages).unwrap_or(10) {
                return Ok(messages);
            }
            if *wait_time_seconds == 0 {
                return Ok(messages);
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(messages);
            }

            if received_messages {
                tokio::task::yield_now().await;
                continue;
            }

            let sample_delay = remaining.min(std::time::Duration::from_millis(
                PARTITIONED_QUEUE_EMPTY_RECEIVE_POLL_MS,
            ));
            let _ = self
                .kv_store
                .wait_for_change(&queue_wake_key(queue_id), sample_delay)
                .await?;
        }
    }

    async fn load_partitioned_state(
        &self,
        handle: &QueueReceiptHandleData,
        context: &QueueExecutionContext,
    ) -> QueueResult<(
        PartitionedQueueState,
        MessageId,
        QueuePartitionRoute,
        QueueStorageId,
    )> {
        let message_id = handle
            .message_id_hex
            .parse::<MessageId>()
            .map_err(|error| {
                QueueError::internal_with_detail(
                    QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                    error,
                )
            })?;
        let route = queue_partition_route_for_id(&context.routing_state, handle.partition_id)
            .ok_or_else(|| QueueError::internal(QueueInternalKind::MissingQueuePartitionState))?;
        let queue_id = context.queue_id;
        let state_key = queue_state_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            &handle.message_id_hex,
        );
        let Some(state_bytes) = self.kv_store.get(&state_key, true).await? else {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        };
        let state: PartitionedQueueState = storage_types::storage_serde::from_bytes(&state_bytes)
            .map_err(|error| {
            QueueError::internal_with_detail(
                QueueInternalKind::PartitionedQueueStateDecode,
                format!(
                    "deserialize partitioned queue receipt state key={} error={:?}",
                    String::from_utf8_lossy(&state_key),
                    error.as_ref()
                ),
            )
        })?;
        let Some(claim_nonce) = &state.claim_nonce else {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        };
        if state.delivery_attempt != handle.delivery_attempt || claim_nonce != &handle.claim_nonce {
            return Err(QueueError::ResourceNotFound {
                resource_type: "receipt_handle",
                resource_id: handle.message_id_hex.clone(),
            });
        }
        Ok((state, message_id, route, queue_id))
    }

    async fn delete_partitioned_message_with_state(
        &self,
        handle: &QueueReceiptHandleData,
        context: &QueueExecutionContext,
        started_at: Instant,
    ) -> QueueResult<()> {
        let now = TimestampMillis::now();
        let (state, message_id, route, queue_id) =
            self.load_partitioned_state(handle, context).await?;
        if state.visibility_timestamp < now {
            return Err(QueueError::validation(
                QueueValidationKind::CannotOperateVisibleMessage,
            ));
        }
        let visibility_key =
            MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
        let ready_key = queue_ready_key_with_slot(
            queue_id,
            route.placement_slot,
            route.partition_id,
            &visibility_key,
        );
        self.kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: ready_key.clone(),
                    expected_value: state.claim_nonce.clone().map(|nonce| nonce.into_bytes()),
                },
                DirectWriteOperation::Delete { key: ready_key },
                DirectWriteOperation::Put {
                    key: queue_delete_ledger_key(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &handle.message_id_hex,
                    ),
                    value: queue_delete_ledger_entry_bytes(
                        queue_id,
                        route,
                        &handle.message_id_hex,
                    )?,
                },
            ])
            .await
            .map_err(QueueError::TransactWrite)?;
        record_deferred_payload_cleanup(1);
        tracing::debug!(
            queue_id = queue_id.get(),
            partition_id = handle.partition_id,
            elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            "deleted partitioned queue message through state fallback"
        );
        Ok(())
    }

    async fn queue_identity_by_id(
        &self,
        queue_id: QueueStorageId,
    ) -> QueueResult<Option<StoredQueueIdentity>> {
        self.kv_store
            .get(&compact::queue_metadata_key(queue_id), true)
            .await?
            .map(|value| decode_queue_identity(&value))
            .transpose()
    }

    async fn queue_identity_by_url(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<StoredQueueIdentity>> {
        let queue_id = queue_storage_id_from_url(queue_url)?;
        Ok(self
            .queue_identity_by_id(queue_id)
            .await?
            .filter(|identity| identity.queue.queue_url == queue_url))
    }

    async fn queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        let queue_id = queue_storage_id_from_url(queue_url)?;
        let family_component = queue_family_component(queue_url);
        let metadata_key = compact::queue_metadata_key(queue_id);
        let config_key = partition_family_config_key(
            PartitionFamilyKind::StandardQueue,
            &family_component,
        );
        let partition_prefix = partition_info_prefix(
            PartitionFamilyKind::StandardQueue,
            &family_component,
        );
        let partition_end = increment_bytes(partition_prefix.clone());
        let read_context = self.kv_store.begin_read_context().await?;
        let (metadata, config, partition_values) = tokio::try_join!(
            read_context.get(&metadata_key, true),
            read_context.get(&config_key, true),
            read_context.get_range_values(
                &partition_prefix,
                &partition_end,
                None,
                None,
                true,
            ),
        )?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        let identity = decode_queue_identity(&metadata)?;
        if identity.queue.queue_url != queue_url {
            return Ok(None);
        }
        let Some(config) = config else {
            return Ok(Some((identity.queue, QueueMessageCounts::default())));
        };
        let mut partitions = partition_values
            .values
            .into_iter()
            .map(|value| parse_partition_info(&value))
            .collect::<StorageResult<Vec<_>>>()?;
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });
        let routing_state = QueueRoutingState::Control(ResolvedPartitionFamily {
            config: parse_partition_family_config(&config)?,
            partitions,
        });
        let now = TimestampMillis::now();
        let state_ranges = try_join_all(queue_partition_routes(&routing_state).into_iter().map(
            |route| {
                let prefix = queue_state_key_with_slot(
                    queue_id,
                    route.placement_slot,
                    route.partition_id,
                    "",
                );
                let end = increment_bytes(prefix.clone());
                let read_context = &read_context;
                async move {
                    read_context
                        .get_range_values(&prefix, &end, None, None, true)
                        .await
                }
            },
        ))
        .await?;
        let mut counts = QueueMessageCounts::default();
        for state in state_ranges.into_iter().flat_map(|range| range.values) {
            let state = storage_types::storage_serde::from_bytes::<PartitionedQueueState>(&state)?;
            if state.visibility_timestamp <= now {
                counts.visible = counts.visible.saturating_add(1);
            } else if state.claim_nonce.is_some() {
                counts.not_visible = counts.not_visible.saturating_add(1);
            } else {
                counts.delayed = counts.delayed.saturating_add(1);
            }
        }
        Ok(Some((identity.queue, counts)))
    }

    async fn find_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        let Some(queue_id_bytes) = self
            .kv_store
            .get(&compact::queue_name_lookup_key(queue_name), true)
            .await?
        else {
            return Ok(None);
        };
        let queue_id = decode_queue_storage_id(&queue_id_bytes)?;
        Ok(self
            .queue_identity_by_id(queue_id)
            .await?
            .map(|identity| identity.queue))
    }

    async fn list_queue_identities(&self) -> QueueResult<Vec<StoredQueueIdentity>> {
        let range = compact::queue_metadata_prefix();
        let items = self
            .kv_store
            .get_range(&range.start, &range.end, None, None::<RawKey>, true)
            .await?;
        items
            .items
            .into_iter()
            .map(|(_key, value)| decode_queue_identity(&value))
            .collect()
    }
}

pub(crate) fn partitioned_ready_visibility_key(
    storage_key: &[u8],
) -> QueueResult<MessageVisibilityKey> {
    if let Ok(compact::ParsedCompactKey::PartitionedQueueData {
        kind: compact::QueueRecordKind::Ready,
        suffix,
        ..
    }) = compact::parse_compact_key(storage_key)
    {
        let visibility_key = std::str::from_utf8(suffix).map_err(|error| {
            QueueError::internal_with_detail(
                QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                error,
            )
        })?;
        return Ok(MessageVisibilityKey(visibility_key.to_string()));
    }
    let key_text = String::from_utf8(storage_key.to_vec()).map_err(|error| {
        QueueError::internal_with_detail(
            QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            error,
        )
    })?;
    let (_, suffix) = key_text.rsplit_once("/ready/").ok_or_else(|| {
        QueueError::internal_with_detail(
            QueueInternalKind::InvalidMessageVisibilityKeyFormat,
            key_text.clone(),
        )
    })?;
    Ok(MessageVisibilityKey(suffix.to_string()))
}

fn queue_partition_hash(queue_url: &str, message_id: &MessageId) -> u64 {
    let mut key = queue_url.as_bytes().to_vec();
    key.extend_from_slice(message_id.as_bytes());
    ordered_log_hash(&key)
}

fn queue_partition_routes(routing_state: &QueueRoutingState) -> Vec<QueuePartitionRoute> {
    match routing_state {
        QueueRoutingState::Control(family) => family
            .partitions
            .iter()
            .filter(|partition| partition.is_readable())
            .map(|partition| QueuePartitionRoute {
                partition_id: partition.partition_id,
                placement_slot: partition.placement_slot,
            })
            .collect(),
    }
}

fn queue_partition_route_for_id(
    routing_state: &QueueRoutingState,
    partition_id: u16,
) -> Option<QueuePartitionRoute> {
    match routing_state {
        QueueRoutingState::Control(family) => {
            find_partition_by_id(&family.partitions, partition_id).map(|partition| {
                QueuePartitionRoute {
                    partition_id: partition.partition_id,
                    placement_slot: partition.placement_slot,
                }
            })
        }
    }
}

fn queue_metadata_matches(existing: &Queue, requested: &Queue) -> bool {
    existing.queue_name == requested.queue_name
        && (existing.queue_url == requested.queue_url
            || queue_url_without_storage_id(&existing.queue_url)
                .is_some_and(|queue_url| queue_url == requested.queue_url))
        && existing.attributes == requested.attributes
}

impl<S> SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    async fn existing_queue_for_create(&self, queue: &Queue) -> QueueResult<Option<Queue>> {
        Ok(self
            .find_queue_by_name(&queue.queue_name)
            .await?
            .filter(|existing| queue_metadata_matches(existing, queue)))
    }
}

#[async_trait]
impl<S> QueueProvider for SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    async fn initialize(&self) -> QueueResult<()> {
        self.start_partition_reconcile_task().await?;
        self.start_queue_payload_cleanup_task().await?;
        Ok(())
    }

    async fn create_queue(&self, mut queue: Queue) -> QueueResult<Queue> {
        if let Some(existing) = self.existing_queue_for_create(&queue).await? {
            return Ok(existing);
        }
        let requested_queue = queue.clone();
        let allocator_key = compact::queue_id_allocator_key();
        let allocator_value = self.kv_store.get(&allocator_key, true).await?;
        let queue_id = match allocator_value.as_deref() {
            Some(bytes) => decode_queue_storage_id(bytes)?,
            None => queue_storage_id(1)?,
        };
        let next_queue_id = queue_storage_id(queue_id.get().saturating_add(1))?;
        queue.queue_url = queue_url_with_storage_id(&queue.queue_url, queue_id);
        let identity = StoredQueueIdentity {
            queue_id,
            queue: queue.clone(),
        };
        let queue_id_bytes = encode_queue_storage_id(queue_id);
        if let Err(error) = self
            .kv_store
            .transact_write_unchecked(vec![
                DirectWriteOperation::CheckValue {
                    key: allocator_key.clone(),
                    expected_value: allocator_value,
                },
                DirectWriteOperation::CheckValue {
                    key: compact::queue_name_lookup_key(&queue.queue_name),
                    expected_value: None,
                },
                DirectWriteOperation::Put {
                    key: allocator_key,
                    value: encode_queue_storage_id(next_queue_id),
                },
                DirectWriteOperation::Put {
                    key: compact::queue_name_lookup_key(&queue.queue_name),
                    value: queue_id_bytes,
                },
                DirectWriteOperation::Put {
                    key: compact::queue_metadata_key(queue_id),
                    value: encode_queue_identity(&identity)?,
                },
            ])
            .await
        {
            if let Some(existing) = self
                .existing_queue_for_create(&requested_queue)
                .await
                .unwrap_or(None)
            {
                return Ok(existing);
            }
            return Err(QueueError::TransactWrite(error));
        }
        if self.kv_store.supports_partition_families() {
            let family = self
                .ensure_queue_family_state(&queue.queue_url, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)
                .await?;
            let prewarm_partitions = queue_partition_routes(&QueueRoutingState::Control(family))
                .into_iter()
                .map(|route| QueuePrewarmPartition {
                    placement_slot: route.placement_slot,
                    partition_id: route.partition_id,
                    marker_key: queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        QUEUE_PREWARM_MESSAGE_ID,
                    ),
                })
                .collect();
            self.kv_store
                .prewarm_partitioned_queue(&queue.queue_url, prewarm_partitions)
                .await?;
            let marker_key = queue_partition_marker_key(&queue.queue_url);
            self.kv_store
                .put(
                    &marker_key,
                    &queue_partition_marker_bytes(DEFAULT_STANDARD_QUEUE_PARTITION_COUNT)?,
                    None,
                )
                .await?;
            self.kv_store
                .put(&queue_wake_key(queue_id), &wake_value_bytes()?, None)
                .await?;
        }
        Ok(queue)
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        Ok(self
            .queue_identity_by_url(queue_url)
            .await?
            .map(|identity| identity.queue))
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        self.queue_with_message_counts(queue_url).await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        self.find_queue_by_name(queue_name).await
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        Ok(self
            .list_queue_identities()
            .await?
            .into_iter()
            .map(|identity| identity.queue)
            .filter(|queue| {
                queue_name_prefix.is_none_or(|prefix| queue.queue_name.starts_with(prefix))
            })
            .collect())
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        let identity = self.queue_identity_by_url(queue_url).await?;
        if let Some(family) = self.load_queue_family_state(queue_url).await? {
            for route in queue_partition_routes(&QueueRoutingState::Control(family)) {
                if let Some(identity) = &identity {
                    self.kv_store
                        .delete_prefix(queue_ready_prefix_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                        ))
                        .await?;
                    self.kv_store
                        .delete_prefix(queue_state_key_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                            "",
                        ))
                        .await?;
                    self.kv_store
                        .delete_prefix(queue_body_prefix_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                        ))
                        .await?;
                    record_queue_storage_operation("queue_payload_cleanup", "prefix_delete", 1);
                    self.kv_store
                        .delete_prefix(queue_checkpoint_key_with_slot(
                            identity.queue_id,
                            route.placement_slot,
                            route.partition_id,
                            "",
                        ))
                        .await?;
                }
            }
            self.delete_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &queue_family_component(queue_url),
            )
            .await?;
        }
        self.kv_store
            .delete(&queue_partition_marker_key(queue_url))
            .await?;
        if let Some(identity) = &identity {
            let _ = self
                .kv_store
                .delete(&queue_wake_key(identity.queue_id))
                .await;
        }
        if let Some(identity) = identity {
            self.kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::Delete {
                        key: compact::queue_metadata_key(identity.queue_id),
                    },
                    DirectWriteOperation::Delete {
                        key: compact::queue_name_lookup_key(&identity.queue.queue_name),
                    },
                ])
                .await
                .map_err(QueueError::TransactWrite)?;
        }
        Ok(())
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        let queue_id = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?
            .queue_id;
        if let Some(family) = self.load_queue_family_state(queue_url).await? {
            for route in queue_partition_routes(&QueueRoutingState::Control(family)) {
                self.kv_store
                    .delete_prefix(queue_ready_prefix_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                    ))
                    .await?;
                self.kv_store
                    .delete_prefix(queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        "",
                    ))
                    .await?;
                self.kv_store
                    .delete_prefix(queue_body_prefix_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                    ))
                    .await?;
                record_queue_storage_operation("queue_payload_cleanup", "prefix_delete", 1);
                self.kv_store
                    .delete_prefix(queue_checkpoint_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        "",
                    ))
                    .await?;
            }
        }
        Ok(())
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        let mut identity = self
            .queue_identity_by_url(queue_url)
            .await?
            .ok_or_else(|| QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue_url.to_string(),
            })?;
        identity.queue.attributes = attributes;
        self.kv_store
            .put(
                &compact::queue_metadata_key(identity.queue_id),
                &encode_queue_identity(&identity)?,
                None,
            )
            .await?;
        Ok(())
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        self.send_partitioned_message(message).await
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        let message_ids = self.send_partitioned_messages(messages).await?;
        Ok(message_ids.into_iter().map(Ok).collect())
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        let context = self.ensure_queue_execution_context(queue_url).await?;
        self.receive_partitioned_messages(
            queue_url,
            context.queue_id,
            max_messages,
            visibility_timeout,
            wait_time_seconds,
            context.routing_state,
        )
        .await
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        let started_at = Instant::now();
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let now = TimestampMillis::now();
            let message_id = handle
                .message_id_hex
                .parse::<MessageId>()
                .map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )
                })?;
            let visibility_timestamp = TimestampMillis::from(handle.visibility_timestamp_ms);
            if visibility_timestamp < now {
                return Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                ));
            }
            let route = queue_partition_route_for_id(&context.routing_state, handle.partition_id)
                .ok_or_else(|| {
                    QueueError::internal(QueueInternalKind::MissingQueuePartitionState)
                })?;
            let queue_id = context.queue_id;
            let visibility_key =
                MessageVisibilityKey(visibility_key(visibility_timestamp, &message_id));
            let ready_key = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            );
            if let Err(error) = self
                .kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: ready_key.clone(),
                        expected_value: Some(handle.claim_nonce.as_bytes().to_vec()),
                    },
                    DirectWriteOperation::Delete { key: ready_key },
                    DirectWriteOperation::Put {
                        key: queue_delete_ledger_key(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &handle.message_id_hex,
                        ),
                        value: queue_delete_ledger_entry_bytes(
                            queue_id,
                            route,
                            &handle.message_id_hex,
                        )?,
                    },
                ])
                .await
            {
                return match error.as_ref() {
                    StorageEnum::ConditionalCheckFailed
                    | StorageEnum::TransactionCanceled { .. } => {
                        self.delete_partitioned_message_with_state(&handle, &context, started_at)
                            .await
                    }
                    _ => Err(QueueError::TransactWrite(error)),
                };
            }
            record_deferred_payload_cleanup(1);
            tracing::debug!(
                queue_url,
                partition_id = handle.partition_id,
                "deleted partitioned queue message"
            );
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipt_handles: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let now = TimestampMillis::now();
        let mut results = std::iter::repeat_with(|| None)
            .take(receipt_handles.len())
            .collect::<Vec<Option<QueueResult<()>>>>();
        let context = self.queue_execution_context(queue_url).await?;
        let mut decoded_handles = Vec::with_capacity(receipt_handles.len());
        for (index, receipt_handle) in receipt_handles.iter().enumerate() {
            match Self::decode_receipt_handle(receipt_handle) {
                Ok(handle) => decoded_handles.push(Some(handle)),
                Err(error) => {
                    results[index] = Some(Err(error));
                    decoded_handles.push(None);
                }
            }
        }
        let Some(context) = context else {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(index, result)| {
                    result.unwrap_or_else(|| {
                        Err(Self::invalid_receipt_handle(&receipt_handles[index]))
                    })
                })
                .collect());
        };

        let mut operations = Vec::with_capacity(decoded_handles.len().saturating_mul(2));
        let mut deferred_payload_cleanup = 0u64;
        let queue_id = context.queue_id;

        for (index, handle) in decoded_handles.iter().enumerate() {
            let Some(handle) = handle else {
                continue;
            };
            let message_id = match handle.message_id_hex.parse::<MessageId>() {
                Ok(message_id) => message_id,
                Err(error) => {
                    results[index] = Some(Err(QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )));
                    continue;
                }
            };
            let Some(route) =
                queue_partition_route_for_id(&context.routing_state, handle.partition_id)
            else {
                results[index] = Some(Err(QueueError::internal(
                    QueueInternalKind::MissingQueuePartitionState,
                )));
                continue;
            };
            let visibility_timestamp = TimestampMillis::from(handle.visibility_timestamp_ms);
            if visibility_timestamp < now {
                results[index] = Some(Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                )));
                continue;
            }

            let visibility_key =
                MessageVisibilityKey(visibility_key(visibility_timestamp, &message_id));
            let ready_key = queue_ready_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &visibility_key,
            );
            operations.extend([
                DirectWriteOperation::CheckValue {
                    key: ready_key.clone(),
                    expected_value: Some(handle.claim_nonce.as_bytes().to_vec()),
                },
                DirectWriteOperation::Delete { key: ready_key },
                DirectWriteOperation::Put {
                    key: queue_delete_ledger_key(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &handle.message_id_hex,
                    ),
                    value: queue_delete_ledger_entry_bytes(
                        queue_id,
                        route,
                        &handle.message_id_hex,
                    )?,
                },
            ]);
            deferred_payload_cleanup = deferred_payload_cleanup.saturating_add(1);
            results[index] = Some(Ok(()));
        }

        if !operations.is_empty()
            && let Err(error) = self.kv_store.transact_write_unchecked(operations).await
        {
            if matches!(
                error.as_ref(),
                StorageEnum::ConditionalCheckFailed | StorageEnum::TransactionCanceled { .. }
            ) {
                let mut fallback_results = Vec::with_capacity(decoded_handles.len());
                for (index, handle) in decoded_handles.into_iter().enumerate() {
                    fallback_results.push(match handle {
                        Some(handle) => {
                            self.delete_partitioned_message_with_state(
                                &handle,
                                &context,
                                Instant::now(),
                            )
                            .await
                        }
                        None => Err(Self::invalid_receipt_handle(&receipt_handles[index])),
                    });
                }
                return Ok(fallback_results);
            }
            return Err(QueueError::TransactWrite(error));
        }
        record_deferred_payload_cleanup(deferred_payload_cleanup);

        Ok(results
            .into_iter()
            .map(|result| result.unwrap_or(Ok(())))
            .collect())
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let now = TimestampMillis::now();
            let (mut state, message_id, route, queue_id) =
                self.load_partitioned_state(&handle, &context).await?;
            if state.visibility_timestamp < now {
                return Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                ));
            }
            let old_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let Some(claim_nonce) = state.claim_nonce.clone() else {
                return Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                });
            };
            state.visibility_timestamp = now + visibility_timeout;
            let new_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let message_id_hex = message_id.to_string();
            self.kv_store
                .transact_write_unchecked(vec![
                    DirectWriteOperation::CheckValue {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &old_visibility_key,
                        ),
                        expected_value: Some(claim_nonce.as_bytes().to_vec()),
                    },
                    DirectWriteOperation::Delete {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &old_visibility_key,
                        ),
                    },
                    DirectWriteOperation::Put {
                        key: queue_ready_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &new_visibility_key,
                        ),
                        value: claim_nonce.into_bytes(),
                    },
                    DirectWriteOperation::Put {
                        key: queue_state_key_with_slot(
                            queue_id,
                            route.placement_slot,
                            route.partition_id,
                            &message_id_hex,
                        ),
                        value: storage_types::storage_serde::to_bytes(&state)?,
                    },
                    DirectWriteOperation::Put {
                        key: queue_wake_key(queue_id),
                        value: wake_value_bytes()?,
                    },
                ])
                .await
                .map_err(QueueError::TransactWrite)?;
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let now = TimestampMillis::now();
        let mut results = std::iter::repeat_with(|| None)
            .take(entries.len())
            .collect::<Vec<Option<QueueResult<()>>>>();
        let context = self.queue_execution_context(queue_url).await?;
        let mut decoded_entries = Vec::with_capacity(entries.len());
        for (index, (receipt_handle, visibility_timeout)) in entries.iter().enumerate() {
            match Self::decode_receipt_handle(receipt_handle) {
                Ok(handle) => decoded_entries.push((Some(handle), *visibility_timeout)),
                Err(error) => {
                    results[index] = Some(Err(error));
                    decoded_entries.push((None, *visibility_timeout));
                }
            }
        }
        let Some(context) = context else {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(index, result)| {
                    result.unwrap_or_else(|| Err(Self::invalid_receipt_handle(&entries[index].0)))
                })
                .collect());
        };

        let mut operations = Vec::with_capacity(decoded_entries.len().saturating_mul(3) + 1);
        let mut changed_any = false;
        let queue_id = context.queue_id;

        for (index, (handle, visibility_timeout)) in decoded_entries.into_iter().enumerate() {
            let Some(handle) = handle else {
                continue;
            };
            let message_id = match handle.message_id_hex.parse::<MessageId>() {
                Ok(message_id) => message_id,
                Err(error) => {
                    results[index] = Some(Err(QueueError::internal_with_detail(
                        QueueInternalKind::InvalidMessageVisibilityKeyFormat,
                        error,
                    )));
                    continue;
                }
            };
            let Some(route) =
                queue_partition_route_for_id(&context.routing_state, handle.partition_id)
            else {
                results[index] = Some(Err(QueueError::internal(
                    QueueInternalKind::MissingQueuePartitionState,
                )));
                continue;
            };
            let state_key = queue_state_key_with_slot(
                queue_id,
                route.placement_slot,
                route.partition_id,
                &handle.message_id_hex,
            );
            let Some(state_bytes) = self.kv_store.get(&state_key, true).await? else {
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            };
            let mut state: PartitionedQueueState =
                storage_types::storage_serde::from_bytes(&state_bytes).map_err(|error| {
                    QueueError::internal_with_detail(
                        QueueInternalKind::PartitionedQueueStateDecode,
                        format!(
                            "deserialize partitioned queue receipt state key={} error={:?}",
                            String::from_utf8_lossy(&state_key),
                            error.as_ref()
                        ),
                    )
                })?;
            let Some(claim_nonce) = &state.claim_nonce else {
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            };
            if state.delivery_attempt != handle.delivery_attempt
                || claim_nonce != &handle.claim_nonce
            {
                results[index] = Some(Err(QueueError::ResourceNotFound {
                    resource_type: "receipt_handle",
                    resource_id: handle.message_id_hex,
                }));
                continue;
            }
            if state.visibility_timestamp < now {
                results[index] = Some(Err(QueueError::validation(
                    QueueValidationKind::CannotOperateVisibleMessage,
                )));
                continue;
            }

            let old_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let claim_nonce = claim_nonce.clone();
            state.visibility_timestamp = now + visibility_timeout;
            let new_visibility_key =
                MessageVisibilityKey(visibility_key(state.visibility_timestamp, &message_id));
            let message_id_hex = message_id.to_string();
            operations.extend([
                DirectWriteOperation::CheckValue {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &old_visibility_key,
                    ),
                    expected_value: Some(claim_nonce.as_bytes().to_vec()),
                },
                DirectWriteOperation::Delete {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &old_visibility_key,
                    ),
                },
                DirectWriteOperation::Put {
                    key: queue_ready_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &new_visibility_key,
                    ),
                    value: claim_nonce.into_bytes(),
                },
                DirectWriteOperation::Put {
                    key: queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &message_id_hex,
                    ),
                    value: storage_types::storage_serde::to_bytes(&state)?,
                },
            ]);
            changed_any = true;
            results[index] = Some(Ok(()));
        }

        if changed_any {
            operations.push(DirectWriteOperation::Put {
                key: queue_wake_key(queue_id),
                value: wake_value_bytes()?,
            });
            self.kv_store
                .transact_write_unchecked(operations)
                .await
                .map_err(QueueError::TransactWrite)?;
        }

        Ok(results
            .into_iter()
            .map(|result| result.unwrap_or(Ok(())))
            .collect())
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        let handle = Self::decode_receipt_handle(&receipt_handle)?;
        if let Some(context) = self.queue_execution_context(queue_url).await? {
            let (mut state, _, route, queue_id) =
                self.load_partitioned_state(&handle, &context).await?;
            state.checkpoint_data = Some(checkpoint_data);
            let value = storage_types::storage_serde::to_bytes(&state)?;
            self.kv_store
                .put(
                    &queue_state_key_with_slot(
                        queue_id,
                        route.placement_slot,
                        route.partition_id,
                        &handle.message_id_hex,
                    ),
                    &value,
                    None,
                )
                .await?;
            return Ok(());
        }
        Err(Self::invalid_receipt_handle(&receipt_handle))
    }
}
