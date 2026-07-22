use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::Arc,
    time::{Duration, Instant},
};

use foundationdb::{Database, FdbError, RangeOption, Transaction, TransactionCommitError, options};
use futures_util::future::try_join_all;
#[cfg(test)]
use storage_common::provider_perf;
use storage_condition::{Condition, evaluate_condition_bytes};
use storage_types::{
    DurationSeconds, SerializesToKey, StorageEnum, StorageError, StorageResult, StreamItemId,
    StreamName, TimestampMillis,
};
use stream_provider::StoredStreamPointer;
use tokio::time;
use uuid::Uuid;

use super::{
    error::map_fdb_error,
    keyspace,
    metrics::{
        record_fdb_operation, record_fdb_operation_bytes, record_fdb_operation_latency,
        record_fdb_point_read, record_fdb_range_read, record_fdb_transaction_start,
        record_fdb_write_shape,
    },
    network::{
        FoundationDbNetworkOwnership, init_network, open_database,
        validate_simulated_database_config,
    },
    read_context::FoundationDbReadContext,
};
use crate::{
    backends::common::{
        KvMutation, operation_requires_stream_entries, plan_table_write_preflighted,
        plan_transact_operation, preflight_table_write_operations, table_operation_primary_key,
    },
    constants::FOUNDATIONDB_GET_READ_VERSION_LATENCY_MS_METRIC,
    helpers::increment_bytes,
    key_template::{KeyTemplate, PlaceholderBinding, PlaceholderId},
    keyspace::compact,
    partition_family::{
        DEFAULT_ORDERED_LOG_PARTITION_COUNT, OrderedLogSplitMarker, PartitionFamilyKind,
        PartitionFamilyKvStore, PartitionLoadSample, ResolvedPartitionFamily,
        RuntimePartitionLoadSample, default_partition_family_config, find_partition_for_hash,
        initial_partition_infos, merge_partition_load, next_partition_id, next_placement_slot,
        ordered_log_family_component, ordered_log_hash, ordered_log_partition_prefix_with_slot,
        ordered_log_split_marker_bytes, ordered_log_split_marker_prefix,
        parse_partition_family_config, parse_partition_info, partition_family_config_bytes,
        partition_family_epoch_bytes, partition_info_bytes, routing_key_bucket_bit,
        split_partition_children, supports_pointer_stream_partitioning,
    },
    partition_runtime_load::RuntimePartitionLoadTracker,
    queue::{
        PartitionedQueueMessageWrite, QueueClaimBatch, QueueClaimRange, QueueClaimedMessage,
        QueueKvStore, QueuePrewarmPartition,
        constants::QUEUE_PAYLOAD_CHUNK_BYTES,
        storage::{
            queue_payload_chunk_key, queue_prewarm_marker_bytes, read_partitioned_queue_payload,
        },
    },
    sorted_kv_store::{
        AtomicTableWriteDecision, AtomicTableWriteTransform, BatchItem, DirectWriteOperation,
        OldNewItems, RangeResult, SortedKvReadContext, SortedKvStore, TransactWriteOperation,
        TransactWriteOutput, TransactWriteTableOperation,
    },
    stream::item_codec::decode_stream_item,
};

#[derive(Clone, Debug, Default)]
pub struct FoundationDbConfig {
    pub cluster_file_path: Option<String>,
    pub tenant_name: Option<Vec<u8>>,
    pub subspace_prefix: Option<Vec<u8>>,
    pub cache_read_version_ms: u16,
    pub immediate_gsi_consistency: bool,
    pub report_conflicting_keys: bool,
}

type OrderedLogFamilyCache = HashMap<String, ResolvedPartitionFamily>;

async fn read_fdb_keys_sequential(
    trx: &Transaction,
    keys: &[Vec<u8>],
    snapshot: bool,
) -> Result<Vec<Option<Vec<u8>>>, FdbError> {
    let mut values = Vec::with_capacity(keys.len());
    for key in keys {
        values.push(trx.get(key, snapshot).await?.map(|value| value.to_vec()));
    }
    Ok(values)
}

fn queue_ready_hint_is_earlier(candidate: &[u8], existing: &[u8]) -> bool {
    let Some(candidate_timestamp) = candidate.get(2..10) else {
        return false;
    };
    let Some(existing_timestamp) = existing.get(2..10) else {
        return true;
    };
    candidate_timestamp < existing_timestamp
}

fn adjust_versionstamp_offset(bytes: &mut [u8], added_prefix_len: usize) {
    if added_prefix_len == 0 || bytes.len() < 4 {
        return;
    }

    let offset_index = bytes.len() - 4;
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[offset_index..]);
    let current = u32::from_le_bytes(arr);
    let Some(adjusted) = u32::try_from(added_prefix_len)
        .ok()
        .and_then(|added| current.checked_add(added))
    else {
        tracing::warn!(
            current,
            added_prefix_len,
            "skip foundationdb versionstamp offset adjustment because offset would overflow"
        );
        return;
    };
    bytes[offset_index..].copy_from_slice(&adjusted.to_le_bytes());
}

fn rotate_fdb_claim_candidates<T>(items: &mut [T], seed: u64) {
    if items.len() <= 1 {
        return;
    }
    let offset = usize::try_from(seed % u64::try_from(items.len()).unwrap_or(1)).unwrap_or(0);
    items.rotate_left(offset);
}

#[derive(Clone)]
pub struct FoundationDbKvStore {
    database: Arc<Database>,
    _network: FoundationDbNetworkOwnership,
    config: Arc<FoundationDbConfig>,
    runtime_partition_load_tracker: RuntimePartitionLoadTracker,
}

#[derive(Clone)]
pub(crate) struct PendingOrderedLogWrite {
    family_component: String,
    partition_id: u16,
    bytes: u64,
    routing_key_bucket_bitmap: u64,
}

pub(crate) struct FdbTableWriteExecution {
    results: Vec<OldNewItems>,
    ordered_log_writes: Vec<PendingOrderedLogWrite>,
}

pub(crate) enum FdbTableWriteExecutionError {
    Storage(StorageError),
    Fdb {
        scope: &'static str,
        error: FdbError,
    },
}

impl FdbTableWriteExecutionError {
    const fn fdb(scope: &'static str, error: FdbError) -> Self {
        Self::Fdb { scope, error }
    }
}

impl From<StorageError> for FdbTableWriteExecutionError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

pub(crate) enum FdbTransactionAttemptError {
    Storage(StorageError),
    Fdb {
        scope: &'static str,
        error: FdbError,
    },
}

impl FdbTransactionAttemptError {
    const fn fdb(scope: &'static str, error: FdbError) -> Self {
        Self::Fdb { scope, error }
    }
}

impl From<StorageError> for FdbTransactionAttemptError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

mod connection;
mod family_state;
mod ordered_log;
mod partition_family_provider;
mod queue_claim;
mod queue_provider;
mod queue_write;
mod sorted_kv_provider;
mod sorted_reads;
mod sorted_table_writes;
mod sorted_transactions;
mod sorted_writes;
mod transactions;
