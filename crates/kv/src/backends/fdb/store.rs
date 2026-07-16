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
        storage::{queue_payload_chunk_key, read_partitioned_queue_payload},
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
struct PendingOrderedLogWrite {
    family_component: String,
    partition_id: u16,
    bytes: u64,
    routing_key_bucket_bitmap: u64,
}

struct FdbTableWriteExecution {
    results: Vec<OldNewItems>,
    ordered_log_writes: Vec<PendingOrderedLogWrite>,
}

enum FdbTableWriteExecutionError {
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

enum FdbTransactionAttemptError {
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

impl FoundationDbKvStore {
    pub fn connect(config: FoundationDbConfig) -> StorageResult<Self> {
        let network = init_network(&config)?;
        let database = open_database(&config)?;
        Ok(Self {
            database: Arc::new(database),
            _network: network,
            config: Arc::new(config),
            runtime_partition_load_tracker: RuntimePartitionLoadTracker::default(),
        })
    }

    pub async fn check_reachable(&self, timeout: Duration) -> StorageResult<()> {
        let check = async {
            let trx = self.create_transaction()?;
            Self::configure_transaction(&trx, Some("kv.startup_check"), true)?;
            let _ = trx
                .get(b"__aux_healthcheck", false)
                .await
                .map_err(|err| map_fdb_error("foundationdb healthcheck read", err))?;
            Ok::<(), StorageError>(())
        };

        match time::timeout(timeout, check).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(StorageError::internal(&format!(
                "foundationdb server is not reachable: {err}"
            ))),
            Err(_) => Err(StorageError::internal(&format!(
                "foundationdb server is not reachable (timed out after {}s)",
                timeout.as_secs()
            ))),
        }
    }

    pub fn connect_default() -> StorageResult<Self> {
        Self::connect(FoundationDbConfig::default())
    }

    pub fn from_database(
        config: FoundationDbConfig,
        database: Arc<Database>,
    ) -> StorageResult<Self> {
        validate_simulated_database_config(&config)?;
        Ok(Self {
            database,
            _network: FoundationDbNetworkOwnership::Simulated,
            config: Arc::new(config),
            runtime_partition_load_tracker: RuntimePartitionLoadTracker::default(),
        })
    }

    #[must_use]
    pub fn database(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }

    #[must_use]
    pub fn config(&self) -> Arc<FoundationDbConfig> {
        Arc::clone(&self.config)
    }

    pub async fn direct_audit_scan_prefix(
        &self,
        prefix: &[u8],
        limit: u32,
    ) -> StorageResult<RangeResult> {
        self.get_prefix(prefix, true, Some(limit.max(1)), true)
            .await
    }

    pub(super) fn create_transaction(&self) -> StorageResult<Transaction> {
        let trx = self
            .database
            .create_trx()
            .map_err(|err| map_fdb_error("create FoundationDB transaction", err))?;
        if self.config.report_conflicting_keys {
            trx.set_option(options::TransactionOption::ReportConflictingKeys)
                .map_err(|err| map_fdb_error("enable conflict key reporting", err))?;
        }
        Ok(trx)
    }

    fn uses_grv_cache(&self, consistent_read: bool) -> bool {
        !consistent_read && self.config.cache_read_version_ms > 0
    }

    pub(super) fn configure_read_transaction(
        &self,
        trx: &Transaction,
        debug_id: Option<&str>,
        consistent_read: bool,
    ) -> StorageResult<()> {
        Self::configure_transaction(trx, debug_id, consistent_read)?;
        if !self.uses_grv_cache(consistent_read) {
            return Ok(());
        }
        trx.set_option(options::TransactionOption::UseGrvCache)
            .map_err(|err| map_fdb_error("enable use_grv_cache", err))
    }

    pub(super) async fn prepare_uncached_read_version_fdb(
        &self,
        trx: &Transaction,
        consistent_read: bool,
    ) -> Result<(), FdbError> {
        if self.uses_grv_cache(consistent_read) {
            return Ok(());
        }

        let started_at = Instant::now();
        trx.get_read_version().await?;
        metrics_facade::histogram!(FOUNDATIONDB_GET_READ_VERSION_LATENCY_MS_METRIC)
            .record(started_at.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }

    pub(super) async fn retry_transaction_after_fdb_error(
        &self,
        trx: Transaction,
        operation: &'static str,
        scope: &'static str,
        attempt: u32,
        err: FdbError,
        candidate_keys: &[Vec<u8>],
    ) -> StorageResult<Transaction> {
        record_fdb_operation(operation, "retry", 1);
        let error_code = err.code();
        let retryable = err.is_retryable();
        let on_error_started = Instant::now();
        let retry_result = trx.on_error(err).await;
        record_fdb_operation_latency(operation, "on_error", on_error_started.elapsed());
        match retry_result {
            Ok(mut new_trx) => {
                self.log_conflict_details(
                    &new_trx,
                    operation,
                    attempt,
                    retryable,
                    error_code,
                    candidate_keys,
                )
                .await;
                new_trx.reset();
                Ok(new_trx)
            }
            Err(retry_err) => Err(map_fdb_error(scope, retry_err)),
        }
    }

    fn configure_transaction(
        trx: &Transaction,
        debug_id: Option<&str>,
        consistent_read: bool,
    ) -> StorageResult<()> {
        if let Some(debug_id) = debug_id {
            trx.set_option(options::TransactionOption::DebugTransactionIdentifier(
                debug_id.to_string(),
            ))
            .map_err(|err| map_fdb_error("set debug transaction identifier", err))?;
        }

        if !consistent_read {
            trx.set_option(options::TransactionOption::CausalReadRisky)
                .map_err(|err| map_fdb_error("set causal read option", err))?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|err| map_fdb_error("disable read-your-writes", err))?;
            trx.set_option(options::TransactionOption::SnapshotRywDisable)
                .map_err(|err| map_fdb_error("disable snapshot read-your-writes", err))?;
        }

        Ok(())
    }

    pub(super) fn prefix_bytes(prefix: Option<&Vec<u8>>, key: &[u8]) -> Vec<u8> {
        keyspace::prefix_bytes(prefix, key)
    }

    async fn commit_transaction(
        path: &'static str,
        trx: Transaction,
    ) -> Result<(), TransactionCommitError> {
        let started = Instant::now();
        let result = trx.commit().await;
        record_fdb_operation_latency(path, "commit", started.elapsed());
        result.map(|_| ())
    }

    pub(super) fn strip_prefix<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        keyspace::strip_prefix(key, self.config.subspace_prefix.as_ref())
    }

    pub(super) fn prefix_slice(&self, key: &[u8]) -> Vec<u8> {
        Self::prefix_bytes(self.config.subspace_prefix.as_ref(), key)
    }

    fn collect_transact_write_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[TransactWriteOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            match operation {
                TransactWriteOperation::Put { key, .. }
                | TransactWriteOperation::Delete { key, .. }
                | TransactWriteOperation::Check { key, .. }
                | TransactWriteOperation::CheckValue { key, .. }
                | TransactWriteOperation::Update { key, .. } => {
                    keys.push(Self::prefix_bytes(prefix, key));
                }
                TransactWriteOperation::PutTemplate { template, .. } => {
                    if let Some(mut versioned) = template.foundationdb_key() {
                        if let Some(prefix_bytes) = prefix {
                            let mut composed = prefix_bytes.clone();
                            composed.extend_from_slice(&versioned);
                            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                            versioned = composed;
                        }
                        keys.push(versioned);
                    } else {
                        let key = template.rocks_key();
                        keys.push(Self::prefix_bytes(prefix, &key));
                    }
                }
            }
        }
        keys
    }

    fn collect_unchecked_write_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[DirectWriteOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            match operation {
                DirectWriteOperation::Put { key, .. }
                | DirectWriteOperation::Delete { key }
                | DirectWriteOperation::CheckValue { key, .. } => {
                    keys.push(Self::prefix_bytes(prefix, key));
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    keys.push(Self::prefix_bytes(prefix, start));
                    keys.push(Self::prefix_bytes(prefix, exclusive_end));
                }
                DirectWriteOperation::PutTemplate { template, .. } => {
                    if let Some(mut versioned) = template.foundationdb_key() {
                        if let Some(prefix_bytes) = prefix {
                            let mut composed = prefix_bytes.clone();
                            composed.extend_from_slice(&versioned);
                            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                            versioned = composed;
                        }
                        keys.push(versioned);
                    } else {
                        let key = template.rocks_key();
                        keys.push(Self::prefix_bytes(prefix, &key));
                    }
                }
            }
        }
        keys
    }

    fn collect_transact_write_table_keys(
        prefix: Option<&Vec<u8>>,
        operations: &[TransactWriteTableOperation],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for operation in operations {
            if let Ok(key) = table_operation_primary_key(operation) {
                keys.push(Self::prefix_bytes(prefix, &key));
            }
        }
        keys
    }

    async fn read_key_prefix(
        trx: &Transaction,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbError> {
        let start = prefix.to_vec();
        let end = increment_bytes(prefix.to_vec());
        let mut option = RangeOption::from((start, end));
        option.limit = Some(limit);
        option.mode = options::StreamingMode::WantAll;

        let mut iteration = 1;
        let mut out = Vec::new();

        loop {
            let values = trx.get_range(&option, iteration, false).await?;
            for kv in &values {
                out.push((kv.key().to_vec(), kv.value().to_vec()));
                if out.len() >= limit {
                    return Ok(out);
                }
            }

            if let Some(next) = option.next_range(&values) {
                option = next;
                iteration += 1;
            } else {
                break;
            }
        }

        Ok(out)
    }

    async fn load_partition_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let Some(config_bytes) = trx
            .get(&config_key, false)
            .await
            .map_err(|err| map_fdb_error("read partition family config", err))?
        else {
            return Ok(None);
        };
        let config = parse_partition_family_config(&config_bytes)?;

        let partition_prefix = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let partition_entries = Self::read_key_prefix(trx, &partition_prefix, 1024)
            .await
            .map_err(|err| map_fdb_error("read partition family partitions", err))?;
        let mut partitions = Vec::with_capacity(partition_entries.len());
        for (_key, value) in partition_entries {
            partitions.push(parse_partition_info(&value)?);
        }
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });

        Ok(Some(ResolvedPartitionFamily { config, partitions }))
    }

    async fn load_partition_family_state_tx_retryable(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> Result<Option<ResolvedPartitionFamily>, FdbTransactionAttemptError> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let Some(config_bytes) = trx
            .get(&config_key, false)
            .await
            .map_err(|err| FdbTransactionAttemptError::fdb("read partition family config", err))?
        else {
            return Ok(None);
        };
        let config = parse_partition_family_config(&config_bytes)?;

        let partition_prefix = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let partition_entries = Self::read_key_prefix(trx, &partition_prefix, 1024)
            .await
            .map_err(|err| {
                FdbTransactionAttemptError::fdb("read partition family partitions", err)
            })?;
        let mut partitions = Vec::with_capacity(partition_entries.len());
        for (_key, value) in partition_entries {
            partitions.push(parse_partition_info(&value)?);
        }
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });

        Ok(Some(ResolvedPartitionFamily { config, partitions }))
    }

    fn save_partition_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        family: &ResolvedPartitionFamily,
    ) -> StorageResult<()> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        trx.set(
            &config_key,
            &partition_family_config_bytes(family_component, &family.config)?,
        );
        let epoch_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_epoch_key(family_kind, family_component),
        );
        trx.set(&epoch_key, &partition_family_epoch_bytes(&family.config));
        for partition in &family.partitions {
            let partition_key = Self::prefix_bytes(
                prefix,
                &crate::partition_family::partition_info_key(
                    family_kind,
                    family_component,
                    partition.partition_id,
                ),
            );
            trx.set(&partition_key, &partition_info_bytes(partition)?);
        }
        Ok(())
    }

    async fn ensure_ordered_log_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
    ) -> StorageResult<ResolvedPartitionFamily> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(existing) = Self::load_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
        )
        .await?
        {
            return Ok(existing);
        }

        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::OrderedLog,
                DEFAULT_ORDERED_LOG_PARTITION_COUNT,
            ),
            partitions: initial_partition_infos(DEFAULT_ORDERED_LOG_PARTITION_COUNT),
        };
        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
            &family,
        )?;
        Ok(family)
    }

    async fn ensure_ordered_log_family_state_tx_retryable(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
    ) -> Result<ResolvedPartitionFamily, FdbTransactionAttemptError> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(existing) = Self::load_partition_family_state_tx_retryable(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
        )
        .await?
        {
            return Ok(existing);
        }

        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::OrderedLog,
                DEFAULT_ORDERED_LOG_PARTITION_COUNT,
            ),
            partitions: initial_partition_infos(DEFAULT_ORDERED_LOG_PARTITION_COUNT),
        };
        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
            &family,
        )?;
        Ok(family)
    }

    async fn ensure_ordered_log_family_state_cached_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
        cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<ResolvedPartitionFamily> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(family) = cache.get(&family_component) {
            return Ok(family.clone());
        }

        let family = Self::ensure_ordered_log_family_state_tx(trx, prefix, stream_name).await?;
        cache.insert(family_component, family.clone());
        Ok(family)
    }

    async fn split_partitioned_ordered_log_family_tx(
        &self,
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> Result<bool, FdbTransactionAttemptError> {
        let Some(mut family) = Self::load_partition_family_state_tx_retryable(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            family_component,
        )
        .await?
        else {
            return Ok(false);
        };

        let Some(index) = family
            .partitions
            .iter()
            .position(|partition| partition.partition_id == partition_id)
        else {
            return Ok(false);
        };
        if !family.partitions[index].is_writable() {
            return Ok(false);
        }

        let parent = family.partitions[index].clone();
        let left_partition_id = next_partition_id(&family.partitions);
        let right_partition_id = left_partition_id.saturating_add(1);
        let left_slot = next_placement_slot(&family.partitions);
        let right_slot = left_slot.saturating_add(1);
        let Some((mut left_child, mut right_child)) = split_partition_children(
            &parent,
            left_partition_id,
            right_partition_id,
            left_slot,
            right_slot,
        ) else {
            return Ok(false);
        };

        let mut parent = parent;
        parent.mark_write_closed().map_err(|error| {
            StorageError::internal(&format!(
                "ordered-log split requires open parent partition, found {:?} -> {:?}",
                error.from(),
                error.to()
            ))
        })?;
        parent.sealed_after_id = None;
        left_child.opened_after_id = None;
        right_child.opened_after_id = None;

        family.partitions[index] = parent;
        family.partitions.push(left_child);
        family.partitions.push(right_child);
        family.sort_by_hash_range();
        family.config.note_topology_change(now_ms);
        family.refresh_partition_count();
        family.config.min_open_partitions = family.config.min_open_partitions.max(
            u16::try_from(
                family
                    .partitions
                    .iter()
                    .filter(|partition| partition.is_writable())
                    .count(),
            )
            .unwrap_or(u16::MAX),
        );

        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            family_component,
            &family,
        )?;

        let split_marker = OrderedLogSplitMarker {
            parent_partition_id: partition_id,
            left_child_partition_id: left_partition_id,
            right_child_partition_id: right_partition_id,
        };
        let marker_bytes = ordered_log_split_marker_bytes(&split_marker)?;
        let marker_template = KeyTemplate::placeholder(
            ordered_log_split_marker_prefix(family_component, partition_id),
            Vec::new(),
            PlaceholderBinding::new(PlaceholderId::Shared(partition_id), vec![0; 12], [0, 0]),
        );
        let mut versioned_key = marker_template.foundationdb_key().ok_or_else(|| {
            StorageError::internal("ordered-log split marker template must be versionstamped")
        })?;
        if let Some(prefix_bytes) = prefix {
            let mut composed = prefix_bytes.clone();
            composed.extend_from_slice(&versioned_key);
            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
            versioned_key = composed;
        }
        trx.atomic_op(
            &versioned_key,
            &marker_bytes,
            options::MutationType::SetVersionstampedKey,
        );

        Ok(true)
    }

    async fn rewrite_partitioned_pointer_template(
        &self,
        trx: &Transaction,
        subspace_prefix: Option<&Vec<u8>>,
        template: &crate::key_template::KeyTemplate,
        value: &[u8],
        ordered_log_family_cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<(
        crate::key_template::KeyTemplate,
        Option<PendingOrderedLogWrite>,
    )> {
        let Some(template_prefix) = template.prefix() else {
            return Ok((template.clone(), None));
        };
        if template_prefix.is_empty() {
            return Ok((template.clone(), None));
        }
        let family_name = if template_prefix == compact::system_stream_prefix().start {
            storage_types::StreamName::system_table_stream()
        } else {
            storage_types::StreamName::from(
                &template_prefix[..template_prefix.len().saturating_sub(1)],
            )
        };
        if !supports_pointer_stream_partitioning(&family_name) {
            return Ok((template.clone(), None));
        }

        let stored_item = match decode_stream_item(value) {
            Ok(item) => item,
            Err(_) => return Ok((template.clone(), None)),
        };
        if stored_item.data_type != stream_provider::StreamDataType::StreamPointer {
            return Ok((template.clone(), None));
        }
        let pointer: StoredStreamPointer =
            match storage_types::storage_serde::from_bytes(&stored_item.data) {
                Ok(pointer) => pointer,
                Err(_) => return Ok((template.clone(), None)),
            };
        let family = Self::ensure_ordered_log_family_state_cached_tx(
            trx,
            subspace_prefix,
            &family_name,
            ordered_log_family_cache,
        )
        .await?;
        let routing_hash = ordered_log_hash(pointer.stream_name().as_ref());
        let partition =
            find_partition_for_hash(&family.partitions, routing_hash).ok_or_else(|| {
                StorageError::internal("pointer stream family has no writable partition")
            })?;

        Ok((
            template.with_replaced_prefix(ordered_log_partition_prefix_with_slot(
                &family_name,
                partition.placement_slot,
                partition.partition_id,
            )),
            Some(PendingOrderedLogWrite {
                family_component: ordered_log_family_component(&family_name),
                partition_id: partition.partition_id,
                bytes: u64::try_from(value.len()).unwrap_or(u64::MAX),
                routing_key_bucket_bitmap: routing_key_bucket_bit(routing_hash),
            }),
        ))
    }

    fn record_ordered_log_writes(&self, writes: &[PendingOrderedLogWrite], conflict_count: u64) {
        let mut aggregated: HashMap<(String, u16), PartitionLoadSample> = HashMap::new();
        for write in writes {
            let entry = aggregated
                .entry((write.family_component.clone(), write.partition_id))
                .or_default();
            merge_partition_load(
                entry,
                &PartitionLoadSample {
                    writes: 1,
                    bytes: write.bytes,
                    conflicts: 0,
                    routing_key_bucket_bitmap: write.routing_key_bucket_bitmap,
                    queue_scan_work: 0,
                    queue_claim_conflicts: 0,
                    oldest_visible_age_ms: 0,
                    visible_count: 0,
                    invisible_count: 0,
                },
            );
        }

        for ((family_component, partition_id), mut sample) in aggregated {
            sample.conflicts = sample.conflicts.saturating_add(conflict_count);
            self.runtime_partition_load_tracker
                .record(RuntimePartitionLoadSample {
                    family_kind: PartitionFamilyKind::OrderedLog,
                    family_component,
                    partition_id,
                    sample,
                });
        }
    }

    async fn apply_mutations(
        &self,
        prefix: Option<&Vec<u8>>,
        trx: &Transaction,
        mutations: Vec<KvMutation>,
        ordered_log_writes: &mut Vec<PendingOrderedLogWrite>,
        ordered_log_family_cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<()> {
        for mutation in &mutations {
            match mutation {
                KvMutation::Put { key, value } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.set(&prefixed, value);
                }
                KvMutation::Delete { key } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.clear(&prefixed);
                }
                KvMutation::PutTemplate { .. } => {}
            }
        }

        for mutation in &mutations {
            let KvMutation::PutTemplate { template, value } = mutation else {
                continue;
            };
            let (template, ordered_log_write) = self
                .rewrite_partitioned_pointer_template(
                    trx,
                    prefix,
                    template,
                    value,
                    ordered_log_family_cache,
                )
                .await?;
            if let Some(ordered_log_write) = ordered_log_write {
                ordered_log_writes.push(ordered_log_write);
            }
            if let Some(mut versioned) = template.foundationdb_key() {
                if let Some(prefix_bytes) = prefix {
                    let mut composed = prefix_bytes.clone();
                    composed.extend_from_slice(&versioned);
                    adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                    versioned = composed;
                }

                trx.atomic_op(
                    &versioned,
                    value,
                    options::MutationType::SetVersionstampedKey,
                );
            } else {
                let key = template.rocks_key();
                let prefixed = Self::prefix_bytes(prefix, &key);
                trx.set(&prefixed, value);
            }
        }

        Ok(())
    }

    async fn execute_transact_write_tx(
        &self,
        trx: &Transaction,
        operations: &[TransactWriteOperation],
        prefix: Option<&Vec<u8>>,
    ) -> StorageResult<(
        Vec<OldNewItems>,
        HashMap<PlaceholderId, PlaceholderBinding>,
        Vec<PendingOrderedLogWrite>,
    )> {
        let mut results = Vec::with_capacity(operations.len());
        let mut bindings: HashMap<PlaceholderId, PlaceholderBinding> = HashMap::new();
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();

        let current_read_keys = operations
            .iter()
            .map(|operation| {
                match operation {
                    TransactWriteOperation::Put { key, condition, .. } => {
                        if condition.is_some() {
                            Some(Self::prefix_bytes(prefix, key))
                        } else {
                            // Shortcut: unconditional Put writes do not need a
                            // current-item read for correctness.
                            None
                        }
                    }
                    TransactWriteOperation::Delete { key, .. }
                    | TransactWriteOperation::Check { key, .. }
                    | TransactWriteOperation::CheckValue { key, .. }
                    | TransactWriteOperation::Update { key, .. } => {
                        Some(Self::prefix_bytes(prefix, key))
                    }
                    TransactWriteOperation::PutTemplate { .. } => None,
                }
            })
            .collect::<Vec<_>>();

        let current_values = try_join_all(current_read_keys.into_iter().map(|key| async move {
            let Some(key) = key else {
                return Ok(None);
            };
            trx.get(&key, false)
                .await
                .map_err(|err| map_fdb_error("read key", err))
                .map(|value| value.map(|value| value.to_vec()))
        }))
        .await?;

        for (index, (operation, current)) in operations.iter().zip(current_values).enumerate() {
            // FDB retries may re-run this loop with the same operation slice, so
            // keep owned planning local by cloning here.
            let (old_new, mutations) =
                plan_transact_operation(operation.clone(), current.as_deref(), index)?;

            for mutation in &mutations {
                if let KvMutation::PutTemplate { template, .. } = mutation
                    && let Some(binding) = template.placeholder_binding().cloned()
                {
                    bindings.entry(binding.id()).or_insert(binding);
                }
            }

            self.apply_mutations(
                prefix,
                trx,
                mutations,
                &mut ordered_log_writes,
                &mut ordered_log_family_cache,
            )
            .await?;
            results.push(old_new);
        }

        Ok((results, bindings, ordered_log_writes))
    }

    async fn execute_transact_write_table_tx(
        &self,
        trx: &Transaction,
        operations: &[TransactWriteTableOperation],
        stream_ids: &[Option<StreamItemId>],
        prefix: Option<&Vec<u8>>,
        immediate_gsi_consistency: bool,
    ) -> Result<FdbTableWriteExecution, FdbTableWriteExecutionError> {
        preflight_table_write_operations(operations)?;
        let read_started = Instant::now();
        let current_reads = operations
            .iter()
            .map(|operation| -> StorageResult<Option<Vec<u8>>> {
                let key_bytes = table_operation_primary_key(operation)?;
                Ok(Some(Self::prefix_bytes(prefix, &key_bytes)))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let current_read_count = current_reads.iter().filter(|key| key.is_some()).count();
        let current_values = try_join_all(current_reads.into_iter().map(|key| async move {
            let Some(key) = key else {
                return Ok(None);
            };
            trx.get(&key, false)
                .await
                .map_err(|err| FdbTableWriteExecutionError::fdb("read table item", err))
                .map(|value| value.map(|value| value.to_vec()))
        }))
        .await?;
        #[cfg(test)]
        provider_perf::record(
            "foundationdb",
            "table_write_current_read",
            read_started.elapsed(),
        );

        let plan_started = Instant::now();
        let plan = plan_table_write_preflighted(
            operations,
            current_values,
            stream_ids,
            immediate_gsi_consistency,
        )?;
        let plan_elapsed = plan_started.elapsed();
        #[cfg(test)]
        provider_perf::record("foundationdb", "table_write_plan", plan_elapsed);
        let applied_mutation_count = plan.mutations.len();
        #[cfg(test)]
        {
            provider_perf::record_amount(
                "foundationdb",
                "table_write_mutations",
                plan.stats.mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_applied_mutations",
                applied_mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_mutations",
                plan.stats.gsi_mutation_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_key_overlap",
                plan.stats.gsi_key_overlap_count as u64,
            );
            provider_perf::record_amount(
                "foundationdb",
                "table_write_gsi_collapsed",
                plan.stats.collapsed_gsi_mutation_count as u64,
            );
        }

        let apply_started = Instant::now();
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
        self.apply_mutations(
            prefix,
            trx,
            plan.mutations,
            &mut ordered_log_writes,
            &mut ordered_log_family_cache,
        )
        .await?;
        let apply_elapsed = apply_started.elapsed();
        #[cfg(test)]
        provider_perf::record("foundationdb", "table_write_apply", apply_elapsed);
        tracing::debug!(
            operation_count = operations.len(),
            current_read_count,
            current_read_ms = read_started.elapsed().as_secs_f64() * 1000.0,
            plan_ms = plan_elapsed.as_secs_f64() * 1000.0,
            apply_ms = apply_elapsed.as_secs_f64() * 1000.0,
            mutation_count = plan.stats.mutation_count,
            applied_mutation_count,
            gsi_mutation_count = plan.stats.gsi_mutation_count,
            gsi_distinct_key_count = plan.stats.gsi_distinct_key_count,
            gsi_key_overlap_count = plan.stats.gsi_key_overlap_count,
            collapsed_gsi_mutation_count = plan.stats.collapsed_gsi_mutation_count,
            ordered_log_write_count = ordered_log_writes.len(),
            immediate_gsi_consistency,
            "foundationdb transact_write_table phase timing"
        );

        Ok(FdbTableWriteExecution {
            results: plan.results,
            ordered_log_writes,
        })
    }

    async fn execute_transact_write_unchecked_tx(
        &self,
        trx: &Transaction,
        operations: &[DirectWriteOperation],
        prefix: Option<&Vec<u8>>,
    ) -> Result<Vec<PendingOrderedLogWrite>, FdbTableWriteExecutionError> {
        let mut ordered_log_writes = Vec::new();
        let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
        for operation in operations {
            match operation {
                DirectWriteOperation::Put { key, value } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.set(&prefixed, value);
                }
                DirectWriteOperation::PutTemplate { template, value } => {
                    let (template, ordered_log_write) = self
                        .rewrite_partitioned_pointer_template(
                            trx,
                            prefix,
                            template,
                            value,
                            &mut ordered_log_family_cache,
                        )
                        .await?;
                    if let Some(ordered_log_write) = ordered_log_write {
                        ordered_log_writes.push(ordered_log_write);
                    }
                    if let Some(mut versioned) = template.foundationdb_key() {
                        if let Some(prefix_bytes) = prefix {
                            let mut composed = prefix_bytes.clone();
                            composed.extend_from_slice(&versioned);
                            adjust_versionstamp_offset(&mut composed, prefix_bytes.len());
                            versioned = composed;
                        }

                        trx.atomic_op(
                            &versioned,
                            value,
                            options::MutationType::SetVersionstampedKey,
                        );
                    } else {
                        let key = template.rocks_key();
                        let prefixed = Self::prefix_bytes(prefix, &key);
                        trx.set(&prefixed, value);
                    }
                }
                DirectWriteOperation::Delete { key } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    trx.clear(&prefixed);
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    let prefixed_start = Self::prefix_bytes(prefix, start);
                    let prefixed_end = Self::prefix_bytes(prefix, exclusive_end);
                    trx.clear_range(&prefixed_start, &prefixed_end);
                }
                DirectWriteOperation::CheckValue {
                    key,
                    expected_value,
                } => {
                    let prefixed = Self::prefix_bytes(prefix, key);
                    let current = trx
                        .get(&prefixed, false)
                        .await
                        .map_err(|err| {
                            FdbTableWriteExecutionError::fdb("read key for exact value check", err)
                        })?
                        .map(|value| value.to_vec());
                    if current != *expected_value {
                        return Err(FdbTableWriteExecutionError::Storage(
                            StorageEnum::ConditionalCheckFailed.into(),
                        ));
                    }
                }
            }
        }

        Ok(ordered_log_writes)
    }

    async fn build_stream_ids(
        &self,
        operations: &[TransactWriteTableOperation],
    ) -> Vec<Option<StreamItemId>> {
        let mut ids = Vec::with_capacity(operations.len());
        for operation in operations {
            if operation_requires_stream_entries(operation, self.config.immediate_gsi_consistency) {
                ids.push(Some(storage_types::StreamItemId::random()));
            } else {
                ids.push(None);
            }
        }
        ids
    }
}

#[async_trait::async_trait]
impl QueueKvStore for FoundationDbKvStore {
    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        self.write_partitioned_queue_messages(vec![message]).await
    }

    async fn write_partitioned_queue_messages(
        &self,
        messages: Vec<PartitionedQueueMessageWrite>,
    ) -> StorageResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("queue_send");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            let mut set_count = 0u64;
            let mut ready_hints = HashMap::<&[u8], &[u8]>::new();
            let mut wake_writes = HashMap::<&[u8], &[u8]>::new();
            for message in &messages {
                let mut set_value = |key: &[u8], value: &[u8]| -> StorageResult<()> {
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                    write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                    trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                        .map_err(|err| map_fdb_error("disable queue send write conflict", err))?;
                    trx.set(&prefixed_key, value);
                    set_count = set_count.saturating_add(1);
                    Ok(())
                };
                set_value(&message.state_key, &message.state_bytes)?;
                set_value(&message.ready_key, &[])?;
                if let Some(record_bytes) = &message.payload_record_bytes {
                    set_value(&message.payload_key, record_bytes)?;
                    for (index, chunk) in message
                        .payload_bytes
                        .chunks(QUEUE_PAYLOAD_CHUNK_BYTES)
                        .enumerate()
                    {
                        let chunk_key = queue_payload_chunk_key(
                            &message.payload_key,
                            u16::try_from(index).unwrap_or(u16::MAX),
                        );
                        set_value(&chunk_key, chunk)?;
                    }
                } else {
                    set_value(&message.payload_key, &message.payload_bytes)?;
                }
                ready_hints
                    .entry(&message.ready_hint_key)
                    .and_modify(|existing| {
                        if queue_ready_hint_is_earlier(&message.ready_hint_bytes, existing) {
                            *existing = &message.ready_hint_bytes;
                        }
                    })
                    .or_insert(&message.ready_hint_bytes);
                wake_writes
                    .entry(&message.wake_key)
                    .or_insert(&message.wake_bytes);
            }
            for (key, value) in ready_hints.into_iter().chain(wake_writes) {
                write_bytes = write_bytes.saturating_add(value.len() as u64);
                let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                    .map_err(|err| map_fdb_error("disable queue send write conflict", err))?;
                trx.set(&prefixed_key, value);
                set_count = set_count.saturating_add(1);
            }
            record_fdb_operation("queue_send", "set", set_count);
            record_fdb_write_shape("queue_send", set_count, 0);
            record_fdb_operation_bytes("queue_send", "write", write_bytes);
            record_fdb_operation_bytes("queue_send", "write_key", write_key_bytes);
            record_fdb_operation("queue_send", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("queue_send", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = messages
                                .iter()
                                .flat_map(|message| {
                                    [
                                        &message.state_key,
                                        &message.payload_key,
                                        &message.ready_key,
                                        &message.ready_hint_key,
                                        &message.wake_key,
                                    ]
                                })
                                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                                .collect::<Vec<_>>();
                            self.log_conflict_details(
                                &new_trx,
                                "write_partitioned_queue_messages",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("queue send commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    async fn prewarm_partitioned_queue(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        for chunk in partitions.chunks(64) {
            let trx = self.create_transaction()?;
            Self::configure_transaction(&trx, Some("queue.prewarm_partitioned_queue"), true)?;
            record_fdb_transaction_start("queue_prewarm");
            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            for partition in chunk {
                let marker_value = format!(
                    "{}:{}:{}",
                    queue_url, partition.placement_slot, partition.partition_id
                )
                .into_bytes();
                write_bytes = write_bytes.saturating_add(marker_value.len() as u64);
                let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &partition.marker_key);
                write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                trx.set_option(options::TransactionOption::NextWriteNoWriteConflictRange)
                    .map_err(|err| map_fdb_error("disable queue prewarm write conflict", err))?;
                trx.set(&prefixed_key, &marker_value);
            }
            record_fdb_operation("queue_prewarm", "set", chunk.len() as u64);
            record_fdb_write_shape("queue_prewarm", chunk.len() as u64, 0);
            record_fdb_operation_bytes("queue_prewarm", "write", write_bytes);
            record_fdb_operation_bytes("queue_prewarm", "write_key", write_key_bytes);
            record_fdb_operation("queue_prewarm", "commit", 1);
            trx.commit()
                .await
                .map_err(|err| map_fdb_error("queue prewarm commit", *err))?;
        }

        Ok(())
    }

    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        let mut batch = QueueClaimBatch::default();
        if ranges.is_empty() || max_claims == 0 {
            return Ok(batch);
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("queue_claim");

        'retry: loop {
            attempt += 1;
            batch = QueueClaimBatch::default();
            Self::configure_transaction(&trx, None, true)?;
            let mut read_bytes = 0u64;
            let mut read_key_bytes = 0u64;
            let mut write_bytes = 0u64;
            let mut write_key_bytes = 0u64;
            let mut ordinary_gets = 0u64;
            let mut snapshot_gets = 0u64;
            let mut sets = 0u64;
            let mut clears = 0u64;
            let mut pending_claims = Vec::new();

            for range in &ranges {
                if batch.messages.len() >= max_claims {
                    break;
                }

                let start = Self::prefix_bytes(prefix.as_ref(), &range.ready_start);
                let end = Self::prefix_bytes(prefix.as_ref(), &range.ready_end);
                read_key_bytes =
                    read_key_bytes.saturating_add(start.len().saturating_add(end.len()) as u64);
                let scan_limit =
                    usize::try_from(range.scan_limit.max(range.limit)).unwrap_or(usize::MAX);
                let mut option = RangeOption::from((start, end));
                option.limit = Some(scan_limit);
                option.mode = options::StreamingMode::WantAll;
                let ready_entries = match trx.get_range(&option, 1, true).await {
                    Ok(ready_entries) => ready_entries,
                    Err(err) => {
                        let candidate_keys = ranges
                            .iter()
                            .flat_map(|range| {
                                [
                                    Self::prefix_bytes(prefix.as_ref(), &range.ready_start),
                                    Self::prefix_bytes(prefix.as_ref(), &range.ready_end),
                                ]
                            })
                            .collect::<Vec<_>>();
                        trx = self
                            .retry_transaction_after_fdb_error(
                                trx,
                                "queue_claim",
                                "read queue claim ready range",
                                attempt,
                                err,
                                &candidate_keys,
                            )
                            .await?;
                        continue 'retry;
                    }
                };
                record_fdb_range_read("queue_claim", true, 1);
                record_fdb_operation("queue_claim", "range_entry", ready_entries.len() as u64);
                let ready_entries_len = ready_entries.len();
                if ready_entries.is_empty() {
                    continue;
                }
                read_bytes = read_bytes.saturating_add(
                    ready_entries
                        .iter()
                        .map(|entry| entry.key().len().saturating_add(entry.value().len()) as u64)
                        .sum::<u64>(),
                );
                batch.ready_entries_seen =
                    batch.ready_entries_seen.saturating_add(ready_entries.len());
                let mut ready_entries = ready_entries.into_iter().collect::<Vec<_>>();
                rotate_fdb_claim_candidates(&mut ready_entries, range.candidate_seed);

                let range_claim_limit =
                    usize::try_from(range.claim_limit.max(1)).unwrap_or(usize::MAX);
                let mut range_claims = 0usize;
                let claim_budget = max_claims
                    .saturating_sub(batch.messages.len())
                    .min(range_claim_limit);
                let mut candidates = Vec::with_capacity(claim_budget.min(ready_entries.len()));
                for ready_entry in ready_entries {
                    if candidates.len() >= claim_budget {
                        break;
                    }
                    candidates.push(ready_entry);
                }
                let candidate_ready_keys = candidates
                    .iter()
                    .map(|entry| entry.key().to_vec())
                    .collect::<Vec<_>>();
                let ready_reads =
                    match read_fdb_keys_sequential(&trx, &candidate_ready_keys, false).await {
                        Ok(ready_reads) => ready_reads,
                        Err(err) => {
                            trx = self
                                .retry_transaction_after_fdb_error(
                                    trx,
                                    "queue_claim",
                                    "read queue claim ready keys",
                                    attempt,
                                    err,
                                    &candidate_ready_keys,
                                )
                                .await?;
                            continue 'retry;
                        }
                    };
                ordinary_gets = ordinary_gets
                    .saturating_add(u64::try_from(ready_reads.len()).unwrap_or(u64::MAX));

                let mut claim_candidates = Vec::with_capacity(candidates.len());
                for (ready_entry, ready_value) in candidates.into_iter().zip(ready_reads) {
                    if batch.messages.len() >= max_claims || range_claims >= range_claim_limit {
                        break;
                    }
                    read_bytes = read_bytes.saturating_add(
                        ready_entry
                            .key()
                            .len()
                            .saturating_add(ready_value.as_ref().map_or(0, |value| value.len()))
                            as u64,
                    );
                    read_key_bytes = read_key_bytes.saturating_add(ready_entry.key().len() as u64);
                    let Some(ready_value) = ready_value else {
                        continue;
                    };

                    let ready_key = self.strip_prefix(ready_entry.key()).to_vec();
                    let ready_visibility_key =
                        crate::queue_provider::partitioned_ready_visibility_key(&ready_key)
                            .map_err(|error| StorageError::internal(&error.to_string()))?;
                    let message_id = ready_visibility_key
                        .get_message_id()
                        .map_err(|error| StorageError::internal(&error.to_string()))?;
                    let message_id_hex = message_id.to_string();
                    let state_key = crate::partition_family::queue_state_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &message_id_hex,
                    );
                    let prefixed_state_key = Self::prefix_bytes(prefix.as_ref(), &state_key);
                    let payload_key = crate::partition_family::queue_payload_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &message_id_hex,
                    );

                    claim_candidates.push((
                        ready_entry.key().to_vec(),
                        ready_value.to_vec(),
                        message_id,
                        message_id_hex,
                        state_key,
                        prefixed_state_key,
                        payload_key,
                    ));
                }

                let prefixed_state_keys = claim_candidates
                    .iter()
                    .map(|(_, _, _, _, _, prefixed_state_key, _)| prefixed_state_key.clone())
                    .collect::<Vec<_>>();
                let state_reads =
                    match read_fdb_keys_sequential(&trx, &prefixed_state_keys, true).await {
                        Ok(state_reads) => state_reads,
                        Err(err) => {
                            trx = self
                                .retry_transaction_after_fdb_error(
                                    trx,
                                    "queue_claim",
                                    "read queue claim states",
                                    attempt,
                                    err,
                                    &prefixed_state_keys,
                                )
                                .await?;
                            continue 'retry;
                        }
                    };
                snapshot_gets = snapshot_gets
                    .saturating_add(u64::try_from(state_reads.len()).unwrap_or(u64::MAX));

                for (ready_candidate, state_bytes) in claim_candidates.into_iter().zip(state_reads)
                {
                    let (
                        ready_key,
                        ready_value,
                        message_id,
                        message_id_hex,
                        state_key,
                        prefixed_state_key,
                        payload_key,
                    ) = ready_candidate;
                    if batch.messages.len() >= max_claims || range_claims >= range_claim_limit {
                        break;
                    }
                    read_bytes = read_bytes.saturating_add(
                        prefixed_state_key
                            .len()
                            .saturating_add(state_bytes.as_ref().map_or(0, |value| value.len()))
                            as u64,
                    );
                    read_key_bytes = read_key_bytes.saturating_add(prefixed_state_key.len() as u64);
                    let Some(state_bytes) = state_bytes else {
                        trx.clear(&ready_key);
                        clears = clears.saturating_add(1);
                        continue;
                    };

                    let mut state: crate::queue_provider::PartitionedQueueState =
                        storage_types::storage_serde::from_bytes(&state_bytes).map_err(
                            |error| {
                                StorageError::internal(&format!(
                                    "deserialize partitioned queue state key={} error={:?}",
                                    String::from_utf8_lossy(&state_key),
                                    error.as_ref()
                                ))
                            },
                        )?;
                    if state.visibility_timestamp > now {
                        continue;
                    }
                    let expected_ready_value = state
                        .claim_nonce
                        .as_ref()
                        .map_or_else(Vec::new, |nonce| nonce.as_bytes().to_vec());
                    if ready_value != expected_ready_value {
                        continue;
                    }

                    state.delivery_attempt = state.delivery_attempt.saturating_add(1);
                    state.visibility_timestamp = now + visibility_timeout;
                    state.claim_nonce = Some(Uuid::now_v7().to_string());
                    let new_visibility_key = crate::newtypes::MessageVisibilityKey(
                        crate::queue_provider::visibility_key(
                            state.visibility_timestamp,
                            &message_id,
                        ),
                    );
                    let new_ready_key = crate::partition_family::queue_ready_key_with_slot(
                        range.queue_id,
                        range.placement_slot,
                        range.partition_id,
                        &new_visibility_key,
                    );
                    let prefixed_new_ready_key =
                        Self::prefix_bytes(prefix.as_ref(), &new_ready_key);
                    let state_value = storage_types::storage_serde::to_bytes(&state)?;

                    trx.clear(&ready_key);
                    write_key_bytes = write_key_bytes.saturating_add(ready_key.len() as u64);
                    trx.set(
                        &prefixed_new_ready_key,
                        state.claim_nonce.as_deref().unwrap_or_default().as_bytes(),
                    );
                    trx.set(&prefixed_state_key, &state_value);
                    write_key_bytes = write_key_bytes.saturating_add(
                        prefixed_new_ready_key
                            .len()
                            .saturating_add(prefixed_state_key.len())
                            as u64,
                    );
                    clears = clears.saturating_add(1);
                    sets = sets.saturating_add(2);
                    write_bytes = write_bytes.saturating_add(state_value.len() as u64);

                    range_claims = range_claims.saturating_add(1);
                    pending_claims.push((
                        payload_key,
                        QueueClaimedMessage {
                            partition_id: range.partition_id,
                            message_id_hex,
                            body_bytes: Vec::new(),
                            visibility_timestamp: state.visibility_timestamp,
                            delivery_attempt: state.delivery_attempt,
                            claim_nonce: state.claim_nonce.clone().unwrap_or_default(),
                        },
                    ));
                    batch.messages.push(QueueClaimedMessage {
                        partition_id: range.partition_id,
                        message_id_hex: String::new(),
                        body_bytes: Vec::new(),
                        visibility_timestamp: state.visibility_timestamp,
                        delivery_attempt: state.delivery_attempt,
                        claim_nonce: state.claim_nonce.clone().unwrap_or_default(),
                    });
                }
                if range_claims > 0
                    && range_claims == ready_entries_len
                    && ready_entries_len < scan_limit
                {
                    let prefixed_hint_key =
                        Self::prefix_bytes(prefix.as_ref(), &range.ready_hint_key);
                    let hint_value =
                        crate::partition_family::queue_ready_hint_bytes(range.partition_id, now);
                    trx.set(&prefixed_hint_key, &hint_value);
                    sets = sets.saturating_add(1);
                    write_key_bytes =
                        write_key_bytes.saturating_add(prefixed_hint_key.len() as u64);
                    write_bytes = write_bytes.saturating_add(hint_value.len() as u64);
                }
            }

            record_fdb_point_read("queue_claim", false, ordinary_gets);
            record_fdb_point_read("queue_claim", true, snapshot_gets);
            record_fdb_operation("queue_claim", "set", sets);
            record_fdb_operation("queue_claim", "clear", clears);
            record_fdb_write_shape("queue_claim", 0, sets.saturating_add(clears));
            record_fdb_operation_bytes("queue_claim", "read", read_bytes);
            record_fdb_operation_bytes("queue_claim", "read_key", read_key_bytes);
            record_fdb_operation_bytes("queue_claim", "write", write_bytes);
            record_fdb_operation_bytes("queue_claim", "write_key", write_key_bytes);
            record_fdb_operation("queue_claim", "commit", 1);
            match trx.commit().await {
                Ok(_) => {
                    if pending_claims.is_empty() {
                        return Ok(batch);
                    }
                    let payload_trx = self.create_transaction()?;
                    record_fdb_transaction_start("queue_claim_payload");
                    self.configure_read_transaction(&payload_trx, None, false)?;
                    let payload_read_count =
                        u64::try_from(pending_claims.len()).unwrap_or(u64::MAX);
                    let prefixed_payload_keys = pending_claims
                        .iter()
                        .map(|(payload_key, _)| Self::prefix_bytes(prefix.as_ref(), payload_key))
                        .collect::<Vec<_>>();
                    let payload_reads =
                        read_fdb_keys_sequential(&payload_trx, &prefixed_payload_keys, true)
                            .await
                            .map_err(|err| map_fdb_error("read queue claim payloads", err))?;
                    let mut claimed = Vec::with_capacity(payload_reads.len());
                    let mut payload_read_bytes = 0u64;
                    let mut payload_read_key_bytes = 0u64;
                    for ((payload_key, mut message), payload_bytes) in
                        pending_claims.into_iter().zip(payload_reads)
                    {
                        payload_read_key_bytes =
                            payload_read_key_bytes.saturating_add(payload_key.len() as u64);
                        let Some(payload_bytes) = payload_bytes else {
                            continue;
                        };
                        payload_read_bytes = payload_read_bytes.saturating_add(
                            payload_key.len().saturating_add(payload_bytes.len()) as u64,
                        );
                        message.body_bytes = read_partitioned_queue_payload(
                            self,
                            &payload_key,
                            payload_bytes.to_vec(),
                        )
                        .await?;
                        claimed.push(message);
                    }
                    record_fdb_point_read("queue_claim_payload", true, payload_read_count);
                    record_fdb_operation_bytes("queue_claim_payload", "read", payload_read_bytes);
                    record_fdb_operation_bytes(
                        "queue_claim_payload",
                        "read_key",
                        payload_read_key_bytes,
                    );
                    batch.messages = claimed;
                    return Ok(batch);
                }
                Err(commit_err) => {
                    record_fdb_operation("queue_claim", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = ranges
                                .iter()
                                .flat_map(|range| {
                                    [
                                        Self::prefix_bytes(prefix.as_ref(), &range.ready_start),
                                        Self::prefix_bytes(prefix.as_ref(), &range.ready_end),
                                    ]
                                })
                                .collect::<Vec<_>>();
                            self.log_conflict_details(
                                &new_trx,
                                "claim_queue_messages_from_ranges",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("queue claim commit", retry_err));
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl PartitionFamilyKvStore for FoundationDbKvStore {
    fn supports_partition_families(&self) -> bool {
        true
    }

    async fn load_partition_family_state_raw(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        let prefix = self.config.subspace_prefix.clone();
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let partition_prefix = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            match Self::load_partition_family_state_tx_retryable(
                &trx,
                prefix.as_ref(),
                family_kind,
                family_component,
            )
            .await
            {
                Ok(family) => return Ok(family),
                Err(FdbTransactionAttemptError::Storage(storage_err)) => return Err(storage_err),
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone(), partition_prefix.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "load_partition_family_state_raw",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    async fn append_partitioned_ordered_log_item(
        &self,
        stream_name: &StreamName,
        routing_key: &[u8],
        value: &[u8],
        fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>> {
        let prefix = self.config.subspace_prefix.clone();
        let family_component = ordered_log_family_component(stream_name);
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(
                PartitionFamilyKind::OrderedLog,
                &family_component,
            ),
        );
        let value = value.to_vec();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let mut ordered_log_family_cache = OrderedLogFamilyCache::new();
            let family = match Self::ensure_ordered_log_family_state_tx_retryable(
                &trx,
                prefix.as_ref(),
                stream_name,
            )
            .await
            {
                Ok(family) => family,
                Err(FdbTransactionAttemptError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "append_partitioned_ordered_log_item",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            ordered_log_family_cache.insert(family_component.clone(), family.clone());
            let partition =
                find_partition_for_hash(&family.partitions, ordered_log_hash(routing_key))
                    .ok_or_else(|| {
                        StorageError::internal("ordered log family has no writable partition")
                    })?;
            let partition_prefix = ordered_log_partition_prefix_with_slot(
                stream_name,
                partition.placement_slot,
                partition.partition_id,
            );
            let binding = PlaceholderBinding::unique(fallback_item_id.as_bytes().to_vec());
            let template = crate::key_template::KeyTemplate::placeholder(
                partition_prefix.clone(),
                Vec::new(),
                binding.clone(),
            );
            let version_future = trx.get_versionstamp();

            self.apply_mutations(
                prefix.as_ref(),
                &trx,
                vec![KvMutation::PutTemplate {
                    template,
                    value: value.clone(),
                }],
                &mut Vec::new(),
                &mut ordered_log_family_cache,
            )
            .await?;

            match trx.commit().await {
                Ok(_) => {
                    self.runtime_partition_load_tracker
                        .record(RuntimePartitionLoadSample {
                            family_kind: PartitionFamilyKind::OrderedLog,
                            family_component: family_component.clone(),
                            partition_id: partition.partition_id,
                            sample: PartitionLoadSample {
                                writes: 1,
                                bytes: u64::try_from(value.len()).unwrap_or(u64::MAX),
                                conflicts: u64::from(attempt.saturating_sub(1)),
                                routing_key_bucket_bitmap: routing_key_bucket_bit(
                                    ordered_log_hash(routing_key),
                                ),
                                queue_scan_work: 0,
                                queue_claim_conflicts: 0,
                                oldest_visible_age_ms: 0,
                                visible_count: 0,
                                invisible_count: 0,
                            },
                        });
                    let committed = version_future
                        .await
                        .map_err(|err| map_fdb_error("get versionstamp", err))?;
                    let data = committed.as_ref();
                    if data.len() != 10 {
                        return Err(StorageError::internal("unexpected versionstamp length"));
                    }
                    let mut bytes = [0u8; 12];
                    bytes[..10].copy_from_slice(data);
                    bytes[10..].copy_from_slice(&binding.user_bytes);
                    return Ok(Some(StreamItemId::from(bytes)));
                }
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![
                                family_config_key.clone(),
                                Self::prefix_bytes(prefix.as_ref(), &partition_prefix),
                            ];
                            self.log_conflict_details(
                                &new_trx,
                                "append_partitioned_ordered_log_item",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "append partitioned ordered log commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>> {
        Ok(self.runtime_partition_load_tracker.drain())
    }

    fn partition_runtime_load_hint(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        partition_id: u16,
    ) -> u64 {
        self.runtime_partition_load_tracker
            .load_hint(family_kind, family_component, partition_id)
    }

    async fn wait_for_change(&self, key: &[u8], timeout: Duration) -> StorageResult<bool> {
        let prefixed_key = self.prefix_slice(key);
        let trx = self.create_transaction()?;
        Self::configure_transaction(&trx, Some("kv.wait_for_change"), true)?;
        let watch = trx.watch(&prefixed_key);
        trx.commit()
            .await
            .map_err(|err| map_fdb_error("commit FoundationDB watch", *err))?;

        match time::timeout(timeout, watch).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(err)) => Err(map_fdb_error("await FoundationDB watch", err)),
            Err(_) => Ok(false),
        }
    }

    async fn split_partitioned_ordered_log_family(
        &self,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> StorageResult<bool> {
        let prefix = self.config.subspace_prefix.clone();
        let family_config_key = Self::prefix_bytes(
            prefix.as_ref(),
            &crate::partition_family::partition_family_config_key(
                PartitionFamilyKind::OrderedLog,
                family_component,
            ),
        );
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let changed = match self
                .split_partitioned_ordered_log_family_tx(
                    &trx,
                    prefix.as_ref(),
                    family_component,
                    partition_id,
                    now_ms,
                )
                .await
            {
                Ok(changed) => changed,
                Err(FdbTransactionAttemptError::Storage(storage_err)) => return Err(storage_err),
                Err(FdbTransactionAttemptError::Fdb { scope, error }) => {
                    let candidate_keys = vec![family_config_key.clone()];
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "split_partitioned_ordered_log_family",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            if !changed {
                return Ok(false);
            }

            match trx.commit().await {
                Ok(_) => return Ok(true),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![family_config_key.clone()];
                            self.log_conflict_details(
                                &new_trx,
                                "split_partitioned_ordered_log_family",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "split partitioned ordered log family commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl SortedKvStore for FoundationDbKvStore {
    async fn atomic_read_modify_write_table(
        &self,
        read_key: Vec<u8>,
        transform: AtomicTableWriteTransform,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<u8>> {
        let prefix = self.config.subspace_prefix.clone();
        let prefixed_read_key = Self::prefix_bytes(prefix.as_ref(), &read_key);
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        loop {
            attempt = attempt.saturating_add(1);
            Self::configure_transaction(&trx, None, true)?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|error| map_fdb_error("disable atomic RMW read-your-writes", error))?;
            let current = trx
                .get(&prefixed_read_key, false)
                .await
                .map_err(|error| map_fdb_error("atomic RMW read item", error))?
                .map(|value| value.to_vec());
            let (operations, output) = match transform(current.as_deref())? {
                AtomicTableWriteDecision::NoWrite { output } => return Ok(output),
                AtomicTableWriteDecision::Write { operations, output } => (operations, output),
            };
            let stream_ids = self.build_stream_ids(&operations).await;
            match self
                .execute_transact_write_table_tx(
                    &trx,
                    &operations,
                    &stream_ids,
                    prefix.as_ref(),
                    immediate_gsi_consistency,
                )
                .await
            {
                Ok(execution) => match Self::commit_transaction("atomic_item_rmw", trx).await {
                    Ok(_) => {
                        self.record_ordered_log_writes(
                            &execution.ordered_log_writes,
                            u64::from(attempt.saturating_sub(1)),
                        );
                        return Ok(output);
                    }
                    Err(error) => match error.on_error().await {
                        Ok(mut new_trx) => {
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(error) => {
                            return Err(map_fdb_error("atomic item RMW commit", error));
                        }
                    },
                },
                Err(FdbTableWriteExecutionError::Storage(error)) => return Err(error),
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_transact_write_table_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "atomic_item_rmw",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        if operations.is_empty() {
            return Ok(TransactWriteOutput::new(Vec::new()));
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut set_count = 0u64;
        let mut clear_count = 0u64;
        let mut get_count = 0u64;
        let mut write_bytes = 0u64;
        let mut read_key_bytes = 0u64;
        let mut write_key_bytes = 0u64;
        let mut blind_writes = 0u64;
        let mut read_modify_writes = 0u64;
        for operation in &operations {
            match operation {
                TransactWriteOperation::Put {
                    key,
                    value,
                    condition,
                } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::PutTemplate {
                    template,
                    value,
                    condition,
                } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let key = template.rocks_key();
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::Delete { key, condition } => {
                    clear_count = clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes
                            .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                        read_modify_writes = read_modify_writes.saturating_add(1);
                    } else {
                        blind_writes = blind_writes.saturating_add(1);
                    }
                }
                TransactWriteOperation::Check { key, .. }
                | TransactWriteOperation::CheckValue { key, .. } => {
                    get_count = get_count.saturating_add(1);
                    read_key_bytes = read_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                TransactWriteOperation::Update { key, condition, .. } => {
                    set_count = set_count.saturating_add(1);
                    get_count = get_count.saturating_add(1);
                    let prefixed_key = Self::prefix_bytes(prefix.as_ref(), key);
                    write_key_bytes = write_key_bytes.saturating_add(prefixed_key.len() as u64);
                    read_key_bytes = read_key_bytes.saturating_add(prefixed_key.len() as u64);
                    if condition.is_some() {
                        get_count = get_count.saturating_add(1);
                        read_key_bytes = read_key_bytes.saturating_add(prefixed_key.len() as u64);
                    }
                    read_modify_writes = read_modify_writes.saturating_add(1);
                }
            }
        }
        record_fdb_transaction_start("transact_write");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let execute_started = Instant::now();
            match self
                .execute_transact_write_tx(&trx, &operations, prefix.as_ref())
                .await
            {
                Ok((result, bindings, ordered_log_writes)) => {
                    let version_future = if bindings.is_empty() {
                        None
                    } else {
                        Some(trx.get_versionstamp())
                    };
                    record_fdb_operation_latency(
                        "transact_write",
                        "execute",
                        execute_started.elapsed(),
                    );
                    record_fdb_point_read("transact_write", false, get_count);
                    record_fdb_operation("transact_write", "set", set_count);
                    record_fdb_operation("transact_write", "clear", clear_count);
                    record_fdb_write_shape("transact_write", blind_writes, read_modify_writes);
                    record_fdb_operation_bytes("transact_write", "read_key", read_key_bytes);
                    record_fdb_operation_bytes("transact_write", "write", write_bytes);
                    record_fdb_operation_bytes("transact_write", "write_key", write_key_bytes);
                    record_fdb_operation("transact_write", "commit", 1);
                    match Self::commit_transaction("transact_write", trx).await {
                        Ok(_) => {
                            self.record_ordered_log_writes(
                                &ordered_log_writes,
                                u64::from(attempt.saturating_sub(1)),
                            );
                            let mut placeholder_versions = HashMap::new();
                            if let Some(fut) = version_future {
                                let committed = fut
                                    .await
                                    .map_err(|err| map_fdb_error("get versionstamp", err))?;
                                let data = committed.as_ref();
                                if data.len() != 10 {
                                    return Err(StorageError::internal(
                                        "unexpected versionstamp length",
                                    ));
                                }
                                let mut commit_bytes = [0u8; 10];
                                commit_bytes.copy_from_slice(data);
                                for (id, binding) in bindings {
                                    let mut bytes = [0u8; 12];
                                    bytes[..10].copy_from_slice(&commit_bytes);
                                    bytes[10..].copy_from_slice(&binding.user_bytes);
                                    placeholder_versions.insert(id, bytes);
                                }
                            }

                            return Ok(TransactWriteOutput {
                                items: result,
                                placeholder_versions,
                            });
                        }
                        Err(commit_err) => {
                            record_fdb_operation("transact_write", "retry", 1);
                            let error_code = commit_err.code();
                            let retryable = commit_err.is_retryable();
                            let on_error_started = Instant::now();
                            let retry_result = commit_err.on_error().await;
                            record_fdb_operation_latency(
                                "transact_write",
                                "on_error",
                                on_error_started.elapsed(),
                            );
                            match retry_result {
                                Ok(mut new_trx) => {
                                    let candidate_keys = Self::collect_transact_write_keys(
                                        prefix.as_ref(),
                                        &operations,
                                    );
                                    self.log_conflict_details(
                                        &new_trx,
                                        "transact_write",
                                        attempt,
                                        retryable,
                                        error_code,
                                        &candidate_keys,
                                    )
                                    .await;
                                    new_trx.reset();
                                    trx = new_trx;
                                }
                                Err(retry_err) => {
                                    return Err(map_fdb_error("transact_write commit", retry_err));
                                }
                            }
                        }
                    }
                }
                Err(storage_err) => return Err(storage_err),
            }
        }
    }

    async fn transact_write_unchecked(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut set_count = 0u64;
        let mut clear_count = 0u64;
        let mut check_count = 0u64;
        let mut write_bytes = 0u64;
        let mut read_key_bytes = 0u64;
        let mut write_key_bytes = 0u64;
        let mut has_check = false;
        let mut range_clear_count = 0u64;
        for operation in &operations {
            match operation {
                DirectWriteOperation::Put { key, value } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                DirectWriteOperation::PutTemplate { template, value } => {
                    set_count = set_count.saturating_add(1);
                    write_bytes = write_bytes.saturating_add(value.len() as u64);
                    let key = template.rocks_key();
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), &key).len() as u64);
                }
                DirectWriteOperation::Delete { key } => {
                    clear_count = clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
                DirectWriteOperation::DeleteRange {
                    start,
                    exclusive_end,
                } => {
                    clear_count = clear_count.saturating_add(1);
                    range_clear_count = range_clear_count.saturating_add(1);
                    write_key_bytes = write_key_bytes.saturating_add(
                        Self::prefix_bytes(prefix.as_ref(), start)
                            .len()
                            .saturating_add(
                                Self::prefix_bytes(prefix.as_ref(), exclusive_end).len(),
                            ) as u64,
                    );
                }
                DirectWriteOperation::CheckValue { key, .. } => {
                    check_count = check_count.saturating_add(1);
                    has_check = true;
                    read_key_bytes = read_key_bytes
                        .saturating_add(Self::prefix_bytes(prefix.as_ref(), key).len() as u64);
                }
            }
        }
        record_fdb_transaction_start("transact_write_unchecked");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let execute_started = Instant::now();
            let ordered_log_writes = match self
                .execute_transact_write_unchecked_tx(&trx, &operations, prefix.as_ref())
                .await
            {
                Ok(ordered_log_writes) => ordered_log_writes,
                Err(FdbTableWriteExecutionError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_unchecked_write_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "transact_write_unchecked",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            record_fdb_operation_latency(
                "transact_write_unchecked",
                "execute",
                execute_started.elapsed(),
            );

            record_fdb_point_read("transact_write_unchecked", false, check_count);
            record_fdb_operation("transact_write_unchecked", "set", set_count);
            record_fdb_operation("transact_write_unchecked", "clear", clear_count);
            record_fdb_operation("transact_write_unchecked", "range_clear", range_clear_count);
            let write_count = set_count.saturating_add(clear_count);
            if has_check {
                record_fdb_write_shape("transact_write_unchecked", 0, write_count);
            } else {
                record_fdb_write_shape("transact_write_unchecked", write_count, 0);
            }
            record_fdb_operation_bytes("transact_write_unchecked", "read_key", read_key_bytes);
            record_fdb_operation_bytes("transact_write_unchecked", "write", write_bytes);
            record_fdb_operation_bytes("transact_write_unchecked", "write_key", write_key_bytes);
            record_fdb_operation("transact_write_unchecked", "commit", 1);
            match Self::commit_transaction("transact_write_unchecked", trx).await {
                Ok(_) => {
                    self.record_ordered_log_writes(
                        &ordered_log_writes,
                        u64::from(attempt.saturating_sub(1)),
                    );
                    return Ok(());
                }
                Err(commit_err) => {
                    record_fdb_operation("transact_write_unchecked", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    let on_error_started = Instant::now();
                    let retry_result = commit_err.on_error().await;
                    record_fdb_operation_latency(
                        "transact_write_unchecked",
                        "on_error",
                        on_error_started.elapsed(),
                    );
                    match retry_result {
                        Ok(diagnostic_trx) => {
                            let candidate_keys =
                                Self::collect_unchecked_write_keys(prefix.as_ref(), &operations);
                            self.log_conflict_details(
                                &diagnostic_trx,
                                "transact_write_unchecked",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            trx = self.create_transaction()?;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error(
                                "transact_write_unchecked commit",
                                retry_err,
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let stream_ids = self.build_stream_ids(&operations).await;
        let prefix = self.config.subspace_prefix.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            #[cfg(test)]
            provider_perf::record_amount("foundationdb", "table_write_attempt", 1);
            Self::configure_transaction(&trx, None, true)?;
            trx.set_option(options::TransactionOption::ReadYourWritesDisable)
                .map_err(|err| map_fdb_error("disable table-write read-your-writes", err))?;

            let execute_started = Instant::now();
            match self
                .execute_transact_write_table_tx(
                    &trx,
                    &operations,
                    &stream_ids,
                    prefix.as_ref(),
                    immediate_gsi_consistency,
                )
                .await
            {
                Ok(execution) => {
                    let execute_elapsed = execute_started.elapsed();
                    let execute_ms = execute_elapsed.as_secs_f64() * 1000.0;
                    record_fdb_operation_latency(
                        "transact_write_table",
                        "execute",
                        execute_elapsed,
                    );
                    record_fdb_operation("transact_write_table", "commit", 1);
                    let commit_started = Instant::now();
                    match Self::commit_transaction("transact_write_table", trx).await {
                        Ok(_) => {
                            let commit_elapsed = commit_started.elapsed();
                            #[cfg(test)]
                            provider_perf::record(
                                "foundationdb",
                                "table_write_commit",
                                commit_elapsed,
                            );
                            tracing::debug!(
                                attempt,
                                operation_count = operations.len(),
                                execute_ms,
                                commit_ms = commit_elapsed.as_secs_f64() * 1000.0,
                                ordered_log_write_count = execution.ordered_log_writes.len(),
                                "foundationdb transact_write_table committed"
                            );
                            self.record_ordered_log_writes(
                                &execution.ordered_log_writes,
                                u64::from(attempt.saturating_sub(1)),
                            );
                            return Ok(execution.results);
                        }
                        Err(commit_err) => {
                            let commit_ms = commit_started.elapsed().as_secs_f64() * 1000.0;
                            record_fdb_operation("transact_write_table", "retry", 1);
                            let error_code = commit_err.code();
                            let retryable = commit_err.is_retryable();
                            #[cfg(test)]
                            {
                                provider_perf::record_amount(
                                    "foundationdb",
                                    "table_write_commit_retry",
                                    1,
                                );
                                if retryable {
                                    provider_perf::record_amount(
                                        "foundationdb",
                                        "table_write_commit_retryable",
                                        1,
                                    );
                                } else {
                                    provider_perf::record_amount(
                                        "foundationdb",
                                        "table_write_commit_non_retryable",
                                        1,
                                    );
                                }
                            }
                            tracing::debug!(
                                attempt,
                                operation_count = operations.len(),
                                execute_ms,
                                commit_ms,
                                error_code,
                                retryable,
                                "foundationdb transact_write_table commit retry"
                            );
                            let on_error_started = Instant::now();
                            let retry_result = commit_err.on_error().await;
                            record_fdb_operation_latency(
                                "transact_write_table",
                                "on_error",
                                on_error_started.elapsed(),
                            );
                            match retry_result {
                                Ok(mut new_trx) => {
                                    let candidate_keys = Self::collect_transact_write_table_keys(
                                        prefix.as_ref(),
                                        &operations,
                                    );
                                    self.log_conflict_details(
                                        &new_trx,
                                        "transact_write_table",
                                        attempt,
                                        retryable,
                                        error_code,
                                        &candidate_keys,
                                    )
                                    .await;
                                    new_trx.reset();
                                    trx = new_trx;
                                }
                                Err(retry_err) => {
                                    return Err(map_fdb_error(
                                        "transact_write_table commit",
                                        retry_err,
                                    ));
                                }
                            }
                        }
                    }
                }
                Err(FdbTableWriteExecutionError::Storage(storage_err)) => {
                    return Err(storage_err);
                }
                Err(FdbTableWriteExecutionError::Fdb { scope, error }) => {
                    let candidate_keys =
                        Self::collect_transact_write_table_keys(prefix.as_ref(), &operations);
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "transact_write_table",
                            scope,
                            attempt,
                            error,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        if items.is_empty() {
            return Ok(());
        }

        let prefix = self.config.subspace_prefix.clone();
        let prefixed_items: Vec<BatchItem> = items
            .into_iter()
            .map(|item| BatchItem {
                key: Self::prefix_bytes(prefix.as_ref(), &item.key),
                value: item.value,
            })
            .collect();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            for item in &prefixed_items {
                match &item.value {
                    Some(value) => trx.set(&item.key, value),
                    None => trx.clear(&item.key),
                }
            }

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys: Vec<Vec<u8>> =
                                prefixed_items.iter().map(|item| item.key.clone()).collect();
                            self.log_conflict_details(
                                &new_trx,
                                "batch_write",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("batch_write commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    async fn begin_read_context(&self) -> StorageResult<Box<dyn SortedKvReadContext>> {
        let trx = self.create_transaction()?;
        self.configure_read_transaction(&trx, None, true)?;
        self.prepare_uncached_read_version_fdb(&trx, true)
            .await
            .map_err(|err| map_fdb_error("read context read version", err))?;
        record_fdb_transaction_start("read_context");
        Ok(Box::new(FoundationDbReadContext {
            store: self.clone(),
            trx,
        }))
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let prefixed_key = self.prefix_slice(key);
        let candidate_keys = vec![prefixed_key.to_vec()];
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("get");

        loop {
            attempt += 1;
            self.configure_read_transaction(&trx, None, consistent_read)?;
            if let Err(err) = self
                .prepare_uncached_read_version_fdb(&trx, consistent_read)
                .await
            {
                trx = self
                    .retry_transaction_after_fdb_error(
                        trx,
                        "get",
                        "get read version",
                        attempt,
                        err,
                        &candidate_keys,
                    )
                    .await?;
                continue;
            }

            let get_started = Instant::now();
            let value = match trx.get(&prefixed_key, true).await {
                Ok(value) => value,
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "get",
                            "read key",
                            attempt,
                            err,
                            &candidate_keys,
                        )
                        .await?;
                    continue;
                }
            };
            record_fdb_operation_latency("get", "point_read", get_started.elapsed());
            record_fdb_point_read("get", true, 1);
            record_fdb_operation_bytes("get", "read_key", prefixed_key.len() as u64);
            record_fdb_operation_bytes(
                "get",
                "read",
                prefixed_key
                    .len()
                    .saturating_add(value.as_ref().map_or(0, |bytes| bytes.len()))
                    as u64,
            );

            return Ok(value.map(|bytes| bytes.to_vec()));
        }
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut trx = self.create_transaction()?;
        record_fdb_transaction_start("multi_get");
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            self.configure_read_transaction(&trx, None, consistent_read)?;
            let prefix = self.config.subspace_prefix.clone();
            let candidate_keys = keys
                .iter()
                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                .collect::<Vec<_>>();
            if let Err(err) = self
                .prepare_uncached_read_version_fdb(&trx, consistent_read)
                .await
            {
                trx = self
                    .retry_transaction_after_fdb_error(
                        trx,
                        "multi_get",
                        "multi_get read version",
                        attempt,
                        err,
                        &candidate_keys,
                    )
                    .await?;
                continue;
            }

            let prefixed_keys = keys
                .iter()
                .map(|key| Self::prefix_bytes(prefix.as_ref(), key))
                .collect::<Vec<_>>();
            let read_started = Instant::now();
            match read_fdb_keys_sequential(&trx, &prefixed_keys, false).await {
                Ok(results) => {
                    record_fdb_operation_latency(
                        "multi_get",
                        "point_read_batch",
                        read_started.elapsed(),
                    );
                    record_fdb_point_read("multi_get", false, keys.len() as u64);
                    record_fdb_operation_bytes(
                        "multi_get",
                        "read_key",
                        prefixed_keys
                            .iter()
                            .map(|key| key.len() as u64)
                            .sum::<u64>(),
                    );
                    record_fdb_operation_bytes(
                        "multi_get",
                        "read",
                        keys.iter()
                            .map(|key| key.len() as u64)
                            .sum::<u64>()
                            .saturating_add(
                                results
                                    .iter()
                                    .map(|value| {
                                        value.as_ref().map_or(0, |bytes| bytes.len() as u64)
                                    })
                                    .sum::<u64>(),
                            ),
                    );
                    return Ok(results
                        .into_iter()
                        .map(|value| value.map(|bytes| bytes.to_vec()))
                        .collect());
                }
                Err(err) => {
                    trx = self
                        .retry_transaction_after_fdb_error(
                            trx,
                            "multi_get",
                            "multi_get",
                            0,
                            err,
                            &candidate_keys,
                        )
                        .await?;
                }
            }
        }
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        let prefix = self.config.subspace_prefix.clone();
        let key_bytes = key.to_vec();
        let value_bytes = value.to_vec();
        let condition = condition.clone();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        let mut maybe_committed = false;
        record_fdb_transaction_start("put");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &key_bytes);

            if let Some(condition) = &condition {
                let current = trx
                    .get(&prefixed_key, false)
                    .await
                    .map_err(|err| map_fdb_error("load key for conditional put", err))?;
                record_fdb_point_read("put", false, 1);
                record_fdb_operation_bytes("put", "read_key", prefixed_key.len() as u64);
                record_fdb_operation_bytes(
                    "put",
                    "read",
                    prefixed_key
                        .len()
                        .saturating_add(current.as_ref().map_or(0, |bytes| bytes.len()))
                        as u64,
                );

                if !evaluate_condition_bytes(current.as_deref(), condition) {
                    if maybe_committed {
                        return Err(StorageError::internal(
                            "maybe_committed: conditional put retry observed condition failure \
                             after a maybe-committed commit",
                        ));
                    }
                    return Err(StorageEnum::TransactionCanceled {
                        reasons: vec!["ConditionalCheckFailed".to_string()],
                    }
                    .into());
                }
            }

            trx.set(&prefixed_key, &value_bytes);
            record_fdb_operation("put", "set", 1);
            if condition.is_some() {
                record_fdb_write_shape("put", 0, 1);
            } else {
                record_fdb_write_shape("put", 1, 0);
            }
            record_fdb_operation_bytes("put", "write", value_bytes.len() as u64);
            record_fdb_operation_bytes("put", "write_key", prefixed_key.len() as u64);
            record_fdb_operation("put", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("put", "retry", 1);
                    maybe_committed |= commit_err.is_maybe_committed();
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys =
                                vec![Self::prefix_bytes(prefix.as_ref(), &key_bytes)];
                            self.log_conflict_details(
                                &new_trx,
                                "put",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("put commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let prefix = self.config.subspace_prefix.clone();
        let key_bytes = key.to_vec();
        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;
        record_fdb_transaction_start("delete");

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;

            let prefixed_key = Self::prefix_bytes(prefix.as_ref(), &key_bytes);
            trx.clear(&prefixed_key);
            record_fdb_operation("delete", "clear", 1);
            record_fdb_write_shape("delete", 1, 0);
            record_fdb_operation_bytes("delete", "write_key", prefixed_key.len() as u64);
            record_fdb_operation("delete", "commit", 1);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    record_fdb_operation("delete", "retry", 1);
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys =
                                vec![Self::prefix_bytes(prefix.as_ref(), &key_bytes)];
                            self.log_conflict_details(
                                &new_trx,
                                "delete",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("delete commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        let start = self.prefix_slice(&prefix);
        let end = increment_bytes(start.clone());

        let mut trx = self.create_transaction()?;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            Self::configure_transaction(&trx, None, true)?;
            trx.clear_range(&start, &end);

            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_err) => {
                    let error_code = commit_err.code();
                    let retryable = commit_err.is_retryable();
                    match commit_err.on_error().await {
                        Ok(mut new_trx) => {
                            let candidate_keys = vec![start.clone(), end.clone()];
                            self.log_conflict_details(
                                &new_trx,
                                "delete_prefix",
                                attempt,
                                retryable,
                                error_code,
                                &candidate_keys,
                            )
                            .await;
                            new_trx.reset();
                            trx = new_trx;
                        }
                        Err(retry_err) => {
                            return Err(map_fdb_error("delete_prefix commit", retry_err));
                        }
                    }
                }
            }
        }
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        let page_bytes = if let Some(token) = page_token {
            Some(token.serialize_to_bytes()?)
        } else {
            None
        };

        self.read_range(start, exclusive_end, limit, page_bytes, consistent_read)
            .await
    }
}
