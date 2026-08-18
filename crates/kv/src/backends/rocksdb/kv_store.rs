use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use rocksdb::{
    DB, DEFAULT_COLUMN_FAMILY_NAME, Error, ErrorKind, OptimisticTransactionDB, Options,
    Transaction, WriteOptions, checkpoint::Checkpoint,
};
use storage_condition::{Condition, evaluate_condition};
use storage_types::{
    ItemStreamVersion, SerializesToKey, StorageEnum, StorageError, StorageResult, StreamItemId,
    StreamName, TimestampMillis, context::WrappedError as _,
};
use tracing::{error, warn};

use crate::{
    backends::{
        common::{
            KvMutation, RangeKeyDecision, RangeScanSettings, operation_requires_stream_entries,
            plan_table_write_preflighted, plan_transact_operation,
            preflight_table_write_operations, table_operation_primary_key,
        },
        rocksdb::constants::{
            ROCKSDB_BATCH_WRITE_RETRIES, ROCKSDB_CONDITIONAL_PUT_FAILURE_METRIC,
            ROCKSDB_CONDITIONAL_PUT_RETRIES, ROCKSDB_CONDITIONAL_PUT_RETRY_METRIC,
        },
    },
    helpers::{deserialize_item_from_bytes, increment_bytes},
    partition_family::{PartitionFamilyKind, PartitionFamilyKvStore, RuntimePartitionLoadSample},
    queue::{
        PartitionedQueueMessageWrite, QueueClaimBatch, QueueClaimRange, QueueKvStore,
        QueuePrewarmPartition,
        storage::{
            claim_queue_messages_from_ranges_generic, prewarm_partitioned_queue_generic,
            write_partitioned_queue_message_generic,
        },
    },
    sorted_kv_store::{
        AtomicTableWriteDecision, AtomicTableWriteTransform, BatchItem, DirectWriteOperation,
        OldNewItems, RangeResult, RangeValuesResult, RawKey, SortedKvReadContext, SortedKvStore,
        TransactWriteOperation, TransactWriteOutput, TransactWriteTableOperation,
    },
};

#[derive(Clone)]
pub struct RocksDbKvStore {
    db: Arc<tokio::sync::RwLock<rocksdb::OptimisticTransactionDB>>,
}

struct RocksDbReadContext {
    db: Arc<DB>,
    checkpoint_path: PathBuf,
}

#[async_trait::async_trait]
impl SortedKvReadContext for RocksDbReadContext {
    async fn get(&self, key: &[u8], _consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map_err(|e| StorageError::internal(&format!("rocksdb snapshot get failed: {e}")))
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        _consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let results = db.multi_get(keys.iter());

            let mut values = Vec::with_capacity(results.len());
            for result in results {
                match result {
                    Ok(data) => values.push(data),
                    Err(e) => {
                        return Err(StorageError::internal(&format!(
                            "rocksdb snapshot multi_get failed: {e}"
                        )));
                    }
                }
            }

            Ok(values)
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<RawKey>,
        _consistent_read: bool,
    ) -> StorageResult<RangeValuesResult> {
        let scan_start = start.to_vec();
        let mut scan_end = exclusive_end.to_vec();
        if start == exclusive_end {
            scan_end = increment_bytes(scan_start.clone());
        }
        let forward = scan_start <= scan_end;
        let (page_bytes, iterator_start) = match page_token {
            Some(token) => {
                let serialized = token.serialize_to_bytes()?;
                let iter_start = if forward {
                    token.increment_bytes_and_serialize()?
                } else {
                    token.decrement_bytes_and_serialize()?
                };
                (Some(serialized), iter_start)
            }
            None => (None, scan_start.clone()),
        };
        let scan = RangeScanSettings::new(&scan_start, &scan_end, limit, page_bytes)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let direction = if scan.forward() {
                rocksdb::Direction::Forward
            } else {
                rocksdb::Direction::Reverse
            };

            let iter = db.iterator(rocksdb::IteratorMode::From(&iterator_start, direction));

            let mut values = Vec::new();

            for entry in iter.flatten() {
                let (key, value) = entry;
                match scan.evaluate_key(&key) {
                    RangeKeyDecision::Include => {
                        values.push(value.into_vec());
                        if values.len() >= scan.fetch_limit() {
                            break;
                        }
                    }
                    RangeKeyDecision::Skip => {}
                    RangeKeyDecision::Stop => break,
                }
            }

            Ok(scan.finalize_values(values, false))
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }
}

impl Drop for RocksDbReadContext {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.checkpoint_path);
    }
}

static NEXT_ROCKSDB_STREAM_ITEM_VERSION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn rocksdb_stream_high_water_key() -> Vec<u8> {
    crate::keyspace::compact::stream_high_water_key()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RocksDbDurabilityPolicy {
    pub sync_writes: bool,
    pub use_fsync: bool,
}

const ROCKSDB_DURABILITY_POLICY: RocksDbDurabilityPolicy = RocksDbDurabilityPolicy {
    sync_writes: true,
    use_fsync: true,
};

pub(super) const fn rocksdb_durability_policy() -> RocksDbDurabilityPolicy {
    ROCKSDB_DURABILITY_POLICY
}

impl RocksDbKvStore {
    pub fn new(file_path: PathBuf) -> StorageResult<Self> {
        if let Some(parent) = file_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                StorageError::internal(&format!(
                    "create rocksdb parent directory failed: {}: {error}",
                    file_path.display()
                ))
            })?;
        }

        let opts = rocksdb_options();

        let cf = vec![DEFAULT_COLUMN_FAMILY_NAME];
        let db_handle = rocksdb::OptimisticTransactionDB::open_cf(&opts, file_path, cf)
            .map_err(|e| StorageError::internal(&format!("open rocksdb failed: {e}")))?;
        let db_store = Arc::new(tokio::sync::RwLock::new(db_handle));
        Ok(Self { db: db_store })
    }

    async fn transact_write_table_once(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        direct_operations: Vec<DirectWriteOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        preflight_table_write_operations(&operations)?;
        let db_guard = self.db.read().await;
        let opts = write_options_sync();
        let otxn_opts = transaction_options_with_snapshot();
        let txn = db_guard.transaction_opt(&opts, &otxn_opts);
        let mut current_values = Vec::with_capacity(operations.len());
        let mut stream_ids = Vec::with_capacity(operations.len());
        for operation in &operations {
            let key = table_operation_primary_key(operation)?;
            current_values.push(txn.get_for_update(&key, true).map_err(generic_err)?);
            let needs_stream =
                operation_requires_stream_entries(operation, immediate_gsi_consistency);
            stream_ids.push(if needs_stream {
                Some(allocate_rocksdb_stream_item_id(
                    &txn,
                    next_rocksdb_stream_item_id(),
                )?)
            } else {
                None
            });
        }
        let plan = plan_table_write_preflighted(
            &operations,
            current_values,
            &stream_ids,
            immediate_gsi_consistency,
        )?;
        apply_mutations(&txn, plan.mutations)?;

        apply_direct_write_operations(&db_guard, &txn, direct_operations)?;

        txn.commit().map_err(map_rocksdb_transaction_commit)?;

        Ok(plan.results)
    }

    async fn transact_write_once(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        let db_guard = self.db.read().await;
        let opts = write_options_sync();
        let otxn_opts = transaction_options_with_snapshot();
        let txn = db_guard.transaction_opt(&opts, &otxn_opts);
        let mut operation_results = Vec::with_capacity(operations.len());

        for (index, operation) in operations.into_iter().enumerate() {
            let current_bytes = match &operation {
                TransactWriteOperation::Put { key, condition, .. } => {
                    if condition.is_some() {
                        txn.get_for_update(key.as_slice(), true)
                            .map_err(generic_err)?
                    } else {
                        None
                    }
                }
                TransactWriteOperation::Delete { key, .. }
                | TransactWriteOperation::Check { key, .. }
                | TransactWriteOperation::CheckValue { key, .. }
                | TransactWriteOperation::Update { key, .. } => txn
                    .get_for_update(key.as_slice(), true)
                    .map_err(generic_err)?,
                TransactWriteOperation::PutTemplate { .. } => None,
            };

            let (old_new, mutations) =
                plan_transact_operation(operation, current_bytes.as_deref(), index)?;
            apply_mutations(&txn, mutations)?;
            operation_results.push(old_new);
        }

        txn.commit().map_err(map_rocksdb_transaction_commit)?;
        Ok(TransactWriteOutput {
            items: operation_results,
            placeholder_versions: HashMap::new(),
        })
    }

    async fn transact_write_unchecked_once(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let db_guard = self.db.read().await;
        let opts = write_options_sync();
        let otxn_opts = transaction_options_with_snapshot();
        let txn = db_guard.transaction_opt(&opts, &otxn_opts);
        apply_direct_write_operations(&db_guard, &txn, operations)?;

        txn.commit().map_err(map_rocksdb_transaction_commit)?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl PartitionFamilyKvStore for RocksDbKvStore {
    fn supports_partition_families(&self) -> bool {
        true
    }

    async fn append_partitioned_ordered_log_item(
        &self,
        _stream_name: &StreamName,
        _routing_key: &[u8],
        _value: &[u8],
        _fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>> {
        Ok(None)
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>> {
        Ok(Vec::new())
    }

    fn partition_runtime_load_hint(
        &self,
        _family_kind: PartitionFamilyKind,
        _family_component: &str,
        _partition_id: u16,
    ) -> u64 {
        0
    }

    async fn wait_for_change(&self, _key: &[u8], _timeout: Duration) -> StorageResult<bool> {
        Ok(false)
    }

    async fn split_partitioned_ordered_log_family(
        &self,
        _family_component: &str,
        _partition_id: u16,
        _now_ms: i64,
    ) -> StorageResult<bool> {
        Ok(false)
    }
}

#[async_trait::async_trait]
impl SortedKvStore for RocksDbKvStore {
    async fn atomic_read_modify_write_table(
        &self,
        read_key: Vec<u8>,
        transform: AtomicTableWriteTransform,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<u8>> {
        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            let db_guard = self.db.read().await;
            let opts = write_options_sync();
            let otxn_opts = transaction_options_with_snapshot();
            let txn = db_guard.transaction_opt(&opts, &otxn_opts);
            let current = txn.get_for_update(&read_key, true).map_err(generic_err)?;
            let (operations, output) = match transform(current.as_deref())? {
                AtomicTableWriteDecision::NoWrite { output } => return Ok(output),
                AtomicTableWriteDecision::Write { operations, output } => (operations, output),
            };
            preflight_table_write_operations(&operations)?;
            let mut current_values = Vec::with_capacity(operations.len());
            let mut stream_ids = Vec::with_capacity(operations.len());
            for operation in &operations {
                let key = table_operation_primary_key(operation)?;
                current_values.push(txn.get_for_update(&key, true).map_err(generic_err)?);
                stream_ids.push(
                    operation_requires_stream_entries(operation, immediate_gsi_consistency)
                        .then(|| {
                            allocate_rocksdb_stream_item_id(&txn, next_rocksdb_stream_item_id())
                        })
                        .transpose()?,
                );
            }
            let plan = plan_table_write_preflighted(
                &operations,
                current_values,
                &stream_ids,
                immediate_gsi_consistency,
            )?;
            apply_mutations(&txn, plan.mutations)?;
            match txn
                .commit()
                .map_err(|error| StorageError::from(map_rocksdb_transaction_commit(error)))
            {
                Ok(()) => return Ok(output),
                Err(error)
                    if is_rocksdb_transaction_retryable(&error)
                        && attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES =>
                {
                    warn!(attempt, error = %error, "rocksdb atomic item RMW retry");
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("rocksdb atomic item RMW retry loop returns on success or final failure")
    }

    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            match self
                .transact_write_table_once(
                    operations.clone(),
                    Vec::new(),
                    immediate_gsi_consistency,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error)
                    if is_rocksdb_transaction_retryable(&error)
                        && attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES =>
                {
                    warn!(attempt, error = %error, "rocksdb table transaction retry");
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("rocksdb table transaction retry loop returns on success or final failure")
    }

    async fn transact_write_table_with_direct_writes(
        &self,
        table_operations: Vec<TransactWriteTableOperation>,
        direct_operations: Vec<DirectWriteOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            match self
                .transact_write_table_once(
                    table_operations.clone(),
                    direct_operations.clone(),
                    immediate_gsi_consistency,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error)
                    if is_rocksdb_transaction_retryable(&error)
                        && attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES =>
                {
                    warn!(attempt, error = %error, "rocksdb table transaction with direct writes retry");
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!(
            "rocksdb table transaction with direct writes retry loop returns on success or final \
             failure"
        )
    }

    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            match self.transact_write_once(operations.clone()).await {
                Ok(result) => return Ok(result),
                Err(error)
                    if is_rocksdb_transaction_retryable(&error)
                        && attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES =>
                {
                    warn!(attempt, error = %error, "rocksdb transaction retry");
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("rocksdb transaction retry loop returns on success or final failure")
    }

    async fn transact_write_unchecked(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            match self.transact_write_unchecked_once(operations.clone()).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_rocksdb_transaction_retryable(&error)
                        && attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES =>
                {
                    warn!(attempt, error = %error, "rocksdb unchecked transaction retry");
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("rocksdb unchecked transaction retry loop returns on success or final failure")
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        let db_guard = self.db.read().await;
        let opts = write_options_sync();

        let do_batch = move || {
            let mut batch = rocksdb::WriteBatchWithTransaction::default();

            for item in &items {
                if let Some(value) = item.value.as_ref() {
                    batch.put(&item.key, value);
                } else {
                    batch.delete(&item.key);
                }
            }

            db_guard.write_opt(batch, &opts)
        };

        for attempt in 0..ROCKSDB_BATCH_WRITE_RETRIES {
            match do_batch() {
                Ok(()) => return Ok(()),
                Err(error) if attempt + 1 < ROCKSDB_BATCH_WRITE_RETRIES => {
                    warn!(attempt, error = %error, "rocksdb batch write retry");
                }
                Err(error) => {
                    return Err(StorageError::internal(&format!(
                        "batch write key-values failed: {error}"
                    )));
                }
            }
        }
        unreachable!("rocksdb batch write loop returns on success or final failure")
    }

    async fn begin_read_context(&self) -> StorageResult<Box<dyn SortedKvReadContext>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let checkpoint_path = std::env::temp_dir().join(format!(
                "aux-storage-rocksdb-read-context-{}",
                uuid::Uuid::now_v7()
            ));
            let db_guard = db.blocking_read();
            let checkpoint = Checkpoint::new(&*db_guard).map_err(generic_err)?;
            checkpoint
                .create_checkpoint(&checkpoint_path)
                .map_err(generic_err)?;
            let opts = rocksdb_options();
            let cf = vec![DEFAULT_COLUMN_FAMILY_NAME];
            let read_db = DB::open_cf_for_read_only(&opts, &checkpoint_path, cf, false)
                .map_err(generic_err)?;
            Ok(Box::new(RocksDbReadContext {
                db: Arc::new(read_db),
                checkpoint_path,
            }) as Box<dyn SortedKvReadContext>)
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn get(&self, key: &[u8], _consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            let db_guard = db.blocking_read();
            match db_guard.get(key) {
                Ok(Some(data)) => Ok(Some(data)),
                Ok(None) => Ok(None),
                Err(e) => Err(StorageError::internal(&format!("get key failed: {e}"))),
            }
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        _consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let db_guard = db.blocking_read();
            let results = db_guard.multi_get(keys.iter());

            let mut values = Vec::with_capacity(results.len());
            for result in results {
                match result {
                    Ok(data) => values.push(data),
                    Err(e) => {
                        return Err(StorageError::internal(&format!(
                            "multi_get keys failed: {e}"
                        )));
                    }
                }
            }

            Ok(values)
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        let db_guard = self.db.read().await;

        if let Some(condition) = condition {
            let opts = write_options_sync();
            let otxn_opts = transaction_options_with_snapshot();

            let do_txn = move || {
                let txn = db_guard.transaction_opt(&opts, &otxn_opts);
                let data = txn.get_for_update(key, true);

                let item = match data {
                    Ok(Some(data)) => deserialize_item_from_bytes(&data).map_err(|e| {
                        StorageError::internal(&format!("deserialize item failed: {e}"))
                    })?,
                    Ok(None) => HashMap::new(),
                    Err(e) => {
                        txn.rollback().ok();
                        return Err(StorageError::internal(&format!("get key failed: {e}")));
                    }
                };

                if !evaluate_condition(&item, &condition) {
                    let _ = txn.rollback();
                    return Err(StorageError::internal("put condition failed"));
                }

                txn.put(key, value)
                    .map_err(|e| StorageError::internal(&format!("put key-value failed: {e}")))?;

                txn.commit().map_err(|_| {
                    StorageEnum::TransactionConflict {
                        message: "rocksdb transaction conflict".to_string(),
                    }
                    .into()
                })
            };

            for attempt in 0..ROCKSDB_CONDITIONAL_PUT_RETRIES {
                match do_txn() {
                    Ok(()) => return Ok(()),
                    Err(error) if attempt + 1 < ROCKSDB_CONDITIONAL_PUT_RETRIES => {
                        metrics_facade::counter!(ROCKSDB_CONDITIONAL_PUT_RETRY_METRIC).increment(1);
                        warn!(attempt, error = %error, "rocksdb conditional put retry");
                    }
                    Err(error) => {
                        metrics_facade::counter!(ROCKSDB_CONDITIONAL_PUT_FAILURE_METRIC)
                            .increment(1);
                        return Err(error);
                    }
                }
            }
            unreachable!("rocksdb conditional put loop returns on success or final failure");
        }

        let opts = write_options_sync();
        db_guard
            .put_opt(key, value, &opts)
            .map_err(|e| StorageError::internal(&format!("put key-value failed: {e}")))
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let db_guard = self.db.read().await;
        let opts = write_options_sync();
        db_guard
            .delete_opt(key, &opts)
            .map_err(|e| StorageError::internal(&format!("delete key failed: {e}")))
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        let db_guard = self.db.read().await;
        let opts = write_options_sync();
        let next_byte = increment_bytes(prefix.clone());
        for key in range_keys(&db_guard, &prefix, &next_byte)? {
            db_guard
                .delete_opt(key, &opts)
                .map_err(|e| StorageError::internal(&format!("delete prefix key failed: {e}")))?;
        }
        Ok(())
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        _consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        let scan_start = start.to_vec();
        let mut scan_end = exclusive_end.to_vec();
        if start == exclusive_end {
            scan_end = increment_bytes(scan_start.clone());
        }
        let forward = scan_start <= scan_end;
        let (page_bytes, iterator_start) = match page_token {
            Some(token) => {
                let serialized = token.serialize_to_bytes()?;
                let iter_start = if forward {
                    token.increment_bytes_and_serialize()?
                } else {
                    token.decrement_bytes_and_serialize()?
                };
                (Some(serialized), iter_start)
            }
            None => (None, scan_start.clone()),
        };
        let scan = RangeScanSettings::new(&scan_start, &scan_end, limit, page_bytes)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let db_guard = db.blocking_read();
            let direction = if scan.forward() {
                rocksdb::Direction::Forward
            } else {
                rocksdb::Direction::Reverse
            };

            let iter = db_guard.iterator(rocksdb::IteratorMode::From(&iterator_start, direction));

            let mut items = Vec::new();

            for entry in iter.flatten() {
                let (key, value) = entry;
                match scan.evaluate_key(&key) {
                    RangeKeyDecision::Include => {
                        items.push((key.into_vec(), value.into_vec()));
                        if items.len() >= scan.fetch_limit() {
                            break;
                        }
                    }
                    RangeKeyDecision::Skip => {}
                    RangeKeyDecision::Stop => break,
                }
            }

            Ok(scan.finalize(items, false))
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        _consistent_read: bool,
    ) -> StorageResult<RangeValuesResult> {
        let scan_start = start.to_vec();
        let mut scan_end = exclusive_end.to_vec();
        if start == exclusive_end {
            scan_end = increment_bytes(scan_start.clone());
        }
        let forward = scan_start <= scan_end;
        let (page_bytes, iterator_start) = match page_token {
            Some(token) => {
                let serialized = token.serialize_to_bytes()?;
                let iter_start = if forward {
                    token.increment_bytes_and_serialize()?
                } else {
                    token.decrement_bytes_and_serialize()?
                };
                (Some(serialized), iter_start)
            }
            None => (None, scan_start.clone()),
        };
        let scan = RangeScanSettings::new(&scan_start, &scan_end, limit, page_bytes)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let db_guard = db.blocking_read();
            let direction = if scan.forward() {
                rocksdb::Direction::Forward
            } else {
                rocksdb::Direction::Reverse
            };

            let iter = db_guard.iterator(rocksdb::IteratorMode::From(&iterator_start, direction));

            let mut values = Vec::new();

            for entry in iter.flatten() {
                let (key, value) = entry;
                match scan.evaluate_key(&key) {
                    RangeKeyDecision::Include => {
                        values.push(value.into_vec());
                        if values.len() >= scan.fetch_limit() {
                            break;
                        }
                    }
                    RangeKeyDecision::Skip => {}
                    RangeKeyDecision::Stop => break,
                }
            }

            Ok(scan.finalize_values(values, false))
        })
        .await
        .map_err(map_rocksdb_blocking_join_error)?
    }
}

#[async_trait::async_trait]
impl QueueKvStore for RocksDbKvStore {
    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: storage_types::TimestampMillis,
        visibility_timeout: storage_types::DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        claim_queue_messages_from_ranges_generic(self, ranges, now, visibility_timeout, max_claims)
            .await
    }

    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        write_partitioned_queue_message_generic(self, message).await
    }

    async fn prewarm_partitioned_queue(
        &self,
        _queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        prewarm_partitioned_queue_generic(partitions).await
    }
}

pub(super) fn rocksdb_options() -> Options {
    let policy = rocksdb_durability_policy();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_enable_pipelined_write(true);
    opts.set_log_level(rocksdb::LogLevel::Warn);
    if policy.use_fsync {
        opts.set_use_fsync(true);
    }
    opts
}

fn write_options_sync() -> WriteOptions {
    let policy = rocksdb_durability_policy();
    let mut opts = WriteOptions::default();
    opts.set_sync(policy.sync_writes);
    opts
}

fn transaction_options_with_snapshot() -> rocksdb::OptimisticTransactionOptions {
    let mut opts = rocksdb::OptimisticTransactionOptions::default();
    opts.set_snapshot(true);
    opts
}

fn next_rocksdb_stream_item_id() -> StreamItemId {
    let now_ms = u64::try_from(*TimestampMillis::now()).unwrap_or(0);
    let now_component = now_ms.checked_shl(20).unwrap_or(u64::MAX);
    let mut observed = NEXT_ROCKSDB_STREAM_ITEM_VERSION.load(Ordering::Relaxed);
    loop {
        let candidate = now_component.max(observed.saturating_add(1));
        match NEXT_ROCKSDB_STREAM_ITEM_VERSION.compare_exchange_weak(
            observed,
            candidate,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return StreamItemId::from(ItemStreamVersion::new(candidate)),
            Err(current) => observed = current,
        }
    }
}

fn allocate_rocksdb_stream_item_id(
    txn: &Transaction<OptimisticTransactionDB>,
    fallback_item_id: StreamItemId,
) -> StorageResult<StreamItemId> {
    let high_water_key = rocksdb_stream_high_water_key();
    let current = txn
        .get_for_update(&high_water_key, true)
        .map_err(generic_err)?
        .as_deref()
        .map(StreamItemId::try_from)
        .transpose()
        .map_err(|error| {
            StorageError::internal(&format!(
                "decode rocksdb stream high-water id failed: {error}"
            ))
        })?;
    let allocated = current
        .map(|current| current.increment().max(fallback_item_id))
        .unwrap_or(fallback_item_id);
    txn.put(&high_water_key, allocated.as_bytes())
        .map_err(|error| {
            StorageError::internal(&format!(
                "persist rocksdb stream high-water id failed: {error}"
            ))
        })?;
    Ok(allocated)
}

fn range_keys(
    db: &OptimisticTransactionDB,
    start: &[u8],
    exclusive_end: &[u8],
) -> StorageResult<Vec<Vec<u8>>> {
    let iter = db.iterator(rocksdb::IteratorMode::From(
        start,
        rocksdb::Direction::Forward,
    ));
    let mut keys = Vec::new();
    for entry in iter {
        let (key, _) = entry.map_err(|error| {
            StorageError::internal(&format!("iterate delete range failed: {error}"))
        })?;
        if key.as_ref() >= exclusive_end {
            break;
        }
        keys.push(key.into_vec());
    }
    Ok(keys)
}

fn apply_mutations(
    txn: &Transaction<OptimisticTransactionDB>,
    mutations: Vec<KvMutation>,
) -> StorageResult<()> {
    let mut placeholder_allocations = HashMap::new();
    for mutation in mutations {
        match mutation {
            KvMutation::Put { key, value } => txn
                .put(&key, &value)
                .map_err(|e| StorageError::internal(&format!("put key-value failed: {e}")))?,
            KvMutation::PutTemplate { template, value } => {
                let key =
                    materialize_rocksdb_template_key(txn, &template, &mut placeholder_allocations)?;
                txn.put(&key, &value).map_err(|e| {
                    StorageError::internal(&format!("put templated key-value failed: {e}"))
                })?;
            }
            KvMutation::Delete { key } => txn
                .delete(&key)
                .map_err(|e| StorageError::internal(&format!("delete key failed: {e}")))?,
        }
    }
    Ok(())
}

fn apply_direct_write_operations(
    db: &OptimisticTransactionDB,
    txn: &Transaction<OptimisticTransactionDB>,
    operations: Vec<DirectWriteOperation>,
) -> StorageResult<()> {
    let mut placeholder_allocations = HashMap::new();
    for operation in operations {
        match operation {
            DirectWriteOperation::Put { key, value } => txn.put(&key, &value).map_err(|error| {
                StorageError::internal(&format!("put key-value failed: {error}"))
            })?,
            DirectWriteOperation::PutTemplate { template, value } => {
                let key =
                    materialize_rocksdb_template_key(txn, &template, &mut placeholder_allocations)?;
                txn.put(&key, &value).map_err(|error| {
                    StorageError::internal(&format!("put templated key-value failed: {error}"))
                })?;
            }
            DirectWriteOperation::Delete { key } => txn
                .delete(&key)
                .map_err(|error| StorageError::internal(&format!("delete key failed: {error}")))?,
            DirectWriteOperation::DeleteRange {
                start,
                exclusive_end,
            } => {
                for key in range_keys(db, &start, &exclusive_end)? {
                    txn.delete(&key).map_err(|error| {
                        StorageError::internal(&format!("delete range key failed: {error}"))
                    })?;
                }
            }
            DirectWriteOperation::CheckValue {
                key,
                expected_value,
            } => {
                let current = txn.get_for_update(key.as_slice(), true).map_err(|error| {
                    StorageError::internal(&format!(
                        "read key for exact value check failed: {error}"
                    ))
                })?;
                if current != expected_value {
                    return Err(StorageEnum::ConditionalCheckFailed.into());
                }
            }
        }
    }
    Ok(())
}

fn materialize_rocksdb_template_key(
    txn: &Transaction<OptimisticTransactionDB>,
    template: &crate::key_template::KeyTemplate,
    placeholder_allocations: &mut HashMap<crate::key_template::PlaceholderId, StreamItemId>,
) -> StorageResult<Vec<u8>> {
    let Some(binding) = template.placeholder_binding() else {
        return Ok(template.rocks_key());
    };
    let Ok(fallback_item_id) = StreamItemId::try_from(binding.fallback_value()) else {
        return Ok(template.rocks_key());
    };
    let allocated = match placeholder_allocations.get(&binding.id()) {
        Some(allocated) => *allocated,
        None => {
            let allocated = allocate_rocksdb_stream_item_id(txn, fallback_item_id)?;
            placeholder_allocations.insert(binding.id(), allocated);
            allocated
        }
    };
    Ok(template.rocks_key_with_fallback(allocated.as_bytes()))
}

fn map_rocksdb_transaction_commit(error: Error) -> StorageEnum {
    if error.kind() == ErrorKind::Busy {
        return StorageEnum::TransactionCanceled {
            reasons: vec!["TransactionConflict".to_string()],
        };
    }
    StorageEnum::TransactionConflict {
        message: "rocksdb transaction conflict".to_string(),
    }
}

fn is_rocksdb_transaction_retryable(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::TransactionCanceled { .. } | StorageEnum::TransactionConflict { .. }
    )
}

fn map_rocksdb_blocking_join_error(error: tokio::task::JoinError) -> StorageError {
    StorageError::internal(&format!("rocksdb blocking task failed: {error}"))
}

#[expect(clippy::needless_pass_by_value)]
fn generic_err(e: Error) -> StorageError {
    error!(error = e.to_string());
    StorageError::internal(&e.to_string())
}
