use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bg_jobs::{BackgroundJob, BackgroundJobName, JobConfig};
use storage_backfill::{BackfillConfig, BackfillCoordinator};
#[cfg(test)]
use storage_common::provider_perf;
use storage_common::{
    GSI_BACKFILL_JOB, GsiJobConfig, JobIntervalMillis, RegistersJobs, STREAM_TRIM_JOB,
    apply_gsi_write_pressure as apply_shared_gsi_write_pressure, register_gsi_jobs,
};
use storage_condition::{Condition, parse_condition_expression};
use storage_provider::{
    StorageProvider, UpdateOperation, parse_update_expression, return_values_need_old_item,
    update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, BatchWriteItemResponse, CreateTableRequest,
    DeleteRequest, DescribeTimeToLiveResponse, DurablePointReadProof, DurablePointReadRequest,
    EncodeWriteRequest, IndexName, ItemKey, ItemStreamVersion, ItemVersionedWireItem,
    KeyAttributeType, KeyAttributes, KeysAndAttributes, Projection, PutItemResponse, PutRequest,
    QueryTableRequest, ReplicationMutation, ScanTableRequest, SerializesToKey, StorageEnum,
    StorageError, StorageResult, StorageValidationKind, StoredTableInfo, StreamItemId, StreamName,
    TableName, TableStatus, TimeToLiveDescription, TimeToLiveStatus, TimestampMillis,
    TransactWriteItem, TransactWriteItemsEncodeRequest, TransactWriteItemsRequest,
    TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse, UpdateTimeToLiveRequest,
    UpdateTimeToLiveResponse, WireItem, WriteRequest,
    attribute_map_numbers_need_write_normalization, context::WrappedError as _,
    normalize_attribute_map_numbers_for_write,
};
use tracing::{Span, instrument, warn};

use crate::{
    storage_ops::constants::{IDEMPOTENCY_TOKEN_TTL_MS, REPLICATION_APPLY_PARALLELISM_HINT},
    storage_provider::{GsiBackfillJob, GsiUpdateJob},
};

type QueryItemsPage = (Vec<WireItem>, Option<String>);
static NEXT_STREAM_ITEM_VERSION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn now_ms_u64() -> u64 {
    let now = *TimestampMillis::now();
    u64::try_from(now).unwrap_or(0)
}

fn next_stream_item_id() -> StreamItemId {
    let now_component = now_ms_u64().checked_shl(20).unwrap_or(u64::MAX);
    let mut observed = NEXT_STREAM_ITEM_VERSION.load(Ordering::Relaxed);
    loop {
        let candidate = now_component.max(observed.saturating_add(1));
        match NEXT_STREAM_ITEM_VERSION.compare_exchange_weak(
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

pub(crate) fn should_log_job(last_log_ms: &AtomicU64, now_ms: u64, interval_ms: u64) -> bool {
    let last = last_log_ms.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= interval_ms {
        last_log_ms.store(now_ms, Ordering::Relaxed);
        true
    } else {
        false
    }
}

async fn apply_gsi_write_pressure<S: crate::partition_family::PartitionFamilyKvStore + 'static>(
    provider: &SortedKvDbStorageProvider<S>,
) -> StorageResult<()> {
    apply_shared_gsi_write_pressure(
        provider.immediate_gsi_consistency,
        &provider.gsi_propagation_governor,
        now_ms_u64(),
    )
    .await
}

pub(crate) fn record_read(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_returned", items as u64);
    span.record("bytes_read", bytes as u64);
}

pub(crate) fn record_write(items: usize, bytes: usize) {
    let span = Span::current();
    span.record("items_updated", items as u64);
    span.record("bytes_written", bytes as u64);
}

pub(crate) fn compute_items_bytes(
    items: &[HashMap<String, AttributeValue>],
) -> StorageResult<usize> {
    let mut total = 0_usize;
    for item in items {
        total += storage_types::storage_serde::to_bytes(item)?.len();
    }
    Ok(total)
}

pub(crate) fn normalized_attribute_map_for_write(
    item: &HashMap<String, AttributeValue>,
) -> Cow<'_, HashMap<String, AttributeValue>> {
    if !attribute_map_numbers_need_write_normalization(item) {
        return Cow::Borrowed(item);
    }

    let mut normalized = item.clone();
    normalize_attribute_map_numbers_for_write(&mut normalized);
    Cow::Owned(normalized)
}

pub(crate) fn normalized_wire_item_for_write(item: &WireItem) -> StorageResult<Cow<'_, WireItem>> {
    if let WireItem::DynamoJson { data } = item
        && !data.iter().any(|byte| matches!(byte, b'e' | b'E'))
    {
        return Ok(Cow::Borrowed(item));
    }

    let mut attributes = item.to_attribute_map()?;
    if !normalize_attribute_map_numbers_for_write(&mut attributes) {
        return Ok(Cow::Borrowed(item));
    }

    Ok(Cow::Owned(WireItem::from_attribute_map(&attributes)?))
}

pub(crate) fn encode_requests_to_write_requests(
    requests: &[EncodeWriteRequest],
) -> StorageResult<Vec<WriteRequest>> {
    requests
        .iter()
        .map(|request| match request {
            EncodeWriteRequest {
                put_request: Some(put_request),
                delete_request: None,
            } => Ok(WriteRequest {
                put_request: Some(PutRequest {
                    item: put_request.item.clone().into_attribute_map()?,
                }),
                delete_request: None,
            }),
            EncodeWriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest { key }),
            } => Ok(WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest { key: key.clone() }),
            }),
            _ => Err(StorageError::validation(
                "Each WriteRequest must contain exactly one of PutRequest or DeleteRequest",
            )),
        })
        .collect()
}

fn is_conditional_only_transaction_cancel(reasons: &[String]) -> bool {
    let mut saw_conditional_check_failed = false;

    for reason in reasons {
        if reason == "ConditionalCheckFailed" {
            saw_conditional_check_failed = true;
            continue;
        }
        if reason == "None" {
            continue;
        }
        return false;
    }

    saw_conditional_check_failed
}

pub(crate) fn normalize_conditional_transaction_error(error: StorageError) -> StorageError {
    if let StorageEnum::TransactionCanceled { reasons } = error.to_enum()
        && is_conditional_only_transaction_cancel(reasons)
    {
        return StorageEnum::ConditionalCheckFailed.into();
    }
    error
}

pub(crate) fn encode_wire_item_storage_bytes(item: &WireItem) -> StorageResult<Vec<u8>> {
    match item {
        WireItem::DynamoJson { data } => {
            Ok(storage_types::storage_serde::compress_json_bytes(data))
        }
        WireItem::LocalSplit { .. } => {
            let map = item.to_attribute_map()?;
            storage_types::storage_serde::to_bytes(&map)
        }
    }
}

pub(crate) fn decode_wire_item_from_storage_bytes(bytes: &[u8]) -> StorageResult<WireItem> {
    let json = storage_types::storage_serde::decompress_bytes(bytes)?;
    Ok(WireItem::dynamo_json(json))
}

fn key_attribute_type_for_name(
    table_info: &StoredTableInfo,
    attribute_name: &str,
) -> StorageResult<KeyAttributeType> {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == attribute_name)
        .map(|definition| definition.attribute_type.clone())
        .ok_or_else(|| {
            StorageError::internal(&format!(
                "missing key attribute definition for {attribute_name}"
            ))
        })
}

fn key_attribute_value_from_scalar(
    value: &str,
    attribute_type: KeyAttributeType,
) -> AttributeValue {
    match attribute_type {
        KeyAttributeType::S => AttributeValue::S(value.to_string()),
        KeyAttributeType::N => AttributeValue::N(value.to_string()),
        KeyAttributeType::B => AttributeValue::B(value.to_string()),
    }
}

pub(crate) fn project_wire_item_table_key_and_ttl(
    item: &WireItem,
    table_info: &StoredTableInfo,
    ttl_attribute: Option<&str>,
) -> StorageResult<(ItemKey, Option<i64>)> {
    // Shortcut: for primary writes and TTL index maintenance we only need
    // table key attributes plus optional TTL attribute.
    // DynamoDB business rule: primary key drives item identity, TTL index key
    // is derived from (ttl, primary_key_token), so parsing the full item map is
    // unnecessary work.
    let hash_key = table_info
        .key_schema
        .iter()
        .find(|key| key.key_type == storage_types::KeyType::Hash)
        .ok_or_else(|| StorageError::internal("missing hash key in table schema"))?;
    let range_key = table_info
        .key_schema
        .iter()
        .find(|key| key.key_type == storage_types::KeyType::Range);

    let mut fields = Vec::with_capacity(2 + usize::from(ttl_attribute.is_some()));
    if let Some(ttl_attribute) = ttl_attribute {
        fields.push(ttl_attribute);
    }
    // Projection order is fixed so we can parse once and index into the result
    // without allocating intermediary attribute maps.
    fields.push(hash_key.attribute_name.as_str());
    if let Some(range_key) = range_key {
        fields.push(range_key.attribute_name.as_str());
    }

    let values = item.scalar_attributes(&fields)?;
    let mut index = 0usize;

    let ttl_value = if ttl_attribute.is_some() {
        let value = values[index]
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok());
        index += 1;
        value
    } else {
        None
    };

    let hash_scalar = values[index]
        .as_deref()
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    index += 1;

    let hash_attribute_type = key_attribute_type_for_name(table_info, &hash_key.attribute_name)?;
    let hash_attribute = key_attribute_value_from_scalar(hash_scalar, hash_attribute_type);

    let range_attribute = if let Some(range_key) = range_key {
        let range_scalar = values[index]
            .as_deref()
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let range_attribute_type =
            key_attribute_type_for_name(table_info, &range_key.attribute_name)?;
        Some(key_attribute_value_from_scalar(
            range_scalar,
            range_attribute_type,
        ))
    } else {
        None
    };

    let item_key = ItemKey::table_key(
        table_info.table_name.clone(),
        hash_attribute,
        range_attribute,
    );
    Ok((item_key, ttl_value))
}

pub(crate) fn wire_item_key_token_from_item_key(item_key: &ItemKey) -> StorageResult<String> {
    item_key
        .next_page_token()
        .map_err(|err| StorageError::internal(&format!("wire item key token build failed: {err}")))
}

fn ttl_index_key_for_wire_item_with_token(
    table_name: &TableName,
    ttl_attribute: &str,
    key_token: &str,
    item: &WireItem,
) -> StorageResult<Option<Vec<u8>>> {
    let ttl_value = storage_common::ttl::ttl_value_from_wire_item(item, ttl_attribute)?;
    Ok(ttl_value.map(|ttl| storage_common::ttl::ttl_index_key(table_name, ttl, key_token)))
}

pub(crate) fn ttl_tracking_enabled(config: Option<&TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        )
    })
}

pub(crate) fn ttl_index_direct_operations_for_wire_items(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    ttl_config: Option<&TtlConfigRecord>,
    old_item: Option<&WireItem>,
    new_item: Option<&WireItem>,
    new_item_key_token: Option<&str>,
    new_item_ttl_value: Option<i64>,
) -> StorageResult<Vec<TransactWriteOperation>> {
    let Some(config) = ttl_config else {
        return Ok(Vec::new());
    };
    if !matches!(
        config.status,
        TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
    ) {
        return Ok(Vec::new());
    }

    let old_key = if let Some(item) = old_item {
        storage_common::ttl::ttl_index_key_for_wire_item(
            table_name,
            table_info,
            &config.attribute_name,
            item,
        )?
    } else {
        None
    };
    let new_key = if let Some(item) = new_item {
        // Shortcut: when caller already projected key token and TTL value from
        // the wire payload, reuse them to avoid reparsing wire JSON.
        // This preserves TTL behavior because TTL key shape is deterministic:
        // "__ttl-index/<table>/<ttl>/<primary_key_token>".
        if let Some(token) = new_item_key_token {
            if let Some(ttl) = new_item_ttl_value {
                Some(storage_common::ttl::ttl_index_key(table_name, ttl, token))
            } else {
                ttl_index_key_for_wire_item_with_token(
                    table_name,
                    &config.attribute_name,
                    token,
                    item,
                )?
            }
        } else {
            storage_common::ttl::ttl_index_key_for_wire_item(
                table_name,
                table_info,
                &config.attribute_name,
                item,
            )?
        }
    } else {
        None
    };

    if old_key.is_some() && old_key == new_key {
        // Business rule: unchanged TTL index key means the item stays in the
        // same expiration bucket, so no index mutation is required.
        return Ok(Vec::new());
    }

    let mut operations = Vec::new();
    if let Some(key) = old_key {
        operations.push(TransactWriteOperation::Delete {
            key,
            condition: None,
        });
    }
    if let Some(key) = new_key {
        operations.push(TransactWriteOperation::Put {
            key,
            value: Vec::new(),
            condition: None,
        });
    }
    Ok(operations)
}

pub(crate) fn record_query_result(result: QueryItemsPage) -> QueryItemsPage {
    let (items, lek) = result;
    let bytes = wire_items_payload_bytes(&items);
    record_read(items.len(), bytes as usize);
    record_read_cost("query_table", "query", 1, bytes);
    (items, lek)
}

pub(crate) fn record_provider_stage(
    operation: &'static str,
    stage: &'static str,
    elapsed: Duration,
) {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    if let Some(handles) = provider_stage_metric_handles(operation, stage) {
        handles.total.increment(1);
        handles.latency_ms.record(elapsed_ms);
        return;
    }
    metrics_facade::counter!(
        constants::STORAGE_PROVIDER_STAGE_TOTAL_METRIC,
        "operation" => operation,
        "stage" => stage,
    )
    .increment(1);
    metrics_facade::histogram!(
        constants::STORAGE_PROVIDER_STAGE_LATENCY_MS_METRIC,
        "operation" => operation,
        "stage" => stage,
    )
    .record(elapsed_ms);
}

struct ProviderStageMetricHandles {
    total: metrics::Counter,
    latency_ms: metrics::Histogram,
}

static PROVIDER_STAGE_METRICS: LazyLock<[ProviderStageMetricHandles; 5]> = LazyLock::new(|| {
    [
        provider_stage_metric("batch_get_item", "decode"),
        provider_stage_metric("batch_get_item", "fdb_wait"),
        provider_stage_metric("batch_get_item", "response_materialization"),
        provider_stage_metric("query", "decode"),
        provider_stage_metric("query", "fdb_wait"),
    ]
});

fn provider_stage_metric(
    operation: &'static str,
    stage: &'static str,
) -> ProviderStageMetricHandles {
    ProviderStageMetricHandles {
        total: metrics::counter!(
            constants::STORAGE_PROVIDER_STAGE_TOTAL_METRIC.name(),
            "operation" => operation,
            "stage" => stage,
        ),
        latency_ms: metrics::histogram!(
            constants::STORAGE_PROVIDER_STAGE_LATENCY_MS_METRIC.name(),
            "operation" => operation,
            "stage" => stage,
        ),
    }
}

fn provider_stage_metric_handles(
    operation: &'static str,
    stage: &'static str,
) -> Option<&'static ProviderStageMetricHandles> {
    let index = match (operation, stage) {
        ("batch_get_item", "decode") => 0,
        ("batch_get_item", "fdb_wait") => 1,
        ("batch_get_item", "response_materialization") => 2,
        ("query", "decode") => 3,
        ("query", "fdb_wait") => 4,
        _ => return None,
    };
    Some(&PROVIDER_STAGE_METRICS[index])
}

use storage_common::ttl::TtlConfigRecord;

use crate::{
    SortedKvDbStorageProvider,
    backends::common::KvMutation,
    billing_metrics::{
        WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
        serializable_payload_bytes, wire_items_payload_bytes,
    },
    constants,
    helpers::increment_bytes,
    keys::{TABLES_PREFIX, table_metadata_key},
    newtypes::TablePageKey,
    sorted_kv_store::{
        BatchItem, DirectWriteOperation, TransactWriteOperation, TransactWriteTableOperation,
    },
    ttl,
};

fn to_direct_write_operation(
    operation: TransactWriteOperation,
) -> StorageResult<DirectWriteOperation> {
    match operation {
        TransactWriteOperation::Put {
            key,
            value,
            condition,
        } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::Put { key, value })
        }
        TransactWriteOperation::PutTemplate {
            template,
            value,
            condition,
        } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::PutTemplate { template, value })
        }
        TransactWriteOperation::Delete { key, condition } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::Delete { key })
        }
        TransactWriteOperation::CheckValue {
            key,
            expected_value,
        } => Ok(DirectWriteOperation::CheckValue {
            key,
            expected_value,
        }),
        TransactWriteOperation::Check { .. } | TransactWriteOperation::Update { .. } => {
            Err(StorageError::validation(
                "direct write operation requires put/delete or exact-value checks only",
            ))
        }
    }
}

/// Apply a GSI projection while ensuring base table primary key attributes are
/// always present. `DynamoDB` behavior: `KeysOnly` includes table primary key +
/// index key attributes. Include adds the specified non-key attributes plus all
/// key attributes (table + index). All returns the full item.

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimestampedIdempotencyResponse {
    response: TransactWriteItemsResponse,
    created_at: TimestampMillis,
    expires_at: TimestampMillis,
}

struct TransactUpdateBindingCacheEntry {
    update_expression: String,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    operations: Arc<[UpdateOperation]>,
    condition: Option<Condition>,
}

impl TransactUpdateBindingCacheEntry {
    fn matches(
        &self,
        update_expression: &str,
        condition_expression: Option<&str>,
        expression_attribute_names: Option<&HashMap<String, String>>,
        expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    ) -> bool {
        self.update_expression == update_expression
            && self.condition_expression.as_deref() == condition_expression
            && self.expression_attribute_names.as_ref() == expression_attribute_names
            && self.expression_attribute_values.as_ref() == expression_attribute_values
    }
}

pub(crate) struct TransactConditionBindingCacheEntry {
    condition_expression: String,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    condition: Condition,
}

impl TransactConditionBindingCacheEntry {
    fn matches(
        &self,
        condition_expression: &str,
        expression_attribute_names: Option<&HashMap<String, String>>,
        expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    ) -> bool {
        self.condition_expression == condition_expression
            && self.expression_attribute_names.as_ref() == expression_attribute_names
            && self.expression_attribute_values.as_ref() == expression_attribute_values
    }
}

pub(crate) fn cached_transact_condition_binding(
    cache: &mut Vec<TransactConditionBindingCacheEntry>,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
) -> StorageResult<Option<Condition>> {
    let Some(condition_expression) = condition_expression else {
        return Ok(None);
    };

    if let Some(entry) = cache.iter().find(|entry| {
        entry.matches(
            condition_expression.as_str(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
    }) {
        return Ok(Some(entry.condition.clone()));
    }

    let condition = parse_condition_expression(
        condition_expression.as_str(),
        expression_attribute_names.as_ref(),
        expression_attribute_values.as_ref(),
    )
    .map_err(StorageError::validation)?;
    cache.push(TransactConditionBindingCacheEntry {
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
        condition: condition.clone(),
    });
    Ok(Some(condition))
}

fn cached_transact_update_binding(
    cache: &mut Vec<TransactUpdateBindingCacheEntry>,
    update_expression: String,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
) -> StorageResult<(Arc<[UpdateOperation]>, Option<Condition>)> {
    if let Some(entry) = cache.iter().find(|entry| {
        entry.matches(
            update_expression.as_str(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
    }) {
        return Ok((Arc::clone(&entry.operations), entry.condition.clone()));
    }

    let operations = parse_update_expression(
        update_expression.as_str(),
        expression_attribute_names.as_ref(),
        expression_attribute_values.as_ref(),
    )?;
    let condition = if let Some(condition_expression) = condition_expression.as_deref() {
        Some(
            parse_condition_expression(
                condition_expression,
                expression_attribute_names.as_ref(),
                expression_attribute_values.as_ref(),
            )
            .map_err(StorageError::validation)?,
        )
    } else {
        None
    };
    let operations = Arc::<[UpdateOperation]>::from(operations);
    cache.push(TransactUpdateBindingCacheEntry {
        update_expression,
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
        operations: Arc::clone(&operations),
        condition: condition.clone(),
    });
    Ok((operations, condition))
}

#[async_trait]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> StorageProvider
    for SortedKvDbStorageProvider<S>
{
    async fn run_job(&self, name: BackgroundJobName) -> StorageResult<()> {
        match name {
            storage_common::GSI_UPDATE_JOB => {
                if self.immediate_gsi_consistency {
                    return Ok(());
                }
                loop {
                    let progressed = self.process_gsi_updates().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            storage_common::GSI_BACKFILL_JOB => {
                let coordinator = BackfillCoordinator::new(
                    std::sync::Arc::new(self.clone()),
                    BackfillConfig::default(),
                );
                loop {
                    let progressed = self.process_gsi_backfills_with(&coordinator).await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            storage_common::TTL_SWEEP_JOB => {
                loop {
                    let progressed = self.run_ttl_sweep().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            STREAM_TRIM_JOB => {
                loop {
                    let progressed = self.run_stream_trim().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            BackgroundJobName::Database {
                kind: bg_jobs::DatabaseJobKind::PartitionFamilyReconcile,
            } => {
                loop {
                    let progressed = self.run_partition_reconcile().await?;
                    if !progressed {
                        break;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    async fn initialize_storage(&self) -> StorageResult<()> {
        if !self.database_jobs_enabled {
            return Ok(());
        }

        // RocksDB doesn't need explicit table creation like SQLite; just register
        // background jobs.
        struct KvRegistrar<'a> {
            mgr: &'a bg_jobs::JobManager,
        }
        #[async_trait]
        impl RegistersJobs for KvRegistrar<'_> {
            type Error = StorageError;
            async fn register_timed_job<J>(
                &self,
                name: BackgroundJobName,
                interval_ms: JobIntervalMillis,
                job: J,
            ) -> Result<(), Self::Error>
            where
                J: BackgroundJob + 'static,
            {
                let config = JobConfig {
                    start_immediately: true,
                    sleep_duration: std::time::Duration::from_millis(interval_ms.0),
                    jitter_percent: 10,
                };
                self.mgr
                    .register_job(name, job, config)
                    .await
                    .map_err(|e| {
                        StorageError::internal(&format!("register job {name} failed: {e}"))
                    })?;
                Ok(())
            }
        }

        let registrar = KvRegistrar {
            mgr: &self.job_manager,
        };
        let gsi_cfg = GsiJobConfig::default();
        let update_job = GsiUpdateJob::new_with_interval(
            std::sync::Arc::new(self.clone()),
            gsi_cfg.update_interval_ms,
        );
        let backfill_job = GsiBackfillJob::new(std::sync::Arc::new(self.clone()));
        if self.immediate_gsi_consistency {
            registrar
                .register_timed_job(GSI_BACKFILL_JOB, gsi_cfg.backfill_interval_ms, backfill_job)
                .await
                .map_err(|e| {
                    StorageError::internal(&format!("register gsi backfill job failed: {e}"))
                })?;
        } else {
            register_gsi_jobs(&registrar, gsi_cfg, update_job, backfill_job)
                .await
                .map_err(|e| StorageError::internal(&format!("register gsi jobs failed: {e}")))?;
        }

        let ttl_job = crate::ttl::TtlSweepJob::new(std::sync::Arc::new(self.clone()));
        let ttl_interval_ms = JobIntervalMillis(constants::TTL_SWEEP_INTERVAL_MINUTES * 60_000);
        registrar
            .register_timed_job(storage_common::TTL_SWEEP_JOB, ttl_interval_ms, ttl_job)
            .await
            .map_err(|e| StorageError::internal(&format!("register ttl sweep job failed: {e}")))?;

        let trim_job = crate::stream::StreamTrimJob::new(std::sync::Arc::new(self.clone()));
        let trim_interval_ms = JobIntervalMillis(constants::STREAM_TRIM_INTERVAL_MINUTES * 60_000);
        registrar
            .register_timed_job(STREAM_TRIM_JOB, trim_interval_ms, trim_job)
            .await
            .map_err(|e| {
                StorageError::internal(&format!("register stream trim job failed: {e}"))
            })?;

        self.start_partition_reconcile_task().await?;
        Ok(())
    }

    async fn apply_resolved_sync_mutations(
        &self,
        metadata: storage_sync::SyncCommitMetadata,
        batch: storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
        super::resolved_sync_apply::apply_resolved_sync_mutations(self, metadata, batch).await
    }

    async fn last_resolved_sync_log_id(&self) -> StorageResult<Option<storage_sync::SyncLogId>> {
        super::resolved_sync_apply::last_resolved_sync_log_id(self).await
    }

    async fn persist_resolved_sync_log_entry(
        &self,
        metadata: &storage_sync::SyncCommitMetadata,
        batch: &storage_sync::ResolvedSyncMutationBatch,
    ) -> StorageResult<()> {
        super::resolved_sync_apply::persist_resolved_sync_log_entry(self, metadata, batch).await
    }

    async fn get_resolved_sync_log_entry(
        &self,
        log_id: storage_sync::SyncLogId,
    ) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
        super::resolved_sync_apply::get_resolved_sync_log_entry(self, log_id).await
    }

    async fn resolved_sync_log_entries_after(
        &self,
        log_id: Option<storage_sync::SyncLogId>,
        limit: usize,
    ) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
        super::resolved_sync_apply::resolved_sync_log_entries_after(self, log_id, limit).await
    }

    async fn scan_table_with_item_stream_versions(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        self.scan_table_with_item_stream_versions_impl(request)
            .await
    }

    async fn get_item_with_durable_proof(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        self.get_item_with_durable_proof_impl(request).await
    }

    async fn export_logical_backfill_page(
        &self,
        request: storage_backfill::LogicalExportRequest,
    ) -> StorageResult<storage_backfill::LogicalExportPage> {
        self.export_logical_page_impl(request).await
    }

    async fn import_logical_backfill_chunk(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: storage_backfill::LogicalBackfillChunk,
    ) -> StorageResult<storage_backfill::LogicalBackfillResult> {
        self.import_logical_chunk_impl(manifest, chunk).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "table_exists",
            table_name = %table_name,
        )
    )]
    async fn table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let key = table_metadata_key(table_name);

        Ok(self.kv_store.get(&key, true).await?.is_some())
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "create_table",
            table_name = %request.table_name,
        )
    )]
    async fn create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        let table_name = &request.table_name;

        storage_common::validate_create_table(request)?;

        if self.table_exists(table_name).await? {
            return Err(StorageError::table_already_exists(table_name));
        }

        let created_at = TimestampMillis::now();

        let global_secondary_indexes = request
            .global_secondary_indexes
            .clone()
            .map(|indexes| indexes.into_iter().map(Into::into).collect());

        let table_info = StoredTableInfo {
            table_name: table_name.clone(),
            table_status: TableStatus::Active,
            created_at,
            attribute_definitions: request.attribute_definitions.clone(),
            key_schema: request.key_schema.clone(),
            global_secondary_indexes,
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: request.stream_specification.clone(),
            deletion_protection_enabled: request.deletion_protection_enabled.unwrap_or(false),
        };

        let key = crate::keys::table_metadata_key(table_name);
        let value = storage_types::storage_serde::to_bytes(&table_info)?;

        self.kv_store.put(&key, &value, None).await?;

        self.update_table_status(table_name, TableStatus::Active)
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_table_status",
            table_name = %table_name,
        )
    )]
    async fn update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        let key = crate::keys::table_metadata_key(table_name);

        let existing_data = self
            .kv_store
            .get(&key, true)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let mut table_info: StoredTableInfo =
            storage_types::storage_serde::from_bytes(&existing_data)?;
        table_info.table_status = status.clone();

        let updated_value = storage_types::storage_serde::to_bytes(&table_info)?;
        self.kv_store.put(&key, &updated_value, None).await?;
        self.cache_table_metadata(table_name.clone(), Arc::new(table_info));

        Ok(())
    }

    async fn get_table_info(&self, table_name: &TableName) -> StorageResult<StoredTableInfo> {
        self.get_table_metadata_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))
    }

    #[instrument(skip_all, fields(feature = "storage", ddb_op = "list_tables",))]
    async fn list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        let prefix = TABLES_PREFIX.as_bytes();

        let range_end = increment_bytes(prefix.to_vec());
        let prefix_result = self
            .kv_store
            .get_range(
                prefix,
                &range_end,
                Some(limit),
                exclusive_start_table_name.map(Into::<TablePageKey>::into),
                true,
            )
            .await?;

        prefix_result
            .items
            .into_iter()
            .map(|(_k, v)| storage_types::storage_serde::from_bytes::<StoredTableInfo>(&v))
            .collect::<Result<Vec<_>, _>>()
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_table",
            table_name = %table_name,
        )
    )]
    async fn delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        let table_info = self
            .get_table_metadata_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        if table_info.deletion_protection_enabled {
            return Err(StorageError::deletion_protection_enabled(table_name));
        }

        let metadata_key = crate::keys::table_metadata_key(table_name);
        self.kv_store.delete(&metadata_key).await?;

        self.invalidate_table_metadata_cache(table_name);

        let data_prefix = ItemKey::all_table_prefix(&table_info.table_name);

        self.kv_store.delete_prefix(data_prefix).await?;
        self.delete_table_stream_storage(table_name).await?;
        let ttl_index_prefix = ttl::ttl_index_prefix(table_name);
        self.kv_store.delete_prefix(ttl_index_prefix).await?;
        self.delete_ttl_config(table_name).await?;

        Ok(())
    }

    async fn create_table_storage(
        &self,
        _table_name: &TableName,
        _request: &CreateTableRequest,
    ) -> StorageResult<()> {
        // RocksDB doesn't need separate table storage creation like SQLite
        // The table data structure is handled through key prefixes
        Ok(())
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "get_item",
            table_name = %table_name,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
        )
    )]
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        if key.is_empty() {
            record_read(0, 0);
            return Ok(None);
        }

        let table_info = self
            .get_table_metadata_from_name_arc(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;

        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)?
                .serialize_to_bytes()?;

        if let Some(data) = self.kv_store.get(&item_key, consistent_read).await? {
            record_read(1, data.len());
            let json = storage_types::storage_serde::decompress_owned_bytes(data)?;
            let item = WireItem::dynamo_json(json);
            record_read_cost("get_item", "get", 1, item.payload_len() as u64);
            Ok(Some(item))
        } else {
            record_read(0, 0);
            record_read_cost("get_item", "get", 1, 0);
            Ok(None)
        }
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "put_item",
            table_name = %table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn put_item(
        &self,
        table_name: TableName,
        mut item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        if item.is_empty() {
            return Err(StorageError::validation(
                "Item must have at least one attribute",
            ));
        }
        apply_gsi_write_pressure(self).await?;
        normalize_attribute_map_numbers_for_write(&mut item);
        let billed_bytes = attr_map_payload_bytes(&item);

        let table_info = self
            .get_table_metadata_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?; // single clone only on error path
        let ttl_config = self.load_ttl_config(&table_name).await?;

        let condition = if let Some(condition_expression) = condition_expression {
            Some(
                parse_condition_expression(
                    &condition_expression,
                    expression_attribute_names.as_ref(),
                    expression_attribute_values.as_ref(),
                )
                .map_err(|c| {
                    warn!(c);
                    StorageError::validation(StorageValidationKind::InvalidConditionExpression)
                })?,
            )
        } else {
            None
        };

        let bytes_written = compute_items_bytes(std::slice::from_ref(&item))?;

        let mut old_new_items = self
            .kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Put {
                    table_info,
                    item,
                    condition,
                    return_values_on_condition_check_failure: None,
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        let (old_item, _new_item) = old_new_items.pop().unwrap_or((None, None));

        record_write(1, bytes_written);
        record_write_cost("put_item", "put", 1, billed_bytes);

        let attributes = if let Some(return_values) = return_values
            && return_values == AllOld::AllOld
        {
            old_item
        } else {
            None
        };

        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "put_item",
            table_name = %table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn put_item_encode(
        &self,
        table_name: TableName,
        item: WireItem,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let table_info = self
            .get_table_metadata_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;

        let ttl_config = self.load_ttl_config(&table_name).await?;
        let should_write_stream = crate::backends::common::should_write_stream_entries(
            &table_info,
            self.requires_immediate_gsi_updates(&table_info),
        );
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let should_write_gsi_immediately = self.requires_immediate_gsi_updates(&table_info);
        let can_wire_native_path = condition_expression.is_none()
            && expression_attribute_names.is_none()
            && expression_attribute_values.is_none()
            && !matches!(return_values, Some(AllOld::AllOld))
            && !should_write_gsi_immediately;

        if can_wire_native_path {
            // Shortcut: take a wire-native put path only when no feature needs a
            // materialized AttributeValue map.
            // DynamoDB business rule:
            // - conditions require reading/evaluating current attributes
            // - ALL_OLD requires returning previous item image
            // If neither is requested, byte-preserving write is valid.
            let ttl_attribute = if should_track_ttl {
                ttl_config
                    .as_ref()
                    .map(|config| config.attribute_name.as_str())
            } else {
                None
            };
            let (item_key, projected_ttl_value) =
                project_wire_item_table_key_and_ttl(&item, &table_info, ttl_attribute)?;
            let item_key_bytes = item_key.serialize_to_bytes()?;
            let item_key_token = if should_track_ttl {
                Some(wire_item_key_token_from_item_key(&item_key)?)
            } else {
                None
            };
            let bytes = encode_wire_item_storage_bytes(&item)?;
            let bytes_written = bytes.len();
            if should_write_stream || should_track_ttl {
                // Load prior bytes once and fan out to stream old-image and TTL
                // diff logic. This keeps transactional side effects consistent
                // with DynamoDB write semantics while avoiding duplicate reads.
                let old_bytes = self.kv_store.get(&item_key_bytes, true).await?;
                let old_item = if should_track_ttl {
                    old_bytes
                        .as_deref()
                        .map(decode_wire_item_from_storage_bytes)
                        .transpose()?
                } else {
                    None
                };

                let mut operations = Vec::with_capacity(6);

                if should_write_stream {
                    let stream_item_id = next_stream_item_id();
                    // Stream side effects are written in the same transaction as
                    // the primary item put, so stream visibility matches the
                    // committed write boundary.
                    let stream_entries =
                        crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                            &table_name,
                            &item_key,
                            bytes.as_slice(),
                            old_bytes.as_deref(),
                            stream_item_id,
                            false,
                            None,
                        )?;
                    operations.extend(stream_entries.into_iter().map(|(template, value)| {
                        TransactWriteOperation::PutTemplate {
                            template,
                            value,
                            condition: None,
                        }
                    }));
                }

                if should_track_ttl {
                    let ttl_ops = ttl_index_direct_operations_for_wire_items(
                        &table_name,
                        &table_info,
                        ttl_config.as_ref(),
                        old_item.as_ref(),
                        Some(&item),
                        item_key_token.as_deref(),
                        projected_ttl_value,
                    )?;
                    operations.extend(ttl_ops);
                }

                operations.push(TransactWriteOperation::Put {
                    key: item_key_bytes,
                    value: bytes,
                    condition: None,
                });

                // Fast path shortcut: operations are unconditional puts/deletes
                // only, so we can skip transact planner/result materialization
                // and commit the direct mutations atomically.
                let direct_operations = operations
                    .into_iter()
                    .map(to_direct_write_operation)
                    .collect::<StorageResult<Vec<_>>>()?;
                self.kv_store
                    .transact_write_unchecked(direct_operations)
                    .await?;
            } else {
                self.kv_store.put(&item_key_bytes, &bytes, None).await?;
            }
            record_write(1, bytes_written);
            record_write_cost("put_item", "put", 1, item.payload_len() as u64);
            return Ok(PutItemResponse { attributes: None });
        }

        let item = item.into_attribute_map()?;
        self.put_item(
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        )
        .await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "delete_item",
            table_name = %table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn delete_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        if key.is_empty() {
            record_write(0, 0);
            return Ok(None);
        }
        apply_gsi_write_pressure(self).await?;
        let billed_bytes = attr_map_payload_bytes(&key);
        let table_info = self
            .get_table_metadata_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let ttl_config = self.load_ttl_config(&table_name).await?;

        let condition = if let Some(condition_expression) = condition_expression {
            Some(
                parse_condition_expression(
                    &condition_expression,
                    expression_attribute_names.as_ref(),
                    expression_attribute_values.as_ref(),
                )
                .map_err(|c| {
                    warn!(c);
                    StorageError::validation(StorageValidationKind::InvalidConditionExpression)
                })?,
            )
        } else {
            None
        };

        let mut old_new_items = self
            .kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_info,
                    key,
                    condition,
                    return_values_on_condition_check_failure: None,
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        let Some((old_item, _)) = old_new_items.pop() else {
            record_write(0, 0);
            record_write_cost("delete_item", "delete", 1, billed_bytes);
            return Ok(None);
        };
        record_write(usize::from(old_item.is_some()), 0);
        record_write_cost("delete_item", "delete", 1, billed_bytes);

        Ok(old_item)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "scan_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
        )
    )]
    async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let consistent_read = request.consistent_read;
        if consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        if let Some(idx) = request.index_name.as_ref() {
            Span::current().record("index_name", idx.to_string());
        }
        let table_name = request.table_name.clone();
        let table_info = self
            .get_table_metadata_from_name_arc(&request.table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;

        let data_prefix = if let Some(index_name) = &request.index_name {
            ItemKey::index_prefix_from_name(&table_info.table_name, index_name)
        } else {
            ItemKey::table_prefix_from_name(&table_info.table_name)
        };

        let page_token = request
            .exclusive_start_key
            .as_ref()
            .and_then(|token| {
                ItemKey::item_key_from_next_page_token(token, &table_info, &request.index_name).ok()
            })
            .flatten();
        let range_end = increment_bytes(data_prefix.clone());

        let prefix_result = self
            .kv_store
            .get_range_values(
                &data_prefix,
                &range_end,
                request.limit,
                page_token,
                consistent_read,
            )
            .await?;

        let has_more_items = prefix_result.has_more;
        let mut result_items = Vec::new();
        let mut bytes_read = 0_usize;
        for data in prefix_result.values {
            bytes_read += data.len();
            let json = storage_types::storage_serde::decompress_bytes(&data)?;
            result_items.push(WireItem::dynamo_json(json));
        }

        record_read(result_items.len(), bytes_read);
        record_read_cost(
            "scan_table",
            "scan",
            1,
            wire_items_payload_bytes(&result_items),
        );

        let last_evaluated_key = if has_more_items && !result_items.is_empty() {
            result_items
                .last()
                .ok_or_else(|| StorageError::internal("missing last scan result item"))?
                .last_evaluated_key(&table_info, &request.index_name)?
        } else {
            None
        };

        Ok((result_items, last_evaluated_key))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "query_table",
            table_name = %request.table_name,
            index_name = tracing::field::Empty,
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
        )
    )]
    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_impl(request).await
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        if should_write_to_stream {
            let mapped = BatchWriteItemEncodeRequest::try_from(request)?;
            return self.batch_write_item_encode(mapped, true).await;
        }
        apply_gsi_write_pressure(self).await?;
        let mut requested_tally = WriteCostTally::default();
        for write_requests in request.request_items.values() {
            for write_request in write_requests {
                requested_tally.record_write_request(write_request);
            }
        }

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;
        let mut unprocessed_items: HashMap<TableName, Vec<WriteRequest>> = HashMap::new();

        for (table_name, write_requests) in &request.request_items {
            let table_metadata = match self.get_table_metadata_from_name(table_name).await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => {
                    return Err(StorageError::table_not_found(table_name));
                }
                Err(_e) => {
                    self.handle_batch_write_error(
                        table_name,
                        write_requests,
                        &mut unprocessed_items,
                    )?;
                    continue;
                }
            };
            let ttl_config = self.load_ttl_config(table_name).await?;
            let ttl_config = ttl_config.filter(|cfg| {
                matches!(
                    cfg.status,
                    TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
                )
            });

            let mut batch_items = Vec::new();
            let mut unprocessed_table_items = Vec::new();
            let mut table_items_updated = 0usize;
            let mut table_bytes_written = 0usize;
            let requires_immediate_gsi_updates =
                self.requires_immediate_gsi_updates(&table_metadata);
            let needs_existing_items =
                ttl_config.is_some() || should_write_to_stream || requires_immediate_gsi_updates;
            let existing_items = if needs_existing_items {
                match self
                    .batch_existing_items_for_write_requests(
                        table_name,
                        &table_metadata,
                        write_requests,
                    )
                    .await
                {
                    Ok(items) => items,
                    Err(e)
                        if matches!(e.to_enum(), StorageEnum::Validation { .. })
                            || matches!(e.to_enum(), StorageEnum::KeyValidation(_)) =>
                    {
                        return Err(e);
                    }
                    Err(_) => {
                        Self::collect_unprocessed_batch_items(
                            write_requests.to_vec(),
                            table_name,
                            &mut unprocessed_items,
                        );
                        continue;
                    }
                }
            } else {
                vec![None; write_requests.len()]
            };

            for (index, write_request) in write_requests.iter().enumerate() {
                match write_request {
                    WriteRequest {
                        put_request: Some(PutRequest { item }),
                        delete_request: None,
                    } => {
                        let item = normalized_attribute_map_for_write(item);
                        match Self::prepare_batch_put_item(
                            table_name,
                            &table_metadata,
                            item.as_ref(),
                            should_write_to_stream,
                            existing_items[index].as_ref(),
                            requires_immediate_gsi_updates,
                        ) {
                            Ok(mut items) => {
                                let ttl_mutations = Self::ttl_index_mutations_for_items(
                                    table_name,
                                    &table_metadata,
                                    ttl_config.as_ref(),
                                    existing_items[index].as_ref(),
                                    Some(item.as_ref()),
                                )?;
                                if !ttl_mutations.is_empty() {
                                    items.extend(ttl_mutations);
                                }
                                table_items_updated += 1;
                                table_bytes_written +=
                                    compute_items_bytes(std::slice::from_ref(item.as_ref()))?;
                                batch_items.append(&mut items);
                            }
                            Err(e)
                                if matches!(e.to_enum(), StorageEnum::Validation { .. })
                                    || matches!(e.to_enum(), StorageEnum::KeyValidation(_))
                                    || matches!(e.to_enum(), StorageEnum::TableNotFound { .. }) =>
                            {
                                return Err(e);
                            }
                            Err(_) => unprocessed_table_items.push(write_request.clone()),
                        }
                    }
                    WriteRequest {
                        put_request: None,
                        delete_request: Some(DeleteRequest { key }),
                    } => {
                        match Self::prepare_batch_delete_item(
                            table_name,
                            &table_metadata,
                            key,
                            should_write_to_stream,
                            existing_items[index].as_ref(),
                            requires_immediate_gsi_updates,
                        ) {
                            Ok(mut items) => {
                                let ttl_mutations = Self::ttl_index_mutations_for_items(
                                    table_name,
                                    &table_metadata,
                                    ttl_config.as_ref(),
                                    existing_items[index].as_ref(),
                                    None,
                                )?;
                                if !ttl_mutations.is_empty() {
                                    items.extend(ttl_mutations);
                                }
                                table_items_updated += 1;
                                batch_items.append(&mut items);
                            }
                            Err(e)
                                if matches!(e.to_enum(), StorageEnum::Validation { .. })
                                    || matches!(e.to_enum(), StorageEnum::KeyValidation(_))
                                    || matches!(e.to_enum(), StorageEnum::TableNotFound { .. }) =>
                            {
                                return Err(e);
                            }
                            Err(_) => unprocessed_table_items.push(write_request.clone()),
                        }
                    }
                    _ => {
                        unprocessed_table_items.push(write_request.clone());
                    }
                }
            }

            if !batch_items.is_empty() {
                match self.kv_store.batch_write(batch_items).await {
                    Ok(()) => {
                        total_items_updated += table_items_updated;
                        total_bytes_written += table_bytes_written;
                    }
                    Err(_e) => {
                        // Instead of cloning the entire slice again, reuse individual cloned items
                        // already in write_requests
                        unprocessed_table_items.extend(write_requests.iter().cloned());
                    }
                }
            }

            Self::collect_unprocessed_batch_items(
                unprocessed_table_items,
                table_name,
                &mut unprocessed_items,
            );
        }

        let response = BatchWriteItemResponse {
            unprocessed_items: if unprocessed_items.is_empty() {
                None
            } else {
                Some(unprocessed_items)
            },
            item_collection_metrics: None,
            consumed_capacity: None,
        };

        record_write(total_items_updated, total_bytes_written);
        let mut unprocessed_tally = WriteCostTally::default();
        if let Some(unprocessed_items) = response.unprocessed_items.as_ref() {
            for write_requests in unprocessed_items.values() {
                for write_request in write_requests {
                    unprocessed_tally.record_write_request(write_request);
                }
            }
        }
        requested_tally
            .subtract(&unprocessed_tally)
            .emit("batch_write_item");

        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_write_item",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
        should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        if !should_write_to_stream {
            let mapped = BatchWriteItemRequest::try_from(request)?;
            return self.batch_write_item(mapped, false).await;
        }
        apply_gsi_write_pressure(self).await?;
        let mut requested_tally = WriteCostTally::default();
        for write_requests in request.request_items.values() {
            for write_request in write_requests {
                requested_tally.record_encode_write_request(write_request);
            }
        }

        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;
        let mut unprocessed_items: HashMap<TableName, Vec<WriteRequest>> = HashMap::new();

        for (table_name, write_requests) in &request.request_items {
            let table_info = self
                .get_table_metadata_from_name(table_name)
                .await?
                .ok_or_else(|| StorageError::table_not_found(table_name))?;
            let ttl_config = self.load_ttl_config(table_name).await?;
            if self.requires_immediate_gsi_updates(&table_info)
                && !ttl_tracking_enabled(ttl_config.as_ref())
                && write_requests.iter().all(|request| {
                    matches!(
                        request,
                        EncodeWriteRequest {
                            put_request: Some(_),
                            delete_request: None
                        }
                    )
                })
            {
                match self
                    .apply_batch_encode_put_items_immediate_gsi(
                        table_name,
                        &table_info,
                        write_requests,
                    )
                    .await
                {
                    Ok((items_updated, bytes_written)) => {
                        total_items_updated += items_updated;
                        total_bytes_written += bytes_written;
                    }
                    Err(e)
                        if matches!(e.to_enum(), StorageEnum::Validation { .. })
                            || matches!(e.to_enum(), StorageEnum::KeyValidation(_)) =>
                    {
                        return Err(e);
                    }
                    Err(_) => {
                        unprocessed_items.insert(
                            table_name.clone(),
                            encode_requests_to_write_requests(write_requests)?,
                        );
                    }
                }
                continue;
            }

            let mut unprocessed_table_items = Vec::new();

            for write_request in write_requests {
                match write_request {
                    EncodeWriteRequest {
                        put_request: Some(put_request),
                        delete_request: None,
                    } => {
                        match self
                            .apply_batch_encode_put_item(table_name, &put_request.item)
                            .await
                        {
                            Ok(item_bytes) => {
                                total_items_updated += 1;
                                total_bytes_written += item_bytes;
                            }
                            Err(e)
                                if matches!(e.to_enum(), StorageEnum::Validation { .. })
                                    || matches!(e.to_enum(), StorageEnum::KeyValidation(_)) =>
                            {
                                return Err(e);
                            }
                            Err(_) => {
                                unprocessed_table_items.push(WriteRequest {
                                    put_request: Some(PutRequest {
                                        item: put_request.item.clone().into_attribute_map()?,
                                    }),
                                    delete_request: None,
                                });
                            }
                        }
                    }
                    EncodeWriteRequest {
                        put_request: None,
                        delete_request: Some(DeleteRequest { key }),
                    } => match self.apply_batch_delete_item(table_name, key).await {
                        Ok(_) => {
                            total_items_updated += 1;
                        }
                        Err(e)
                            if matches!(e.to_enum(), StorageEnum::Validation { .. })
                                || matches!(e.to_enum(), StorageEnum::KeyValidation(_)) =>
                        {
                            return Err(e);
                        }
                        Err(_) => {
                            unprocessed_table_items.push(WriteRequest {
                                put_request: None,
                                delete_request: Some(DeleteRequest { key: key.clone() }),
                            });
                        }
                    },
                    _ => {
                        return Err(StorageError::validation(
                            "Each WriteRequest must contain exactly one of PutRequest or \
                             DeleteRequest",
                        ));
                    }
                }
            }

            if !unprocessed_table_items.is_empty() {
                unprocessed_items.insert(table_name.clone(), unprocessed_table_items);
            }
        }

        let response = BatchWriteItemResponse {
            unprocessed_items: if unprocessed_items.is_empty() {
                None
            } else {
                Some(unprocessed_items)
            },
            item_collection_metrics: None,
            consumed_capacity: None,
        };
        record_write(total_items_updated, total_bytes_written);
        let mut unprocessed_tally = WriteCostTally::default();
        if let Some(unprocessed_items) = response.unprocessed_items.as_ref() {
            for write_requests in unprocessed_items.values() {
                for write_request in write_requests {
                    unprocessed_tally.record_write_request(write_request);
                }
            }
        }
        requested_tally
            .subtract(&unprocessed_tally)
            .emit("batch_write_item");
        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "update_item",
            table_name = %request.table_name,
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn update_item(&self, request: UpdateItemRequest) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let billed_bytes = serializable_payload_bytes(&request);
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            ..
        } = request;
        let table_info = self
            .get_table_metadata_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let preserve_old_item = return_values_need_old_item(return_values.as_ref())
            || ttl_tracking_enabled(ttl_config.as_ref());

        let operations = if let Some(update_expression) = update_expression.as_deref() {
            storage_provider::parse_update_expression(
                update_expression,
                expression_attribute_names.as_ref(),
                expression_attribute_values.as_ref(),
            )?
        } else {
            Vec::new()
        };
        let condition = if let Some(condition_expression) = condition_expression.as_deref() {
            Some(
                parse_condition_expression(
                    condition_expression,
                    expression_attribute_names.as_ref(),
                    expression_attribute_values.as_ref(),
                )
                .map_err(StorageError::validation)?,
            )
        } else {
            None
        };
        let operations = Arc::<[storage_provider::UpdateOperation]>::from(operations);

        let mut last_retryable_error: Option<StorageError> = None;
        for _ in 0..10 {
            let table_info_for_write = table_info.clone();
            let condition = condition.clone();
            let result_old_new_items = self
                .kv_store
                .transact_write_table(
                    vec![TransactWriteTableOperation::Update {
                        table_info: table_info_for_write,
                        key: key.clone(),
                        operations: Arc::clone(&operations),
                        condition,
                        return_values_on_condition_check_failure:
                            return_values_on_condition_check_failure.clone(),
                        replication: None,
                        preserve_old_item,
                        transaction_validation: false,
                        ttl_config: ttl_config.clone(),
                    }],
                    self.immediate_gsi_consistency,
                )
                .await;

            match result_old_new_items {
                Ok(mut old_new_items) => {
                    let (old_item, new_item) = old_new_items.pop().unwrap_or((None, None));
                    let response = update_item_response(
                        &operations,
                        old_item.clone(),
                        new_item.clone(),
                        return_values.as_ref(),
                    )?;

                    let items_updated = new_item.as_ref().map_or(0, |_| 1);
                    let bytes_written = new_item
                        .as_ref()
                        .map(|item| compute_items_bytes(std::slice::from_ref(item)))
                        .transpose()? // Option<Result> -> Result<Option>
                        .unwrap_or(0);
                    record_write(items_updated, bytes_written);
                    record_write_cost("update_item", "update", items_updated, billed_bytes);

                    return Ok(response);
                }
                Err(e) => {
                    let normalized_error = normalize_conditional_transaction_error(e);
                    if matches!(
                        normalized_error.as_ref(),
                        StorageEnum::ConditionalCheckFailed
                            | StorageEnum::TransactionCanceled { .. }
                            | StorageEnum::InternalServerError { .. }
                    ) {
                        return Err(normalized_error);
                    }

                    // Retry only for remaining conflicts (optimistic
                    // concurrency) after backoff.
                    last_retryable_error = Some(normalized_error);
                }
            }
        }

        Err(last_retryable_error.unwrap_or_else(|| {
            StorageEnum::TransactionConflict {
                message: "TransactionConflict".to_string(),
            }
            .into()
        }))
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "batch_get_item",
            ddb_read = true,
            items_returned = tracing::field::Empty,
            bytes_read = tracing::field::Empty,
        )
    )]
    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let total_requested_keys: usize = request
            .request_items
            .values()
            .map(|item| item.keys.len())
            .sum();
        let mut total_items_returned = 0usize;
        let mut total_bytes_read = 0usize;
        let mut billed_bytes_read = 0usize;
        let mut responses: HashMap<TableName, Vec<WireItem>> =
            HashMap::with_capacity(request.request_items.len());
        let mut unprocessed_keys: HashMap<TableName, KeysAndAttributes> = HashMap::new();

        for (table_name, keys_and_attributes) in &request.request_items {
            if keys_and_attributes.keys.is_empty() {
                continue;
            }
            let consistent_read = keys_and_attributes.consistent_read.unwrap_or(false);

            let table_info = self
                .get_table_metadata_from_name_arc(table_name)
                .await?
                .ok_or_else(|| StorageError::table_not_found(table_name))?;

            let mut serialized_keys = Vec::with_capacity(keys_and_attributes.keys.len());

            for key in &keys_and_attributes.keys {
                let item_key = ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    key,
                )?;
                serialized_keys.push(item_key.serialize_to_bytes()?);
            }

            let fdb_wait_started = Instant::now();
            let multi_get_result = self
                .kv_store
                .multi_get(serialized_keys, consistent_read)
                .await;
            record_provider_stage("batch_get_item", "fdb_wait", fdb_wait_started.elapsed());
            let raw_results = match multi_get_result {
                Ok(results) => results,
                Err(e)
                    if matches!(e.to_enum(), StorageEnum::TableNotFound { .. })
                        || matches!(e.to_enum(), StorageEnum::KeyValidation { .. }) =>
                {
                    return Err(e);
                }
                Err(e) => {
                    unprocessed_keys.insert(
                        table_name.clone(),
                        KeysAndAttributes {
                            keys: keys_and_attributes.keys.clone(),
                            attributes_to_get: keys_and_attributes.attributes_to_get.clone(),
                            projection_expression: keys_and_attributes
                                .projection_expression
                                .clone(),
                            expression_attribute_names: keys_and_attributes
                                .expression_attribute_names
                                .clone(),
                            consistent_read: keys_and_attributes.consistent_read,
                        },
                    );
                    tracing::warn!(
                        error = %e,
                        table_name = %table_name,
                        "batch_get_item.multi_get_failed"
                    );
                    continue;
                }
            };

            let decode_started = Instant::now();
            let mut retrieved_items = Vec::with_capacity(raw_results.len());
            for raw in raw_results.into_iter().flatten() {
                total_bytes_read += raw.len();
                let json = storage_types::storage_serde::decompress_bytes(&raw)?;
                let item = WireItem::dynamo_json(json);
                billed_bytes_read += item.payload_len();
                retrieved_items.push(item);
            }
            record_provider_stage("batch_get_item", "decode", decode_started.elapsed());

            total_items_returned += retrieved_items.len();

            if !retrieved_items.is_empty() {
                responses.insert(table_name.clone(), retrieved_items);
            }
        }

        let materialize_started = Instant::now();
        let response = BatchGetWireItemResponse {
            responses: if responses.is_empty() {
                None
            } else {
                Some(responses)
            },
            unprocessed_keys: if unprocessed_keys.is_empty() {
                None
            } else {
                Some(unprocessed_keys)
            },
            consumed_capacity: None,
        };
        record_provider_stage(
            "batch_get_item",
            "response_materialization",
            materialize_started.elapsed(),
        );

        record_read(total_items_returned, total_bytes_read);
        record_read_cost(
            "batch_get_item",
            "get",
            total_requested_keys,
            billed_bytes_read as u64,
        );

        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "transact_write_items",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        apply_gsi_write_pressure(self).await?;
        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_item(item);
        }
        let mut total_items_updated = 0usize;
        let mut total_bytes_written = 0usize;
        if let Some(token) = &request.client_request_token {
            let token_key = format!("idempotency_token:{token}");

            if let Some(cached_data) = self.kv_store.get(token_key.as_bytes(), true).await?
                && let Ok(timestamped_response) = storage_types::storage_serde::from_bytes::<
                    TimestampedIdempotencyResponse,
                >(&cached_data)
            {
                let current_time = TimestampMillis::now();

                if current_time < timestamped_response.expires_at {
                    return Ok(timestamped_response.response);
                }
                let _ = self.kv_store.delete(token_key.as_bytes()).await;
            }
        }

        let mut operations = Vec::new();
        let mut table_info_cache: HashMap<TableName, StoredTableInfo> = HashMap::new();
        let mut ttl_config_cache: HashMap<TableName, Option<TtlConfigRecord>> = HashMap::new();
        let mut condition_binding_cache = Vec::<TransactConditionBindingCacheEntry>::new();
        let mut update_binding_cache = Vec::<TransactUpdateBindingCacheEntry>::new();

        for item in request.transact_items {
            let op = match item {
                TransactWriteItem {
                    put: Some(mut put_request),
                    ..
                } => {
                    let table_info = self
                        .get_table_metadata_cached(&mut table_info_cache, &put_request.table_name)
                        .await?;
                    let ttl_config = self
                        .load_ttl_config_cached(&mut ttl_config_cache, &put_request.table_name)
                        .await?;
                    total_items_updated += 1;
                    normalize_attribute_map_numbers_for_write(&mut put_request.item);
                    total_bytes_written +=
                        compute_items_bytes(std::slice::from_ref(&put_request.item))?;
                    TransactWriteTableOperation::Put {
                        table_info,
                        item: put_request.item,
                        condition: cached_transact_condition_binding(
                            &mut condition_binding_cache,
                            put_request.condition_expression,
                            put_request.expression_attribute_names,
                            put_request.expression_attribute_values,
                        )?,
                        return_values_on_condition_check_failure: put_request
                            .return_values_on_condition_check_failure,
                        replication: None,
                        ttl_config,
                    }
                }
                TransactWriteItem {
                    delete: Some(delete_request),
                    ..
                } => {
                    let table_info = self
                        .get_table_metadata_cached(
                            &mut table_info_cache,
                            &delete_request.table_name,
                        )
                        .await?;
                    let ttl_config = self
                        .load_ttl_config_cached(&mut ttl_config_cache, &delete_request.table_name)
                        .await?;
                    total_items_updated += 1;
                    TransactWriteTableOperation::Delete {
                        table_info,
                        key: delete_request.key,
                        condition: cached_transact_condition_binding(
                            &mut condition_binding_cache,
                            delete_request.condition_expression,
                            delete_request.expression_attribute_names,
                            delete_request.expression_attribute_values,
                        )?,
                        return_values_on_condition_check_failure: delete_request
                            .return_values_on_condition_check_failure,
                        replication: None,
                        ttl_config,
                    }
                }
                TransactWriteItem {
                    update: Some(update_request),
                    ..
                } => {
                    let storage_types::TransactUpdateRequest {
                        table_name,
                        key,
                        update_expression,
                        condition_expression,
                        expression_attribute_names,
                        expression_attribute_values,
                        return_values_on_condition_check_failure,
                    } = update_request;
                    let table_info = self
                        .get_table_metadata_cached(&mut table_info_cache, &table_name)
                        .await?;
                    let ttl_config = self
                        .load_ttl_config_cached(&mut ttl_config_cache, &table_name)
                        .await?;
                    total_items_updated += 1;
                    let preserve_old_item = ttl_tracking_enabled(ttl_config.as_ref());
                    let (operations, condition) = cached_transact_update_binding(
                        &mut update_binding_cache,
                        update_expression,
                        condition_expression,
                        expression_attribute_names,
                        expression_attribute_values,
                    )?;
                    TransactWriteTableOperation::Update {
                        table_info,
                        key,
                        operations,
                        condition,
                        return_values_on_condition_check_failure,
                        replication: None,
                        preserve_old_item,
                        transaction_validation: true,
                        ttl_config,
                    }
                }
                TransactWriteItem {
                    condition_check: Some(check_request),
                    ..
                } => {
                    let table_info = self
                        .get_table_metadata_cached(&mut table_info_cache, &check_request.table_name)
                        .await?;
                    TransactWriteTableOperation::Check {
                        table_info,
                        key: check_request.key,
                        condition: cached_transact_condition_binding(
                            &mut condition_binding_cache,
                            Some(check_request.condition_expression),
                            check_request.expression_attribute_names,
                            check_request.expression_attribute_values,
                        )?
                        .ok_or(StorageError::validation(
                            StorageValidationKind::InvalidConditionExpression,
                        ))?,
                        return_values_on_condition_check_failure: check_request
                            .return_values_on_condition_check_failure,
                    }
                }
                _ => {
                    return Err(StorageError::validation(
                        "Invalid Transact Write Item request",
                    ));
                }
            };

            operations.push(op);
        }

        // Execute all operations atomically using the KV store's transact_write
        self.kv_store
            .transact_write_table(operations, self.immediate_gsi_consistency)
            .await?;

        let response = TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        };

        record_write(total_items_updated, total_bytes_written);
        billed_tally.emit("transact_write_items");

        if let Some(token) = &request.client_request_token {
            let token_key = format!("idempotency_token:{token}");

            let current_time = TimestampMillis::now();
            let expires_at = current_time + IDEMPOTENCY_TOKEN_TTL_MS;

            let timestamped_response = TimestampedIdempotencyResponse {
                response: response.clone(),
                created_at: current_time,
                expires_at,
            };

            let response_bytes = storage_types::storage_serde::to_bytes(&timestamped_response)?;

            self.kv_store
                .put(token_key.as_bytes(), &response_bytes, None)
                .await?;
        }

        Ok(response)
    }

    #[instrument(
        skip_all,
        fields(feature = "storage",
            ddb_op = "transact_write_items",
            ddb_write = true,
            items_updated = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
        )
    )]
    async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        apply_gsi_write_pressure(self).await?;
        if request.transact_items.is_empty() {
            return Err(StorageError::validation(
                "Transaction request must contain at least one item",
            ));
        }
        if request.transact_items.len() > 100 {
            return Err(StorageError::validation(
                "Transaction request cannot contain more than 100 items",
            ));
        }

        if request.client_request_token.is_none() {
            let mut billed_tally = WriteCostTally::default();
            for item in &request.transact_items {
                billed_tally.record_transact_encode_item(item);
            }
            let mut operations = Vec::with_capacity(request.transact_items.len() * 4);
            let mut total_items_updated = 0usize;
            let mut total_bytes_written = 0usize;
            let mut can_fast_path = true;

            for item in &request.transact_items {
                let storage_types::TransactEncodeItem {
                    put,
                    update,
                    delete,
                    condition_check,
                } = item;

                if update.is_some() || delete.is_some() || condition_check.is_some() {
                    // Fast path intentionally supports only put-only, no-condition
                    // transactions. Mixed ops require full API-shape evaluation.
                    can_fast_path = false;
                    break;
                }

                let Some(put_request) = put.as_ref() else {
                    can_fast_path = false;
                    break;
                };

                if put_request.condition_expression.is_some()
                    || put_request.expression_attribute_names.is_some()
                    || put_request.expression_attribute_values.is_some()
                {
                    // Condition expressions require AttributeValue map semantics,
                    // so we fall back to the canonical path.
                    can_fast_path = false;
                    break;
                }

                let table_info = self
                    .get_table_metadata_from_name(&put_request.table_name)
                    .await?
                    .ok_or(StorageError::table_not_found(&put_request.table_name))?;
                let ttl_config = self.load_ttl_config(&put_request.table_name).await?;
                let should_write_stream = crate::backends::common::should_write_stream_entries(
                    &table_info,
                    self.requires_immediate_gsi_updates(&table_info),
                );
                let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
                if self.requires_immediate_gsi_updates(&table_info) {
                    can_fast_path = false;
                    break;
                }

                let ttl_attribute = if should_track_ttl {
                    ttl_config
                        .as_ref()
                        .map(|config| config.attribute_name.as_str())
                } else {
                    None
                };
                let (item_key, projected_ttl_value) = project_wire_item_table_key_and_ttl(
                    &put_request.item,
                    &table_info,
                    ttl_attribute,
                )?;
                let item_key_bytes = item_key.serialize_to_bytes()?;
                let item_key_token = if should_track_ttl {
                    Some(wire_item_key_token_from_item_key(&item_key)?)
                } else {
                    None
                };
                let value = encode_wire_item_storage_bytes(&put_request.item)?;

                let old_bytes = if should_write_stream || should_track_ttl {
                    // Same shortcut as single put: one current-image read feeds
                    // both stream old-image and TTL transition handling.
                    self.kv_store.get(&item_key_bytes, true).await?
                } else {
                    None
                };
                let old_item = if should_track_ttl {
                    old_bytes
                        .as_deref()
                        .map(decode_wire_item_from_storage_bytes)
                        .transpose()?
                } else {
                    None
                };

                total_items_updated += 1;
                total_bytes_written += value.len();

                if should_write_stream {
                    let stream_item_id = next_stream_item_id();
                    let stream_entries =
                        crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                            &put_request.table_name,
                            &item_key,
                            value.as_slice(),
                            old_bytes.as_deref(),
                            stream_item_id,
                            false,
                            None,
                        )?;
                    operations.extend(stream_entries.into_iter().map(|(template, value)| {
                        TransactWriteOperation::PutTemplate {
                            template,
                            value,
                            condition: None,
                        }
                    }));
                }

                if should_track_ttl {
                    let ttl_ops = ttl_index_direct_operations_for_wire_items(
                        &put_request.table_name,
                        &table_info,
                        ttl_config.as_ref(),
                        old_item.as_ref(),
                        Some(&put_request.item),
                        item_key_token.as_deref(),
                        projected_ttl_value,
                    )?;
                    operations.extend(ttl_ops);
                }

                operations.push(TransactWriteOperation::Put {
                    key: item_key_bytes,
                    value,
                    condition: None,
                });
            }

            if can_fast_path {
                // Same direct-write shortcut as put_item_encode: put-only encode
                // transactions do not need planner old/new image bookkeeping.
                let direct_operations = operations
                    .into_iter()
                    .map(to_direct_write_operation)
                    .collect::<StorageResult<Vec<_>>>()?;
                self.kv_store
                    .transact_write_unchecked(direct_operations)
                    .await?;
                let response = TransactWriteItemsResponse {
                    consumed_capacity: None,
                    item_collection_metrics: None,
                };
                record_write(total_items_updated, total_bytes_written);
                billed_tally.emit("transact_write_items");
                return Ok(response);
            }
        }

        let mapped = TransactWriteItemsRequest::try_from(request)?;
        self.transact_write_items(mapped).await
    }

    async fn update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let mut table_info = self.get_table_info(&table_name).await?;

        // Transition to UPDATING
        self.update_table_status(&table_name, TableStatus::Updating)
            .await?;

        // Apply StreamSpecification update if present
        if let Some(spec) = request.stream_specification.clone() {
            table_info.stream_specification = Some(spec);
            let key = crate::keys::table_metadata_key(&table_name);
            let value = storage_types::storage_serde::to_bytes(&table_info)?;
            self.kv_store.put(&key, &value, None).await?;
            self.cache_table_metadata(table_name.clone(), Arc::new(table_info.clone()));
        }

        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            table_info.deletion_protection_enabled = deletion_protection_enabled;
            let key = crate::keys::table_metadata_key(&table_name);
            let value = storage_types::storage_serde::to_bytes(&table_info)?;
            self.kv_store.put(&key, &value, None).await?;
            self.cache_table_metadata(table_name.clone(), Arc::new(table_info.clone()));
        }

        // Process GSI updates
        if let Some(gsi_updates) = request.global_secondary_index_updates.clone() {
            for gsi_update in gsi_updates {
                if let Some(create) = gsi_update.create {
                    // Validate not exists
                    if table_info
                        .global_secondary_indexes
                        .as_ref()
                        .is_some_and(|v| v.iter().any(|g| g.index_name == create.index_name))
                    {
                        return Err(StorageError::validation(format!(
                            "Global secondary index already exists: {}",
                            create.index_name
                        )));
                    }

                    let mut new_gsis = table_info
                        .global_secondary_indexes
                        .clone()
                        .unwrap_or_default();
                    new_gsis.push(storage_types::GlobalSecondaryIndex {
                        index_name: create.index_name.clone(),
                        key_schema: create.key_schema.clone(),
                        projection: create.projection.clone(),
                    });
                    table_info.global_secondary_indexes = Some(new_gsis);

                    let key = crate::keys::table_metadata_key(&table_name);
                    let value = storage_types::storage_serde::to_bytes(&table_info)?;
                    self.kv_store.put(&key, &value, None).await?;
                    self.cache_table_metadata(table_name.clone(), Arc::new(table_info.clone()));

                    // Capture stream tail and enqueue background backfill job
                    let tail = self
                        .kv_store
                        .get_prefix(&StreamName::system_table_stream(), false, Some(1), true)
                        .await?
                        .items
                        .first()
                        .map(|(k, _)| String::from_utf8_lossy(k).into_owned());
                    self.initialize_backfill_record(&table_name, &create.index_name, tail)
                        .await?;
                }

                if let Some(del) = gsi_update.delete {
                    // Remove from metadata
                    if let Some(mut gsis) = table_info.global_secondary_indexes.clone() {
                        gsis.retain(|g| g.index_name != del.index_name);
                        table_info.global_secondary_indexes =
                            if gsis.is_empty() { None } else { Some(gsis) };
                    }

                    let key = crate::keys::table_metadata_key(&table_name);
                    let value = storage_types::storage_serde::to_bytes(&table_info)?;
                    self.kv_store.put(&key, &value, None).await?;
                    self.cache_table_metadata(table_name.clone(), Arc::new(table_info.clone()));

                    let index_prefix =
                        ItemKey::index_prefix_from_name(&table_info.table_name, &del.index_name);
                    self.kv_store.delete_prefix(index_prefix).await?;
                }

                if let Some(_upd) = gsi_update.update {
                    // Throughput-only in our model; no-op
                }
            }
        }

        // Back to ACTIVE
        self.update_table_status(&table_name, TableStatus::Active)
            .await?;

        // Build response
        let resp = storage_types::UpdateTableResponse {
            table_description: storage_types::TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: TableStatus::Active,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions.clone(),
                key_schema: table_info.key_schema.clone(),
                table_size_bytes: table_info.table_size_bytes,
                item_count: table_info.item_count,
                table_arn: format!(
                    "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
                    table_info.table_name
                ),
                replicas: None,
                multi_region_consistency: None,
                billing_mode_summary: Some(storage_types::BillingModeSummary {
                    billing_mode: Some(storage_types::BillingMode::PayPerRequest),
                    last_update_to_pay_per_request_date_time: None,
                }),
                global_secondary_indexes: table_info.global_secondary_indexes.clone().map(
                    |indexes| {
                        indexes
                            .into_iter()
                            .map(|index| storage_types::GlobalSecondaryIndexDescription {
                                index_name: index.index_name,
                                key_schema: index.key_schema,
                                projection: index.projection,
                                index_status: None,
                                backfilling: None,
                                provisioned_throughput: None,
                                index_size_bytes: None,
                                item_count: None,
                                index_arn: None,
                            })
                            .collect()
                    },
                ),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification.clone(),
                latest_stream_arn: None,
                latest_stream_label: None,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        };

        Ok(resp)
    }

    async fn apply_replication_mutation(&self, mutation: ReplicationMutation) -> StorageResult<()> {
        let table_name = mutation.table_name.clone();
        let table_info = self
            .get_table_metadata_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let replication = Some(mutation.metadata);

        if let Some(mut new_image) = mutation.new_image {
            normalize_attribute_map_numbers_for_write(&mut new_image);
            self.kv_store
                .transact_write_table(
                    vec![TransactWriteTableOperation::Put {
                        table_info: table_info.clone(),
                        item: new_image,
                        condition: None,
                        return_values_on_condition_check_failure: None,
                        replication: replication.clone(),
                        ttl_config: ttl_config.clone(),
                    }],
                    self.immediate_gsi_consistency,
                )
                .await?;
            return Ok(());
        }

        self.kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_info,
                    key: mutation.key,
                    condition: None,
                    return_values_on_condition_check_failure: None,
                    replication,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await?;
        Ok(())
    }

    fn replication_apply_parallelism_hint(&self) -> usize {
        REPLICATION_APPLY_PARALLELISM_HINT
    }

    async fn update_time_to_live(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let UpdateTimeToLiveRequest {
            table_name,
            mut time_to_live_specification,
        } = request;

        let mut table_info = self.get_table_info(&table_name).await?;
        let enabled = time_to_live_specification.enabled;
        let attribute_name = time_to_live_specification.attribute_name.clone();
        let existing_config = self.load_ttl_config(&table_name).await?;

        if enabled {
            if attribute_name.trim().is_empty() {
                return Err(StorageError::validation(
                    "Time to live attribute name must not be empty",
                ));
            }

            if let Some(config) = existing_config.as_ref() {
                if matches!(
                    config.status,
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling
                ) {
                    return Err(StorageError::validation(
                        "Time to live configuration update in progress; retry later",
                    ));
                }
                if config.status == TimeToLiveStatus::Enabled {
                    if config.attribute_name == attribute_name {
                        return Ok(UpdateTimeToLiveResponse {
                            time_to_live_specification,
                        });
                    }

                    return Err(StorageError::validation(
                        "Disable time to live before changing attribute name",
                    ));
                }
            }

            let gsi_name = ttl::ttl_gsi_name(&table_name);
            let config = TtlConfigRecord::new(
                attribute_name.clone(),
                &gsi_name,
                TimeToLiveStatus::Enabling,
            );
            self.save_ttl_config(&table_name, &config).await?;

            let tail = self.capture_stream_tail().await?;
            self.initialize_backfill_record(&table_name, &gsi_name, tail)
                .await?;

            Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            })
        } else {
            if let Some(config) = existing_config {
                if let Some(ref mut indexes) = table_info.global_secondary_indexes {
                    indexes.retain(|idx| idx.index_name != config.gsi_name());
                    if indexes.is_empty() {
                        table_info.global_secondary_indexes = None;
                    }
                }

                self.save_table_info(&table_name, &table_info).await?;

                let ttl_index_prefix = ttl::ttl_index_prefix(&table_name);
                self.kv_store.delete_prefix(ttl_index_prefix).await?;

                let index_prefix =
                    ItemKey::index_prefix_from_name(&table_info.table_name, &config.gsi_name());
                self.kv_store.delete_prefix(index_prefix).await?;

                let bf_key = crate::keys::gsi_backfill_key(&table_name, &config.gsi_name());
                let _ = self.kv_store.delete(&bf_key).await;

                self.delete_ttl_config(&table_name).await?;
                time_to_live_specification.attribute_name = config.attribute_name;
            }

            time_to_live_specification.enabled = false;
            Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            })
        }
    }

    async fn describe_time_to_live(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        // Ensure table exists
        let _ = self.get_table_info(table_name).await?;

        let description = match self.load_ttl_config(table_name).await? {
            Some(config) => TimeToLiveDescription {
                attribute_name: Some(config.attribute_name),
                time_to_live_status: config.status,
            },
            None => TimeToLiveDescription {
                attribute_name: None,
                time_to_live_status: TimeToLiveStatus::Disabled,
            },
        };

        Ok(DescribeTimeToLiveResponse {
            time_to_live_description: Some(description),
        })
    }
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    async fn delete_table_stream_storage(&self, table_name: &TableName) -> StorageResult<()> {
        let table_stream = StreamName::table_stream(table_name);
        if let Some(family) = self
            .load_ordered_log_family_state(&table_stream)
            .await
            .map_err(stream_provider::StreamError::into_storage_enum)?
        {
            for prefix in crate::partition_family::ordered_log_partition_prefixes_for_infos(
                &table_stream,
                &family.partitions,
            ) {
                self.kv_store.delete_prefix(prefix).await?;
            }
            self.delete_partition_family_state(
                crate::partition_family::PartitionFamilyKind::OrderedLog,
                &crate::partition_family::ordered_log_family_component(&table_stream),
            )
            .await?;
            let marker_key = crate::partition_family::stream_partition_marker_key(&table_stream);
            let _ = self.kv_store.delete(&marker_key).await;
        }

        self.kv_store
            .delete_prefix(stream_storage_prefix(&table_stream))
            .await?;
        self.kv_store
            .delete_prefix(table_item_stream_storage_prefix(table_name))
            .await?;
        Ok(())
    }

    pub(super) async fn get_table_metadata_cached(
        &self,
        cache: &mut HashMap<TableName, StoredTableInfo>,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        if let Some(table_info) = cache.get(table_name) {
            return Ok(table_info.clone());
        }
        let table_info = self
            .get_table_metadata_from_name(table_name)
            .await?
            .ok_or(StorageError::table_not_found(table_name))?;
        cache.insert(table_name.clone(), table_info.clone());
        Ok(table_info)
    }

    pub(super) async fn load_ttl_config_cached(
        &self,
        cache: &mut HashMap<TableName, Option<TtlConfigRecord>>,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(config) = cache.get(table_name) {
            return Ok(config.clone());
        }
        let config = self.load_ttl_config(table_name).await?;
        cache.insert(table_name.clone(), config.clone());
        Ok(config)
    }

    pub(crate) fn requires_immediate_gsi_updates(&self, table_info: &StoredTableInfo) -> bool {
        self.immediate_gsi_consistency
            && table_info
                .global_secondary_indexes
                .as_ref()
                .is_some_and(|indexes| {
                    indexes
                        .iter()
                        .any(|gsi| !ttl::is_ttl_index(&gsi.index_name))
                })
    }

    pub(super) fn gsi_batch_mutations_for_items(
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<Vec<BatchItem>> {
        let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
            return Ok(Vec::new());
        };

        let mut mutations = Vec::new();
        for gsi in gsis
            .iter()
            .filter(|gsi| !ttl::is_ttl_index(&gsi.index_name))
        {
            let old_key = Self::gsi_batch_item_key(table_info, gsi, old_item)?;
            let new_key = Self::gsi_batch_item_key(table_info, gsi, new_item)?;

            if let Some(old_key) = old_key
                && Some(old_key.clone()) != new_key
            {
                mutations.push(BatchItem {
                    key: old_key,
                    value: None,
                });
            }

            if let (Some(item), Some(key)) = (new_item, new_key) {
                let projected = storage_common::apply_gsi_projection(
                    item,
                    Some(&gsi.projection),
                    &table_info.key_schema,
                    &gsi.key_schema,
                );
                mutations.push(BatchItem {
                    key,
                    value: Some(storage_types::storage_serde::to_bytes(&projected)?),
                });
            }
        }

        Ok(mutations)
    }

    fn gsi_batch_item_key(
        table_info: &StoredTableInfo,
        gsi: &storage_types::GlobalSecondaryIndex,
        item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        let Some(item) = item else {
            return Ok(None);
        };

        Ok(ItemKey::from_key_schema_for_index(
            table_info.table_name.clone(),
            &table_info.key_schema,
            &gsi.index_name,
            &gsi.key_schema,
            item,
        )?
        .map(|key| key.serialize_to_bytes())
        .transpose()?)
    }

    #[inline]
    pub(crate) fn find_gsi_projection<'a>(
        table_info: &'a StoredTableInfo,
        index_name: &IndexName,
    ) -> Option<&'a Projection> {
        table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|gsis| gsis.iter().find(|g| g.index_name == *index_name))
            .map(|g| &g.projection)
    }

    async fn apply_batch_encode_put_item(
        &self,
        table_name: &TableName,
        item: &WireItem,
    ) -> StorageResult<usize> {
        let table_info = self
            .get_table_metadata_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;

        let ttl_config = self.load_ttl_config(table_name).await?;
        let should_write_stream = crate::backends::common::should_write_stream_entries(
            &table_info,
            self.requires_immediate_gsi_updates(&table_info),
        );
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let should_write_gsi_immediately = self.requires_immediate_gsi_updates(&table_info);
        let ttl_attribute = if should_track_ttl {
            ttl_config
                .as_ref()
                .map(|config| config.attribute_name.as_str())
        } else {
            None
        };
        let item = normalized_wire_item_for_write(item)?;
        let item = item.as_ref();
        let (item_key, projected_ttl_value) =
            project_wire_item_table_key_and_ttl(item, &table_info, ttl_attribute)?;
        let item_key_bytes = item_key.serialize_to_bytes()?;
        let item_key_token = if should_track_ttl {
            Some(wire_item_key_token_from_item_key(&item_key)?)
        } else {
            None
        };
        let value = encode_wire_item_storage_bytes(item)?;

        if should_write_gsi_immediately {
            let old_item = self
                .kv_store
                .get(&item_key_bytes, true)
                .await?
                .as_deref()
                .map(decode_wire_item_from_storage_bytes)
                .transpose()?;
            let old_item_map = old_item
                .as_ref()
                .map(WireItem::to_attribute_map)
                .transpose()?;
            let mapped_item = item.to_attribute_map()?;
            let mut batch_items = Self::prepare_batch_put_item(
                table_name,
                &table_info,
                &mapped_item,
                should_write_stream,
                old_item_map.as_ref(),
                true,
            )?;
            let ttl_mutations = Self::ttl_index_mutations_for_items(
                table_name,
                &table_info,
                ttl_config.as_ref(),
                old_item_map.as_ref(),
                Some(&mapped_item),
            )?;
            if !ttl_mutations.is_empty() {
                batch_items.extend(ttl_mutations);
            }
            self.kv_store.batch_write(batch_items).await?;
        } else if should_write_stream || should_track_ttl {
            let old_bytes = self.kv_store.get(&item_key_bytes, true).await?;
            let old_item = if should_track_ttl {
                old_bytes
                    .as_deref()
                    .map(decode_wire_item_from_storage_bytes)
                    .transpose()?
            } else {
                None
            };

            let mut operations = Vec::with_capacity(6);
            if should_write_stream {
                let stream_item_id = next_stream_item_id();
                let stream_entries =
                    crate::stream::helpers::create_item_update_stream_entries_wire_encoded(
                        table_name,
                        &item_key,
                        value.as_slice(),
                        old_bytes.as_deref(),
                        stream_item_id,
                        false,
                        None,
                    )?;
                operations.extend(stream_entries.into_iter().map(|(template, value)| {
                    TransactWriteOperation::PutTemplate {
                        template,
                        value,
                        condition: None,
                    }
                }));
            }

            if should_track_ttl {
                let ttl_ops = ttl_index_direct_operations_for_wire_items(
                    table_name,
                    &table_info,
                    ttl_config.as_ref(),
                    old_item.as_ref(),
                    Some(item),
                    item_key_token.as_deref(),
                    projected_ttl_value,
                )?;
                operations.extend(ttl_ops);
            }

            operations.push(TransactWriteOperation::Put {
                key: item_key_bytes,
                value,
                condition: None,
            });

            let direct_operations = operations
                .into_iter()
                .map(to_direct_write_operation)
                .collect::<StorageResult<Vec<_>>>()?;
            self.kv_store
                .transact_write_unchecked(direct_operations)
                .await?;
        } else {
            self.kv_store.put(&item_key_bytes, &value, None).await?;
        }

        Ok(item.payload_len())
    }

    async fn apply_batch_encode_put_items_immediate_gsi(
        &self,
        _table_name: &TableName,
        table_info: &StoredTableInfo,
        write_requests: &[EncodeWriteRequest],
    ) -> StorageResult<(usize, usize)> {
        let mut planned_items = Vec::with_capacity(write_requests.len());
        let mut planned_values = Vec::with_capacity(write_requests.len());
        let mut keys = Vec::with_capacity(write_requests.len());
        let mut total_bytes_written = 0usize;

        for write_request in write_requests {
            let EncodeWriteRequest {
                put_request: Some(put_request),
                delete_request: None,
            } = write_request
            else {
                return Err(StorageError::validation(
                    "Each WriteRequest must contain exactly one PutRequest",
                ));
            };

            let item = normalized_wire_item_for_write(&put_request.item)?;
            let mapped_item = item.to_attribute_map()?;
            let item_key = ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                &mapped_item,
            )?
            .serialize_to_bytes()?;
            total_bytes_written += item.payload_len();
            keys.push(item_key);
            planned_values.push(encode_wire_item_storage_bytes(item.as_ref())?);
            planned_items.push(mapped_item);
        }

        let old_values = self.kv_store.multi_get(keys, true).await?;
        let mut batch_items = Vec::with_capacity(write_requests.len() * 2);
        for ((mapped_item, value), old_value) in planned_items
            .iter()
            .zip(planned_values.into_iter())
            .zip(old_values)
        {
            let old_item = old_value
                .as_deref()
                .map(decode_wire_item_from_storage_bytes)
                .transpose()?;
            let old_item_map = old_item
                .as_ref()
                .map(WireItem::to_attribute_map)
                .transpose()?;
            if mapped_item.is_empty() {
                return Err(StorageError::validation(
                    "Item must have at least one attribute",
                ));
            }
            let item_key = ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                mapped_item,
            )?
            .serialize_to_bytes()?;
            batch_items.push(BatchItem {
                key: item_key,
                value: Some(value),
            });
            batch_items.extend(Self::gsi_batch_mutations_for_items(
                table_info,
                old_item_map.as_ref(),
                Some(mapped_item),
            )?);
        }

        self.kv_store.batch_write(batch_items).await?;
        Ok((write_requests.len(), total_bytes_written))
    }

    async fn apply_batch_delete_item(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> StorageResult<()> {
        if key.is_empty() {
            return Ok(());
        }

        let table_info = self
            .get_table_metadata_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let ttl_config = self.load_ttl_config(table_name).await?;

        self.kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_info,
                    key: key.clone(),
                    condition: None,
                    return_values_on_condition_check_failure: None,
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn get_item_map(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        if key.is_empty() {
            return Ok(None);
        }

        let table_info = self
            .get_table_metadata_from_name_arc(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)?
                .serialize_to_bytes()?;
        let raw = self.kv_store.get(&item_key, consistent_read).await?;
        let wire_item = raw
            .as_deref()
            .map(decode_wire_item_from_storage_bytes)
            .transpose()?;
        wire_item.map(WireItem::into_attribute_map).transpose()
    }

    async fn batch_existing_items_for_write_requests(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        write_requests: &[WriteRequest],
    ) -> StorageResult<Vec<Option<HashMap<String, AttributeValue>>>> {
        let mut keys = Vec::new();
        let mut key_positions = Vec::new();
        let mut existing_items = vec![None; write_requests.len()];

        for (index, write_request) in write_requests.iter().enumerate() {
            let key = match write_request {
                WriteRequest {
                    put_request: Some(PutRequest { item }),
                    delete_request: None,
                } => Some(ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    item,
                )?),
                WriteRequest {
                    put_request: None,
                    delete_request: Some(DeleteRequest { key }),
                } => Some(ItemKey::from_key_schema(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    key,
                )?),
                _ => None,
            };

            if let Some(key) = key {
                keys.push(key.serialize_to_bytes()?);
                key_positions.push(index);
            }
        }

        if keys.is_empty() {
            return Ok(existing_items);
        }

        #[cfg(test)]
        let started = std::time::Instant::now();
        let values = self.kv_store.multi_get(keys, true).await?;
        #[cfg(test)]
        provider_perf::record(
            "storage_provider",
            "batch_existing_multi_get",
            started.elapsed(),
        );
        for (position, value) in key_positions.into_iter().zip(values) {
            let wire_item = value
                .as_deref()
                .map(decode_wire_item_from_storage_bytes)
                .transpose()?;
            existing_items[position] = wire_item.map(WireItem::into_attribute_map).transpose()?;
        }

        tracing::debug!(
            table_name = %table_name,
            requested_items = write_requests.len(),
            loaded_items = existing_items.iter().filter(|item| item.is_some()).count(),
            "loaded existing batch write items"
        );

        Ok(existing_items)
    }

    pub(crate) async fn save_table_info(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
    ) -> StorageResult<()> {
        let key = crate::keys::table_metadata_key(table_name);
        let value = storage_types::storage_serde::to_bytes(table_info)?;
        self.kv_store.put(&key, &value, None).await?;
        self.cache_table_metadata(table_name.clone(), Arc::new(table_info.clone()));
        Ok(())
    }

    pub(crate) async fn capture_stream_tail(&self) -> StorageResult<Option<String>> {
        let prefix = self
            .kv_store
            .get_prefix(&StreamName::system_table_stream(), false, Some(1), true)
            .await?;
        Ok(prefix
            .items
            .first()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned()))
    }

    pub(crate) async fn load_ttl_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(entry) = self.ttl_config_cache_lru.get(table_name) {
            return Ok(entry.config());
        }

        let key = ttl::ttl_config_key(table_name);
        let config = match self.kv_store.get(&key, true).await? {
            Some(bytes) => Some(storage_types::storage_serde::from_bytes(&bytes)?),
            None => None,
        };
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(config.clone())),
        );
        Ok(config)
    }

    pub(crate) async fn save_ttl_config(
        &self,
        table_name: &TableName,
        config: &TtlConfigRecord,
    ) -> StorageResult<()> {
        let key = ttl::ttl_config_key(table_name);
        let value = storage_types::storage_serde::to_bytes(config)?;
        self.kv_store.put(&key, &value, None).await?;
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(Some(
                config.clone(),
            ))),
        );
        Ok(())
    }

    pub(crate) async fn delete_ttl_config(&self, table_name: &TableName) -> StorageResult<()> {
        let key = ttl::ttl_config_key(table_name);
        let _ = self.kv_store.delete(&key).await;
        self.ttl_config_cache_lru.insert(
            table_name.clone(),
            Arc::new(crate::sorted_kv::TtlConfigCacheEntry::new(None)),
        );
        Ok(())
    }

    pub(crate) async fn list_ttl_configs(
        &self,
    ) -> StorageResult<Vec<(TableName, TtlConfigRecord)>> {
        let prefix = TABLES_PREFIX.as_bytes();
        let range_end = increment_bytes(prefix.to_vec());
        let scan_result = self
            .kv_store
            .get_range(prefix, &range_end, None, None::<TablePageKey>, true)
            .await?;

        let mut configs = Vec::new();
        for (raw_key, raw_value) in scan_result.items {
            let key_str = String::from_utf8_lossy(&raw_key);
            if let Some(stripped) = key_str.strip_prefix(TABLES_PREFIX)
                && let Some((table_part, suffix)) = stripped.split_once('/')
                && suffix == "ttl-config"
            {
                let table_name = TableName::new(table_part);
                match storage_types::storage_serde::from_bytes::<TtlConfigRecord>(&raw_value) {
                    Ok(config) => configs.push((table_name, config)),
                    Err(err) => warn!(table=%table_name, error = %err, "ttl.config.decode_failed"),
                }
            }
        }

        Ok(configs)
    }
}

fn stream_storage_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut prefix: Vec<u8> = stream_name.into();
    prefix.push(b'/');
    prefix
}

pub(super) fn kv_mutation_to_direct(mutation: KvMutation) -> DirectWriteOperation {
    match mutation {
        KvMutation::Put { key, value } => DirectWriteOperation::Put { key, value },
        KvMutation::PutTemplate { template, value } => {
            DirectWriteOperation::PutTemplate { template, value }
        }
        KvMutation::Delete { key } => DirectWriteOperation::Delete { key },
    }
}

pub(super) fn kv_mutation_to_direct_with_literal_templates(
    mutation: KvMutation,
) -> DirectWriteOperation {
    match mutation {
        KvMutation::PutTemplate { template, value } => DirectWriteOperation::Put {
            key: template.rocks_key(),
            value,
        },
        other => kv_mutation_to_direct(other),
    }
}

fn table_item_stream_storage_prefix(table_name: &TableName) -> Vec<u8> {
    let mut prefix = table_name.sanitized_name().as_bytes().to_vec();
    prefix.extend_from_slice(b"/stream-item/");
    prefix
}

#[cfg(test)]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::scan_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    pub async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::query_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    pub async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetItemResponse> {
        let response = <Self as StorageProvider>::batch_get_item(self, request).await?;
        decode_batch_get_response_to_maps(response)
    }
}

#[cfg(test)]
fn decode_batch_get_response_to_maps(
    response: BatchGetWireItemResponse,
) -> StorageResult<storage_types::BatchGetItemResponse> {
    let responses = if let Some(table_items) = response.responses {
        let mut decoded = HashMap::with_capacity(table_items.len());
        for (table, items) in table_items {
            let mut table_rows = Vec::with_capacity(items.len());
            for item in items {
                table_rows.push(item.into_attribute_map()?.into());
            }
            decoded.insert(table, table_rows);
        }
        Some(decoded)
    } else {
        None
    };

    Ok(storage_types::BatchGetItemResponse {
        responses,
        unprocessed_keys: response.unprocessed_keys,
        consumed_capacity: response.consumed_capacity,
    })
}
