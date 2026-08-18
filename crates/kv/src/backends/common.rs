use std::collections::{HashMap, HashSet};

use storage_common::{
    DEFAULT_GENERIC_LIMIT, GsiKeyPart, GsiWriteAction, MAX_GENERIC_LIMIT, TtlConfigRecord,
    plan_gsi_write_actions, ttl::is_ttl_index,
};
use storage_condition::{Condition, evaluate_condition};
use storage_provider::{UpdateOperation, apply_update_operations};
use storage_types::{
    AttributeValue, AttributeValueLookup, IndexedWireItem, IndexerDeclaration, ItemKey,
    KeyAttributes, ReplicationEventMetadata, StorageEnum, StorageError, StorageResult,
    StoredTableInfo, StreamItemId, conditional_check_failed_reason, context::WrappedError as _,
    normalize_dynamodb_number_for_write, preflight_transact_put_item_key_with_table_info,
    preflight_transact_write_key_with_table_info, return_values_on_condition_check_failure_all_old,
    transaction_canceled_for_indexed_reasons, transaction_canceled_for_item_error,
    transaction_canceled_for_item_error_with_len, transaction_canceled_for_reason,
    transaction_cancellation_reason_at, transaction_key_preflight_from_key_result,
};

#[cfg(feature = "rocksdb-backend")]
use crate::keyspace::compact::KeyFamily;
use crate::{
    helpers::deserialize_item_from_bytes,
    key_template::KeyTemplate,
    keyspace::{table_identity::TableIdentity, table_keys},
    sorted_kv_store::{
        ItemValueCodec, OldNewItems, RangeResult, RangeValuesResult, TransactWriteOperation,
        TransactWriteTableOperation,
    },
    storage_ops::{
        change_index_key, change_index_slot, decode_indexed_wire_item, encode_indexed_wire_item,
    },
    stream::helpers::{StreamEntryContext, create_item_update_stream_entries},
    ttl::{TtlIndexMutation, plan_ttl_index_mutations},
};

#[derive(Clone, Debug)]
pub enum KvMutation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutTemplate {
        template: KeyTemplate,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

fn change_index_marker_mutation(
    table_info: &StoredTableInfo,
    stream_item_id: StreamItemId,
) -> KvMutation {
    let slot = change_index_slot(&table_info.table_name);
    let versionstamp = stream_item_id.to_string();
    KvMutation::Put {
        key: change_index_key(slot, versionstamp.as_bytes(), &table_info.table_name),
        value: Vec::new(),
    }
}

pub struct TableWritePlan {
    pub results: Vec<OldNewItems>,
    pub mutations: Vec<KvMutation>,
    pub stats: TableWritePlanStats,
}

#[derive(Default)]
pub struct TableWritePlanStats {
    pub mutation_count: usize,
    pub gsi_mutation_count: usize,
    pub gsi_distinct_key_count: usize,
    pub gsi_key_overlap_count: usize,
    pub collapsed_gsi_mutation_count: usize,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RangeKeyDecision {
    Include,
    Skip,
    Stop,
}

pub struct RangeScanSettings {
    start: Vec<u8>,
    exclusive_end: Vec<u8>,
    forward: bool,
    limit: usize,
    page_token: Option<Vec<u8>>,
}

impl RangeScanSettings {
    pub fn new(
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<Vec<u8>>,
    ) -> StorageResult<Self> {
        let forward = start <= exclusive_end;
        let limit =
            storage_common::normalize_limit(limit, DEFAULT_GENERIC_LIMIT, MAX_GENERIC_LIMIT)?
                as usize;
        Ok(Self {
            start: start.to_vec(),
            exclusive_end: exclusive_end.to_vec(),
            forward,
            limit,
            page_token,
        })
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    #[must_use]
    pub fn fetch_limit(&self) -> usize {
        self.limit + 1
    }

    #[must_use]
    pub fn forward(&self) -> bool {
        self.forward
    }

    #[must_use]
    pub fn ordered_bounds(&self) -> (&[u8], &[u8]) {
        if self.forward {
            (&self.start, &self.exclusive_end)
        } else {
            (&self.exclusive_end, &self.start)
        }
    }

    #[must_use]
    pub fn page_token(&self) -> Option<&[u8]> {
        self.page_token.as_deref()
    }

    #[must_use]
    pub fn evaluate_key(&self, key: &[u8]) -> RangeKeyDecision {
        if self.forward {
            if key < self.start.as_slice() {
                return RangeKeyDecision::Skip;
            }
            if key >= self.exclusive_end.as_slice() {
                return RangeKeyDecision::Stop;
            }
            if let Some(token) = &self.page_token
                && key <= token.as_slice()
            {
                return RangeKeyDecision::Skip;
            }
        } else {
            if key > self.start.as_slice() {
                return RangeKeyDecision::Skip;
            }
            if key <= self.exclusive_end.as_slice() {
                return RangeKeyDecision::Stop;
            }
            if let Some(token) = &self.page_token
                && key >= token.as_slice()
            {
                return RangeKeyDecision::Skip;
            }
        }

        RangeKeyDecision::Include
    }

    #[must_use]
    pub fn finalize(
        &self,
        mut items: Vec<(Vec<u8>, Vec<u8>)>,
        backend_has_more: bool,
    ) -> RangeResult {
        let mut has_more = backend_has_more;
        if items.len() > self.limit {
            has_more = true;
            items.truncate(self.limit);
        }

        let items = items
            .into_iter()
            .map(|(key, value)| (key.into_boxed_slice(), value.into_boxed_slice()))
            .collect();

        RangeResult { items, has_more }
    }

    #[must_use]
    pub fn finalize_values(
        &self,
        mut values: Vec<Vec<u8>>,
        backend_has_more: bool,
    ) -> RangeValuesResult {
        let mut has_more = backend_has_more;
        if values.len() > self.limit {
            has_more = true;
            values.truncate(self.limit);
        }

        RangeValuesResult { values, has_more }
    }
}

#[derive(Clone, Copy)]
struct TableStreamContext<'a> {
    stream_item_id: Option<StreamItemId>,
    replication: Option<&'a ReplicationEventMetadata>,
    immediate_gsi_consistency: bool,
}

#[derive(Clone, Copy)]
struct TableUpdateContext<'a> {
    stream: TableStreamContext<'a>,
    index: usize,
    preserve_old_item: bool,
    ttl_config: Option<&'a TtlConfigRecord>,
    item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

pub fn plan_transact_operation(
    operation: TransactWriteOperation,
    current_bytes: Option<&[u8]>,
    index: usize,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    match operation {
        TransactWriteOperation::Put {
            key,
            value,
            condition,
        } => {
            if let Some(condition) = condition {
                let current = deserialize_optional_item(current_bytes)?;
                ensure_condition(index, &condition, &current)?;
                let mutation = KvMutation::Put { key, value };
                return Ok(((Some(current), None), vec![mutation]));
            }

            // Shortcut: unconditional Put is unconditional upsert in DynamoDB semantics.
            // No condition evaluation and no old-image return is required, so we
            // can skip deserializing current bytes and apply the mutation
            // directly.
            let mutation = KvMutation::Put { key, value };
            Ok(((None, None), vec![mutation]))
        }
        TransactWriteOperation::PutTemplate {
            template,
            value,
            condition,
        } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "conditions are not supported for templated keys",
                ));
            }
            let mutation = KvMutation::PutTemplate { template, value };
            Ok(((None, None), vec![mutation]))
        }
        TransactWriteOperation::Delete { key, condition } => {
            let current = deserialize_optional_item(current_bytes)?;
            if let Some(condition) = condition {
                ensure_condition(index, &condition, &current)?;
            }
            let mutation = KvMutation::Delete { key };
            Ok(((Some(current), None), vec![mutation]))
        }
        TransactWriteOperation::Check { condition, .. } => {
            let current = deserialize_optional_item(current_bytes)?;
            ensure_condition(index, &condition, &current)?;
            Ok(((None, None), Vec::new()))
        }
        TransactWriteOperation::CheckValue { expected_value, .. } => {
            if current_bytes != expected_value.as_deref() {
                return Err(StorageEnum::ConditionalCheckFailed.into());
            }
            Ok(((None, None), Vec::new()))
        }
        TransactWriteOperation::Update {
            key,
            operations,
            condition,
        } => {
            let current = deserialize_optional_item(current_bytes)?;
            if let Some(condition) = &condition {
                ensure_condition(index, condition, &current)?;
            }
            let new_item = apply_update_operations(current.clone(), &operations)?;
            let serialized = storage_types::storage_serde::to_bytes(&new_item)?;
            let mutation = KvMutation::Put {
                key,
                value: serialized,
            };
            Ok(((Some(current), Some(new_item)), vec![mutation]))
        }
    }
}

pub fn plan_table_operation(
    operation: &TransactWriteTableOperation,
    current_bytes: Option<&[u8]>,
    stream_item_id: Option<StreamItemId>,
    immediate_gsi_consistency: bool,
    codec: ItemValueCodec,
    index: usize,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    use TransactWriteTableOperation as TableOp;

    match operation {
        TableOp::Put {
            table_identity,
            table_info,
            item,
            indexers,
            item_stream_ttl_hours,
            condition,
            return_values_on_condition_check_failure,
            replication,
            ttl_config,
        } => plan_table_put(
            table_identity,
            table_info,
            item,
            indexers.as_deref(),
            condition.as_ref(),
            return_values_on_condition_check_failure.as_ref(),
            current_bytes,
            TableStreamContext {
                stream_item_id,
                replication: replication.as_ref(),
                immediate_gsi_consistency,
            },
            ttl_config.as_ref(),
            *item_stream_ttl_hours,
            codec,
            index,
        ),
        TableOp::Delete {
            table_identity,
            table_info,
            key,
            item_stream_ttl_hours,
            use_key_attributes_for_missing_item_condition,
            condition,
            return_values_on_condition_check_failure,
            replication,
            ttl_config,
        } => plan_table_delete(
            table_identity,
            table_info,
            key,
            *item_stream_ttl_hours,
            *use_key_attributes_for_missing_item_condition,
            condition.as_ref(),
            return_values_on_condition_check_failure.as_ref(),
            current_bytes,
            TableStreamContext {
                stream_item_id,
                replication: replication.as_ref(),
                immediate_gsi_consistency,
            },
            ttl_config.as_ref(),
            codec,
            index,
        ),
        TableOp::Check {
            table_info,
            key,
            condition,
            return_values_on_condition_check_failure,
            ..
        } => plan_table_check(
            table_info,
            key,
            condition,
            return_values_on_condition_check_failure.as_ref(),
            current_bytes,
            codec,
            index,
        ),
        TableOp::Update {
            table_identity,
            table_info,
            key,
            operations,
            indexers,
            item_stream_ttl_hours,
            condition,
            return_values_on_condition_check_failure,
            replication,
            preserve_old_item,
            transaction_validation,
            ttl_config,
        } => plan_table_update(
            table_identity,
            table_info,
            key,
            operations,
            condition.as_ref(),
            return_values_on_condition_check_failure.as_ref(),
            current_bytes,
            indexers.as_deref(),
            TableUpdateContext {
                stream: TableStreamContext {
                    stream_item_id,
                    replication: replication.as_ref(),
                    immediate_gsi_consistency,
                },
                index,
                preserve_old_item: *preserve_old_item,
                ttl_config: ttl_config.as_ref(),
                item_stream_ttl_hours: *item_stream_ttl_hours,
            },
            codec,
        )
        .map_err(|error| {
            if *transaction_validation {
                transaction_canceled_for_item_error(index, error)
            } else {
                error
            }
        }),
    }
}

pub fn plan_table_write(
    operations: &[TransactWriteTableOperation],
    current_values: Vec<Option<Vec<u8>>>,
    stream_ids: &[Option<StreamItemId>],
    immediate_gsi_consistency: bool,
) -> StorageResult<TableWritePlan> {
    plan_table_write_with_codec(
        operations,
        current_values,
        stream_ids,
        immediate_gsi_consistency,
        ItemValueCodec::RocksDbEnvelope,
    )
}

pub fn plan_table_write_with_codec(
    operations: &[TransactWriteTableOperation],
    current_values: Vec<Option<Vec<u8>>>,
    stream_ids: &[Option<StreamItemId>],
    immediate_gsi_consistency: bool,
    codec: ItemValueCodec,
) -> StorageResult<TableWritePlan> {
    preflight_table_write_operations(operations)?;
    plan_table_write_preflighted_with_codec(
        operations,
        current_values,
        stream_ids,
        immediate_gsi_consistency,
        codec,
    )
}

#[cfg(any(feature = "rocksdb-backend", test))]
pub(crate) fn plan_table_write_preflighted(
    operations: &[TransactWriteTableOperation],
    current_values: Vec<Option<Vec<u8>>>,
    stream_ids: &[Option<StreamItemId>],
    immediate_gsi_consistency: bool,
) -> StorageResult<TableWritePlan> {
    plan_table_write_preflighted_with_codec(
        operations,
        current_values,
        stream_ids,
        immediate_gsi_consistency,
        ItemValueCodec::RocksDbEnvelope,
    )
}

pub(crate) fn plan_table_write_preflighted_with_codec(
    operations: &[TransactWriteTableOperation],
    current_values: Vec<Option<Vec<u8>>>,
    stream_ids: &[Option<StreamItemId>],
    immediate_gsi_consistency: bool,
    codec: ItemValueCodec,
) -> StorageResult<TableWritePlan> {
    let mut plan = TableWritePlan {
        results: Vec::with_capacity(operations.len()),
        mutations: Vec::new(),
        stats: TableWritePlanStats::default(),
    };
    let mut cancellation_reasons: Option<Vec<Option<String>>> = None;

    for (index, (operation, current)) in operations.iter().zip(current_values).enumerate() {
        let result = plan_table_operation(
            operation,
            current.as_deref(),
            stream_ids[index],
            immediate_gsi_consistency,
            codec,
            index,
        )
        .map_err(|error| {
            if operation_uses_transaction_validation(operation) {
                transaction_canceled_for_item_error_with_len(index, operations.len(), error)
            } else {
                error
            }
        });
        let (old_new, mutations) = match result {
            Ok(result) => result,
            Err(error) => {
                if matches!(error.to_enum(), StorageEnum::TransactionCanceled { .. })
                    && let Some(reason) = transaction_cancellation_reason_at(&error, index)
                {
                    cancellation_reasons.get_or_insert_with(|| vec![None; operations.len()])
                        [index] = Some(reason);
                    plan.results.push((None, None));
                    continue;
                }
                return Err(error);
            }
        };
        for mutation in mutations {
            plan.stats.mutation_count += 1;
            if is_gsi_mutation(&mutation) {
                plan.stats.gsi_mutation_count += 1;
            }
            plan.mutations.push(mutation);
        }
        plan.results.push(old_new);
    }

    if let Some(cancellation_reasons) = cancellation_reasons
        && let Some(error) = transaction_canceled_for_indexed_reasons(cancellation_reasons)
    {
        return Err(error);
    }

    collapse_redundant_gsi_mutations(&mut plan);
    Ok(plan)
}

fn operation_uses_transaction_validation(operation: &TransactWriteTableOperation) -> bool {
    !matches!(
        operation,
        TransactWriteTableOperation::Update {
            transaction_validation: false,
            ..
        }
    )
}

pub(crate) fn preflight_table_write_operations(
    operations: &[TransactWriteTableOperation],
) -> StorageResult<()> {
    if let [operation] = operations {
        let preflight = preflight_table_write_operation(operation)?;
        if let Some(validation_reason) = preflight.validation_reason {
            return Err(transaction_canceled_for_reason(0, validation_reason));
        }
        return Ok(());
    }

    let mut fingerprints = Vec::with_capacity(operations.len());
    let mut cancellation_reasons: Option<Vec<Option<String>>> = None;
    for (index, operation) in operations.iter().enumerate() {
        let preflight = preflight_table_write_operation(operation)?;
        if let Some(validation_reason) = preflight.validation_reason {
            cancellation_reasons.get_or_insert_with(|| vec![None; operations.len()])[index] =
                Some(validation_reason);
            continue;
        }
        if cancellation_reasons.is_none()
            && let Some(fingerprint) = preflight.key_fingerprint
        {
            fingerprints.push(fingerprint);
        }
    }
    if let Some(cancellation_reasons) = cancellation_reasons {
        if let Some(error) = transaction_canceled_for_indexed_reasons(cancellation_reasons) {
            return Err(error);
        }
        return Ok(());
    }
    validate_no_duplicate_transact_key_fingerprints(&fingerprints)
}

fn validate_no_duplicate_transact_key_fingerprints(fingerprints: &[String]) -> StorageResult<()> {
    let mut seen = HashSet::with_capacity(fingerprints.len());
    for fingerprint in fingerprints {
        if !seen.insert(fingerprint.as_str()) {
            return Err(StorageError::validation(
                "Transaction request cannot include multiple operations on one item",
            ));
        }
    }
    Ok(())
}

fn preflight_table_write_operation(
    operation: &TransactWriteTableOperation,
) -> StorageResult<storage_types::TransactionKeyPreflight> {
    match operation {
        TransactWriteTableOperation::Put {
            table_info,
            item,
            indexers,
            ..
        } => {
            let indexer_preflight = transaction_key_preflight_from_key_result((|| {
                let declaration = IndexerDeclaration::try_new(
                    indexers.clone().unwrap_or_default(),
                    table_info.max_indexers,
                )?;
                IndexedWireItem::extract(item, &declaration)?;
                Ok(String::new())
            })())?;
            if indexer_preflight.validation_reason.is_some() {
                return Ok(indexer_preflight);
            }
            preflight_transact_put_item_key_with_table_info(table_info, item)
        }
        TransactWriteTableOperation::Delete {
            table_info, key, ..
        }
        | TransactWriteTableOperation::Check {
            table_info, key, ..
        } => preflight_transact_write_key_with_table_info(table_info, key),
        TransactWriteTableOperation::Update {
            table_info,
            key,
            indexers,
            ..
        } => {
            if let Some(indexers) = indexers {
                let indexer_preflight = transaction_key_preflight_from_key_result(
                    IndexerDeclaration::try_new(indexers.clone(), table_info.max_indexers)
                        .map(|_| String::new()),
                )?;
                if indexer_preflight.validation_reason.is_some() {
                    return Ok(indexer_preflight);
                }
            }
            preflight_transact_write_key_with_table_info(table_info, key)
        }
    }
}

fn collapse_redundant_gsi_mutations(plan: &mut TableWritePlan) {
    let mut last_by_key: HashMap<&[u8], usize> = HashMap::new();
    let mut dropped = vec![false; plan.mutations.len()];

    for (index, mutation) in plan.mutations.iter().enumerate() {
        let Some(key) = gsi_mutation_key(mutation) else {
            continue;
        };
        if let Some(previous) = last_by_key.insert(key, index) {
            dropped[previous] = true;
        }
    }

    let gsi_distinct_key_count = last_by_key.len();
    drop(last_by_key);

    plan.stats.gsi_distinct_key_count = gsi_distinct_key_count;
    plan.stats.gsi_key_overlap_count = plan
        .stats
        .gsi_mutation_count
        .saturating_sub(plan.stats.gsi_distinct_key_count);
    plan.stats.collapsed_gsi_mutation_count = dropped.iter().filter(|dropped| **dropped).count();
    if plan.stats.collapsed_gsi_mutation_count == 0 {
        return;
    }

    let mutations = std::mem::take(&mut plan.mutations);
    plan.mutations = mutations
        .into_iter()
        .enumerate()
        .filter_map(|(index, mutation)| (!dropped[index]).then_some(mutation))
        .collect();
}

fn is_gsi_mutation(mutation: &KvMutation) -> bool {
    gsi_mutation_key(mutation).is_some()
}

pub(crate) fn gsi_mutation_key(mutation: &KvMutation) -> Option<&[u8]> {
    let key = match mutation {
        KvMutation::Put { key, .. } | KvMutation::Delete { key } => key.as_slice(),
        KvMutation::PutTemplate { .. } => return None,
    };
    #[cfg(feature = "foundationdb-backend")]
    if crate::keyspace::tuple_keys::is_gsi_key(key) {
        return Some(key);
    }
    #[cfg(feature = "rocksdb-backend")]
    if matches!(
        key.first()
            .copied()
            .and_then(|code| KeyFamily::from_code(code).ok()),
        Some(KeyFamily::GsiItem | KeyFamily::GsiTombstone)
    ) {
        return Some(key);
    }
    None
}

#[expect(clippy::too_many_arguments)]
fn plan_table_put(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
    indexers: Option<&[String]>,
    condition: Option<&Condition>,
    return_values_on_condition_check_failure: Option<&String>,
    current_bytes: Option<&[u8]>,
    stream_context: TableStreamContext<'_>,
    ttl_config: Option<&TtlConfigRecord>,
    item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    codec: ItemValueCodec,
    index: usize,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    let item_clone = item.clone();
    let item_key = storage_types::ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        &item_clone,
    )?;
    let item_key_bytes = table_keys::item_key(table_identity, &item_key)?;

    let (mut current, stored_indexers) =
        decode_optional_table_item(current_bytes, table_info, codec)?;
    if current_bytes.is_some() {
        merge_key_attributes(&mut current, item, table_info)?;
    }
    if let Some(condition) = condition {
        ensure_condition_for_table(
            index,
            condition,
            &current,
            return_values_on_condition_check_failure,
            table_info,
        )?;
    }

    let declaration = IndexerDeclaration::try_new(
        indexers.unwrap_or_default().to_vec(),
        table_info.max_indexers,
    )?;
    let primary_value = encode_table_item(codec, &item_clone, &item_clone, &declaration)?;
    let mut mutations = vec![KvMutation::Put {
        key: item_key_bytes,
        value: primary_value,
    }];

    if should_write_stream_entries(table_info, stream_context.immediate_gsi_consistency) {
        let stream_item_id = stream_context.stream_item_id.ok_or_else(|| {
            StorageError::internal("stream_item_id required for table put operation")
        })?;
        let old_item = if current_bytes.is_some() {
            Some(&current)
        } else {
            None
        };
        let stream_entries = create_item_update_stream_entries(
            StreamEntryContext {
                table_identity,
                table_name: &table_info.table_name,
                item_key: &item_key,
                indexers: declaration.names(),
                old_indexers: current_bytes
                    .is_some()
                    .then_some(stored_indexers.as_slice()),
            },
            &item_clone,
            old_item,
            stream_item_id,
            false,
            stream_context.replication,
        )?;
        mutations.extend(
            stream_entries
                .into_iter()
                .map(|(template, value)| KvMutation::PutTemplate { template, value }),
        );
        mutations.push(change_index_marker_mutation(table_info, stream_item_id));
    }

    if stream_context.immediate_gsi_consistency {
        let old_item = current_bytes.is_some().then_some(&current);
        mutations.extend(plan_immediate_gsi_mutations(
            table_identity,
            table_info,
            old_item,
            Some(&item_clone),
            Some(&declaration),
            codec,
        )?);
    }

    mutations.extend(ttl_index_kv_mutations(plan_ttl_index_mutations(
        &table_info.table_name,
        table_identity,
        table_info,
        ttl_config,
        current_bytes.is_some().then_some(&current),
        Some(&item_clone),
    )?));

    mutations.extend(item_stream_duration_kv_mutations(
        table_identity,
        table_info,
        &item_clone,
        item_stream_ttl_hours,
    )?);

    Ok(((Some(current), Some(item_clone)), mutations))
}

#[expect(clippy::too_many_arguments)]
fn plan_table_delete(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
    item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    use_key_attributes_for_missing_item_condition: bool,
    condition: Option<&Condition>,
    return_values_on_condition_check_failure: Option<&String>,
    current_bytes: Option<&[u8]>,
    stream_context: TableStreamContext<'_>,
    ttl_config: Option<&TtlConfigRecord>,
    codec: ItemValueCodec,
    index: usize,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    let key_item = key_attributes_to_item_map(key, table_info)?;
    let item_key = storage_types::ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        key,
    )?;
    let item_key_bytes = table_keys::item_key(table_identity, &item_key)?;

    let (mut current, stored_indexers) =
        decode_optional_table_item(current_bytes, table_info, codec)?;
    if current_bytes.is_some() {
        merge_key_attributes(&mut current, key, table_info)?;
    }
    if let Some(condition) = condition {
        let condition_item =
            if current_bytes.is_none() && use_key_attributes_for_missing_item_condition {
                &key_item
            } else {
                &current
            };
        ensure_condition_for_table(
            index,
            condition,
            condition_item,
            return_values_on_condition_check_failure,
            table_info,
        )?;
    }

    let mut mutations = vec![KvMutation::Delete {
        key: item_key_bytes,
    }];
    if should_write_stream_entries(table_info, stream_context.immediate_gsi_consistency) {
        let stream_item_id = stream_context.stream_item_id.ok_or_else(|| {
            StorageError::internal("stream_item_id required for table delete operation")
        })?;
        let old_item = if current_bytes.is_some() {
            Some(&current)
        } else {
            None
        };
        let stream_entries = create_item_update_stream_entries(
            StreamEntryContext {
                table_identity,
                table_name: &table_info.table_name,
                item_key: &item_key,
                indexers: &[],
                old_indexers: current_bytes
                    .is_some()
                    .then_some(stored_indexers.as_slice()),
            },
            &key_item,
            old_item,
            stream_item_id,
            true,
            stream_context.replication,
        )?;
        mutations.extend(
            stream_entries
                .into_iter()
                .map(|(template, value)| KvMutation::PutTemplate { template, value }),
        );
        mutations.push(change_index_marker_mutation(table_info, stream_item_id));
    }

    if stream_context.immediate_gsi_consistency {
        let old_item = current_bytes.is_some().then_some(&current);
        mutations.extend(plan_immediate_gsi_mutations(
            table_identity,
            table_info,
            old_item,
            None,
            None,
            codec,
        )?);
    }

    mutations.extend(ttl_index_kv_mutations(plan_ttl_index_mutations(
        &table_info.table_name,
        table_identity,
        table_info,
        ttl_config,
        current_bytes.is_some().then_some(&current),
        None,
    )?));

    mutations.extend(
        crate::storage_ops::stream_duration::item_stream_duration_kv_mutations(
            table_identity,
            table_info,
            key,
            0,
            item_stream_ttl_hours,
        )?,
    );

    Ok(((Some(current), None), mutations))
}

fn plan_table_check(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
    condition: &Condition,
    return_values_on_condition_check_failure: Option<&String>,
    current_bytes: Option<&[u8]>,
    codec: ItemValueCodec,
    index: usize,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    // Even for check we load current item to evaluate condition
    let (mut current, _) = decode_optional_table_item(current_bytes, table_info, codec)?;
    if current_bytes.is_some() {
        merge_key_attributes(&mut current, key, table_info)?;
    }
    ensure_condition_for_table(
        index,
        condition,
        &current,
        return_values_on_condition_check_failure,
        table_info,
    )?;

    Ok(((Some(current), None), Vec::new()))
}

#[expect(clippy::too_many_arguments)]
fn plan_table_update(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
    operations: &[UpdateOperation],
    condition: Option<&Condition>,
    return_values_on_condition_check_failure: Option<&String>,
    current_bytes: Option<&[u8]>,
    requested_indexers: Option<&[String]>,
    update_context: TableUpdateContext<'_>,
    codec: ItemValueCodec,
) -> StorageResult<(OldNewItems, Vec<KvMutation>)> {
    let item_key = storage_types::ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        key,
    )?;
    let item_key_bytes = table_keys::item_key(table_identity, &item_key)?;

    let (mut current, stored_indexers) =
        decode_optional_table_item(current_bytes, table_info, codec)?;
    if current_bytes.is_some() {
        merge_key_attributes(&mut current, key, table_info)?;
    }
    if let Some(condition) = condition {
        ensure_condition_for_table(
            update_context.index,
            condition,
            &current,
            return_values_on_condition_check_failure,
            table_info,
        )?;
    }

    let internal_old_item_needed =
        should_write_stream_entries(table_info, update_context.stream.immediate_gsi_consistency)
            || update_context.stream.immediate_gsi_consistency;
    let result_old_item =
        (update_context.preserve_old_item || internal_old_item_needed).then(|| current.clone());
    let ttl_old_item = update_context
        .ttl_config
        .is_some()
        .then(|| current.clone())
        .filter(|_| current_bytes.is_some());
    let mut new_item = apply_update_operations(current, operations)?;
    merge_key_attributes(&mut new_item, key, table_info)?;
    let effective_indexers = requested_indexers.unwrap_or(&stored_indexers);
    let declaration =
        IndexerDeclaration::try_new(effective_indexers.to_vec(), table_info.max_indexers)?;
    let serialized = encode_table_item(codec, &new_item, &new_item, &declaration)?;

    let mut mutations = vec![KvMutation::Put {
        key: item_key_bytes,
        value: serialized,
    }];
    if should_write_stream_entries(table_info, update_context.stream.immediate_gsi_consistency) {
        let stream_item_id = update_context.stream.stream_item_id.ok_or_else(|| {
            StorageError::internal("stream_item_id required for table update operation")
        })?;
        let old_item = if current_bytes.is_some() {
            result_old_item.as_ref()
        } else {
            None
        };
        let stream_entries = create_item_update_stream_entries(
            StreamEntryContext {
                table_identity,
                table_name: &table_info.table_name,
                item_key: &item_key,
                indexers: declaration.names(),
                old_indexers: current_bytes
                    .is_some()
                    .then_some(stored_indexers.as_slice()),
            },
            &new_item,
            old_item,
            stream_item_id,
            false,
            update_context.stream.replication,
        )?;
        mutations.extend(
            stream_entries
                .into_iter()
                .map(|(template, value)| KvMutation::PutTemplate { template, value }),
        );
        mutations.push(change_index_marker_mutation(table_info, stream_item_id));
    }

    if update_context.stream.immediate_gsi_consistency {
        let old_item = current_bytes
            .is_some()
            .then_some(result_old_item.as_ref())
            .flatten();
        mutations.extend(plan_immediate_gsi_mutations(
            table_identity,
            table_info,
            old_item,
            Some(&new_item),
            Some(&declaration),
            codec,
        )?);
    }

    mutations.extend(ttl_index_kv_mutations(plan_ttl_index_mutations(
        &table_info.table_name,
        table_identity,
        table_info,
        update_context.ttl_config,
        ttl_old_item.as_ref(),
        Some(&new_item),
    )?));

    mutations.extend(item_stream_duration_kv_mutations(
        table_identity,
        table_info,
        &new_item,
        update_context.item_stream_ttl_hours,
    )?);

    Ok(((result_old_item, Some(new_item)), mutations))
}

fn item_stream_duration_kv_mutations(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
    retention: Option<storage_types::StreamRetentionDuration>,
) -> StorageResult<Vec<KvMutation>> {
    let Some(retention) = retention else {
        return Ok(Vec::new());
    };
    let key_attributes = item_key_attributes(table_info, item)?;
    let policy_version = 0;
    crate::storage_ops::stream_duration::item_stream_duration_kv_mutations(
        table_identity,
        table_info,
        &key_attributes,
        policy_version,
        Some(retention),
    )
}

fn item_key_attributes(
    table_info: &StoredTableInfo,
    item: &impl AttributeValueLookup,
) -> StorageResult<KeyAttributes> {
    let mut key_attributes = KeyAttributes::with_capacity(table_info.key_schema.len());
    for key in &table_info.key_schema {
        let value = item
            .get_attribute_value(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        key_attributes.insert(key.attribute_name.clone(), value.clone());
    }
    Ok(key_attributes)
}

fn ttl_index_kv_mutations(mutations: Vec<TtlIndexMutation>) -> Vec<KvMutation> {
    mutations
        .into_iter()
        .map(|mutation| match mutation {
            TtlIndexMutation::Delete(key) => KvMutation::Delete { key },
            TtlIndexMutation::Put(key) => KvMutation::Put {
                key,
                value: Vec::new(),
            },
        })
        .collect()
}

fn plan_immediate_gsi_mutations(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
    declaration: Option<&IndexerDeclaration>,
    codec: ItemValueCodec,
) -> StorageResult<Vec<KvMutation>> {
    let mut mutations = Vec::new();
    let mut serialized_all_projection_item = None;
    for action in plan_gsi_write_actions(table_info, old_item, new_item)? {
        match action {
            GsiWriteAction::Delete {
                index,
                gsi_key,
                table_key,
            } => {
                let key =
                    gsi_item_key_bytes(table_identity, table_info, index, &gsi_key, &table_key)?
                        .ok_or_else(|| StorageError::internal("planned GSI delete missing key"))?;
                mutations.push(KvMutation::Delete { key });
            }
            GsiWriteAction::Put {
                index,
                gsi_key,
                table_key,
                projected_item,
            } => {
                let declaration = declaration.ok_or_else(|| {
                    StorageError::internal("GSI put requires an indexer declaration")
                })?;
                let logical_item = new_item
                    .ok_or_else(|| StorageError::internal("GSI put requires a logical item"))?;
                let key =
                    gsi_item_key_bytes(table_identity, table_info, index, &gsi_key, &table_key)?
                        .ok_or_else(|| StorageError::internal("planned GSI put missing key"))?;
                let value = if is_all_projection(&index.projection) {
                    if serialized_all_projection_item.is_none() {
                        serialized_all_projection_item = Some(encode_table_item(
                            codec,
                            logical_item,
                            &projected_item,
                            declaration,
                        )?);
                    }
                    serialized_all_projection_item
                        .as_ref()
                        .ok_or_else(|| StorageError::internal("missing serialized GSI item"))?
                        .clone()
                } else {
                    encode_table_item(codec, logical_item, &projected_item, declaration)?
                };
                mutations.push(KvMutation::Put { key, value });
            }
        }
    }

    Ok(mutations)
}

fn decode_optional_table_item(
    bytes: Option<&[u8]>,
    table_info: &StoredTableInfo,
    codec: ItemValueCodec,
) -> StorageResult<(HashMap<String, AttributeValue>, Vec<String>)> {
    let Some(bytes) = bytes else {
        return Ok((HashMap::new(), Vec::new()));
    };
    let indexed = decode_indexed_wire_item(codec, bytes)?;
    if indexed.slots().len() > table_info.max_indexers.as_usize() {
        return Err(StorageError::internal(
            "stored_item_corruption:declaration_exceeds_table_capacity",
        ));
    }
    let (item, declaration) = indexed.into_attribute_map_with_declaration()?;
    Ok((item, declaration.into_names()))
}

fn encode_table_item(
    codec: ItemValueCodec,
    logical_item: &HashMap<String, AttributeValue>,
    projected_item: &HashMap<String, AttributeValue>,
    declaration: &IndexerDeclaration,
) -> StorageResult<Vec<u8>> {
    let indexed = IndexedWireItem::extract_projected(logical_item, projected_item, declaration)?;
    encode_indexed_wire_item(codec, &indexed)
}

fn is_all_projection(projection: &storage_types::Projection) -> bool {
    projection
        .projection_type
        .as_ref()
        .is_none_or(|projection_type| *projection_type == storage_types::ProjectionType::All)
}

fn gsi_item_key_bytes(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    gsi: &storage_types::GlobalSecondaryIndex,
    gsi_key: &[GsiKeyPart],
    table_key: &[GsiKeyPart],
) -> StorageResult<Option<Vec<u8>>> {
    let item = GsiActionKeyLookup { gsi_key, table_key };
    ItemKey::from_key_schema_for_index(
        table_info.table_name.clone(),
        &table_info.key_schema,
        &gsi.index_name,
        &gsi.key_schema,
        &item,
    )?
    .map(|key| table_keys::item_key(table_identity, &key))
    .transpose()
}

fn compact_item_key(table_identity: &TableIdentity, item_key: &ItemKey) -> StorageResult<Vec<u8>> {
    table_keys::item_key(table_identity, item_key)
}

struct GsiActionKeyLookup<'a> {
    gsi_key: &'a [GsiKeyPart<'a>],
    table_key: &'a [GsiKeyPart<'a>],
}

impl AttributeValueLookup for GsiActionKeyLookup<'_> {
    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.table_key
            .iter()
            .find(|part| part.name == name)
            .or_else(|| self.gsi_key.iter().find(|part| part.name == name))
            .map(|part| part.value)
    }

    fn attribute_count(&self) -> usize {
        self.gsi_key.len() + self.table_key.len()
    }
}

fn has_non_ttl_gsi(table_info: &StoredTableInfo) -> bool {
    table_info
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|idx| !is_ttl_index(&idx.index_name)))
}

pub(crate) fn should_write_stream_entries(
    table_info: &StoredTableInfo,
    immediate_gsi_consistency: bool,
) -> bool {
    let stream_enabled = table_info
        .stream_specification
        .as_ref()
        .is_some_and(|spec| spec.stream_enabled);

    let has_gsi = has_non_ttl_gsi(table_info);

    stream_enabled || (has_gsi && !immediate_gsi_consistency)
}

pub(crate) fn operation_requires_stream_entries(
    operation: &TransactWriteTableOperation,
    immediate_gsi_consistency: bool,
) -> bool {
    use TransactWriteTableOperation as TableOp;

    match operation {
        TableOp::Check { .. } => false,
        TableOp::Put { table_info, .. }
        | TableOp::Delete { table_info, .. }
        | TableOp::Update { table_info, .. } => {
            should_write_stream_entries(table_info, immediate_gsi_consistency)
        }
    }
}

pub fn table_operation_primary_key(
    operation: &TransactWriteTableOperation,
) -> StorageResult<Vec<u8>> {
    use TransactWriteTableOperation as Op;

    let key_bytes = match operation {
        Op::Put {
            table_identity,
            table_info,
            item,
            ..
        } => {
            let item_key = storage_types::ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                item,
            )?;
            compact_item_key(table_identity, &item_key)?
        }
        Op::Delete {
            table_identity,
            table_info,
            key,
            ..
        }
        | Op::Update {
            table_identity,
            table_info,
            key,
            ..
        }
        | Op::Check {
            table_identity,
            table_info,
            key,
            ..
        } => {
            let item_key = storage_types::ItemKey::from_key_schema(
                table_info.table_name.clone(),
                &table_info.key_schema,
                key,
            )?;
            compact_item_key(table_identity, &item_key)?
        }
    };

    Ok(key_bytes)
}

pub fn deserialize_optional_item(
    bytes: Option<&[u8]>,
) -> StorageResult<HashMap<String, AttributeValue>> {
    if let Some(bytes) = bytes {
        deserialize_item_from_bytes(bytes)
    } else {
        Ok(HashMap::new())
    }
}

fn merge_key_attributes(
    current: &mut HashMap<String, AttributeValue>,
    key: &impl AttributeValueLookup,
    table_info: &StoredTableInfo,
) -> StorageResult<()> {
    for key_element in &table_info.key_schema {
        let Some(key_value) = key.get_attribute_value(&key_element.attribute_name) else {
            return Err(StorageError::internal(&format!(
                "Missing key attribute: {}",
                key_element.attribute_name
            )));
        };

        if let Some(existing) = current.get(&key_element.attribute_name) {
            if !key_attribute_values_match(existing, key_value) {
                return Err(StorageError::internal(&format!(
                    "Key attribute mismatch for {}",
                    key_element.attribute_name
                )));
            }
        } else {
            current.insert(
                key_element.attribute_name.clone(),
                normalize_key_attribute_value_for_write(key_value),
            );
        }
    }

    Ok(())
}

fn key_attribute_values_match(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            normalize_dynamodb_number_for_write(left) == normalize_dynamodb_number_for_write(right)
        }
        _ => left == right,
    }
}

fn normalize_key_attribute_value_for_write(value: &AttributeValue) -> AttributeValue {
    match value {
        AttributeValue::N(number) => {
            AttributeValue::N(normalize_dynamodb_number_for_write(number).into_owned())
        }
        _ => value.clone(),
    }
}

fn key_attributes_to_item_map(
    key: &KeyAttributes,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let mut item = HashMap::with_capacity(table_info.key_schema.len());
    merge_key_attributes(&mut item, key, table_info)?;
    Ok(item)
}

fn ensure_condition(
    index: usize,
    condition: &Condition,
    current: &HashMap<String, AttributeValue>,
) -> StorageResult<()> {
    if evaluate_condition(current, condition) {
        return Ok(());
    }
    Err(build_condition_err(index)?)
}

fn ensure_condition_for_table(
    index: usize,
    condition: &Condition,
    current: &HashMap<String, AttributeValue>,
    return_values_on_condition_check_failure: Option<&String>,
    _table_info: &StoredTableInfo,
) -> StorageResult<()> {
    if evaluate_condition(current, condition) {
        return Ok(());
    }
    Err(build_condition_err_with_item(
        index,
        return_values_on_condition_check_failure_all_old(return_values_on_condition_check_failure)
            .then_some(current)
            .filter(|current| !current.is_empty()),
    )?)
}

fn build_condition_err(index: usize) -> StorageResult<StorageError> {
    build_condition_err_with_item(index, None)
}

fn build_condition_err_with_item(
    index: usize,
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<StorageError> {
    let mut reasons = vec![];
    for _ in 0..index {
        reasons.push("None".to_string());
    }
    reasons.push(conditional_check_failed_reason(old_item)?);
    Ok(StorageEnum::TransactionCanceled { reasons }.into())
}
