use storage_types::{
    BatchWriteItemRequest, KeyAttributes, StorageError, StorageResult, StreamRetentionDuration,
    TableName, TimestampMillis, TransactWriteItemsRequest, UpdateTableRequest,
};

use crate::{StreamTrimDueMarker, StreamTrimScope, StreamTrimState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStreamDurationPlan {
    pub table_name: TableName,
    pub policy_version: u64,
    pub retention: StreamRetentionDuration,
    pub default_item_retention: StreamRetentionDuration,
    pub trim_state: StreamTrimState,
    pub due_marker: Option<StreamTrimDueMarker>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemStreamDurationPlan {
    pub table_name: TableName,
    pub item_key: KeyAttributes,
    pub policy_version: u64,
    pub requested_retention: StreamRetentionDuration,
    pub effective_retention: StreamRetentionDuration,
    pub trim_state: StreamTrimState,
    pub due_marker: Option<StreamTrimDueMarker>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemStreamTtlIntent {
    pub table_name: TableName,
    pub item_key: KeyAttributes,
    pub retention: StreamRetentionDuration,
}

pub fn plan_table_stream_duration(
    table_name: TableName,
    table_scope_id: impl Into<String>,
    policy_version: u64,
    retention: StreamRetentionDuration,
    default_item_retention: StreamRetentionDuration,
    updated_at: TimestampMillis,
) -> TableStreamDurationPlan {
    let scope = StreamTrimScope::table(table_scope_id, table_name.clone());
    let next_due_at = next_due_for_retention(updated_at, retention);
    let trim_state = StreamTrimState {
        scope: scope.clone(),
        policy_version,
        retention,
        effective_retention: retention,
        next_due_at,
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at,
    };
    let due_marker =
        next_due_at.map(|due_at| StreamTrimDueMarker::new(due_at, scope, policy_version));

    TableStreamDurationPlan {
        table_name,
        policy_version,
        retention,
        default_item_retention,
        trim_state,
        due_marker,
    }
}

pub fn plan_item_stream_duration(
    intent: ItemStreamTtlIntent,
    item_scope_id: impl Into<String>,
    item_key_hash: impl Into<String>,
    policy_version: u64,
    table_retention: StreamRetentionDuration,
    updated_at: TimestampMillis,
) -> StorageResult<ItemStreamDurationPlan> {
    if intent.item_key.is_empty() {
        return Err(StorageError::validation(
            "custom item stream TTL requires one concrete item key",
        ));
    }

    let plan = plan_validated_item_stream_duration(
        intent.table_name.clone(),
        item_scope_id,
        item_key_hash,
        policy_version,
        intent.retention,
        table_retention,
        updated_at,
    );

    Ok(ItemStreamDurationPlan {
        table_name: intent.table_name,
        item_key: intent.item_key,
        policy_version: plan.policy_version,
        requested_retention: plan.requested_retention,
        effective_retention: plan.effective_retention,
        trim_state: plan.trim_state,
        due_marker: plan.due_marker,
    })
}

pub fn plan_validated_item_stream_duration(
    table_name: TableName,
    item_scope_id: impl Into<String>,
    item_key_hash: impl Into<String>,
    policy_version: u64,
    requested_retention: StreamRetentionDuration,
    table_retention: StreamRetentionDuration,
    updated_at: TimestampMillis,
) -> ValidatedItemStreamDurationPlan {
    let effective_retention =
        StreamRetentionDuration::effective_item_retention(table_retention, requested_retention);
    let scope = StreamTrimScope::item(item_scope_id, table_name, item_key_hash);
    let next_due_at = next_due_for_retention(updated_at, effective_retention);
    let trim_state = StreamTrimState {
        scope: scope.clone(),
        policy_version,
        retention: requested_retention,
        effective_retention,
        next_due_at,
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at,
    };
    let due_marker =
        next_due_at.map(|due_at| StreamTrimDueMarker::new(due_at, scope, policy_version));

    ValidatedItemStreamDurationPlan {
        policy_version,
        requested_retention,
        effective_retention,
        trim_state,
        due_marker,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedItemStreamDurationPlan {
    pub policy_version: u64,
    pub requested_retention: StreamRetentionDuration,
    pub effective_retention: StreamRetentionDuration,
    pub trim_state: StreamTrimState,
    pub due_marker: Option<StreamTrimDueMarker>,
}

pub fn validate_transaction_item_ttl_intents(intents: &[ItemStreamTtlIntent]) -> StorageResult<()> {
    let mut seen: Vec<&ItemStreamTtlIntent> = Vec::with_capacity(intents.len());
    for intent in intents {
        if intent.item_key.is_empty() {
            return Err(StorageError::validation(
                "custom item stream TTL requires one concrete item key",
            ));
        }
        if seen.iter().any(|existing| {
            existing.table_name == intent.table_name
                && existing.item_key == intent.item_key
                && existing.retention != intent.retention
        }) {
            return Err(StorageError::validation(
                "conflicting custom item stream TTL declarations for the same item",
            ));
        }
        seen.push(intent);
    }
    Ok(())
}

pub fn batch_write_request_has_custom_item_stream_ttl(request: &BatchWriteItemRequest) -> bool {
    request.request_items.values().any(|writes| {
        writes.iter().any(|write| {
            write
                .put_request
                .as_ref()
                .and_then(|put| put.aux_item_stream_ttl_hours)
                .is_some()
                || write
                    .delete_request
                    .as_ref()
                    .and_then(|delete| delete.aux_item_stream_ttl_hours)
                    .is_some()
        })
    })
}

pub fn transaction_request_has_custom_item_stream_ttl(request: &TransactWriteItemsRequest) -> bool {
    request.transact_items.iter().any(|item| {
        item.put
            .as_ref()
            .and_then(|put| put.aux_item_stream_ttl_hours)
            .is_some()
            || item
                .update
                .as_ref()
                .and_then(|update| update.aux_item_stream_ttl_hours)
                .is_some()
            || item
                .delete
                .as_ref()
                .and_then(|delete| delete.aux_item_stream_ttl_hours)
                .is_some()
    })
}

pub fn update_table_request_has_custom_stream_duration(request: &UpdateTableRequest) -> bool {
    request.aux_stream_duration_hours.is_some()
        || request.aux_default_item_stream_duration_hours.is_some()
}

fn next_due_for_retention(
    updated_at: TimestampMillis,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(updated_at + (i64::from(hours) * 60 * 60 * 1000))
        }
    }
}
