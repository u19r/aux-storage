use storage_types::{
    BatchWriteItemRequest, ItemHistoryAddress, StorageError, StorageResult, StreamName,
    StreamRetentionDuration, TableHistoryId, TableName, TimestampMillis, TransactWriteItemsRequest,
    UpdateTableRequest,
};
use stream_provider::StreamRowKind;

use super::{
    ItemStreamDurationInput, ItemStreamTtlIntent, StreamTrimDueMarker, StreamTrimScope,
    StreamTrimScopeKind, StreamTrimState, StreamTrimStateWrite, TableStreamDurationPlan,
};

/// One stable table-incarnation namespace across SQL and KV retention layouts.
pub fn table_retention_scope_id(history_id: TableHistoryId) -> String {
    format!("table:{:032x}", u128::from_be_bytes(*history_id.as_ref()))
}

/// The item scope names its exact physical stream, including table incarnation.
pub fn item_retention_scope_id(address: &ItemHistoryAddress) -> String {
    format!(
        "item:{}",
        hex::encode(address.physical_stream_name().as_ref())
    )
}

/// Resolve a persisted scope to the typed physical stream used by both backend
/// families.
pub fn stream_retention_kind(scope: &StreamTrimScope) -> StorageResult<StreamRowKind> {
    match scope.kind {
        StreamTrimScopeKind::Table => {
            let id = decode_history_id(&scope.scope_id, "table:")?;
            let address = ItemHistoryAddress {
                history_id: id,
                stream_name: StreamName::table_stream(&scope.table_name),
            };
            if table_retention_scope_id(id) != scope.scope_id {
                return Err(StorageError::internal(
                    "table retention scope is not canonical",
                ));
            }
            Ok(StreamRowKind::Table(address.physical_stream_name()))
        }
        StreamTrimScopeKind::Item => item_retention_physical_name(scope).map(StreamRowKind::Item),
    }
}

fn item_retention_physical_name(scope: &StreamTrimScope) -> StorageResult<StreamName> {
    let hex = scope
        .scope_id
        .strip_prefix("item:")
        .ok_or_else(|| StorageError::internal("item retention scope is invalid"))?;
    let bytes =
        hex::decode(hex).map_err(|_| StorageError::internal("item retention scope is invalid"))?;
    const HISTORY_PREFIX: &[u8] = b"history/";
    if bytes.len() <= 41 || !bytes.starts_with(HISTORY_PREFIX) || bytes[40] != b'/' {
        return Err(StorageError::internal("item retention address is invalid"));
    }
    let id_bytes: [u8; 16] = hex::decode(&bytes[8..40])
        .map_err(|_| StorageError::internal("item retention history is invalid"))?
        .try_into()
        .map_err(|_| StorageError::internal("item retention history is invalid"))?;
    let address = ItemHistoryAddress {
        history_id: TableHistoryId::from(id_bytes),
        stream_name: StreamName::new(&bytes[41..]),
    };
    if address
        .stream_name
        .item_table_name()
        .is_none_or(|name| name.sanitized_name() != scope.table_name.sanitized_name())
        || address.physical_stream_name().as_ref() != bytes
        || item_retention_scope_id(&address) != scope.scope_id
    {
        return Err(StorageError::internal(
            "item retention address differs from its scope",
        ));
    }
    Ok(address.physical_stream_name())
}

fn decode_history_id(scope_id: &str, prefix: &str) -> StorageResult<TableHistoryId> {
    let hex = scope_id
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("stream retention scope is invalid"))?;
    let bytes: [u8; 16] = hex::decode(hex)
        .map_err(|_| StorageError::internal("stream retention history is invalid"))?
        .try_into()
        .map_err(|_| StorageError::internal("stream retention history is invalid"))?;
    Ok(TableHistoryId::from(bytes))
}

pub fn merge_table_stream_duration_request(
    current_table_retention: StreamRetentionDuration,
    current_default_item_retention: StreamRetentionDuration,
    requested_table_retention: Option<StreamRetentionDuration>,
    requested_default_item_retention: Option<StreamRetentionDuration>,
) -> (StreamRetentionDuration, StreamRetentionDuration) {
    (
        requested_table_retention.unwrap_or(current_table_retention),
        requested_default_item_retention.unwrap_or(current_default_item_retention),
    )
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

pub fn plan_item_stream_duration(input: ItemStreamDurationInput) -> StreamTrimStateWrite {
    let ItemStreamDurationInput {
        table_name,
        scope_id,
        key_hash,
        requested_retention,
        table_retention,
        updated_at,
    } = input;
    let effective_retention =
        StreamRetentionDuration::effective_item_retention(table_retention, requested_retention);
    let policy_version = (u64::from(encode_duration_policy(requested_retention)) << 32)
        | u64::from(encode_duration_policy(effective_retention));
    let scope = StreamTrimScope::item(scope_id, table_name, key_hash);
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

    StreamTrimStateWrite {
        state: trim_state,
        next_marker: due_marker,
    }
}

pub fn validate_transaction_item_ttl_intents(intents: &[ItemStreamTtlIntent]) -> StorageResult<()> {
    for (index, intent) in intents.iter().enumerate() {
        if intent.item_key.is_empty() {
            return Err(StorageError::validation(
                "custom item stream TTL requires one concrete item key",
            ));
        }
        if intents[..index].iter().any(|existing| {
            existing.table_name == intent.table_name
                && existing.item_key == intent.item_key
                && existing.retention != intent.retention
        }) {
            return Err(StorageError::validation(
                "conflicting custom item stream TTL declarations for the same item",
            ));
        }
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

fn encode_duration_policy(duration: StreamRetentionDuration) -> u32 {
    match duration {
        StreamRetentionDuration::Forever => u32::MAX,
        StreamRetentionDuration::FiniteHours(hours) => u32::from(hours),
    }
}
