// Table Management SQL Statements

use core::str;

use queue_provider::{MessageId, ReceiptHandle};
use storage_types::{
    DurationSeconds, IndexName, StreamItemId, StreamName, StreamRetentionDuration, TableName,
    TableStatus, TimestampMillis, UserStreamName,
};
use stream_provider::{CursorName, ReadDirection, StreamDataType};

use crate::{
    dialect::SqliteDialect,
    provider_core::statements::{metadata, queue, stream, ttl},
};

#[must_use]
pub fn create_tables_table() -> (&'static str, impl rusqlite::Params) {
    (metadata::create_tables_table(&SqliteDialect).sql, ())
}

#[must_use]
pub fn add_deletion_protection_column() -> (&'static str, impl rusqlite::Params) {
    (
        metadata::add_deletion_protection_column(&SqliteDialect).sql,
        (),
    )
}

#[must_use]
pub fn add_table_stream_duration_column() -> (&'static str, impl rusqlite::Params) {
    (
        metadata::add_table_stream_duration_column(&SqliteDialect).sql,
        (),
    )
}

#[must_use]
pub fn add_default_item_stream_duration_column() -> (&'static str, impl rusqlite::Params) {
    (
        metadata::add_default_item_stream_duration_column(&SqliteDialect).sql,
        (),
    )
}

#[must_use]
pub fn create_gsi_backfill_table() -> (&'static str, impl rusqlite::Params) {
    (metadata::create_gsi_backfill_table(&SqliteDialect).sql, ())
}

#[must_use]
pub fn create_ttl_config_table() -> (&'static str, impl rusqlite::Params) {
    (metadata::create_ttl_config_table(&SqliteDialect).sql, ())
}

#[must_use]
pub fn create_item_revisions_table() -> (&'static str, impl rusqlite::Params) {
    (
        metadata::create_item_revisions_table(&SqliteDialect).sql,
        (),
    )
}

#[must_use]
pub fn upsert_ttl_config(
    table_name: &TableName,
    blob: &[u8],
) -> (&'static str, impl rusqlite::Params) {
    (
        ttl::upsert_ttl_config(&SqliteDialect),
        (table_name.as_ref(), blob),
    )
}

#[must_use]
pub fn delete_ttl_config(table_name: &TableName) -> (&'static str, impl rusqlite::Params) {
    (
        ttl::delete_ttl_config(&SqliteDialect),
        (table_name.as_ref(),),
    )
}

#[must_use]
pub fn get_ttl_config(table_name: &TableName) -> (&'static str, impl rusqlite::Params) {
    (ttl::get_ttl_config(&SqliteDialect), (table_name.as_ref(),))
}

#[must_use]
pub fn list_ttl_configs() -> (&'static str, impl rusqlite::Params) {
    (ttl::list_ttl_configs(), ())
}

#[must_use]
pub fn upsert_gsi_backfill_start(
    table_name: &TableName,
    index_name: &IndexName,
    status: &str,
    scan_lek: Option<&str>,
    captured_stream_tail: Option<&str>,
    created_at: &TimestampMillis,
    updated_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    (
        r"INSERT INTO gsi_backfill (table_name, index_name, status, scan_lek, captured_stream_tail, created_at, updated_at)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
    ON CONFLICT(table_name, index_name) DO UPDATE SET status = excluded.status, scan_lek = excluded.scan_lek, captured_stream_tail = excluded.captured_stream_tail, updated_at = excluded.updated_at",
        (
            table_name.as_ref(),
            index_name.as_ref(),
            status,
            scan_lek,
            captured_stream_tail,
            **created_at,
            **updated_at,
        ),
    )
}

#[must_use]
pub fn update_gsi_backfill_progress(
    table_name: &TableName,
    index_name: &IndexName,
    scan_lek: Option<&str>,
    updated_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    (
        r"UPDATE gsi_backfill SET scan_lek = ?3, updated_at = ?4 WHERE table_name = ?1 AND index_name = ?2",
        (
            table_name.as_ref(),
            index_name.as_ref(),
            scan_lek,
            **updated_at,
        ),
    )
}

#[must_use]
pub fn mark_gsi_backfill_done(
    table_name: &TableName,
    index_name: &IndexName,
    updated_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    (
        r"UPDATE gsi_backfill SET status = 'Done', updated_at = ?3 WHERE table_name = ?1 AND index_name = ?2",
        (table_name.as_ref(), index_name.as_ref(), **updated_at),
    )
}

#[must_use]
pub fn get_gsi_backfill(
    table_name: &TableName,
    index_name: &IndexName,
) -> (&'static str, impl rusqlite::Params) {
    (
        r"SELECT status, scan_lek, captured_stream_tail, created_at, updated_at FROM gsi_backfill WHERE table_name = ?1 AND index_name = ?2",
        (table_name.as_ref(), index_name.as_ref()),
    )
}

#[must_use]
pub fn list_pending_gsi_backfills() -> (&'static str, impl rusqlite::Params) {
    (
        r"SELECT table_name, index_name, status, scan_lek, captured_stream_tail, created_at, updated_at FROM gsi_backfill WHERE status = 'Backfilling'",
        (),
    )
}

#[must_use]
pub fn check_table_exists(table_name: &str) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::table_exists(&SqliteDialect, table_name).sql,
        [table_name],
    )
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn insert_table(
    table_id: u128,
    table_name: &TableName,
    created_at: &TimestampMillis,
    attribute_definitions: &str,
    key_schema: &str,
    global_secondary_indexes: Option<&str>,
    stream_specification: Option<&str>,
    deletion_protection_enabled: bool,
    table_stream_duration: StreamRetentionDuration,
    default_item_stream_duration: StreamRetentionDuration,
) -> (&'static str, impl rusqlite::Params) {
    let table_id = table_id.to_string();
    let table_stream_duration_hours = table_stream_duration.as_hours_wire_value();
    let default_item_stream_duration_hours = default_item_stream_duration.as_hours_wire_value();
    (
        metadata::insert_table(
            &SqliteDialect,
            table_id.clone(),
            table_name.as_ref(),
            **created_at,
            attribute_definitions,
            key_schema,
            global_secondary_indexes.map(str::to_owned),
            stream_specification.map(str::to_owned),
            deletion_protection_enabled,
            table_stream_duration_hours,
            default_item_stream_duration_hours,
        )
        .sql,
        (
            table_id,
            table_name.as_ref(),
            "CREATING",
            **created_at,
            attribute_definitions,
            key_schema,
            global_secondary_indexes,
            0i64,
            0i64,
            stream_specification,
            if deletion_protection_enabled {
                1i64
            } else {
                0i64
            },
            table_stream_duration_hours,
            default_item_stream_duration_hours,
        ),
    )
}

#[must_use]
pub fn update_table_status(
    table_status: &TableStatus,
    table_name: &TableName,
) -> (&'static str, impl rusqlite::Params) {
    let table_status: String = table_status.into();
    (
        metadata::update_table_status(&SqliteDialect, table_status.clone(), table_name.as_ref())
            .sql,
        (table_status, table_name.as_ref()),
    )
}

#[must_use]
pub fn update_deletion_protection(
    table_name: &TableName,
    deletion_protection_enabled: bool,
) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::update_deletion_protection(
            &SqliteDialect,
            deletion_protection_enabled,
            table_name.as_ref(),
        )
        .sql,
        (
            if deletion_protection_enabled {
                1i64
            } else {
                0i64
            },
            table_name.as_ref(),
        ),
    )
}

#[must_use]
pub fn update_stream_durations(
    table_name: &TableName,
    table_stream_duration: StreamRetentionDuration,
    default_item_stream_duration: StreamRetentionDuration,
) -> (&'static str, impl rusqlite::Params) {
    let table_stream_duration_hours = table_stream_duration.as_hours_wire_value();
    let default_item_stream_duration_hours = default_item_stream_duration.as_hours_wire_value();
    (
        metadata::update_stream_durations(
            &SqliteDialect,
            table_stream_duration_hours,
            default_item_stream_duration_hours,
            table_name.as_ref(),
        )
        .sql,
        (
            table_stream_duration_hours,
            default_item_stream_duration_hours,
            table_name.as_ref(),
        ),
    )
}

#[must_use]
pub fn update_gsis(
    table_name: &TableName,
    global_secondary_indexes_json: &Option<String>,
) -> (&'static str, impl rusqlite::Params) {
    (
        "UPDATE tables SET global_secondary_indexes = ?1 WHERE table_name = ?2",
        (global_secondary_indexes_json, table_name.as_ref()),
    )
}

#[must_use]
pub fn update_attribute_definitions(
    table_name: &TableName,
    attribute_definitions_json: &str,
) -> (&'static str, impl rusqlite::Params) {
    (
        "UPDATE tables SET attribute_definitions = ?1 WHERE table_name = ?2",
        (attribute_definitions_json, table_name.as_ref()),
    )
}

#[must_use]
pub fn update_stream_specification(
    table_name: &TableName,
    stream_specification_json: &Option<String>,
) -> (&'static str, impl rusqlite::Params) {
    (
        "UPDATE tables SET stream_specification = ?1 WHERE table_name = ?2",
        (stream_specification_json, table_name.as_ref()),
    )
}

#[must_use]
pub fn get_table_info(table_name: &TableName) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::get_table_info(&SqliteDialect, table_name.as_ref()).sql,
        [table_name.as_ref()],
    )
}

#[must_use]
pub fn list_all_tables(limit: u32) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::list_all_tables(&SqliteDialect, limit).sql,
        [limit],
    )
}

#[must_use]
pub fn list_tables_after(
    limit: u32,
    exclusive_start_table_name: String,
) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::list_tables_after(&SqliteDialect, limit, exclusive_start_table_name.clone()).sql,
        (exclusive_start_table_name, limit),
    )
}

#[must_use]
pub fn delete_table(table_name: &TableName) -> (&'static str, impl rusqlite::Params) {
    (
        metadata::delete_table_metadata(&SqliteDialect, table_name.as_ref()).sql,
        [table_name.as_ref()],
    )
}

#[must_use]
pub fn drop_table(table_name_safe: &str) -> (String, impl rusqlite::Params) {
    (
        format!("DROP TABLE IF EXISTS \"table_{table_name_safe}\""),
        [],
    )
}

// Stream Table Operations

#[must_use]
pub fn check_stream_tables_exist() -> (&'static str, impl rusqlite::Params) {
    (
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sys_stream_items'",
        [],
    )
}

#[must_use]
pub fn insert_stream_entry(
    stream_name: &StreamName,
    item_id: &StreamItemId,
    data: &[u8],
    created_at: &TimestampMillis,
    data_type: StreamDataType,
) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    let item_id = item_id.to_string();
    let created_at = **created_at;
    let data_type = data_type as i32;

    (
        stream::insert_stream_entry(&SqliteDialect),
        (stream_name, item_id, data, created_at, data_type),
    )
}

#[must_use]
pub fn insert_change_index_marker(
    slot: u16,
    versionstamp: &str,
    table_id: &str,
    created_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    (
        stream::insert_change_index_marker(&SqliteDialect),
        (
            i64::from(slot),
            versionstamp,
            table_id,
            created_at.timestamp_millis(),
        ),
    )
}

#[must_use]
pub fn list_change_index_markers(
    slot: u16,
    after_versionstamp: &str,
    limit: i64,
) -> (&'static str, impl rusqlite::Params) {
    (
        stream::list_change_index_markers(&SqliteDialect),
        (i64::from(slot), after_versionstamp, limit),
    )
}

#[must_use]
pub fn trim_change_index_markers_older_than(
    cutoff_created_at_ms: i64,
) -> (&'static str, impl rusqlite::Params) {
    (
        stream::trim_change_index_markers_older_than(&SqliteDialect),
        (cutoff_created_at_ms,),
    )
}

// Queue Management SQL Statements

#[must_use]
pub fn create_queues_table() -> (&'static str, impl rusqlite::Params) {
    (queue::create_queues_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_messages_table() -> (&'static str, impl rusqlite::Params) {
    (queue::create_messages_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_messages_queue_visibility_index() -> (&'static str, impl rusqlite::Params) {
    (
        queue::create_messages_queue_visibility_index(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn create_messages_queue_receipt_index() -> (&'static str, impl rusqlite::Params) {
    (
        queue::create_messages_queue_receipt_index(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn insert_or_replace_queue(
    queue_name: &str,
    queue_url: &str,
    attributes_json: &str,
    created_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    let created_at = **created_at;
    (
        queue::upsert_queue(&SqliteDialect),
        (queue_name, queue_url, attributes_json, created_at),
    )
}

#[must_use]
pub fn get_queue(queue_url: &str) -> (&'static str, impl rusqlite::Params) {
    (queue::get_queue(&SqliteDialect), [queue_url])
}

#[must_use]
pub fn get_queue_by_name(queue_name: &str) -> (&'static str, impl rusqlite::Params) {
    (queue::get_queue_by_name(&SqliteDialect), [queue_name])
}

#[must_use]
pub fn list_queues(queue_name_prefix: Option<&str>) -> (&'static str, Vec<rusqlite::types::Value>) {
    match queue_name_prefix {
        Some(prefix) => (
            queue::list_queues_with_prefix(&SqliteDialect),
            vec![rusqlite::types::Value::Text(format!("{prefix}%"))],
        ),
        None => (queue::list_all_queues(&SqliteDialect), Vec::new()),
    }
}

#[must_use]
pub fn purge_queue(queue_name: &str) -> (&'static str, impl rusqlite::Params) {
    (
        queue::delete_messages_for_queue(&SqliteDialect),
        [queue_name],
    )
}

#[must_use]
pub fn set_queue_attributes(
    queue_url: &str,
    attributes_json: &str,
) -> (&'static str, impl rusqlite::Params) {
    (
        queue::set_queue_attributes(&SqliteDialect),
        (attributes_json, queue_url),
    )
}

#[must_use]
pub fn send_message(
    message_id: &MessageId,
    queue_name: &str,
    body: &str,
    message_attributes_json: Option<&str>,
    visibility_timestamp: &TimestampMillis,
    created_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    let message_id = message_id.to_string();
    let visibility_timestamp = **visibility_timestamp;
    let created_at = **created_at;
    (
        queue::send_message(&SqliteDialect),
        (
            message_id,
            queue_name,
            body,
            message_attributes_json,
            visibility_timestamp,
            created_at,
        ),
    )
}

#[must_use]
pub fn receive_messages(
    queue_name: &str,
    now_timestamp: &TimestampMillis,
    max_messages: u32,
) -> (&'static str, impl rusqlite::Params) {
    let now_timestamp = **now_timestamp;
    (
        queue::receive_messages(&SqliteDialect),
        (queue_name, now_timestamp, max_messages),
    )
}

#[must_use]
pub fn update_message_visibility(
    next_visible: &TimestampMillis,
    next_receipt_handle: &ReceiptHandle,
    message_id: &MessageId,
    queue_name: &str,
    receipt_handle: Option<&ReceiptHandle>,
) -> (&'static str, impl rusqlite::Params) {
    let next_visible = **next_visible;
    let next_receipt_handle = next_receipt_handle.0.as_str();
    let message_id = message_id.to_string();
    let receipt_handle = receipt_handle.map(|rh| rh.0.as_str());
    (
        queue::claim_message(&SqliteDialect),
        (
            next_visible,
            next_receipt_handle,
            message_id,
            queue_name,
            receipt_handle,
        ),
    )
}

#[must_use]
pub fn delete_message(
    queue_name: &str,
    receipt_handle: &ReceiptHandle,
) -> (&'static str, impl rusqlite::Params) {
    let receipt_handle = receipt_handle.0.as_str();
    (
        queue::delete_message(&SqliteDialect),
        (queue_name, receipt_handle),
    )
}

#[must_use]
pub fn change_message_visibility(
    new_visibility_timestamp: &TimestampMillis,
    queue_name: &str,
    receipt_handle: &ReceiptHandle,
) -> (&'static str, impl rusqlite::Params) {
    let new_visibility_timestamp = **new_visibility_timestamp;
    let receipt_handle = receipt_handle.0.as_str();
    (
        queue::change_message_visibility(&SqliteDialect),
        (new_visibility_timestamp, queue_name, receipt_handle),
    )
}

#[must_use]
pub fn update_message_checkpoint(
    checkpoint_data: &str,
    queue_name: &str,
    receipt_handle: &ReceiptHandle,
) -> (&'static str, impl rusqlite::Params) {
    let receipt_handle = receipt_handle.0.as_str();
    (
        queue::update_message_checkpoint(&SqliteDialect),
        (checkpoint_data, queue_name, receipt_handle),
    )
}

// Stream Management SQL Statements

#[must_use]
pub fn create_user_streams_table() -> (&'static str, impl rusqlite::Params) {
    (stream::create_user_streams_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_stream_items_table() -> (&'static str, impl rusqlite::Params) {
    (stream::create_stream_items_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_stream_cursors_table() -> (&'static str, impl rusqlite::Params) {
    (stream::create_stream_cursors_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_change_index_table() -> (&'static str, impl rusqlite::Params) {
    (stream::create_change_index_table(&SqliteDialect), [])
}

#[must_use]
pub fn create_stream_format_metadata_table() -> (&'static str, impl rusqlite::Params) {
    (
        stream::create_stream_format_metadata_table(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn get_stream_format_version() -> (&'static str, impl rusqlite::Params) {
    (
        stream::get_stream_format_version(&SqliteDialect),
        (stream::ITEM_VERSIONED_STREAM_FORMAT_KEY,),
    )
}

#[must_use]
pub fn upsert_stream_format_version() -> (&'static str, impl rusqlite::Params) {
    (
        stream::upsert_stream_format_version(&SqliteDialect),
        (
            stream::ITEM_VERSIONED_STREAM_FORMAT_KEY,
            stream::ITEM_VERSIONED_STREAM_FORMAT_VERSION,
        ),
    )
}

#[must_use]
pub fn count_stream_items() -> (&'static str, impl rusqlite::Params) {
    (stream::count_stream_items(&SqliteDialect), [])
}

#[must_use]
pub fn list_stream_pointer_payloads() -> (&'static str, impl rusqlite::Params) {
    (
        stream::list_stream_pointer_payloads(&SqliteDialect),
        (StreamDataType::StreamPointer as i32,),
    )
}

#[must_use]
pub fn create_stream_items_internal_time_index() -> (&'static str, impl rusqlite::Params) {
    (
        stream::create_stream_items_internal_time_index(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn create_stream_cursors_internal_index() -> (&'static str, impl rusqlite::Params) {
    (
        stream::create_stream_cursors_internal_index(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn create_change_index_created_at_index() -> (&'static str, impl rusqlite::Params) {
    (
        stream::create_change_index_created_at_index(&SqliteDialect),
        [],
    )
}

#[must_use]
pub fn check_stream_exists(stream_name: &str) -> (&'static str, impl rusqlite::Params) {
    (
        stream::check_user_stream_exists(&SqliteDialect),
        [stream_name],
    )
}

#[must_use]
pub fn insert_new_stream(
    user_stream_name: &UserStreamName,
    stream_name: &StreamName,
    ttl_seconds: Option<&DurationSeconds>,
    created_at: &TimestampMillis,
    updated_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    let user_stream_name = user_stream_name.as_str();
    let stream_name: String = stream_name.into();
    let created_at = **created_at;
    let updated_at = **updated_at;
    let ttl_seconds = ttl_seconds.map(|ts| **ts);
    (
        stream::insert_user_stream(&SqliteDialect),
        (
            user_stream_name,
            stream_name,
            ttl_seconds,
            created_at,
            updated_at,
        ),
    )
}

#[must_use]
pub fn get_stream_internal_id(
    user_stream_name: &UserStreamName,
) -> (&'static str, impl rusqlite::Params) {
    let user_stream_name = user_stream_name.as_str();
    (
        stream::get_stream_internal_id(&SqliteDialect),
        [user_stream_name],
    )
}

#[must_use]
pub fn delete_stream_cursors(stream_name: &StreamName) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    (stream::delete_stream_cursors(&SqliteDialect), [stream_name])
}

#[must_use]
pub fn delete_stream_items(stream_name: &StreamName) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    (stream::delete_stream_items(&SqliteDialect), [stream_name])
}

#[must_use]
pub fn delete_stream_item(
    stream_name: &StreamName,
    item_id: &StreamItemId,
) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    let item_id = item_id.to_string();
    (
        "DELETE FROM sys_stream_items WHERE stream_name = ?1 AND item_id = ?2",
        (stream_name, item_id),
    )
}

#[must_use]
pub fn delete_user_stream(
    user_stream_name: &UserStreamName,
) -> (&'static str, impl rusqlite::Params) {
    let user_stream_name = user_stream_name.as_str();
    (
        stream::delete_user_stream(&SqliteDialect),
        [user_stream_name],
    )
}

#[must_use]
pub fn get_stream_info(user_stream_name: &UserStreamName) -> (&'static str, impl rusqlite::Params) {
    let user_stream_name = user_stream_name.as_str();
    (stream::get_stream(&SqliteDialect), [user_stream_name])
}

// Cursor Operations

#[must_use]
pub fn check_cursor_exists(
    cursor_name: &CursorName,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    (
        stream::check_cursor_exists(&SqliteDialect),
        (cursor_name, stream_name),
    )
}

#[must_use]
pub fn get_latest_item_for_cursor(
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    (
        stream::get_latest_stream_item(&SqliteDialect),
        [stream_name],
    )
}

#[must_use]
pub fn insert_cursor(
    cursor_name: &CursorName,
    stream_name: &StreamName,
    position_id: &StreamItemId,
    created_at: &TimestampMillis,
) -> (&'static str, impl rusqlite::Params) {
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    let position_id = position_id.to_string();
    let created_at = **created_at;
    (
        stream::insert_cursor(&SqliteDialect),
        (cursor_name, stream_name, position_id, created_at),
    )
}

#[must_use]
pub fn delete_cursor(
    cursor_name: &CursorName,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    (
        stream::delete_cursor(&SqliteDialect),
        (cursor_name, stream_name),
    )
}

#[must_use]
pub fn get_cursor_position(
    cursor_name: &CursorName,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    (
        stream::get_cursor_position(&SqliteDialect),
        (cursor_name, stream_name),
    )
}

#[must_use]
pub fn read_stream_from_position(
    stream_name: &StreamName,
    cursor_position: &StreamItemId,
    limit: u32,
    direction: ReadDirection,
) -> (String, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    let cursor_position = cursor_position.to_string();
    let sql = match direction {
        ReadDirection::Forward => stream::read_stream_forward(&SqliteDialect),
        ReadDirection::Backward => stream::read_stream_backward(&SqliteDialect),
    };

    (
        sql.to_string(),
        (stream_name.clone(), stream_name, cursor_position, limit + 1),
    )
}

#[must_use]
pub fn check_cursor_exists_for_advance(
    cursor_name: &CursorName,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    (
        stream::check_cursor_exists(&SqliteDialect),
        (cursor_name, stream_name),
    )
}

#[must_use]
pub fn check_item_exists_for_advance(
    item_id: &StreamItemId,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let item_id = item_id.to_string();
    let stream_name: String = stream_name.into();
    (
        stream::check_stream_item_exists(&SqliteDialect),
        (item_id, stream_name),
    )
}

#[must_use]
pub fn advance_cursor_position(
    item_id: &StreamItemId,
    cursor_name: &CursorName,
    stream_name: &StreamName,
) -> (&'static str, impl rusqlite::Params) {
    let item_id = item_id.to_string();
    let cursor_name = cursor_name.as_str();
    let stream_name: String = stream_name.into();
    (
        stream::advance_cursor_position(&SqliteDialect),
        (item_id, cursor_name, stream_name),
    )
}

#[must_use]
pub fn get_cursor_details(
    stream_name: &StreamName,
    cursor_name: &CursorName,
) -> (&'static str, impl rusqlite::Params) {
    let stream_name: String = stream_name.into();
    let cursor_name = cursor_name.as_str();
    (
        stream::get_cursor(&SqliteDialect),
        (stream_name.clone(), cursor_name, stream_name),
    )
}
