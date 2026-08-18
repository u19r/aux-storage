#![allow(dead_code)]

pub(crate) const BEGIN_CONCURRENT: &str = "BEGIN CONCURRENT";
pub(crate) const BEGIN_EXCLUSIVE: &str = "BEGIN EXCLUSIVE";
pub(crate) const BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";

use crate::{
    dialect::TursoDialect,
    provider_core::statements::{durable_revision, metadata, queue, stream},
};

#[must_use]
pub fn commit() -> &'static str {
    "COMMIT"
}

#[must_use]
pub fn rollback() -> &'static str {
    "ROLLBACK"
}

#[must_use]
pub fn create_tables_table() -> &'static str {
    metadata::create_tables_table(&TursoDialect).sql
}

#[must_use]
pub fn add_deletion_protection_column() -> &'static str {
    metadata::add_deletion_protection_column(&TursoDialect).sql
}

#[must_use]
pub fn add_table_stream_duration_column() -> &'static str {
    metadata::add_table_stream_duration_column(&TursoDialect).sql
}

#[must_use]
pub fn add_default_item_stream_duration_column() -> &'static str {
    metadata::add_default_item_stream_duration_column(&TursoDialect).sql
}

#[must_use]
pub fn create_gsi_backfill_table() -> &'static str {
    metadata::create_gsi_backfill_table(&TursoDialect).sql
}

#[must_use]
pub fn create_ttl_config_table() -> &'static str {
    metadata::create_ttl_config_table(&TursoDialect).sql
}

#[must_use]
pub fn create_item_revisions_table() -> &'static str {
    metadata::create_item_revisions_table(&TursoDialect).sql
}

#[must_use]
pub fn table_exists() -> &'static str {
    metadata::table_exists(&TursoDialect, "").sql
}

#[must_use]
pub fn get_table_info() -> &'static str {
    metadata::get_table_info(&TursoDialect, "").sql
}

#[must_use]
pub fn list_table_infos() -> &'static str {
    r"SELECT id, table_name, table_status, created_at,
       attribute_definitions, key_schema, max_indexers, global_secondary_indexes,
       table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
       table_stream_duration_hours, default_item_stream_duration_hours
FROM tables"
}

#[must_use]
pub fn insert_table() -> &'static str {
    metadata::insert_table(
        &TursoDialect,
        "",
        "",
        0,
        "",
        "",
        0,
        None,
        None,
        false,
        72,
        72,
    )
    .sql
}

#[must_use]
pub fn update_table_status() -> &'static str {
    metadata::update_table_status(&TursoDialect, "", "").sql
}

#[must_use]
pub fn list_tables_after() -> &'static str {
    metadata::list_tables_after(&TursoDialect, 0, "").sql
}

#[must_use]
pub fn list_all_tables() -> &'static str {
    metadata::list_all_tables(&TursoDialect, 0).sql
}

#[must_use]
pub fn delete_table_metadata() -> &'static str {
    metadata::delete_table_metadata(&TursoDialect, "").sql
}

#[must_use]
pub fn update_deletion_protection() -> &'static str {
    metadata::update_deletion_protection(&TursoDialect, false, "").sql
}

#[must_use]
pub fn update_stream_durations() -> &'static str {
    metadata::update_stream_durations(&TursoDialect, 72, 72, "").sql
}

#[must_use]
pub fn drop_table(table_name_safe: &str) -> String {
    format!("DROP TABLE IF EXISTS \"table_{table_name_safe}\"")
}

#[must_use]
pub fn drop_named_table(table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{table_name}\"")
}

#[must_use]
pub fn create_queues_table() -> &'static str {
    queue::create_queues_table(&TursoDialect)
}

#[must_use]
pub fn create_messages_table() -> &'static str {
    queue::create_messages_table(&TursoDialect)
}

#[must_use]
pub fn create_messages_queue_visibility_index() -> &'static str {
    queue::create_messages_queue_visibility_index(&TursoDialect)
}

#[must_use]
pub fn create_messages_queue_receipt_index() -> &'static str {
    queue::create_messages_queue_receipt_index(&TursoDialect)
}

#[must_use]
pub fn insert_or_replace_queue() -> &'static str {
    queue::upsert_queue(&TursoDialect)
}

#[must_use]
pub fn get_queue() -> &'static str {
    queue::get_queue(&TursoDialect)
}

#[must_use]
pub fn get_queue_by_name() -> &'static str {
    queue::get_queue_by_name(&TursoDialect)
}

#[must_use]
pub fn list_queues_with_prefix() -> &'static str {
    queue::list_queues_with_prefix(&TursoDialect)
}

#[must_use]
pub fn list_all_queues() -> &'static str {
    queue::list_all_queues(&TursoDialect)
}

#[must_use]
pub fn delete_messages_for_queue() -> &'static str {
    queue::delete_messages_for_queue(&TursoDialect)
}

#[must_use]
pub fn delete_queue() -> &'static str {
    queue::delete_queue(&TursoDialect)
}

#[must_use]
pub fn set_queue_attributes() -> &'static str {
    queue::set_queue_attributes(&TursoDialect)
}

#[must_use]
pub fn send_message() -> &'static str {
    queue::send_message(&TursoDialect)
}

#[must_use]
pub fn receive_messages() -> &'static str {
    queue::receive_messages(&TursoDialect)
}

#[must_use]
pub fn claim_message() -> &'static str {
    queue::claim_message(&TursoDialect)
}

#[must_use]
pub fn delete_message() -> &'static str {
    queue::delete_message(&TursoDialect)
}

#[must_use]
pub fn change_message_visibility() -> &'static str {
    queue::change_message_visibility(&TursoDialect)
}

#[must_use]
pub fn update_message_checkpoint() -> &'static str {
    queue::update_message_checkpoint(&TursoDialect)
}

#[must_use]
pub fn create_user_streams_table() -> &'static str {
    stream::create_user_streams_table(&TursoDialect)
}

#[must_use]
pub fn create_stream_items_table() -> &'static str {
    stream::create_stream_items_table(&TursoDialect)
}

#[must_use]
pub fn create_stream_cursors_table() -> &'static str {
    stream::create_stream_cursors_table(&TursoDialect)
}

#[must_use]
pub fn create_change_index_table() -> &'static str {
    stream::create_change_index_table(&TursoDialect)
}

#[must_use]
pub fn create_stream_format_metadata_table() -> &'static str {
    stream::create_stream_format_metadata_table(&TursoDialect)
}

#[must_use]
pub fn get_stream_format_version() -> &'static str {
    stream::get_stream_format_version(&TursoDialect)
}

#[must_use]
pub fn upsert_stream_format_version() -> &'static str {
    stream::upsert_stream_format_version(&TursoDialect)
}

#[must_use]
pub fn count_stream_items() -> &'static str {
    stream::count_stream_items(&TursoDialect)
}

#[must_use]
pub fn stream_items_exist() -> &'static str {
    "SELECT item_id FROM sys_stream_items LIMIT 1"
}

#[must_use]
pub fn stream_items_table_exists() -> &'static str {
    "SELECT name FROM sqlite_master WHERE type='table' AND name='sys_stream_items'"
}

#[must_use]
pub fn list_stream_pointer_payloads() -> &'static str {
    stream::list_stream_pointer_payloads(&TursoDialect)
}

#[must_use]
pub fn create_stream_items_internal_time_index() -> &'static str {
    stream::create_stream_items_internal_time_index(&TursoDialect)
}

#[must_use]
pub fn create_stream_cursors_internal_index() -> &'static str {
    stream::create_stream_cursors_internal_index(&TursoDialect)
}

#[must_use]
pub fn create_change_index_created_at_index() -> &'static str {
    stream::create_change_index_created_at_index(&TursoDialect)
}

#[must_use]
pub fn insert_stream_entry() -> &'static str {
    stream::insert_stream_entry(&TursoDialect)
}

#[must_use]
pub fn insert_change_index_marker() -> &'static str {
    stream::insert_change_index_marker(&TursoDialect)
}

#[must_use]
pub fn list_change_index_markers() -> &'static str {
    stream::list_change_index_markers(&TursoDialect)
}

#[must_use]
pub fn trim_change_index_markers_older_than() -> &'static str {
    stream::trim_change_index_markers_older_than(&TursoDialect)
}

#[must_use]
pub fn batch_insert_stream_entries(values_sql: &str) -> String {
    stream::batch_insert_stream_entries(values_sql)
}

#[must_use]
pub fn insert_user_stream() -> &'static str {
    stream::insert_user_stream(&TursoDialect)
}

#[must_use]
pub fn get_stream_internal_id() -> &'static str {
    stream::get_stream_internal_id(&TursoDialect)
}

#[must_use]
pub fn delete_stream_cursors() -> &'static str {
    stream::delete_stream_cursors(&TursoDialect)
}

#[must_use]
pub fn delete_stream_items() -> &'static str {
    stream::delete_stream_items(&TursoDialect)
}

#[must_use]
pub fn delete_user_stream() -> &'static str {
    stream::delete_user_stream(&TursoDialect)
}

#[must_use]
pub fn get_stream() -> &'static str {
    stream::get_stream(&TursoDialect)
}

#[must_use]
pub fn read_stream_forward() -> &'static str {
    stream::read_stream_forward(&TursoDialect)
}

#[must_use]
pub fn read_stream_backward() -> &'static str {
    stream::read_stream_backward(&TursoDialect)
}

#[must_use]
pub fn check_cursor_exists() -> &'static str {
    stream::check_cursor_exists(&TursoDialect)
}

#[must_use]
pub fn get_latest_stream_item() -> &'static str {
    stream::get_latest_stream_item(&TursoDialect)
}

#[must_use]
pub fn insert_cursor() -> &'static str {
    stream::insert_cursor(&TursoDialect)
}

#[must_use]
pub fn delete_cursor() -> &'static str {
    stream::delete_cursor(&TursoDialect)
}

#[must_use]
pub fn get_cursor_position() -> &'static str {
    stream::get_cursor_position(&TursoDialect)
}

#[must_use]
pub fn check_stream_item_exists() -> &'static str {
    stream::check_stream_item_exists(&TursoDialect)
}

#[must_use]
pub fn advance_cursor_position() -> &'static str {
    stream::advance_cursor_position(&TursoDialect)
}

#[must_use]
pub fn get_cursor() -> &'static str {
    stream::get_cursor(&TursoDialect)
}

#[must_use]
pub fn select_main_row(table_name_safe: &str, where_clause: &str) -> String {
    format!("SELECT * FROM \"table_{table_name_safe}\" WHERE {where_clause}")
}

#[must_use]
pub fn select_all_main_rows(table_name_safe: &str) -> String {
    format!("SELECT * FROM \"table_{table_name_safe}\"")
}

#[must_use]
pub fn select_existing_batch_keys(
    table_name_safe: &str,
    key_columns: &[String],
    predicates: &str,
    operation_count: usize,
) -> String {
    if key_columns.len() == 1 {
        return format!(
            "SELECT {} FROM \"table_{table_name_safe}\" WHERE {} IN ({})",
            key_columns.join(", "),
            key_columns[0],
            std::iter::repeat_n("?", operation_count)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    format!(
        "SELECT {} FROM \"table_{table_name_safe}\" WHERE ({}) IN ({predicates})",
        key_columns.join(", "),
        key_columns.join(", ")
    )
}

#[must_use]
pub fn batch_upsert_main_rows(
    table_name_safe: &str,
    columns: &[String],
    values_sql: &str,
    conflict_target: &str,
    assignments: &str,
) -> String {
    format!(
        "INSERT INTO \"table_{table_name_safe}\" ({}) VALUES {values_sql} ON CONFLICT \
         ({conflict_target}) DO UPDATE SET {assignments}",
        columns.join(", ")
    )
}

#[must_use]
pub fn batch_bump_item_revisions(values_sql: &str) -> String {
    format!(
        "INSERT INTO item_revisions (table_name, key_json, revision) VALUES {values_sql} ON \
         CONFLICT(table_name, key_json) DO UPDATE SET revision = revision + 1"
    )
}

#[must_use]
pub fn batch_upsert_gsi_rows(
    gsi_table: &str,
    columns: &[String],
    values_sql: &str,
    key_columns: &[String],
    assignments: &str,
) -> String {
    format!(
        "INSERT INTO \"{gsi_table}\" ({}) VALUES {values_sql} ON CONFLICT ({}) DO UPDATE SET \
         {assignments}",
        columns.join(", "),
        key_columns.join(", ")
    )
}

#[must_use]
pub fn delete_main_row(table_name_safe: &str, where_clause: &str) -> String {
    format!("DELETE FROM \"table_{table_name_safe}\" WHERE {where_clause}")
}

#[must_use]
pub fn upsert_main_row(
    table_name_safe: &str,
    columns: &[String],
    placeholders: &str,
    conflict_target: &str,
    assignments: &str,
) -> String {
    format!(
        "INSERT INTO \"table_{table_name_safe}\" ({}) VALUES ({}) ON CONFLICT ({conflict_target}) \
         DO UPDATE SET {assignments}",
        columns.join(", "),
        placeholders,
    )
}

#[must_use]
pub fn insert_main_row(table_name_safe: &str, columns: &[String], placeholders: &str) -> String {
    format!(
        "INSERT INTO \"table_{table_name_safe}\" ({}) VALUES ({placeholders})",
        columns.join(", ")
    )
}

#[must_use]
pub fn get_item_revision() -> &'static str {
    durable_revision::get_item_revision(&TursoDialect)
}

#[must_use]
pub fn bump_item_revision() -> &'static str {
    durable_revision::bump_item_revision(&TursoDialect)
}

#[must_use]
pub fn ensure_item_revision() -> &'static str {
    durable_revision::ensure_item_revision(&TursoDialect)
}

#[must_use]
pub fn delete_gsi_row(gsi_table: &str, key_columns: &[String]) -> String {
    let where_clause = key_columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{column} = ?{}", index + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("DELETE FROM \"{gsi_table}\" WHERE {where_clause}")
}

#[must_use]
pub fn delete_all_gsi_rows(gsi_table: &str) -> String {
    format!("DELETE FROM \"{gsi_table}\"")
}

#[must_use]
pub fn upsert_gsi_row(
    gsi_table: &str,
    columns: &[String],
    placeholders: &str,
    key_columns: &[String],
    assignments: &str,
) -> String {
    format!(
        "INSERT INTO \"{gsi_table}\" ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {assignments}",
        columns.join(", "),
        placeholders,
        key_columns.join(", "),
    )
}

#[must_use]
pub fn read_pragma(pragma_name: &str) -> String {
    format!("PRAGMA {pragma_name}")
}
