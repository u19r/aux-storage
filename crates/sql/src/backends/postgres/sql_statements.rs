use crate::{
    dialect::PostgresDialect,
    provider_core::statements::{durable_revision, metadata, queue, stream, ttl},
};

#[must_use]
pub fn select_one() -> &'static str {
    "SELECT 1"
}

#[must_use]
pub fn create_queue_tables() -> &'static str {
    "CREATE TABLE IF NOT EXISTS sys_queues (
        queue_name TEXT PRIMARY KEY,
        queue_url TEXT NOT NULL UNIQUE,
        attributes TEXT NOT NULL,
        created_at BIGINT NOT NULL
    );
CREATE TABLE IF NOT EXISTS sys_messages (
        message_id TEXT PRIMARY KEY,
        queue_name TEXT NOT NULL,
        body TEXT NOT NULL,
        message_attributes TEXT,
        visibility_timestamp BIGINT NOT NULL,
        receipt_handle TEXT,
        checkpoint_json TEXT,
        created_at BIGINT NOT NULL,
        FOREIGN KEY (queue_name) REFERENCES sys_queues(queue_name)
    );
CREATE INDEX IF NOT EXISTS idx_messages_queue_visibility
        ON sys_messages(queue_name, visibility_timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_queue_receipt
        ON sys_messages(queue_name, receipt_handle)"
}

#[must_use]
pub fn create_queue() -> &'static str {
    queue::upsert_queue(&PostgresDialect)
}

#[must_use]
pub fn get_queue() -> &'static str {
    queue::get_queue(&PostgresDialect)
}

#[must_use]
pub fn get_queue_by_name() -> &'static str {
    queue::get_queue_by_name(&PostgresDialect)
}

#[must_use]
pub fn list_queues_with_prefix() -> &'static str {
    queue::list_queues_with_prefix(&PostgresDialect)
}

#[must_use]
pub fn list_all_queues() -> &'static str {
    queue::list_all_queues(&PostgresDialect)
}

#[must_use]
pub fn delete_messages_for_queue() -> &'static str {
    queue::delete_messages_for_queue(&PostgresDialect)
}

#[must_use]
pub fn delete_queue() -> &'static str {
    queue::delete_queue(&PostgresDialect)
}

#[must_use]
pub fn set_queue_attributes() -> &'static str {
    queue::set_queue_attributes(&PostgresDialect)
}

#[must_use]
pub fn send_message() -> &'static str {
    queue::send_message(&PostgresDialect)
}

#[must_use]
pub const fn receive_and_claim_messages() -> &'static str {
    "WITH candidates AS (
         SELECT message_id
         FROM sys_messages
         WHERE queue_name = $1 AND visibility_timestamp <= $2
         ORDER BY visibility_timestamp, message_id
         LIMIT $3
         FOR UPDATE SKIP LOCKED
     )
     UPDATE sys_messages AS message
     SET visibility_timestamp = $4::bigint,
         receipt_handle = message.message_id || '-' || $4::text || '-' || txid_current()::text
     FROM candidates
     WHERE message.queue_name = $1 AND message.message_id = candidates.message_id
     RETURNING message.message_id, message.body, message.message_attributes,
               message.created_at, message.receipt_handle, message.visibility_timestamp"
}

#[must_use]
pub fn delete_message() -> &'static str {
    queue::delete_message(&PostgresDialect)
}

#[must_use]
pub fn change_message_visibility() -> &'static str {
    queue::change_message_visibility(&PostgresDialect)
}

#[must_use]
pub fn update_message_checkpoint() -> &'static str {
    queue::update_message_checkpoint(&PostgresDialect)
}

#[must_use]
pub fn create_stream_tables() -> &'static str {
    "CREATE TABLE IF NOT EXISTS sys_user_streams (
        stream_name TEXT PRIMARY KEY,
        internal_id TEXT NOT NULL UNIQUE,
        ttl_seconds BIGINT,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sys_stream_items (
        stream_name TEXT NOT NULL,
        item_id TEXT NOT NULL,
        data BYTEA NOT NULL,
        created_at BIGINT NOT NULL,
        data_type INTEGER NOT NULL DEFAULT 2,
        PRIMARY KEY (stream_name, item_id)
    );
    CREATE TABLE IF NOT EXISTS sys_stream_cursors (
        cursor_name TEXT NOT NULL,
        stream_name TEXT NOT NULL,
        position TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (cursor_name, stream_name)
    );
    CREATE TABLE IF NOT EXISTS sys_change_index (
        slot INTEGER NOT NULL,
        versionstamp TEXT NOT NULL,
        table_id TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (slot, versionstamp, table_id)
    );
    CREATE INDEX IF NOT EXISTS idx_stream_items_internal_time
        ON sys_stream_items(stream_name, created_at);
    CREATE INDEX IF NOT EXISTS idx_stream_cursors_internal
        ON sys_stream_cursors(stream_name);
    CREATE INDEX IF NOT EXISTS idx_change_index_created_at
        ON sys_change_index(created_at);"
}

#[must_use]
pub fn create_stream_format_metadata_table() -> &'static str {
    stream::create_stream_format_metadata_table(&PostgresDialect)
}

#[must_use]
pub fn get_stream_format_version() -> &'static str {
    stream::get_stream_format_version(&PostgresDialect)
}

#[must_use]
pub fn upsert_stream_format_version() -> &'static str {
    stream::upsert_stream_format_version(&PostgresDialect)
}

#[must_use]
pub fn count_stream_items() -> &'static str {
    stream::count_stream_items(&PostgresDialect)
}

#[must_use]
pub fn list_stream_pointer_payloads() -> &'static str {
    stream::list_stream_pointer_payloads(&PostgresDialect)
}

#[must_use]
pub fn item_versioned_stream_format_key() -> &'static str {
    stream::ITEM_VERSIONED_STREAM_FORMAT_KEY
}

#[must_use]
pub const fn item_versioned_stream_format_version() -> i64 {
    stream::ITEM_VERSIONED_STREAM_FORMAT_VERSION
}

#[must_use]
pub fn check_user_stream_exists() -> &'static str {
    stream::check_user_stream_exists(&PostgresDialect)
}

#[must_use]
pub fn insert_user_stream() -> &'static str {
    stream::insert_user_stream(&PostgresDialect)
}

#[must_use]
pub fn get_stream_internal_id() -> &'static str {
    stream::get_stream_internal_id(&PostgresDialect)
}

#[must_use]
pub fn delete_stream_cursors() -> &'static str {
    stream::delete_stream_cursors(&PostgresDialect)
}

#[must_use]
pub fn delete_stream_items() -> &'static str {
    stream::delete_stream_items(&PostgresDialect)
}

#[must_use]
pub fn delete_user_stream() -> &'static str {
    stream::delete_user_stream(&PostgresDialect)
}

#[must_use]
pub fn get_stream() -> &'static str {
    stream::get_stream(&PostgresDialect)
}

#[must_use]
pub fn insert_stream_entry() -> &'static str {
    stream::insert_stream_entry(&PostgresDialect)
}

#[must_use]
pub fn insert_change_index_marker() -> &'static str {
    stream::insert_change_index_marker(&PostgresDialect)
}

#[must_use]
pub fn list_change_index_markers() -> &'static str {
    stream::list_change_index_markers(&PostgresDialect)
}

#[must_use]
pub fn trim_change_index_markers_older_than() -> &'static str {
    stream::trim_change_index_markers_older_than(&PostgresDialect)
}

#[must_use]
pub fn insert_stream_entries(row_count: usize) -> String {
    let values = (0..row_count)
        .map(|row| {
            let base = row * 5;
            format!(
                "(${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) VALUES \
         {values}"
    )
}

#[must_use]
pub fn read_stream_forward() -> &'static str {
    stream::read_stream_forward(&PostgresDialect)
}

#[must_use]
pub fn read_stream_backward() -> &'static str {
    stream::read_stream_backward(&PostgresDialect)
}

#[must_use]
pub fn check_cursor_exists() -> &'static str {
    stream::check_cursor_exists(&PostgresDialect)
}

#[must_use]
pub fn get_latest_stream_item() -> &'static str {
    stream::get_latest_stream_item(&PostgresDialect)
}

#[must_use]
pub fn insert_cursor() -> &'static str {
    stream::insert_cursor(&PostgresDialect)
}

#[must_use]
pub fn delete_cursor() -> &'static str {
    stream::delete_cursor(&PostgresDialect)
}

#[must_use]
pub fn get_cursor_position() -> &'static str {
    stream::get_cursor_position(&PostgresDialect)
}

#[must_use]
pub fn check_stream_item_exists() -> &'static str {
    stream::check_stream_item_exists(&PostgresDialect)
}

#[must_use]
pub fn advance_cursor_position() -> &'static str {
    stream::advance_cursor_position(&PostgresDialect)
}

#[must_use]
pub fn get_cursor() -> &'static str {
    stream::get_cursor(&PostgresDialect)
}

#[must_use]
pub fn get_item_revision() -> &'static str {
    durable_revision::get_item_revision(&PostgresDialect)
}

#[must_use]
pub fn bump_item_revision() -> &'static str {
    durable_revision::bump_item_revision(&PostgresDialect)
}

#[must_use]
pub fn ensure_item_revision() -> &'static str {
    durable_revision::ensure_item_revision(&PostgresDialect)
}

#[must_use]
pub fn lock_item_revision() -> &'static str {
    durable_revision::lock_item_revision(&PostgresDialect)
}

#[must_use]
pub fn get_ttl_config() -> &'static str {
    ttl::get_ttl_config(&PostgresDialect)
}

#[must_use]
pub fn list_ttl_configs() -> &'static str {
    ttl::list_ttl_configs()
}

#[must_use]
pub fn upsert_ttl_config() -> &'static str {
    ttl::upsert_ttl_config(&PostgresDialect)
}

#[must_use]
pub fn delete_ttl_config() -> &'static str {
    ttl::delete_ttl_config(&PostgresDialect)
}

#[must_use]
pub fn create_ttl_index_table(ttl_table_name: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS \"{ttl_table_name}\" (
    ttl_value BIGINT NOT NULL,
    key_token TEXT NOT NULL,
    PRIMARY KEY (ttl_value, key_token)
)"
    )
}

#[must_use]
pub fn drop_ttl_index_table(ttl_table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{ttl_table_name}\"")
}

#[must_use]
pub fn insert_ttl_index_row(ttl_table_name: &str) -> String {
    format!(
        "INSERT INTO \"{ttl_table_name}\" (ttl_value, key_token)
VALUES ($1, $2)
ON CONFLICT (ttl_value, key_token) DO NOTHING"
    )
}

#[must_use]
pub fn delete_ttl_index_row(ttl_table_name: &str) -> String {
    format!("DELETE FROM \"{ttl_table_name}\" WHERE ttl_value = $1 AND key_token = $2")
}

#[must_use]
pub fn select_expired_ttl_rows(ttl_table_name: &str) -> String {
    format!(
        "SELECT ttl_value, key_token
FROM \"{ttl_table_name}\"
WHERE ttl_value <= $1
ORDER BY ttl_value ASC, key_token ASC
LIMIT $2"
    )
}

#[must_use]
pub fn create_storage_metadata_tables() -> &'static str {
    "CREATE TABLE IF NOT EXISTS tables (
        id TEXT PRIMARY KEY,
        table_name TEXT UNIQUE NOT NULL,
        table_status TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        attribute_definitions TEXT NOT NULL,
        key_schema TEXT NOT NULL,
        max_indexers SMALLINT NOT NULL,
        global_secondary_indexes TEXT,
        table_size_bytes BIGINT DEFAULT 0,
        item_count BIGINT DEFAULT 0,
        stream_specification TEXT,
        deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS gsi_backfill (
        table_name TEXT NOT NULL,
        index_name TEXT NOT NULL,
        status TEXT NOT NULL,
        scan_lek TEXT,
        captured_stream_tail TEXT,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (table_name, index_name)
    );
    CREATE TABLE IF NOT EXISTS sys_ttl_configs (
        table_name TEXT PRIMARY KEY,
        config_blob BYTEA NOT NULL
    );
    CREATE TABLE IF NOT EXISTS item_revisions (
        table_name TEXT NOT NULL,
        key_json TEXT NOT NULL,
        revision BIGINT NOT NULL,
        PRIMARY KEY (table_name, key_json)
    )"
}

#[must_use]
pub fn add_deletion_protection_column() -> &'static str {
    crate::provider_core::statements::metadata::add_deletion_protection_column(&PostgresDialect).sql
}

#[must_use]
pub fn add_table_stream_duration_column() -> &'static str {
    crate::provider_core::statements::metadata::add_table_stream_duration_column(&PostgresDialect)
        .sql
}

#[must_use]
pub fn add_default_item_stream_duration_column() -> &'static str {
    crate::provider_core::statements::metadata::add_default_item_stream_duration_column(
        &PostgresDialect,
    )
    .sql
}

#[must_use]
pub fn table_exists() -> &'static str {
    metadata::table_exists(&PostgresDialect, "").sql
}

#[must_use]
pub fn insert_table_metadata() -> &'static str {
    metadata::insert_table(
        &PostgresDialect,
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
    metadata::update_table_status(&PostgresDialect, "", "").sql
}

#[must_use]
pub fn list_tables_after() -> &'static str {
    metadata::list_tables_after(&PostgresDialect, 0, "").sql
}

#[must_use]
pub fn list_all_tables() -> &'static str {
    metadata::list_all_tables(&PostgresDialect, 0).sql
}

#[must_use]
pub fn delete_table_metadata() -> &'static str {
    metadata::delete_table_metadata(&PostgresDialect, "").sql
}

#[must_use]
pub fn drop_physical_table(physical_table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{physical_table_name}\"")
}

#[must_use]
pub fn drop_named_table(table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{table_name}\"")
}

#[must_use]
pub fn delete_stream_cursors_for_table() -> &'static str {
    "DELETE FROM sys_stream_cursors WHERE stream_name = $1 OR stream_name LIKE $2"
}

#[must_use]
pub fn delete_stream_items_for_table() -> &'static str {
    "DELETE FROM sys_stream_items WHERE stream_name = $1 OR stream_name LIKE $2"
}

#[must_use]
pub fn update_stream_specification() -> &'static str {
    "UPDATE tables SET stream_specification = $1 WHERE table_name = $2"
}

#[must_use]
pub fn update_deletion_protection() -> &'static str {
    "UPDATE tables SET deletion_protection_enabled = $1 WHERE table_name = $2"
}

#[must_use]
pub fn update_stream_durations() -> &'static str {
    metadata::update_stream_durations(&PostgresDialect, 72, 72, "").sql
}

#[must_use]
pub fn update_global_secondary_indexes() -> &'static str {
    "UPDATE tables SET global_secondary_indexes = $1 WHERE table_name = $2"
}

#[must_use]
pub fn upsert_main_row(
    physical_table_name: &str,
    columns_sql: &str,
    placeholders: &str,
    conflict_target: &str,
    assignments: &str,
) -> String {
    format!(
        "INSERT INTO \"{physical_table_name}\" ({columns_sql}) VALUES ({placeholders}) ON \
         CONFLICT ({conflict_target}) DO UPDATE SET {assignments}"
    )
}

#[must_use]
pub fn insert_main_row(physical_table_name: &str, columns_sql: &str, placeholders: &str) -> String {
    format!("INSERT INTO \"{physical_table_name}\" ({columns_sql}) VALUES ({placeholders})")
}

#[must_use]
pub fn upsert_main_row_returning(
    physical_table_name: &str,
    columns_sql: &str,
    placeholders: &str,
    conflict_target: &str,
    assignments: &str,
) -> String {
    format!(
        "{} RETURNING 1",
        upsert_main_row(
            physical_table_name,
            columns_sql,
            placeholders,
            conflict_target,
            assignments
        )
    )
}

#[must_use]
pub fn insert_main_row_returning(
    physical_table_name: &str,
    columns_sql: &str,
    placeholders: &str,
) -> String {
    format!(
        "{} RETURNING 1",
        insert_main_row(physical_table_name, columns_sql, placeholders)
    )
}

#[must_use]
pub fn bump_item_revision_with_placeholders(table_name: &str, key_json: &str) -> String {
    format!(
        "INSERT INTO item_revisions (table_name, key_json, revision) VALUES ({table_name}, \
         {key_json}, 1) ON CONFLICT(table_name, key_json) DO UPDATE SET revision = \
         item_revisions.revision + 1 RETURNING revision"
    )
}

#[must_use]
pub fn delete_main_row(physical_table_name: &str, where_sql: &str) -> String {
    format!("DELETE FROM \"{physical_table_name}\" WHERE {where_sql}")
}

#[must_use]
pub fn select_ordered_rows(
    select_projection: &str,
    physical_name: &str,
    where_sql: Option<&str>,
    order_by: Option<&str>,
    limit: u32,
) -> String {
    let mut sql = format!("SELECT {select_projection} FROM \"{physical_name}\"");
    if let Some(where_sql) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(where_sql);
    }
    if let Some(order_by) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(order_by);
    }
    format!("{sql} LIMIT {}", limit.saturating_add(1))
}

#[must_use]
pub fn batch_get_composite_key(
    select_projection: &str,
    physical_table_name: &str,
    tuple_columns: &str,
    values_rows: &str,
    join_predicates: &str,
) -> String {
    format!(
        "SELECT {select_projection} FROM \"{physical_table_name}\" AS item JOIN (VALUES \
         {values_rows}) AS requested({tuple_columns}) ON {join_predicates}"
    )
}

#[must_use]
pub fn update_attribute_definitions() -> &'static str {
    "UPDATE tables SET attribute_definitions = $1 WHERE table_name = $2"
}

#[must_use]
pub fn create_physical_table(
    physical_table_name: &str,
    key_columns: &[String],
    primary_key_columns: &[String],
    max_indexers: storage_types::MaxIndexers,
) -> String {
    let mut create_sql = format!("CREATE TABLE IF NOT EXISTS \"{physical_table_name}\" (");
    create_sql.push_str(&key_columns.join(", "));
    create_sql.push_str(", attributes_blob TEXT");
    for ordinal in 0..max_indexers.as_usize() {
        create_sql.push_str(&format!(", __aux_indexer_{ordinal} TEXT"));
    }
    if !primary_key_columns.is_empty() {
        create_sql.push_str(", PRIMARY KEY (");
        create_sql.push_str(&primary_key_columns.join(", "));
        create_sql.push(')');
    }
    create_sql.push(')');
    create_sql
}

#[must_use]
pub fn create_gsi_table(
    gsi_table_name: &str,
    key_columns: &[String],
    primary_key_columns: &[String],
    max_indexers: storage_types::MaxIndexers,
) -> String {
    let mut create_sql = format!("CREATE TABLE IF NOT EXISTS \"{gsi_table_name}\" (");
    create_sql.push_str(&key_columns.join(", "));
    create_sql.push_str(", attributes_blob TEXT");
    for ordinal in 0..max_indexers.as_usize() {
        create_sql.push_str(&format!(", __aux_indexer_{ordinal} TEXT"));
    }
    create_sql.push_str(", PRIMARY KEY (");
    create_sql.push_str(&primary_key_columns.join(", "));
    create_sql.push_str("))");
    create_sql
}

#[must_use]
pub fn dml_ctes(statements: &[String]) -> String {
    let ctes = statements
        .iter()
        .enumerate()
        .map(|(index, statement)| format!("write_{index} AS ({statement})"))
        .collect::<Vec<_>>()
        .join(", ");
    let counts = (0..statements.len())
        .map(|index| format!("(SELECT COUNT(*) FROM write_{index})"))
        .collect::<Vec<_>>()
        .join(" + ");
    format!("WITH {ctes} SELECT {counts}")
}

#[must_use]
pub fn dml_ctes_returning_last_column(statements: &[String], column: &str) -> String {
    let ctes = statements
        .iter()
        .enumerate()
        .map(|(index, statement)| format!("write_{index} AS ({statement})"))
        .collect::<Vec<_>>()
        .join(", ");
    let last_index = statements.len().saturating_sub(1);
    if last_index == 0 {
        return format!("WITH {ctes} SELECT {column} FROM write_{last_index}");
    }
    let counts = (0..last_index)
        .map(|index| format!("(SELECT COUNT(*) FROM write_{index})"))
        .collect::<Vec<_>>()
        .join(" + ");
    format!(
        "WITH {ctes} SELECT write_{last_index}.{column} FROM write_{last_index} CROSS JOIN \
         (SELECT {counts} AS affected_rows) AS write_counts"
    )
}

#[must_use]
pub fn get_item(physical_table_name: &str, select_projection: &str, where_sql: &str) -> String {
    format!("SELECT {select_projection} FROM \"{physical_table_name}\" WHERE {where_sql} LIMIT 1")
}

#[must_use]
pub fn get_table_info() -> &'static str {
    metadata::get_table_info(&PostgresDialect, "").sql
}
