use crate::dialect::{SqlDialect, SqlDialectKind};

pub(crate) const ITEM_VERSIONED_STREAM_FORMAT_KEY: &str = "item_versioned_stream";
pub(crate) const ITEM_VERSIONED_STREAM_FORMAT_VERSION: i64 = 1;

pub(crate) fn create_stream_format_metadata_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_stream_format_metadata (
    format_key TEXT PRIMARY KEY,
    format_version INTEGER NOT NULL
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_stream_format_metadata (
        format_key TEXT PRIMARY KEY,
        format_version BIGINT NOT NULL
    )"
        }
    }
}

pub(crate) fn get_stream_format_version(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT format_version FROM sys_stream_format_metadata WHERE format_key = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT format_version FROM sys_stream_format_metadata WHERE format_key = $1"
        }
    }
}

pub(crate) fn upsert_stream_format_version(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT INTO sys_stream_format_metadata (format_key, format_version) VALUES (?1, ?2) \
             ON CONFLICT(format_key) DO UPDATE SET format_version = excluded.format_version"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_stream_format_metadata (format_key, format_version) VALUES ($1, $2) \
             ON CONFLICT(format_key) DO UPDATE SET format_version = excluded.format_version"
        }
    }
}

pub(crate) fn count_stream_items(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT COUNT(*) AS stream_item_count FROM sys_stream_items"
        }
        SqlDialectKind::Postgres => "SELECT COUNT(*) AS stream_item_count FROM sys_stream_items",
    }
}

pub(crate) fn list_stream_pointer_payloads(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT stream_name, item_id, data FROM sys_stream_items WHERE data_type = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT stream_name, item_id, data FROM sys_stream_items WHERE data_type = $1"
        }
    }
}
