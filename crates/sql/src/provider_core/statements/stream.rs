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

pub(crate) fn create_user_streams_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            r"CREATE TABLE IF NOT EXISTS sys_user_streams (
    stream_name TEXT PRIMARY KEY,
    internal_id TEXT NOT NULL UNIQUE,
    ttl_seconds INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
)"
        }
        SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_user_streams (
    stream_name TEXT PRIMARY KEY,
    internal_id TEXT UNIQUE NOT NULL,
    ttl_seconds INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_user_streams (
        stream_name TEXT PRIMARY KEY,
        internal_id TEXT NOT NULL UNIQUE,
        ttl_seconds BIGINT,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
    )"
        }
    }
}

pub(crate) fn create_stream_items_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            r"CREATE TABLE IF NOT EXISTS sys_stream_items (
    stream_name TEXT NOT NULL,
    item_id TEXT NOT NULL,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    data_type INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (stream_name, item_id)
)"
        }
        SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_stream_items (
    stream_name TEXT NOT NULL,
    item_id TEXT NOT NULL,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    data_type INTEGER NOT NULL
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_stream_items (
        stream_name TEXT NOT NULL,
        item_id TEXT NOT NULL,
        data BYTEA NOT NULL,
        created_at BIGINT NOT NULL,
        data_type INTEGER NOT NULL DEFAULT 2,
        PRIMARY KEY (stream_name, item_id)
    )"
        }
    }
}

pub(crate) fn create_stream_cursors_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            r"CREATE TABLE IF NOT EXISTS sys_stream_cursors (
    cursor_name TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    position TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (cursor_name, stream_name)
)"
        }
        SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_stream_cursors (
    cursor_name TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    position TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (cursor_name, stream_name)
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_stream_cursors (
        cursor_name TEXT NOT NULL,
        stream_name TEXT NOT NULL,
        position TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (cursor_name, stream_name)
    )"
        }
    }
}

pub(crate) fn create_stream_items_internal_time_index(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            "CREATE INDEX IF NOT EXISTS idx_stream_items_internal_time ON \
             sys_stream_items(stream_name, created_at)"
        }
        SqlDialectKind::Turso => {
            r"CREATE INDEX IF NOT EXISTS idx_stream_items_name_created
ON sys_stream_items(stream_name, created_at)"
        }
        SqlDialectKind::Postgres => {
            "CREATE INDEX IF NOT EXISTS idx_stream_items_internal_time
        ON sys_stream_items(stream_name, created_at)"
        }
    }
}

pub(crate) fn create_stream_cursors_internal_index(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            "CREATE INDEX IF NOT EXISTS idx_stream_cursors_internal ON \
             sys_stream_cursors(stream_name)"
        }
        SqlDialectKind::Turso => {
            r"CREATE INDEX IF NOT EXISTS idx_stream_cursors_stream
ON sys_stream_cursors(stream_name)"
        }
        SqlDialectKind::Postgres => {
            "CREATE INDEX IF NOT EXISTS idx_stream_cursors_internal
        ON sys_stream_cursors(stream_name)"
        }
    }
}

pub(crate) fn check_user_stream_exists(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT 1 FROM sys_user_streams WHERE stream_name = ?1"
        }
        SqlDialectKind::Postgres => "SELECT 1 FROM sys_user_streams WHERE stream_name = $1",
    }
}

pub(crate) fn insert_stream_entry(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
             VALUES (?1, ?2, ?3, ?4, ?5)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
             VALUES ($1, $2, $3, $4, $5)"
        }
    }
}

#[allow(dead_code)]
pub(crate) fn batch_insert_stream_entries(values_sql: &str) -> String {
    format!(
        "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) VALUES \
         {values_sql}"
    )
}

pub(crate) fn insert_user_stream(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            "INSERT INTO sys_user_streams (stream_name, internal_id, ttl_seconds, created_at, \
             updated_at) VALUES (?1, ?2, ?3, ?4, ?5)"
        }
        SqlDialectKind::Turso => {
            "INSERT OR IGNORE INTO sys_user_streams (stream_name, internal_id, ttl_seconds, \
             created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_user_streams (stream_name, internal_id, ttl_seconds, created_at, \
             updated_at) VALUES ($1, $2, $3, $4, $5)"
        }
    }
}

pub(crate) fn get_stream_internal_id(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT internal_id FROM sys_user_streams WHERE stream_name = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT internal_id FROM sys_user_streams WHERE stream_name = $1"
        }
    }
}

pub(crate) fn delete_stream_cursors(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_stream_cursors WHERE stream_name = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_stream_cursors WHERE stream_name = $1",
    }
}

pub(crate) fn delete_stream_items(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_stream_items WHERE stream_name = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_stream_items WHERE stream_name = $1",
    }
}

pub(crate) fn delete_user_stream(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_user_streams WHERE stream_name = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_user_streams WHERE stream_name = $1",
    }
}

pub(crate) fn get_stream(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT stream_name, internal_id, ttl_seconds, created_at FROM sys_user_streams WHERE \
             stream_name = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT stream_name, internal_id, ttl_seconds, created_at FROM sys_user_streams WHERE \
             stream_name = $1"
        }
    }
}

pub(crate) fn read_stream_forward(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT item_id, ?1 as stream_name, data, created_at, data_type FROM sys_stream_items \
             WHERE stream_name = ?2 AND item_id > ?3 ORDER BY item_id ASC LIMIT ?4"
        }
        SqlDialectKind::Postgres => {
            "SELECT item_id, data, created_at, data_type
     FROM sys_stream_items
     WHERE stream_name = $1 AND item_id > $2
     ORDER BY item_id ASC
     LIMIT $3"
        }
    }
}

pub(crate) fn read_stream_backward(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT item_id, ?1 as stream_name, data, created_at, data_type FROM sys_stream_items \
             WHERE stream_name = ?2 AND item_id < ?3 ORDER BY item_id DESC LIMIT ?4"
        }
        SqlDialectKind::Postgres => {
            "SELECT item_id, data, created_at, data_type
     FROM sys_stream_items
     WHERE stream_name = $1 AND item_id < $2
     ORDER BY item_id DESC
     LIMIT $3"
        }
    }
}

pub(crate) fn check_cursor_exists(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            "SELECT 1 FROM sys_stream_cursors WHERE cursor_name = ?1 AND stream_name = ?2"
        }
        SqlDialectKind::Turso => {
            "SELECT 1 AS present FROM sys_stream_cursors WHERE cursor_name = ?1 AND stream_name = \
             ?2 LIMIT 1"
        }
        SqlDialectKind::Postgres => {
            "SELECT 1 FROM sys_stream_cursors WHERE cursor_name = $1 AND stream_name = $2"
        }
    }
}

pub(crate) fn get_latest_stream_item(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT item_id FROM sys_stream_items WHERE stream_name = ?1 ORDER BY item_id DESC \
             LIMIT 1"
        }
        SqlDialectKind::Postgres => {
            "SELECT item_id FROM sys_stream_items WHERE stream_name = $1 ORDER BY item_id DESC \
             LIMIT 1"
        }
    }
}

pub(crate) fn insert_cursor(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at) \
             VALUES (?1, ?2, ?3, ?4)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at) \
             VALUES ($1, $2, $3, $4)"
        }
    }
}

pub(crate) fn delete_cursor(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_stream_cursors WHERE cursor_name = ?1 AND stream_name = ?2"
        }
        SqlDialectKind::Postgres => {
            "DELETE FROM sys_stream_cursors WHERE cursor_name = $1 AND stream_name = $2"
        }
    }
}

pub(crate) fn get_cursor_position(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT position FROM sys_stream_cursors WHERE cursor_name = ?1 AND stream_name = ?2"
        }
        SqlDialectKind::Postgres => {
            "SELECT position FROM sys_stream_cursors WHERE cursor_name = $1 AND stream_name = $2"
        }
    }
}

pub(crate) fn check_stream_item_exists(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite => {
            "SELECT 1 FROM sys_stream_items WHERE item_id = ?1 AND stream_name = ?2"
        }
        SqlDialectKind::Turso => {
            "SELECT 1 AS present FROM sys_stream_items WHERE item_id = ?1 AND stream_name = ?2 \
             LIMIT 1"
        }
        SqlDialectKind::Postgres => {
            "SELECT 1 FROM sys_stream_items WHERE item_id = $1 AND stream_name = $2"
        }
    }
}

pub(crate) fn advance_cursor_position(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "UPDATE sys_stream_cursors SET position = ?1 WHERE cursor_name = ?2 AND stream_name = \
             ?3"
        }
        SqlDialectKind::Postgres => {
            "UPDATE sys_stream_cursors SET position = $1 WHERE cursor_name = $2 AND stream_name = \
             $3"
        }
    }
}

pub(crate) fn get_cursor(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT cursor_name, ?1 as stream_name, position, created_at FROM sys_stream_cursors \
             WHERE cursor_name = ?2 AND stream_name = ?3"
        }
        SqlDialectKind::Postgres => {
            "SELECT cursor_name, stream_name, position, created_at
     FROM sys_stream_cursors
     WHERE cursor_name = $1 AND stream_name = $2"
        }
    }
}
