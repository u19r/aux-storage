use crate::dialect::{SqlDialect, SqlDialectKind};

pub(crate) fn create_queues_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_queues (
    queue_name TEXT PRIMARY KEY,
    queue_url TEXT NOT NULL UNIQUE,
    attributes TEXT NOT NULL,
    created_at INTEGER NOT NULL
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_queues (
        queue_name TEXT PRIMARY KEY,
        queue_url TEXT NOT NULL UNIQUE,
        attributes TEXT NOT NULL,
        created_at BIGINT NOT NULL
    )"
        }
    }
}

pub(crate) fn create_messages_table(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_messages (
    message_id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    body TEXT NOT NULL,
    message_attributes TEXT,
    visibility_timestamp INTEGER NOT NULL,
    receipt_handle TEXT,
    checkpoint_json TEXT,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (queue_name) REFERENCES sys_queues(queue_name)
)"
        }
        SqlDialectKind::Postgres => {
            "CREATE TABLE IF NOT EXISTS sys_messages (
        message_id TEXT PRIMARY KEY,
        queue_name TEXT NOT NULL,
        body TEXT NOT NULL,
        message_attributes TEXT,
        visibility_timestamp BIGINT NOT NULL,
        receipt_handle TEXT,
        checkpoint_json TEXT,
        created_at BIGINT NOT NULL,
        FOREIGN KEY (queue_name) REFERENCES sys_queues(queue_name)
    )"
        }
    }
}

pub(crate) fn create_messages_queue_visibility_index(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE INDEX IF NOT EXISTS idx_messages_queue_visibility
ON sys_messages(queue_name, visibility_timestamp)"
        }
        SqlDialectKind::Postgres => {
            "CREATE INDEX IF NOT EXISTS idx_messages_queue_visibility
        ON sys_messages(queue_name, visibility_timestamp)"
        }
    }
}

pub(crate) fn create_messages_queue_receipt_index(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE INDEX IF NOT EXISTS idx_messages_queue_receipt
ON sys_messages(queue_name, receipt_handle)"
        }
        SqlDialectKind::Postgres => {
            "CREATE INDEX IF NOT EXISTS idx_messages_queue_receipt
        ON sys_messages(queue_name, receipt_handle)"
        }
    }
}

pub(crate) fn upsert_queue(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT OR REPLACE INTO sys_queues (queue_name, queue_url, attributes, created_at) \
             VALUES (?1, ?2, ?3, ?4)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_queues (queue_name, queue_url, attributes, created_at)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (queue_name)
     DO UPDATE SET
        queue_url = EXCLUDED.queue_url,
        attributes = EXCLUDED.attributes,
        created_at = EXCLUDED.created_at"
        }
    }
}

pub(crate) fn get_queue(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT queue_name, attributes, created_at FROM sys_queues WHERE queue_url = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT queue_name, attributes, created_at FROM sys_queues WHERE queue_url = $1"
        }
    }
}

pub(crate) fn get_queue_by_name(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT queue_url, attributes, created_at FROM sys_queues WHERE queue_name = ?1"
        }
        SqlDialectKind::Postgres => {
            "SELECT queue_url, attributes, created_at FROM sys_queues WHERE queue_name = $1"
        }
    }
}

pub(crate) fn list_queues_with_prefix(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT queue_name, queue_url, attributes, created_at FROM sys_queues WHERE queue_name \
             LIKE ?1 ORDER BY queue_name"
        }
        SqlDialectKind::Postgres => {
            "SELECT queue_name, queue_url, attributes, created_at
     FROM sys_queues
     WHERE queue_name LIKE $1
     ORDER BY queue_name"
        }
    }
}

pub(crate) fn list_all_queues(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT queue_name, queue_url, attributes, created_at FROM sys_queues ORDER BY \
             queue_name"
        }
        SqlDialectKind::Postgres => {
            "SELECT queue_name, queue_url, attributes, created_at
     FROM sys_queues
     ORDER BY queue_name"
        }
    }
}

pub(crate) fn delete_messages_for_queue(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_messages WHERE queue_name = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_messages WHERE queue_name = $1",
    }
}

#[allow(dead_code)]
pub(crate) fn delete_queue(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_queues WHERE queue_url = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_queues WHERE queue_url = $1",
    }
}

pub(crate) fn set_queue_attributes(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "UPDATE sys_queues SET attributes = ?1 WHERE queue_url = ?2"
        }
        SqlDialectKind::Postgres => "UPDATE sys_queues SET attributes = $1 WHERE queue_url = $2",
    }
}

pub(crate) fn send_message(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT INTO sys_messages (message_id, queue_name, body, message_attributes, \
             visibility_timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_messages
    (message_id, queue_name, body, message_attributes, visibility_timestamp, created_at)
    VALUES ($1, $2, $3, $4, $5, $6)"
        }
    }
}

pub(crate) fn receive_messages(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT message_id, body, message_attributes, created_at, receipt_handle, \
             visibility_timestamp
FROM sys_messages
WHERE queue_name = ?1 AND visibility_timestamp <= ?2
ORDER BY visibility_timestamp, message_id
LIMIT ?3"
        }
        SqlDialectKind::Postgres => {
            "SELECT message_id, body, message_attributes, created_at, receipt_handle, \
             visibility_timestamp
     FROM sys_messages
     WHERE queue_name = $1 AND visibility_timestamp <= $2
     ORDER BY visibility_timestamp, message_id
     LIMIT $3"
        }
    }
}

pub(crate) fn claim_message(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "UPDATE sys_messages
SET visibility_timestamp = ?1, receipt_handle = ?2
WHERE message_id = ?3 AND queue_name = ?4 AND (receipt_handle IS NULL OR receipt_handle = ?5)"
        }
        SqlDialectKind::Postgres => {
            "UPDATE sys_messages
     SET visibility_timestamp = $1, receipt_handle = $2
     WHERE message_id = $3 AND queue_name = $4 AND (receipt_handle IS NULL OR receipt_handle = $5)"
        }
    }
}

pub(crate) fn delete_message(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_messages WHERE queue_name = ?1 AND receipt_handle = ?2"
        }
        SqlDialectKind::Postgres => {
            "DELETE FROM sys_messages WHERE queue_name = $1 AND receipt_handle = $2"
        }
    }
}

pub(crate) fn change_message_visibility(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "UPDATE sys_messages SET visibility_timestamp = ?1 WHERE queue_name = ?2 AND \
             receipt_handle = ?3"
        }
        SqlDialectKind::Postgres => {
            "UPDATE sys_messages
     SET visibility_timestamp = $1
     WHERE queue_name = $2 AND receipt_handle = $3"
        }
    }
}

pub(crate) fn update_message_checkpoint(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "UPDATE sys_messages SET checkpoint_json = ?1 WHERE queue_name = ?2 AND receipt_handle \
             = ?3"
        }
        SqlDialectKind::Postgres => {
            "UPDATE sys_messages SET checkpoint_json = $1 WHERE queue_name = $2 AND receipt_handle \
             = $3"
        }
    }
}
