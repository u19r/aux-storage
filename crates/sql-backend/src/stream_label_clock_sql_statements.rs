//! One stream-label clock schema, read and store for SQLite, Turso, and
//! Postgres.

use storage_types::TimestampMillis;

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn create_clock(engine: impl SqlLayout) -> SqlStatement {
    let integer = engine.engine().integer_type();
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_stream_label_clock} (singleton {integer} \
             PRIMARY KEY CHECK (singleton = 1), enabled_at {integer} NOT NULL){}",
            engine.engine().table_suffix(),
            relation_sys_stream_label_clock = engine.relation_name("sys_stream_label_clock"),
        ),
        vec![],
    )
}

pub(super) fn load_clock(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        crate::sql_template::sql_template!(
            engine,
            "SELECT enabled_at FROM "["sys_stream_label_clock"],
            " WHERE singleton = 1"
        )
        .into_owned(),
        vec![],
    )
}

pub(super) fn store_clock(engine: impl SqlLayout, stamp: TimestampMillis) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_stream_label_clock} (singleton, enabled_at) VALUES (1, {}) \
             ON CONFLICT (singleton) DO UPDATE SET enabled_at = excluded.enabled_at",
            engine.engine().placeholder(1),
            relation_sys_stream_label_clock = engine.relation_name("sys_stream_label_clock"),
        ),
        vec![SqlValue::Integer(stamp.timestamp_millis())],
    )
}
