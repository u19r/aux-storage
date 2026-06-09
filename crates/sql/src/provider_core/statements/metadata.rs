use crate::{
    dialect::{SqlDialect, SqlDialectKind},
    sql_types::SqlParam,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlStatement {
    pub(crate) sql: &'static str,
    pub(crate) params: Vec<SqlParam>,
}

impl SqlStatement {
    fn static_sql(sql: &'static str) -> Self {
        Self {
            sql,
            params: Vec::new(),
        }
    }

    fn with_params(sql: &'static str, params: Vec<SqlParam>) -> Self {
        Self { sql, params }
    }
}

pub(crate) fn create_tables_table(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS tables (
    id TEXT PRIMARY KEY,
    table_name TEXT UNIQUE NOT NULL,
    table_status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    attribute_definitions TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    global_secondary_indexes TEXT,
    table_size_bytes INTEGER DEFAULT 0,
    item_count INTEGER DEFAULT 0,
    stream_specification TEXT,
    deletion_protection_enabled INTEGER NOT NULL DEFAULT 0,
    table_stream_duration_hours INTEGER NOT NULL DEFAULT 72,
    default_item_stream_duration_hours INTEGER NOT NULL DEFAULT 72
)"
        }
        SqlDialectKind::Postgres => {
            r"CREATE TABLE IF NOT EXISTS tables (
        id TEXT PRIMARY KEY,
        table_name TEXT UNIQUE NOT NULL,
        table_status TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        attribute_definitions TEXT NOT NULL,
        key_schema TEXT NOT NULL,
        global_secondary_indexes TEXT,
        table_size_bytes BIGINT DEFAULT 0,
        item_count BIGINT DEFAULT 0,
        stream_specification TEXT,
        deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        table_stream_duration_hours BIGINT NOT NULL DEFAULT 72,
        default_item_stream_duration_hours BIGINT NOT NULL DEFAULT 72
    )"
        }
    })
}

pub(crate) fn add_table_stream_duration_column(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "ALTER TABLE tables ADD COLUMN table_stream_duration_hours INTEGER NOT NULL DEFAULT 72"
        }
        SqlDialectKind::Postgres => {
            "ALTER TABLE tables ADD COLUMN IF NOT EXISTS table_stream_duration_hours BIGINT NOT \
             NULL DEFAULT 72"
        }
    })
}

pub(crate) fn add_default_item_stream_duration_column(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "ALTER TABLE tables ADD COLUMN default_item_stream_duration_hours INTEGER NOT NULL \
             DEFAULT 72"
        }
        SqlDialectKind::Postgres => {
            "ALTER TABLE tables ADD COLUMN IF NOT EXISTS default_item_stream_duration_hours BIGINT \
             NOT NULL DEFAULT 72"
        }
    })
}

pub(crate) fn add_deletion_protection_column(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "ALTER TABLE tables ADD COLUMN deletion_protection_enabled INTEGER NOT NULL DEFAULT 0"
        }
        SqlDialectKind::Postgres => {
            "ALTER TABLE tables ADD COLUMN IF NOT EXISTS deletion_protection_enabled BOOLEAN NOT \
             NULL DEFAULT FALSE"
        }
    })
}

pub(crate) fn create_gsi_backfill_table(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS gsi_backfill (
    table_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    status TEXT NOT NULL,
    scan_lek TEXT,
    captured_stream_tail TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (table_name, index_name)
)"
        }
        SqlDialectKind::Postgres => {
            r"CREATE TABLE IF NOT EXISTS gsi_backfill (
        table_name TEXT NOT NULL,
        index_name TEXT NOT NULL,
        status TEXT NOT NULL,
        scan_lek TEXT,
        captured_stream_tail TEXT,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (table_name, index_name)
    )"
        }
    })
}

pub(crate) fn create_ttl_config_table(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS sys_ttl_configs (
    table_name TEXT PRIMARY KEY,
    config_blob BLOB NOT NULL
)"
        }
        SqlDialectKind::Postgres => {
            r"CREATE TABLE IF NOT EXISTS sys_ttl_configs (
        table_name TEXT PRIMARY KEY,
        config_blob BYTEA NOT NULL
    )"
        }
    })
}

pub(crate) fn create_item_revisions_table(dialect: &dyn SqlDialect) -> SqlStatement {
    SqlStatement::static_sql(match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"CREATE TABLE IF NOT EXISTS item_revisions (
    table_name TEXT NOT NULL,
    key_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY (table_name, key_json)
)"
        }
        SqlDialectKind::Postgres => {
            r"CREATE TABLE IF NOT EXISTS item_revisions (
        table_name TEXT NOT NULL,
        key_json TEXT NOT NULL,
        revision BIGINT NOT NULL,
        PRIMARY KEY (table_name, key_json)
    )"
        }
    })
}

pub(crate) fn table_exists(
    dialect: &dyn SqlDialect,
    table_name: impl Into<String>,
) -> SqlStatement {
    let table_name = table_name.into();
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                "SELECT COUNT(*) AS count FROM tables WHERE table_name = ?1"
            }
            SqlDialectKind::Postgres => "SELECT COUNT(*)::BIGINT FROM tables WHERE table_name = $1",
        },
        vec![SqlParam::text(table_name)],
    )
}

pub(crate) fn get_table_info(
    dialect: &dyn SqlDialect,
    table_name: impl Into<String>,
) -> SqlStatement {
    let table_name = table_name.into();
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                r"SELECT id, table_name, table_status, created_at,
       attribute_definitions, key_schema, global_secondary_indexes,
       table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
       table_stream_duration_hours, default_item_stream_duration_hours
FROM tables WHERE table_name = ?1"
            }
            SqlDialectKind::Postgres => {
                "SELECT id, table_name, table_status, created_at,
        attribute_definitions, key_schema, global_secondary_indexes,
        table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
        table_stream_duration_hours, default_item_stream_duration_hours
     FROM tables WHERE table_name = $1"
            }
        },
        vec![SqlParam::text(table_name)],
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_table(
    dialect: &dyn SqlDialect,
    table_id: impl Into<String>,
    table_name: impl Into<String>,
    created_at: i64,
    attribute_definitions: impl Into<String>,
    key_schema: impl Into<String>,
    global_secondary_indexes: Option<String>,
    stream_specification: Option<String>,
    deletion_protection_enabled: bool,
    table_stream_duration_hours: i64,
    default_item_stream_duration_hours: i64,
) -> SqlStatement {
    let sql = match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"INSERT INTO tables (
    id, table_name, table_status, created_at,
    attribute_definitions, key_schema, global_secondary_indexes,
    table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
    table_stream_duration_hours, default_item_stream_duration_hours
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO tables (
        id, table_name, table_status, created_at,
        attribute_definitions, key_schema, global_secondary_indexes,
        table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
        table_stream_duration_hours, default_item_stream_duration_hours
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, $8, $9, $10, $11)"
        }
    };
    let mut params = vec![
        SqlParam::text(table_id),
        SqlParam::text(table_name),
        SqlParam::text("CREATING"),
        SqlParam::integer(created_at),
        SqlParam::text(attribute_definitions),
        SqlParam::text(key_schema),
    ];
    match global_secondary_indexes {
        Some(value) => params.push(SqlParam::text(value)),
        None => params.push(SqlParam::null()),
    }
    if !matches!(dialect.kind(), SqlDialectKind::Postgres) {
        params.push(SqlParam::integer(0));
        params.push(SqlParam::integer(0));
    }
    match stream_specification {
        Some(value) => params.push(SqlParam::text(value)),
        None => params.push(SqlParam::null()),
    }
    params.push(SqlParam::boolean(deletion_protection_enabled));
    params.push(SqlParam::integer(table_stream_duration_hours));
    params.push(SqlParam::integer(default_item_stream_duration_hours));
    SqlStatement::with_params(sql, params)
}

pub(crate) fn update_table_status(
    dialect: &dyn SqlDialect,
    table_status: impl Into<String>,
    table_name: impl Into<String>,
) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                "UPDATE tables SET table_status = ?1 WHERE table_name = ?2"
            }
            SqlDialectKind::Postgres => "UPDATE tables SET table_status = $1 WHERE table_name = $2",
        },
        vec![SqlParam::text(table_status), SqlParam::text(table_name)],
    )
}

pub(crate) fn list_all_tables(dialect: &dyn SqlDialect, limit: u32) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                r"SELECT id, table_name, table_status, created_at,
       attribute_definitions, key_schema, global_secondary_indexes,
       table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
       table_stream_duration_hours, default_item_stream_duration_hours
FROM tables
ORDER BY table_name ASC
LIMIT ?1"
            }
            SqlDialectKind::Postgres => {
                "SELECT id, table_name, table_status, created_at,
        attribute_definitions, key_schema, global_secondary_indexes,
        table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
        table_stream_duration_hours, default_item_stream_duration_hours
    FROM tables
    ORDER BY table_name ASC
    LIMIT $1"
            }
        },
        vec![SqlParam::integer(i64::from(limit))],
    )
}

pub(crate) fn list_tables_after(
    dialect: &dyn SqlDialect,
    limit: u32,
    exclusive_start_table_name: impl Into<String>,
) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                r"SELECT id, table_name, table_status, created_at,
       attribute_definitions, key_schema, global_secondary_indexes,
       table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
       table_stream_duration_hours, default_item_stream_duration_hours
FROM tables
WHERE table_name > ?1
ORDER BY table_name ASC
LIMIT ?2"
            }
            SqlDialectKind::Postgres => {
                "SELECT id, table_name, table_status, created_at,
        attribute_definitions, key_schema, global_secondary_indexes,
        table_size_bytes, item_count, stream_specification, deletion_protection_enabled,
        table_stream_duration_hours, default_item_stream_duration_hours
    FROM tables
    WHERE table_name > $1
    ORDER BY table_name ASC
    LIMIT $2"
            }
        },
        vec![
            SqlParam::text(exclusive_start_table_name),
            SqlParam::integer(i64::from(limit)),
        ],
    )
}

pub(crate) fn update_deletion_protection(
    dialect: &dyn SqlDialect,
    deletion_protection_enabled: bool,
    table_name: impl Into<String>,
) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                "UPDATE tables SET deletion_protection_enabled = ?1 WHERE table_name = ?2"
            }
            SqlDialectKind::Postgres => {
                "UPDATE tables SET deletion_protection_enabled = $1 WHERE table_name = $2"
            }
        },
        vec![
            SqlParam::boolean(deletion_protection_enabled),
            SqlParam::text(table_name),
        ],
    )
}

pub(crate) fn update_stream_durations(
    dialect: &dyn SqlDialect,
    table_stream_duration_hours: i64,
    default_item_stream_duration_hours: i64,
    table_name: impl Into<String>,
) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                "UPDATE tables SET table_stream_duration_hours = ?1, \
                 default_item_stream_duration_hours = ?2 WHERE table_name = ?3"
            }
            SqlDialectKind::Postgres => {
                "UPDATE tables SET table_stream_duration_hours = $1, \
                 default_item_stream_duration_hours = $2 WHERE table_name = $3"
            }
        },
        vec![
            SqlParam::integer(table_stream_duration_hours),
            SqlParam::integer(default_item_stream_duration_hours),
            SqlParam::text(table_name),
        ],
    )
}

pub(crate) fn delete_table_metadata(
    dialect: &dyn SqlDialect,
    table_name: impl Into<String>,
) -> SqlStatement {
    SqlStatement::with_params(
        match dialect.kind() {
            SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
                "DELETE FROM tables WHERE table_name = ?1"
            }
            SqlDialectKind::Postgres => "DELETE FROM tables WHERE table_name = $1",
        },
        vec![SqlParam::text(table_name)],
    )
}
