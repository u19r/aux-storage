use crate::dialect::{SqlDialect, SqlDialectKind};

pub(crate) fn get_ttl_config(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT config_blob FROM sys_ttl_configs WHERE table_name = ?1"
        }
        SqlDialectKind::Postgres => "SELECT config_blob FROM sys_ttl_configs WHERE table_name = $1",
    }
}

pub(crate) fn list_ttl_configs() -> &'static str {
    "SELECT table_name, config_blob FROM sys_ttl_configs"
}

pub(crate) fn upsert_ttl_config(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "INSERT OR REPLACE INTO sys_ttl_configs (table_name, config_blob) VALUES (?1, ?2)"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO sys_ttl_configs (table_name, config_blob)
     VALUES ($1, $2)
     ON CONFLICT (table_name)
     DO UPDATE SET config_blob = EXCLUDED.config_blob"
        }
    }
}

pub(crate) fn delete_ttl_config(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "DELETE FROM sys_ttl_configs WHERE table_name = ?1"
        }
        SqlDialectKind::Postgres => "DELETE FROM sys_ttl_configs WHERE table_name = $1",
    }
}
