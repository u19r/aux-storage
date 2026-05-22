#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialectKind {
    Sqlite,
    Turso,
    Postgres,
}

pub trait SqlDialect: Send + Sync {
    fn kind(&self) -> SqlDialectKind;

    fn bind_param(&self, index_1_based: usize) -> String;

    fn upsert_keyword(&self) -> &'static str;

    fn table_exists_sql(&self) -> &'static str;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn kind(&self) -> SqlDialectKind {
        SqlDialectKind::Sqlite
    }

    fn bind_param(&self, index_1_based: usize) -> String {
        format!("?{index_1_based}")
    }

    fn upsert_keyword(&self) -> &'static str {
        "INSERT OR REPLACE"
    }

    fn table_exists_sql(&self) -> &'static str {
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?1"
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TursoDialect;

impl SqlDialect for TursoDialect {
    fn kind(&self) -> SqlDialectKind {
        SqlDialectKind::Turso
    }

    fn bind_param(&self, index_1_based: usize) -> String {
        format!("?{index_1_based}")
    }

    fn upsert_keyword(&self) -> &'static str {
        "INSERT OR REPLACE"
    }

    fn table_exists_sql(&self) -> &'static str {
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?1"
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn kind(&self) -> SqlDialectKind {
        SqlDialectKind::Postgres
    }

    fn bind_param(&self, index_1_based: usize) -> String {
        format!("${index_1_based}")
    }

    fn upsert_keyword(&self) -> &'static str {
        "INSERT INTO"
    }

    fn table_exists_sql(&self) -> &'static str {
        "SELECT to_regclass($1) IS NOT NULL"
    }
}
