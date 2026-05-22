use crate::dialect::{SqlDialect, SqlDialectKind};

pub(crate) fn get_item_revision(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            "SELECT revision FROM item_revisions WHERE table_name = ?1 AND key_json = ?2"
        }
        SqlDialectKind::Postgres => {
            "SELECT revision FROM item_revisions WHERE table_name = $1 AND key_json = $2"
        }
    }
}

pub(crate) fn bump_item_revision(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"INSERT INTO item_revisions (table_name, key_json, revision)
                  VALUES (?1, ?2, 1)
                  ON CONFLICT(table_name, key_json)
                  DO UPDATE SET revision = revision + 1
                  RETURNING revision"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO item_revisions (table_name, key_json, revision)
     VALUES ($1, $2, 1)
     ON CONFLICT(table_name, key_json)
     DO UPDATE SET revision = item_revisions.revision + 1
     RETURNING revision"
        }
    }
}

#[allow(dead_code)]
pub(crate) fn ensure_item_revision(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => {
            r"INSERT INTO item_revisions (table_name, key_json, revision)
      VALUES (?1, ?2, 0)
      ON CONFLICT(table_name, key_json) DO NOTHING"
        }
        SqlDialectKind::Postgres => {
            "INSERT INTO item_revisions (table_name, key_json, revision)
     VALUES ($1, $2, 0)
     ON CONFLICT(table_name, key_json) DO NOTHING"
        }
    }
}

#[allow(dead_code)]
pub(crate) fn lock_item_revision(dialect: &dyn SqlDialect) -> &'static str {
    match dialect.kind() {
        SqlDialectKind::Sqlite | SqlDialectKind::Turso => get_item_revision(dialect),
        SqlDialectKind::Postgres => {
            "SELECT revision FROM item_revisions WHERE table_name = $1 AND key_json = $2 FOR UPDATE"
        }
    }
}
