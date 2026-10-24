//! One maintenance-identity schema and read for SQLite, Turso, and Postgres.

use storage_types::maintenance::MaintenanceKind;

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue};

pub(super) fn create_identity(engine: impl SqlLayout) -> SqlStatement {
    let integer = engine.engine().integer_type();
    let blob = engine.engine().blob_type();
    let suffix = engine.engine().table_suffix();
    let length = if engine.engine() == SqlEngine::Postgres {
        "OCTET_LENGTH"
    } else {
        "LENGTH"
    };
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_maintenance_identity} (singleton {integer} \
             PRIMARY KEY CHECK (singleton = 1), incarnation {blob} NOT NULL CHECK \
             ({length}(incarnation) = 16)){suffix}",
            relation_sys_maintenance_identity = engine.relation_name("sys_maintenance_identity"),
        ),
        vec![],
    )
}

pub(super) fn seed_identity(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_maintenance_identity} (singleton, incarnation) VALUES (1, \
             {}) ON CONFLICT (singleton) DO NOTHING",
            engine.engine().placeholder(1),
            relation_sys_maintenance_identity = engine.relation_name("sys_maintenance_identity"),
        ),
        vec![SqlValue::Blob(uuid::Uuid::now_v7().as_bytes().to_vec())],
    )
}

pub(super) fn read_identity(engine: impl SqlLayout) -> SqlStatement {
    SqlStatement::new(
        crate::sql_template::sql_template!(
            engine,
            "SELECT incarnation FROM "["sys_maintenance_identity"],
            " WHERE singleton = 1"
        )
        .into_owned(),
        vec![],
    )
}

pub(super) fn create_progress(engine: impl SqlLayout) -> SqlStatement {
    let blob = engine.engine().blob_type();
    let integer = engine.engine().integer_type();
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_maintenance_task} (kind {integer} PRIMARY \
             KEY, target_incarnation {blob} NOT NULL, revision {integer} NOT NULL CHECK (revision \
             >= 0), progress {blob} NOT NULL){}",
            engine.engine().table_suffix(),
            relation_sys_maintenance_task = engine.relation_name("sys_maintenance_task"),
        ),
        vec![],
    )
}

pub(super) fn read_progress(engine: impl SqlLayout, kind: MaintenanceKind) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT target_incarnation, revision, progress FROM {relation_sys_maintenance_task} \
             WHERE kind = {}",
            engine.engine().placeholder(1),
            relation_sys_maintenance_task = engine.relation_name("sys_maintenance_task"),
        ),
        vec![SqlValue::Integer(i64::from(kind.storage_tag()))],
    )
}

pub(super) fn insert_progress(
    engine: impl SqlLayout,
    kind: MaintenanceKind,
    target: Vec<u8>,
    revision: i64,
    progress: Vec<u8>,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_maintenance_task} (kind, target_incarnation, revision, \
             progress) VALUES ({}) ON CONFLICT (kind) DO NOTHING",
            engine.engine().placeholders(4),
            relation_sys_maintenance_task = engine.relation_name("sys_maintenance_task"),
        ),
        vec![
            SqlValue::Integer(i64::from(kind.storage_tag())),
            SqlValue::Blob(target),
            SqlValue::Integer(revision),
            SqlValue::Blob(progress),
        ],
    )
}

pub(super) fn update_progress(
    engine: impl SqlLayout,
    kind: MaintenanceKind,
    target: Vec<u8>,
    expected: i64,
    revision: i64,
    progress: Vec<u8>,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "UPDATE {relation_sys_maintenance_task} SET revision = {}, progress = {} WHERE kind = \
             {} AND target_incarnation = {} AND revision = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            relation_sys_maintenance_task = engine.relation_name("sys_maintenance_task"),
        ),
        vec![
            SqlValue::Integer(revision),
            SqlValue::Blob(progress),
            SqlValue::Integer(i64::from(kind.storage_tag())),
            SqlValue::Blob(target),
            SqlValue::Integer(expected),
        ],
    )
}
