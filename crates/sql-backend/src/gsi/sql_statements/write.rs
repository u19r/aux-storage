//! SQL GSI build, projection, and TTL transition statements.

use storage_backfill::{GsiBuild, GsiStateRecord};
use storage_types::{StorageResult, TableHistoryId};

use crate::{SqlLayout, SqlStatement, SqlValue, sql_template::sql_template};

pub(in crate::gsi) fn insert_build_statement(
    engine: impl SqlLayout,
    build: &GsiBuild<()>,
    state: GsiStateRecord,
) -> SqlStatement {
    let sql = format!(
        "INSERT INTO {relation_sys_gsi_builds} (table_id, table_name, index_name, incarnation, \
         phase, state) VALUES ({})",
        engine.engine().placeholders(6),
        relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Blob(build.target.history_id.as_ref().to_vec()),
            SqlValue::Text(build.target.table_name.to_string()),
            SqlValue::Text(build.target.index_name.to_string()),
            SqlValue::Blob(build.target.incarnation.as_ref().to_vec()),
            SqlValue::Text(state.phase.to_owned()),
            SqlValue::Blob(state.state),
        ],
    )
}

pub(in crate::gsi) fn transition_state_statement<Cursor: serde::Serialize>(
    engine: impl SqlLayout,
    expected: &GsiBuild<Cursor>,
    next: &storage_backfill::GsiState<Cursor>,
) -> StorageResult<SqlStatement> {
    let old = GsiStateRecord::try_from(&expected.state)?;
    let new = GsiStateRecord::try_from(next)?;
    let sql = sql_template!(engine,
        "UPDATE " ["sys_gsi_builds"], " SET phase = " {1}, ", state = " {2},
        " WHERE table_name = " {3}, " AND index_name = " {4},
        " AND incarnation = " {5}, " AND phase = " {6}, " AND state = " {7},
        " AND table_id = " {8}
    );
    Ok(SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(new.phase.to_owned()),
            SqlValue::Blob(new.state),
            SqlValue::Text(expected.target.table_name.to_string()),
            SqlValue::Text(expected.target.index_name.to_string()),
            SqlValue::Blob(expected.target.incarnation.as_ref().to_vec()),
            SqlValue::Text(old.phase.to_owned()),
            SqlValue::Blob(old.state),
            SqlValue::Blob(expected.target.history_id.as_ref().to_vec()),
        ],
    ))
}

pub(in crate::gsi) fn activate_ttl_statement(
    engine: impl SqlLayout,
    table_history_id: TableHistoryId,
    activation: &storage_common::TtlConfigActivation,
) -> StorageResult<SqlStatement> {
    let sql = format!(
        "UPDATE {relation_tables} SET ttl_config_blob = {}, catalog_revision = catalog_revision + \
         1 WHERE table_name = {} AND id = {} AND ttl_config_blob = {} AND catalog_revision < {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        i64::MAX,
        relation_tables = engine.relation_name("tables"),
    );
    Ok(SqlStatement::new(
        sql,
        vec![
            SqlValue::Blob(storage_types::storage_serde::to_bytes(
                &activation.activated,
            )?),
            SqlValue::Text(activation.table_name.to_string()),
            SqlValue::Text(uuid::Uuid::from_bytes(*table_history_id.as_ref()).to_string()),
            SqlValue::Blob(storage_types::storage_serde::to_bytes(
                &activation.expected,
            )?),
        ],
    ))
}
