//! SQL GSI build, projection, and TTL transition statements.

use storage_backfill::{GsiBuild, GsiStateRecord};
use storage_types::{StorageError, StorageResult, TableHistoryId};

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

pub(in crate::gsi) fn render_gsi_upsert(
    engine: impl SqlLayout,
    columns: &[String],
    key_count: usize,
    numeric_keys: &[usize],
) -> StorageResult<GsiUpsertSql> {
    let conflict = columns
        .get(..key_count)
        .ok_or_else(|| StorageError::internal("GSI conflict key count exceeds columns"))?;
    let mut sql = crate::statement::SqlText::default();
    sql.append(format_args!("("))?;
    sql.append_columns(columns)?;
    sql.append(format_args!(") VALUES ("))?;
    for position in 1..=columns.len() {
        sql.append(format_args!("{}", if position == 1 { "" } else { ", " }))?;
        sql.append_parameter(engine.engine(), position, numeric_keys.contains(&position))?;
    }
    sql.append(format_args!(") ON CONFLICT ("))?;
    sql.append_columns(conflict)?;
    sql.append(format_args!(") DO UPDATE SET "))?;
    sql.append_assignments(columns.iter().skip(key_count))?;
    // Equal revisions must still fail the caller's changed-row count fence.
    sql.append(format_args!(" WHERE excluded.__aux_item_version >"))?;
    Ok(GsiUpsertSql(sql.finish()?))
}

pub(in crate::gsi) struct GsiUpsertSql(String);

impl GsiUpsertSql {
    pub(in crate::gsi) fn bind_relation(self, physical: &str) -> StorageResult<String> {
        crate::statement::SqlText::render(format_args!(
            "INSERT INTO \"{physical}\" {} \"{physical}\".__aux_item_version",
            self.0
        ))
    }
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
