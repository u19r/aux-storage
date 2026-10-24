//! SQL GSI build, projection, and TTL transition statements.

use storage_backfill::{GsiBuild, GsiBuildTarget, GsiStateRecord};
use storage_types::{StorageError, StorageResult, TableHistoryId};

use crate::{SqlLayout, SqlStatement, SqlValue, sql_template::sql_template};

pub(in crate::gsi) fn insert_build_statement(
    engine: impl SqlLayout,
    build: &GsiBuild<()>,
    state: GsiStateRecord,
) -> SqlStatement {
    let sql = format!(
        "INSERT INTO {relation_sys_gsi_builds} (table_name, index_name, incarnation, phase, \
         state) VALUES ({})",
        engine.engine().placeholders(5),
        relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
    );
    SqlStatement::new(
        sql,
        vec![
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

pub(in crate::gsi) fn history_hold_statement<Cursor>(
    engine: impl SqlLayout,
    table_history_id: TableHistoryId,
    build: &GsiBuild<Cursor>,
    position: &storage_types::SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let checkpoint = build.state.required_stream_history();
    let generation = i64::try_from(checkpoint.scope().generation())
        .map_err(|_| StorageError::internal("GSI hold generation exceeds SQL range"))?;
    let offset = i64::try_from(
        position
            .offset()
            .order()
            .map_err(|_| StorageError::internal("GSI hold offset is invalid"))?,
    )
    .map_err(|_| StorageError::internal("GSI hold offset exceeds SQL range"))?;
    let sql = format!(
        "INSERT INTO {relation_sys_gsi_history_holds} (table_name, index_name, index_incarnation, \
         source_scope, source_generation, shard_id, lineage_id, consumed_offset, \
         table_incarnation) VALUES ({})",
        engine.engine().placeholders(9),
        relation_sys_gsi_history_holds = engine.relation_name("sys_gsi_history_holds"),
    );
    Ok(SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(build.target.table_name.to_string()),
            SqlValue::Text(build.target.index_name.to_string()),
            SqlValue::Blob(build.target.incarnation.as_ref().to_vec()),
            SqlValue::Text(checkpoint.scope().scope_id().as_str().to_owned()),
            SqlValue::Integer(generation),
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Integer(offset),
            SqlValue::Text(uuid::Uuid::from_bytes(*table_history_id.as_ref()).to_string()),
        ],
    ))
}

/// Update an admitted hold in place so pipelined writes touch independent keys.
pub(in crate::gsi) fn store_history_hold_statement<Cursor>(
    engine: impl SqlLayout,
    table_history_id: TableHistoryId,
    build: &GsiBuild<Cursor>,
    position: &storage_types::SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let (mut sql, binds) =
        history_hold_statement(engine, table_history_id, build, position)?.into_parts();
    sql.to_mut().push_str(&crate::sql_template::sql_template!(
        engine,
        " ON CONFLICT (table_name, index_name, index_incarnation, lineage_id) DO UPDATE SET \
         source_scope = excluded.source_scope, source_generation = excluded.source_generation, \
         shard_id = excluded.shard_id, consumed_offset = excluded.consumed_offset WHERE "
            ["sys_gsi_history_holds"],
        ".table_incarnation = excluded.table_incarnation"
    ));
    Ok(SqlStatement::new(sql, binds))
}

pub(in crate::gsi) fn delete_history_hold_statement(
    engine: impl SqlLayout,
    table_history_id: TableHistoryId,
    target: &GsiBuildTarget,
    position: &storage_types::SystemStreamPosition,
) -> SqlStatement {
    let sql = sql_template!(engine,
        "DELETE FROM " ["sys_gsi_history_holds"], " WHERE table_name = " {1},
        " AND index_name = " {2}, " AND index_incarnation = " {3},
        " AND table_incarnation = " {4}, " AND lineage_id = " {5}
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(target.table_name.to_string()),
            SqlValue::Text(target.index_name.to_string()),
            SqlValue::Blob(target.incarnation.as_ref().to_vec()),
            SqlValue::Text(uuid::Uuid::from_bytes(*table_history_id.as_ref()).to_string()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
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
        " AND incarnation = " {5}, " AND phase = " {6}, " AND state = " {7}
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
