//! SQL GSI discovery, source, and destination-state reads.

use storage_backfill::{
    GsiBuild, GsiBuildTarget, GsiPageLimit, GsiProjectionKey, GsiReferencedHistoryRead,
};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, GlobalSecondaryIndex, IndexName, ItemKey,
    KeyAttributeType, KeyAttributes, KeySchemaElement, StorageError, StorageResult,
    StoredTableInfo, TableHistoryId, maintenance::TargetIncarnation,
};

use super::key::BoundProjectionKey;
use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue, item_read::encode_key_value};

pub(in crate::gsi) fn referenced_histories_statement(
    engine: impl SqlLayout,
    reads: &[GsiReferencedHistoryRead<'_>],
) -> SqlStatement {
    let mut binds = Vec::with_capacity(reads.len() * 2);
    let sql = referenced_history_tree(engine, reads, 0, &mut binds);
    SqlStatement::new(format!("{sql} ORDER BY ordinal, item_id DESC"), binds)
}

fn referenced_history_tree(
    engine: impl SqlLayout,
    reads: &[GsiReferencedHistoryRead<'_>],
    start: usize,
    binds: &mut Vec<SqlValue>,
) -> String {
    if reads.len() == 1 {
        let read = &reads[0];
        binds.push(SqlValue::Blob(
            read.physical_stream_name().as_ref().to_vec(),
        ));
        let stream = engine.engine().placeholder(binds.len());
        binds.push(SqlValue::Blob(
            storage_types::StreamItemId::from(read.through)
                .as_bytes()
                .to_vec(),
        ));
        let through = engine.engine().placeholder(binds.len());
        return format!(
            "SELECT {start} AS ordinal, image.* FROM (SELECT item_id, row_blob FROM \
             {relation_sys_dynamo_stream_items} WHERE stream_name = {stream} AND item_id <= \
             {through} ORDER BY item_id DESC LIMIT 2) AS image",
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        );
    }
    let half = reads.len() / 2;
    let left = referenced_history_tree(engine, &reads[..half], start, binds);
    let right = referenced_history_tree(engine, &reads[half..], start + half, binds);
    format!(
        "SELECT * FROM ({left}) AS left_images UNION ALL SELECT * FROM ({right}) AS right_images"
    )
}

pub(in crate::gsi) fn projection_state_statement(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    keys: &[&GsiProjectionKey],
    incarnation: &TargetIncarnation,
) -> StorageResult<SqlStatement> {
    render_projection_state(
        engine,
        ProjectionStateQuery {
            table,
            index,
            keys,
            incarnation,
        },
        "g.__aux_item_version",
    )
}

pub(in crate::gsi) struct ProjectionStateQuery<'a> {
    pub table: &'a StoredTableInfo,
    pub index: &'a GlobalSecondaryIndex,
    pub keys: &'a [&'a GsiProjectionKey],
    pub incarnation: &'a TargetIncarnation,
}

pub(in crate::gsi) fn projection_descriptor_statement(
    engine: impl SqlLayout,
    query: ProjectionStateQuery<'_>,
) -> StorageResult<SqlStatement> {
    render_projection_state(engine, query, "g.__aux_item_version, g.item_descriptor")
}

fn render_projection_state(
    engine: impl SqlLayout,
    query: ProjectionStateQuery<'_>,
    columns_sql: &str,
) -> StorageResult<SqlStatement> {
    let ProjectionStateQuery {
        table,
        index,
        keys,
        incarnation,
    } = query;
    let count = index
        .key_schema
        .len()
        .checked_add(table.key_schema.len())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| count.checked_mul(keys.len()))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut values = crate::statement::SqlText::default();
    let mut columns = None;
    for (ordinal, key) in keys.iter().enumerate() {
        let mut bound = BoundProjectionKey::try_new(engine, table, index, key)?;
        match &columns {
            Some(expected) if expected != &bound.columns => {
                return Err(StorageError::internal(
                    "SQL GSI projection columns differ within page",
                ));
            }
            None => columns = Some(std::mem::take(&mut bound.columns)),
            _ => {}
        }
        values.append(format_args!("{}", if ordinal == 0 { "" } else { ", " }))?;
        bound.append_request_row(engine, ordinal, &mut binds, &mut values)?;
    }
    let columns = columns.ok_or_else(|| StorageError::internal("GSI state page is empty"))?;
    let physical = engine.gsi_table_name(&table.table_name, &index.index_name, incarnation);
    let sql = projection_state_sql(&physical, columns_sql, &columns, &values.finish()?)?;
    Ok(SqlStatement::new(sql, binds))
}

fn projection_state_sql(
    physical: &str,
    columns_sql: &str,
    columns: &[String],
    values: &str,
) -> StorageResult<String> {
    let mut sql = crate::statement::SqlText::default();
    sql.append(format_args!("WITH requested(ordinal, "))?;
    for ordinal in 0..columns.len() {
        sql.append(format_args!(
            "{}k{ordinal}",
            if ordinal == 0 { "" } else { ", " }
        ))?;
    }
    sql.append(format_args!(
        ") AS (VALUES {values}) SELECT requested.ordinal, g.__aux_tombstone, {columns_sql} FROM \
         requested LEFT JOIN \"{physical}\" AS g ON "
    ))?;
    for (ordinal, column) in columns.iter().enumerate() {
        sql.append(format_args!(
            "{}g.\"{column}\" = requested.k{ordinal}",
            if ordinal == 0 { "" } else { " AND " }
        ))?;
    }
    sql.append(format_args!(" ORDER BY requested.ordinal"))?;
    sql.finish()
}

pub(in crate::gsi) fn ttl_projection_state_statement(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    keys: &[&GsiProjectionKey],
    history_id: TableHistoryId,
) -> StorageResult<SqlStatement> {
    let count = keys
        .len()
        .checked_mul(3)
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut sql = crate::statement::SqlText::default();
    sql.append(format_args!(
        "WITH requested(ordinal, ttl_value, key_token) AS (VALUES "
    ))?;
    for (ordinal, key) in keys.iter().enumerate() {
        let ttl = key
            .ttl_index_key
            .as_ref()
            .ok_or_else(|| StorageError::internal("TTL projection key is missing"))?;
        let (value, token): (i64, &str) = ttl.into();
        let observed = i64::try_from(ordinal)
            .map_err(|_| StorageError::internal("TTL state ordinal exceeds SQL range"))?;
        sql.append(format_args!(
            "{}(CAST(",
            if ordinal == 0 { "" } else { ", " }
        ))?;
        sql.append_parameter(engine.engine(), binds.len() + 1, false)?;
        sql.append(format_args!(" AS BIGINT), CAST("))?;
        sql.append_parameter(engine.engine(), binds.len() + 2, false)?;
        sql.append(format_args!(" AS BIGINT), "))?;
        sql.append_parameter(engine.engine(), binds.len() + 3, false)?;
        sql.append(format_args!(")"))?;
        binds.extend([
            SqlValue::Integer(observed),
            SqlValue::Integer(value),
            SqlValue::try_from(crate::row_view::SqlValueRef::Text(token))?,
        ]);
    }
    let physical = engine.ttl_table_name(&table.table_name, history_id);
    sql.append(format_args!(
        ") SELECT requested.ordinal, CASE WHEN g.key_token IS NULL THEN NULL ELSE 0 END, \
         CAST(NULL AS BIGINT) FROM requested LEFT JOIN \"{physical}\" AS g ON g.ttl_value = \
         requested.ttl_value AND g.key_token = requested.key_token ORDER BY requested.ordinal"
    ))?;
    Ok(SqlStatement::new(sql.finish()?, binds))
}

pub(in crate::gsi) fn scan_source_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    after: Option<&KeyAttributes>,
    limit: GsiPageLimit,
) -> StorageResult<SqlStatement> {
    let columns = KeySchemaElement::ordered(&table.info.key_schema)
        .map(|element| {
            (
                element,
                AttributeName::new(&element.attribute_name)
                    .sanitized()
                    .to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let (where_clause, mut binds) = scan_cursor_clause(engine, table, &columns, after)?;
    let order = columns
        .iter()
        .map(|(_, name)| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    binds.push(SqlValue::Integer(i64::from(
        u32::from(limit).saturating_add(1),
    )));
    let physical_keys = columns
        .iter()
        .map(|(element, name)| {
            let kind = table
                .attribute_definitions
                .iter()
                .find(|definition| definition.attribute_name == element.attribute_name)
                .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
            if engine.engine() == SqlEngine::Postgres && kind == KeyAttributeType::N {
                format!("CAST(\"{name}\" AS TEXT)")
            } else {
                format!("\"{name}\"")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let physical = engine.item_table_name(&table.info.table_name, table.history_id);
    Ok(SqlStatement::new(
        format!(
            "SELECT item_descriptor, __aux_revision, __aux_indexers, {physical_keys} FROM \
             \"{physical}\" {where_clause} ORDER BY {order} LIMIT {}",
            engine.engine().placeholder(binds.len()),
        ),
        binds,
    ))
}

fn scan_cursor_clause(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    columns: &[(&KeySchemaElement, String)],
    after: Option<&KeyAttributes>,
) -> StorageResult<(String, Vec<SqlValue>)> {
    let Some(after) = after else {
        return Ok((String::new(), Vec::new()));
    };
    ItemKey::from_key_schema(table.info.table_name.clone(), &table.info.key_schema, after)?;
    let mut binds = Vec::with_capacity(columns.len());
    let mut right = Vec::with_capacity(columns.len());
    for (element, _) in columns {
        let value = after
            .get(&element.attribute_name)
            .ok_or_else(|| StorageError::internal("GSI scan cursor is missing a key"))?;
        binds.push(encode_key_value(engine, value)?);
        let mut placeholder = engine.engine().placeholder(binds.len());
        let numeric = table.info.attribute_definitions.iter().any(|definition| {
            definition.attribute_name == element.attribute_name
                && definition.attribute_type == KeyAttributeType::N
        });
        if engine.engine() == SqlEngine::Postgres
            && numeric
            && matches!(value, AttributeValue::N(_))
        {
            placeholder = format!("CAST({placeholder} AS TEXT)::NUMERIC").into();
        }
        right.push(placeholder);
    }
    let left = columns
        .iter()
        .map(|(_, name)| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    Ok((format!("WHERE ({left}) > ({})", right.join(", ")), binds))
}

pub(in crate::gsi) fn candidate_statement(
    engine: impl SqlLayout,
    after: Option<&GsiBuildTarget>,
) -> SqlStatement {
    let mut sql = crate::sql_template::sql_template!(
        engine,
        "SELECT table_name, index_name, incarnation, phase, state FROM "["sys_gsi_builds"],
        " WHERE phase <> 'blocked'"
    )
    .into_owned();
    let mut values = Vec::new();
    if let Some(after) = after {
        sql.push_str(&format!(
            " AND (table_name, index_name) > ({}, {})",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
        ));
        values.push(SqlValue::Text(after.table_name.to_string()));
        values.push(SqlValue::Text(after.index_name.to_string()));
    }
    sql.push_str(" ORDER BY table_name, index_name LIMIT 1");
    SqlStatement::new(sql, values)
}

pub(in crate::gsi) fn candidate_binding_statement(
    engine: impl SqlLayout,
    target: &GsiBuildTarget,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT table_name, index_name, incarnation, phase, state FROM \
             {relation_sys_gsi_builds} WHERE table_name = {} AND index_name = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
        ),
        vec![
            SqlValue::Text(target.table_name.to_string()),
            SqlValue::Text(target.index_name.to_string()),
        ],
    )
}

pub(in crate::gsi) fn read_hold_statement(
    engine: impl SqlLayout,
    build: &GsiBuild<Vec<u8>>,
) -> SqlStatement {
    let sql = format!(
        "SELECT source_scope, source_generation, shard_id, lineage_id, consumed_offset FROM \
         {relation_sys_gsi_history_holds} WHERE table_name = {} AND index_name = {} AND \
         index_incarnation = {} ORDER BY lineage_id",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        relation_sys_gsi_history_holds = engine.relation_name("sys_gsi_history_holds"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(build.target.table_name.to_string()),
            SqlValue::Text(build.target.index_name.to_string()),
            SqlValue::Blob(build.target.incarnation.as_ref().to_vec()),
        ],
    )
}

pub(in crate::gsi) fn minimum_hold_statement(
    engine: impl SqlLayout,
    scope: &storage_types::SystemStreamScope,
    lineage: &storage_types::SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let generation = i64::try_from(scope.generation())
        .map_err(|_| StorageError::internal("GSI hold generation exceeds SQL range"))?;
    Ok(SqlStatement::new(
        format!(
            "SELECT table_name, index_name, index_incarnation, consumed_offset FROM \
             {relation_sys_gsi_history_holds} WHERE source_scope = {} AND source_generation = {} \
             AND shard_id = {} AND lineage_id = {} ORDER BY consumed_offset, index_incarnation \
             LIMIT 1",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            relation_sys_gsi_history_holds = engine.relation_name("sys_gsi_history_holds"),
        ),
        vec![
            SqlValue::Text(scope.scope_id().as_str().to_owned()),
            SqlValue::Integer(generation),
            SqlValue::Text(lineage.shard_id().as_str().to_owned()),
            SqlValue::Text(lineage.lineage_id().as_str().to_owned()),
        ],
    ))
}

pub(in crate::gsi) fn build_for_hold_statement(
    engine: impl SqlLayout,
    target: &GsiBuildTarget,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT table_name, index_name, incarnation, phase, state FROM \
             {relation_sys_gsi_builds} WHERE table_name = {} AND index_name = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
        ),
        vec![
            SqlValue::Text(target.table_name.to_string()),
            SqlValue::Text(target.index_name.to_string()),
        ],
    )
}

fn gsi_catalog_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    requested: Option<&IndexName>,
    columns: &str,
) -> SqlStatement {
    let mut sql = format!(
        "SELECT {columns} FROM {relation_sys_gsi_builds} WHERE table_name = {}",
        engine.engine().placeholder(1),
        relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
    );
    let mut values = vec![SqlValue::Text(table.info.table_name.to_string())];
    if let Some(index) = requested {
        sql.push_str(&format!(
            " AND index_name = {}",
            engine.engine().placeholder(2)
        ));
        values.push(SqlValue::Text(index.to_string()));
    }
    sql.push_str(" ORDER BY index_name");
    SqlStatement::new(sql, values)
}

pub(in crate::gsi) fn gsi_status_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    requested: Option<&IndexName>,
) -> SqlStatement {
    gsi_catalog_statement(
        engine,
        table,
        requested,
        "table_name, index_name, incarnation, phase",
    )
}

pub(in crate::gsi) fn gsi_build_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    requested: Option<&IndexName>,
) -> SqlStatement {
    gsi_catalog_statement(
        engine,
        table,
        requested,
        "table_name, index_name, incarnation, phase, state",
    )
}
