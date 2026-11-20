//! SQL GSI discovery, source, and destination-state reads.

use storage_backfill::{GsiBuildTarget, GsiPageLimit};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, ItemKey, KeyAttributeType, KeyAttributes,
    KeySchemaElement, StorageError, StorageResult,
};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue, item_read::encode_key_value};

pub(in crate::gsi) fn scan_source_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    after: Option<&KeyAttributes>,
    limit: GsiPageLimit,
) -> StorageResult<SqlStatement> {
    render_source_scan(
        engine,
        ScanSourceStatement {
            table,
            after,
            limit,
            index: None,
        },
    )
}

pub(in crate::gsi) struct ScanSourceStatement<'a> {
    pub table: &'a BoundTableInfo,
    pub after: Option<&'a KeyAttributes>,
    pub limit: GsiPageLimit,
    pub index: Option<&'a storage_types::BoundGsi>,
}

pub(in crate::gsi) fn render_source_scan(
    engine: impl SqlLayout,
    source: ScanSourceStatement<'_>,
) -> StorageResult<SqlStatement> {
    let ScanSourceStatement {
        table,
        after,
        limit,
        index,
    } = source;
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
    let physical = engine.item_table_name(table.history_id);
    let mut extra = String::new();
    if let Some(index) = index {
        if engine.engine() == SqlEngine::Postgres {
            extra.push_str(", ctid::text, xmin::text");
        }
        for column in &index.columns {
            let name = crate::item_schema::key_column_identifier(column);
            if engine.engine() == SqlEngine::Postgres && column.kind == KeyAttributeType::N {
                extra.push_str(&format!(", CAST(\"{name}\" AS TEXT)"));
            } else {
                extra.push_str(&format!(", \"{name}\""));
            }
        }
    }
    Ok(SqlStatement::new(
        format!(
            "SELECT item_value, {physical_keys}{extra} FROM \"{physical}\" {where_clause} ORDER \
             BY {order} LIMIT {}",
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
        "SELECT table_id, table_name, index_name, incarnation, phase, state FROM "
            ["sys_gsi_builds"],
        " WHERE phase IN ('filling', 'building', 'scanning', 'catching-up')"
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
            "SELECT table_id, table_name, index_name, incarnation, phase, state FROM \
             {relation_sys_gsi_builds} WHERE table_id = {} AND index_name = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
        ),
        vec![
            SqlValue::Blob(target.history_id.as_ref().to_vec()),
            SqlValue::Text(target.index_name.to_string()),
        ],
    )
}
