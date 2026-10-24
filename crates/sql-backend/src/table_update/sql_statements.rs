//! SQL statements for one table metadata update.

use storage_types::{BoundTableInfo, IndexName, StorageResult, TableName};

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(super) fn load_index_name_lock(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    name: &IndexName,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT locked_until FROM {relation_sys_gsi_name_locks} WHERE table_id = {} AND \
             index_name = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_name_locks = engine.relation_name("sys_gsi_name_locks"),
        ),
        vec![
            SqlValue::Text(uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string()),
            SqlValue::Text(name.to_string()),
        ],
    )
}

pub(super) fn save_catalog(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
) -> StorageResult<SqlStatement> {
    let optional_json = |value: Option<String>| value.map_or(SqlValue::Null, SqlValue::Text);
    let values = vec![
        SqlValue::Text(serde_json::to_string(&table.info.attribute_definitions)?),
        optional_json(
            table
                .info
                .global_secondary_indexes
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ),
        SqlValue::Integer(i64::from(table.info.max_indexers.get())),
        optional_json(
            table
                .info
                .stream_specification
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ),
        SqlValue::Boolean(
            table
                .info
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled),
        ),
        SqlValue::Integer(table.info.table_stream_duration.as_hours_wire_value()),
        SqlValue::Integer(
            table
                .info
                .default_item_stream_duration
                .as_hours_wire_value(),
        ),
        SqlValue::Boolean(table.info.deletion_protection_enabled),
        SqlValue::Text(table.info.table_name.to_string()),
        SqlValue::Text(uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string()),
    ];
    let set = [
        "attribute_definitions",
        "global_secondary_indexes",
        "max_indexers",
        "stream_specification",
        "stream_enabled",
        "table_stream_duration_hours",
        "default_item_stream_duration_hours",
        "deletion_protection_enabled",
    ]
    .iter()
    .enumerate()
    .map(|(ordinal, name)| format!("{name} = {}", engine.engine().placeholder(ordinal + 1)))
    .collect::<Vec<_>>()
    .join(", ");
    Ok(SqlStatement::new(
        format!(
            "UPDATE {relation_tables} SET {set}, catalog_revision = catalog_revision + 1 WHERE \
             table_name = {} AND id = {} AND catalog_revision < {}",
            engine.engine().placeholder(9),
            engine.engine().placeholder(10),
            i64::MAX,
            relation_tables = engine.relation_name("tables"),
        ),
        values,
    ))
}

pub(super) fn delete_history_holds(
    engine: impl SqlLayout,
    table: &TableName,
    index: &str,
) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {relation_sys_gsi_history_holds} WHERE table_name = {} AND index_name = \
             {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_history_holds = engine.relation_name("sys_gsi_history_holds"),
        ),
        vec![
            SqlValue::Text(table.to_string()),
            SqlValue::Text(index.to_owned()),
        ],
    )
}

pub(super) fn delete_build(engine: impl SqlLayout, table: &TableName, index: &str) -> SqlStatement {
    SqlStatement::new(
        format!(
            "DELETE FROM {relation_sys_gsi_builds} WHERE table_name = {} AND index_name = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
        ),
        vec![
            SqlValue::Text(table.to_string()),
            SqlValue::Text(index.to_owned()),
        ],
    )
}
