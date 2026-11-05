//! Ordinal-tagged key rows delegate physical equality to the native engine.

use std::fmt::Write as _;

use storage_provider::{ItemWriteObservationRead, canonical_revision_key};
use storage_types::{
    AttributeName, BoundTableInfo, KeyAttributeType, KeySchemaElement, StorageError, StorageResult,
};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue, item_read::encode_key_value};

pub(super) fn live_observation_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    reads: &[ItemWriteObservationRead<'_>],
    read_indices: &[usize],
) -> StorageResult<SqlStatement> {
    let columns = KeySchemaElement::ordered(&table.key_schema)
        .map(|element| {
            let kind = table
                .attribute_definitions
                .iter()
                .find(|definition| definition.attribute_name == element.attribute_name)
                .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
            (
                AttributeName::new(&element.attribute_name)
                    .sanitized()
                    .to_owned(),
                kind,
            )
        })
        .collect::<Vec<_>>();
    let rows = encode_observation_rows(engine, table, reads, read_indices)?;
    let (keys, binds) = render_observation_keys(engine, &columns, rows)?;
    let relation = engine.item_table_name(table.history_id);
    let sql = format!(
        "{keys} SELECT requested.__aux_ordinal, stored.item_value, stored.__aux_revision, \
         stored.__aux_provenance, stored.__aux_ttl_epoch FROM requested JOIN \"{relation}\" AS \
         stored ON {}",
        columns
            .iter()
            .enumerate()
            .map(|(position, (name, _))| format!("stored.\"{name}\" = requested.key{position}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    Ok(SqlStatement::new(sql, binds))
}

pub(super) fn deleted_observation_batch_statement(
    engine: impl SqlLayout,
    reads: &[ItemWriteObservationRead<'_>],
) -> StorageResult<SqlStatement> {
    let columns = [
        ("table_id".to_owned(), KeyAttributeType::B),
        ("key_json".to_owned(), KeyAttributeType::S),
    ];
    let rows = reads
        .iter()
        .map(|read| {
            let (table, key) = read.target();
            Ok(vec![
                SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
                    table.history_id.as_ref(),
                ))?,
                SqlValue::Text(canonical_revision_key(key)?),
            ])
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let (keys, binds) = render_observation_keys(engine, &columns, rows)?;
    Ok(SqlStatement::new(
        format!(
            "{keys} SELECT requested.__aux_ordinal, stored.revision, stored.provenance FROM \
             requested JOIN {} AS stored ON stored.table_id = requested.key0 AND stored.key_json \
             = requested.key1",
            engine.relation_name("item_tombstones")
        ),
        binds,
    ))
}

// Ordinals preserve request order without decoding physical keys or comparing
// wire spellings. VALUES also avoids Turso's 100-level OR-expression limit.
fn render_observation_keys(
    engine: impl SqlLayout,
    columns: &[(String, KeyAttributeType)],
    rows: Vec<Vec<SqlValue>>,
) -> StorageResult<(String, Vec<SqlValue>)> {
    let mut sql = String::from("WITH requested(__aux_ordinal");
    for position in 0..columns.len() {
        write!(sql, ", key{position}")?;
    }
    sql.push_str(") AS (VALUES ");
    let mut binds = Vec::new();
    for (ordinal, row) in rows.into_iter().enumerate() {
        if ordinal > 0 {
            sql.push_str(", ");
        }
        write!(sql, "({ordinal}")?;
        for ((_, kind), value) in columns.iter().zip(row) {
            binds.push(value);
            let placeholder = match (engine.engine(), kind) {
                (SqlEngine::Postgres, KeyAttributeType::N) => {
                    engine.engine().numeric_placeholder(binds.len())
                }
                (SqlEngine::Postgres, KeyAttributeType::B) => format!(
                    "CAST({} AS BYTEA)",
                    engine.engine().placeholder(binds.len())
                )
                .into(),
                _ => engine.engine().placeholder(binds.len()),
            };
            write!(sql, ", {placeholder}")?;
        }
        sql.push(')');
    }
    sql.push(')');
    Ok((sql, binds))
}

fn encode_observation_rows(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    reads: &[ItemWriteObservationRead<'_>],
    read_indices: &[usize],
) -> StorageResult<Vec<Vec<SqlValue>>> {
    read_indices
        .iter()
        .map(|index| {
            let key = reads[*index].target().1;
            KeySchemaElement::ordered(&table.key_schema)
                .map(|element| {
                    encode_key_value(
                        engine,
                        key.get(&element.attribute_name)
                            .ok_or_else(StorageError::invalid_or_missing_key)?,
                    )
                })
                .collect::<StorageResult<Vec<_>>>()
        })
        .collect::<StorageResult<Vec<_>>>()
}
