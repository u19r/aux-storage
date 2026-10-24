//! SQL text and bindings for bounded Query and Scan pages.

use std::borrow::Cow;

use storage_condition::Condition;
use storage_provider::{BoundRangeReadPage, BoundRangeReadPlan};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, KeyAttributeType, KeySchemaElement,
    StorageError, StorageResult, maintenance::TargetIncarnation,
};

use super::projection::RangeProjection;
use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue, item_read::encode_key_value};

pub(super) struct KeyColumn {
    pub logical: String,
    physical: String,
    pub kind: KeyAttributeType,
}

pub(super) fn ordered_columns(
    table: &BoundTableInfo,
    bound: &BoundRangeReadPlan<'_>,
) -> Vec<KeyColumn> {
    let mut columns = Vec::new();
    if let Some(index) = bound.index.as_ref() {
        columns.extend(schema_columns(table, &index.key_schema, ""));
        columns.extend(schema_columns(table, &table.info.key_schema, "table_"));
    } else {
        columns.extend(schema_columns(table, &table.info.key_schema, ""));
    }
    columns
}

fn schema_columns(
    table: &BoundTableInfo,
    schema: &[KeySchemaElement],
    prefix: &str,
) -> impl Iterator<Item = KeyColumn> {
    KeySchemaElement::ordered(schema).map(move |key| {
        let kind = table
            .info
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == key.attribute_name)
            .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
        let sanitized = AttributeName::new(&key.attribute_name)
            .sanitized()
            .to_owned();
        KeyColumn {
            logical: key.attribute_name.clone(),
            physical: format!("{prefix}{sanitized}"),
            kind,
        }
    })
}

pub(super) struct RangeStatementInput<'a, 'b> {
    pub table: &'a BoundTableInfo,
    pub bound: &'a BoundRangeReadPlan<'b>,
    pub page: &'a BoundRangeReadPage<'b>,
    pub columns: &'a [KeyColumn],
    pub incarnation: Option<&'a TargetIncarnation>,
}

pub(super) fn range_statement<P: RangeProjection>(
    engine: impl SqlLayout,
    input: &RangeStatementInput<'_, '_>,
) -> StorageResult<SqlStatement> {
    let RangeStatementInput {
        table,
        bound,
        page,
        columns,
        incarnation,
    } = input;
    let physical = if let Some(index) = bound.index.as_ref() {
        let incarnation = incarnation
            .ok_or_else(|| StorageError::internal("SQL GSI page has no physical incarnation"))?;
        engine.gsi_table_name(&table.info.table_name, &index.index_name, incarnation)
    } else {
        engine.item_table_name(&table.info.table_name, table.history_id)
    };
    let mut binds = Vec::new();
    let mut predicates = Vec::new();
    if bound.index.is_some() {
        predicates.push(P::TOMBSTONE_PREDICATE.to_owned());
    }
    if let Some(condition) = page.condition {
        let condition_columns = if let Some(index) = bound.index.as_ref() {
            index.key_schema.len()
        } else {
            columns.len()
        };
        predicates.push(compile_condition(
            engine,
            condition,
            &columns[..condition_columns],
            &mut binds,
        )?);
    }
    if let Some(start) = page.start.as_ref() {
        let attributes = start.key_attributes(&table.info)?;
        let left = columns
            .iter()
            .map(|column| format!("\"{}\"", column.physical))
            .collect::<Vec<_>>()
            .join(", ");
        let mut right = Vec::with_capacity(columns.len());
        for column in columns.iter() {
            let value = attributes.get(&column.logical).ok_or_else(|| {
                StorageError::internal("bound range continuation lacks a key field")
            })?;
            right.push(bind_key(engine, column, value, &mut binds)?);
        }
        let comparison = if page.forward { ">" } else { "<" };
        predicates.push(format!("({left}) {comparison} ({})", right.join(", ")));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", predicates.join(" AND "))
    };
    let selected = columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            let physical = format!("\"{}\"", column.physical);
            if engine.engine() == SqlEngine::Postgres && column.kind == KeyAttributeType::N {
                format!("CAST({physical} AS TEXT) AS \"__aux_decoded_key_{ordinal}\"")
            } else {
                physical
            }
        })
        .chain([P::EVALUATED_BYTES.to_owned(), P::COLUMNS.to_owned()])
        .collect::<Vec<_>>()
        .join(", ");
    let direction = if page.forward { "ASC" } else { "DESC" };
    let order = columns
        .iter()
        .map(|column| format!("\"{}\" {direction}", column.physical))
        .collect::<Vec<_>>()
        .join(", ");
    let limit = page.budget.max_items.saturating_add(1);
    binds.push(SqlValue::Integer(i64::from(limit)));
    let sql = format!(
        "SELECT {selected} FROM \"{physical}\" {where_clause} ORDER BY {order} LIMIT {}",
        engine.engine().placeholder(binds.len()),
    );
    Ok(SqlStatement::new(sql, binds))
}

fn compile_condition(
    engine: impl SqlLayout,
    condition: &Condition,
    columns: &[KeyColumn],
    binds: &mut Vec<SqlValue>,
) -> StorageResult<String> {
    let field = |name: &str| {
        columns
            .iter()
            .find(|column| column.logical == name)
            .ok_or_else(|| {
                StorageError::internal("bound key condition field is absent from the key schema")
            })
    };
    let comparison = |name: &str, op: &str, value: &str, binds: &mut Vec<SqlValue>| {
        let column = field(name)?;
        let placeholder = bind_raw(engine, column, value, binds)?;
        Ok::<_, StorageError>(format!("\"{}\" {op} {placeholder}", column.physical))
    };
    match condition {
        Condition::Equal { field: name, value } => {
            let column = field(name)?;
            let placeholder = bind_key(engine, column, value, binds)?;
            Ok(format!("\"{}\" = {placeholder}", column.physical))
        }
        Condition::LessThan { field, value } => comparison(field, "<", value, binds),
        Condition::LessThanEqual { field, value } => comparison(field, "<=", value, binds),
        Condition::GreaterThan { field, value } => comparison(field, ">", value, binds),
        Condition::GreaterThanEqual { field, value } => comparison(field, ">=", value, binds),
        Condition::Between {
            field: name,
            min,
            max,
        } => {
            let column = field(name)?;
            let lower = bind_raw(engine, column, min, binds)?;
            let upper = bind_raw(engine, column, max, binds)?;
            Ok(format!(
                "\"{}\" BETWEEN {lower} AND {upper}",
                column.physical
            ))
        }
        Condition::BeginsWith {
            field: name,
            prefix,
        } => {
            let column = field(name)?;
            match (column.kind, prefix) {
                (KeyAttributeType::S, AttributeValue::S(value)) => {
                    let lower = bind_raw(engine, column, value, binds)?;
                    if let Some(upper) = string_prefix_upper_bound(value) {
                        let high = bind_raw(engine, column, &upper, binds)?;
                        Ok(format!(
                            "(\"{}\" >= {lower} AND \"{}\" < {high})",
                            column.physical, column.physical
                        ))
                    } else {
                        Ok(format!("\"{}\" >= {lower}", column.physical))
                    }
                }
                (KeyAttributeType::B, AttributeValue::B(value)) => {
                    let bytes = storage_types::dynamodb_binary::decode_base64_string(value)
                        .map_err(|_| {
                            StorageError::internal("bound binary key prefix is invalid")
                        })?;
                    let lower = bind_raw_bytes(engine, bytes.clone(), binds);
                    let mut upper = bytes;
                    while upper.last() == Some(&0xff) {
                        upper.pop();
                    }
                    if let Some(last) = upper.pop() {
                        upper.push(last + 1);
                        let high = bind_raw_bytes(engine, upper, binds);
                        Ok(format!(
                            "(\"{}\" >= {lower} AND \"{}\" < {high})",
                            column.physical, column.physical
                        ))
                    } else {
                        Ok(format!("\"{}\" >= {lower}", column.physical))
                    }
                }
                _ => Err(StorageError::internal(
                    "bound key prefix has another key type",
                )),
            }
        }
        Condition::And { conditions } if !conditions.is_empty() => conditions
            .iter()
            .map(|part| compile_condition(engine, part, columns, binds))
            .collect::<StorageResult<Vec<_>>>()
            .map(|parts| format!("({})", parts.join(" AND "))),
        _ => Err(StorageError::internal(
            "bound key condition has an unsupported operator",
        )),
    }
}

fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    for (index, character) in prefix.char_indices().rev() {
        let next = u32::from(character) + 1;
        let next = if next == 0xd800 { 0xe000 } else { next };
        if let Some(next) = char::from_u32(next) {
            let mut upper = prefix.get(..index)?.to_owned();
            upper.push(next);
            return Some(upper);
        }
    }
    None
}

fn bind_raw(
    engine: impl SqlLayout,
    column: &KeyColumn,
    raw: &str,
    binds: &mut Vec<SqlValue>,
) -> StorageResult<Cow<'static, str>> {
    let value = match column.kind {
        KeyAttributeType::S => AttributeValue::S(raw.to_owned()),
        KeyAttributeType::N => AttributeValue::N(raw.to_owned()),
        KeyAttributeType::B => AttributeValue::B(raw.to_owned()),
    };
    bind_key(engine, column, &value, binds)
}

fn bind_raw_bytes(
    engine: impl SqlLayout,
    bytes: Vec<u8>,
    binds: &mut Vec<SqlValue>,
) -> Cow<'static, str> {
    binds.push(SqlValue::Blob(bytes));
    engine.engine().placeholder(binds.len())
}

fn bind_key(
    engine: impl SqlLayout,
    column: &KeyColumn,
    value: &AttributeValue,
    binds: &mut Vec<SqlValue>,
) -> StorageResult<Cow<'static, str>> {
    binds.push(encode_key_value(engine, value)?);
    let placeholder = engine.engine().placeholder(binds.len());
    if engine.engine() == SqlEngine::Postgres && column.kind == KeyAttributeType::N {
        Ok(format!("CAST({placeholder} AS TEXT)::NUMERIC").into())
    } else {
        Ok(placeholder)
    }
}
