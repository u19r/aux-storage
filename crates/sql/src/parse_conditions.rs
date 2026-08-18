use std::collections::HashMap;

use storage_condition::{Condition, parse_condition_expression};
use storage_provider::{
    ReadSequenceSqlIdentifier, ReadSequenceSqlOperator, ReadSequenceSqlPredicate,
};
use storage_types::{
    AttributeValue, KeySchemaElement, KeyType, StorageError, StorageResult,
    validate_key_attribute_value_for_schema,
};

#[derive(Debug, Clone)]
pub(crate) struct CompiledKeyCondition {
    condition: String,
    values: Vec<String>,
}

impl CompiledKeyCondition {
    #[cfg(test)]
    pub(crate) fn new(condition: String, values: Vec<String>) -> Self {
        Self { condition, values }
    }

    pub fn into_parts(self) -> (String, Vec<String>) {
        (self.condition, self.values)
    }
}

pub fn parse_key_condition_expression(
    expression: &str,
    key_schema: &[KeySchemaElement],
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<CompiledKeyCondition> {
    let condition = parse_condition_expression(expression, attribute_names, attribute_values)
        .map_err(|err| {
            StorageError::validation(format!("Invalid key condition expression: {err}"))
        })?;
    validate_key_condition(&condition, key_schema)?;
    compile_key_condition(&condition)
}

/// Parse the narrow key-only Query subset used by the one-statement
/// ReadSequence compilers.  The normal Query path remains the owner for
/// filters, projections, index cursors, and all other expressions.
pub(crate) fn parse_read_sequence_query_predicates(
    expression: &str,
    key_schema: &[KeySchemaElement],
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<ReadSequenceSqlPredicate>> {
    let condition = parse_condition_expression(expression, attribute_names, attribute_values)
        .map_err(|err| {
            StorageError::validation(format!("Invalid key condition expression: {err}"))
        })?;
    let hash_key = key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .ok_or_else(|| StorageError::validation("table hash key schema missing"))?;
    let range_key = key_schema.iter().find(|key| key.key_type == KeyType::Range);
    let mut predicates = Vec::new();
    let mut hash_seen = false;
    let mut range_seen = false;
    let mut conditions = Vec::new();
    flatten_and(&condition, &mut conditions)?;
    for condition in conditions {
        let (field, operator, value) = match condition {
            Condition::Equal { field, value } => {
                (field.clone(), ReadSequenceSqlOperator::Equal, value.clone())
            }
            Condition::BeginsWith { field, prefix } => (
                field.clone(),
                ReadSequenceSqlOperator::Prefix,
                prefix.clone(),
            ),
            _ => {
                return Err(StorageError::unsupported(
                    "compiled ReadSequence Query supports only key equality and begins_with",
                ));
            }
        };
        let schema = if field == hash_key.attribute_name {
            if operator != ReadSequenceSqlOperator::Equal || hash_seen {
                return Err(StorageError::validation(
                    "compiled ReadSequence Query requires one hash-key equality",
                ));
            }
            hash_seen = true;
            hash_key
        } else {
            let Some(range_key) = range_key.filter(|key| key.attribute_name == field) else {
                return Err(StorageError::validation(
                    "compiled ReadSequence Query condition references a non-key attribute",
                ));
            };
            if range_seen {
                return Err(StorageError::validation(
                    "compiled ReadSequence Query supports one range-key condition",
                ));
            }
            range_seen = true;
            range_key
        };
        validate_key_attribute_value_for_schema(schema, &value)?;
        let column =
            ReadSequenceSqlIdentifier::new(schema.attribute_name.clone()).map_err(|_| {
                StorageError::unsupported("compiled ReadSequence Query has an unsafe key name")
            })?;
        predicates.push(ReadSequenceSqlPredicate {
            column,
            operator,
            value,
        });
    }
    if !hash_seen {
        return Err(StorageError::validation(
            "compiled ReadSequence Query requires a hash-key equality",
        ));
    }
    Ok(predicates)
}

fn flatten_and<'a>(condition: &'a Condition, output: &mut Vec<&'a Condition>) -> StorageResult<()> {
    match condition {
        Condition::And { conditions } if !conditions.is_empty() => {
            for condition in conditions {
                flatten_and(condition, output)?;
            }
            Ok(())
        }
        Condition::And { .. } => Err(StorageError::validation(
            "compiled ReadSequence Query has an empty AND condition",
        )),
        condition => {
            output.push(condition);
            Ok(())
        }
    }
}

fn validate_key_condition(
    condition: &Condition,
    key_schema: &[KeySchemaElement],
) -> StorageResult<()> {
    let hash_key = key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .map(|key| key.attribute_name.as_str())
        .ok_or_else(|| StorageError::validation("table hash key schema missing"))?;
    let range_key = key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Range)
        .map(|key| key.attribute_name.as_str());

    let mut hash_equality_seen = false;
    let mut range_condition_seen = false;
    validate_key_condition_node(
        condition,
        hash_key,
        range_key,
        &mut hash_equality_seen,
        &mut range_condition_seen,
    )?;

    if !hash_equality_seen {
        return Err(StorageError::validation(format!(
            "Query condition missed key schema element: {hash_key}"
        )));
    }
    Ok(())
}

fn validate_key_condition_node(
    condition: &Condition,
    hash_key: &str,
    range_key: Option<&str>,
    hash_equality_seen: &mut bool,
    range_condition_seen: &mut bool,
) -> StorageResult<()> {
    match condition {
        Condition::And { conditions } => {
            if conditions.is_empty() {
                return Err(StorageError::validation(
                    "Invalid key condition expression: empty AND condition",
                ));
            }
            for child in conditions {
                validate_key_condition_node(
                    child,
                    hash_key,
                    range_key,
                    hash_equality_seen,
                    range_condition_seen,
                )?;
            }
            Ok(())
        }
        Condition::Equal { field, .. } if field == hash_key => {
            if *hash_equality_seen {
                return Err(StorageError::validation(
                    "Query key condition not supported",
                ));
            }
            *hash_equality_seen = true;
            Ok(())
        }
        Condition::Equal { field, .. }
        | Condition::LessThan { field, .. }
        | Condition::LessThanEqual { field, .. }
        | Condition::GreaterThan { field, .. }
        | Condition::GreaterThanEqual { field, .. }
        | Condition::Between { field, .. }
        | Condition::BeginsWith { field, .. } => {
            if Some(field.as_str()) != range_key {
                return Err(StorageError::validation(
                    "Query key condition not supported",
                ));
            }
            if *range_condition_seen {
                return Err(StorageError::validation(
                    "Query key condition not supported",
                ));
            }
            *range_condition_seen = true;
            Ok(())
        }
        _ => Err(StorageError::validation(
            "Invalid operator used in KeyConditionExpression",
        )),
    }
}

fn compile_key_condition(condition: &Condition) -> StorageResult<CompiledKeyCondition> {
    let mut values = Vec::new();
    let condition = compile_key_condition_sql(condition, &mut values)?;
    Ok(CompiledKeyCondition { condition, values })
}

fn compile_key_condition_sql(
    condition: &Condition,
    values: &mut Vec<String>,
) -> StorageResult<String> {
    match condition {
        Condition::Equal { field, value } => {
            let value = key_condition_scalar_value(value)?;
            compile_binary_comparison(field, "=", &value, values)
        }
        Condition::LessThan { field, value } => {
            compile_binary_comparison(field, "<", value, values)
        }
        Condition::LessThanEqual { field, value } => {
            compile_binary_comparison(field, "<=", value, values)
        }
        Condition::GreaterThan { field, value } => {
            compile_binary_comparison(field, ">", value, values)
        }
        Condition::GreaterThanEqual { field, value } => {
            compile_binary_comparison(field, ">=", value, values)
        }
        Condition::Between { field, min, max } => {
            values.push(min.clone());
            let min_placeholder = values.len();
            values.push(max.clone());
            let max_placeholder = values.len();
            Ok(format!(
                "{field} BETWEEN ?{min_placeholder} AND ?{max_placeholder}"
            ))
        }
        Condition::BeginsWith { field, prefix } => {
            let prefix = begins_with_prefix(prefix)?;
            values.push(format!("{prefix}%"));
            Ok(format!("{field} LIKE ?{}", values.len()))
        }
        Condition::And { conditions } => {
            if conditions.is_empty() {
                return Err(StorageError::validation(
                    "Invalid key condition expression: empty AND condition",
                ));
            }
            let compiled = conditions
                .iter()
                .map(|condition| compile_key_condition_sql(condition, values))
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(format!("({})", compiled.join(" AND ")))
        }
        _ => Err(StorageError::validation(
            "Unsupported key condition expression",
        )),
    }
}

fn compile_binary_comparison(
    field: &str,
    operator: &str,
    value: &str,
    values: &mut Vec<String>,
) -> StorageResult<String> {
    values.push(value.to_string());
    Ok(format!("{field} {operator} ?{}", values.len()))
}

fn key_condition_scalar_value(value: &AttributeValue) -> StorageResult<String> {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => {
            Ok(value.clone())
        }
        _ => Err(StorageError::validation(
            "KeyConditionExpression comparison values must be scalar",
        )),
    }
}

fn begins_with_prefix(prefix: &AttributeValue) -> StorageResult<String> {
    match prefix {
        AttributeValue::S(prefix) => Ok(prefix.clone()),
        AttributeValue::B(prefix) => Ok(prefix.trim_end_matches('=').to_string()),
        _ => Err(StorageError::validation(
            "begins_with is only valid for string or binary key attributes",
        )),
    }
}
