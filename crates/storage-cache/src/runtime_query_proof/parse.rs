use std::collections::HashMap;

use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, StorageError, StorageResult, StoredTableInfo, TableName,
};

use crate::runtime_query_proof::{
    RuntimeSortCondition, key_attribute_type_for_name, query_space_key_schema, range_key_name,
    scalar_order_repr_for_type,
};

pub fn parse_partition_key_condition(
    expression: &str,
    names: Option<&HashMap<String, String>>,
) -> StorageResult<(String, String)> {
    let key_clause = first_key_clause(expression).ok_or_else(|| {
        StorageError::validation("query proof cache only supports partition-equality queries")
    })?;
    let (lhs, rhs) = key_clause.split_once('=').ok_or_else(|| {
        StorageError::validation("query proof cache expected partition equality in key condition")
    })?;
    let lhs = lhs.trim().trim_matches('(').trim_matches(')');
    let rhs = rhs.trim().trim_matches('(').trim_matches(')');
    if !rhs.starts_with(':') {
        return Err(StorageError::validation(
            "query proof cache partition equality must bind to a placeholder",
        ));
    }
    Ok((resolve_attribute_name(lhs, names)?, rhs.to_string()))
}

#[must_use]
pub fn query_sort_clause(expression: &str) -> Option<&str> {
    let upper = expression.to_ascii_uppercase();
    let and_pos = upper.find(" AND ")?;
    expression.get(and_pos + 5..).map(str::trim)
}

pub fn parse_runtime_sort_condition<F>(
    sort_clause: Option<&str>,
    names: Option<&HashMap<String, String>>,
    values: &HashMap<String, AttributeValue>,
    expected_sort_key_name: Option<&str>,
    value_repr: F,
) -> StorageResult<Option<RuntimeSortCondition<String>>>
where
    F: Fn(&AttributeValue) -> StorageResult<String>,
{
    let Some(sort_clause) = sort_clause else {
        return Ok(None);
    };
    let Some(expected_sort_key_name) = expected_sort_key_name else {
        return Ok(None);
    };

    let clause = sort_clause.trim();
    let clause_upper = clause.to_ascii_uppercase();

    if clause_upper.starts_with("BEGINS_WITH(") && clause.ends_with(')') {
        let inner = clause
            .trim_start_matches("begins_with")
            .trim_start_matches("BEGINS_WITH")
            .trim()
            .trim_start_matches('(')
            .trim_end_matches(')');
        let Some((field, placeholder)) = inner.split_once(',') else {
            return Ok(None);
        };
        let field = resolve_attribute_name(field.trim(), names)?;
        if field != expected_sort_key_name {
            return Ok(None);
        }
        let placeholder = placeholder.trim();
        if !placeholder.starts_with(':') || !values.contains_key(placeholder) {
            return Ok(None);
        }
        return Ok(Some(RuntimeSortCondition::BeginsWith));
    }

    if let Some(between_pos) = clause_upper.find(" BETWEEN ") {
        let Some(field_token) = clause.get(..between_pos) else {
            return Ok(None);
        };
        let field = resolve_attribute_name(field_token.trim(), names)?;
        if field != expected_sort_key_name {
            return Ok(None);
        }
        let Some(bounds) = clause.get(between_pos + " BETWEEN ".len()..) else {
            return Ok(None);
        };
        let bounds = bounds.trim();
        let bounds_upper = bounds.to_ascii_uppercase();
        let Some(and_pos) = bounds_upper.find(" AND ") else {
            return Ok(None);
        };
        let Some(min_token) = bounds.get(..and_pos) else {
            return Ok(None);
        };
        let Some(max_token) = bounds.get(and_pos + " AND ".len()..) else {
            return Ok(None);
        };
        let min = placeholder_value_repr(min_token.trim(), values, &value_repr)?;
        let max = placeholder_value_repr(max_token.trim(), values, &value_repr)?;
        return Ok(Some(RuntimeSortCondition::Between { min, max }));
    }

    for operator in ["<=", ">=", "<", ">", "="] {
        if let Some(pos) = clause.find(operator) {
            let Some(field_token) = clause.get(..pos) else {
                return Ok(None);
            };
            let field = resolve_attribute_name(field_token.trim(), names)?;
            if field != expected_sort_key_name {
                return Ok(None);
            }
            let Some(value_token) = clause.get(pos + operator.len()..) else {
                return Ok(None);
            };
            let value = placeholder_value_repr(value_token.trim(), values, &value_repr)?;
            let condition = match operator {
                "<=" => RuntimeSortCondition::LessThanEqual { value },
                ">=" => RuntimeSortCondition::GreaterThanEqual { value },
                "<" => RuntimeSortCondition::LessThan,
                ">" => RuntimeSortCondition::GreaterThan,
                "=" => RuntimeSortCondition::Equal { value },
                _ => return Ok(None),
            };
            return Ok(Some(condition));
        }
    }

    Ok(None)
}

pub fn decode_query_start_key_sort_repr(
    table_info: &StoredTableInfo,
    index_name: Option<&str>,
    exclusive_start_key: Option<&str>,
) -> StorageResult<Option<String>> {
    let Some(exclusive_start_key) = exclusive_start_key else {
        return Ok(None);
    };
    let parsed_index_name = index_name.map(storage_types::IndexName::new);
    let item_key =
        ItemKey::item_key_from_next_page_token(exclusive_start_key, table_info, &parsed_index_name)
            .map_err(|err| {
                StorageError::validation(format!("invalid query proof start key: {err}"))
            })?;
    let Some(item_key) = item_key else {
        return Ok(None);
    };
    let Some(sort_key_name) = range_key_name(query_space_key_schema(table_info, index_name)?)
    else {
        return Ok(None);
    };
    let sort_key_type = key_attribute_type_for_name(table_info, sort_key_name)?;
    item_key
        .range_key()
        .map(|value| scalar_order_repr_for_type(&sort_key_type, value))
        .transpose()
}

pub fn next_page_token_for_query_entry(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    index_name: Option<&str>,
    primary_key: &KeyAttributes,
    query_space_key: &KeyAttributes,
) -> StorageResult<String> {
    let item_key = if let Some(index_name) = index_name {
        let index_name = storage_types::IndexName::new(index_name);
        let index_key_schema = query_space_key_schema(table_info, Some(index_name.as_ref()))?;
        let mut merged_item = query_space_key.to_attribute_map();
        merged_item.extend(primary_key.to_attribute_map());
        ItemKey::from_key_schema_for_index(
            table_name.clone(),
            &table_info.key_schema,
            &index_name,
            index_key_schema,
            &merged_item,
        )
        .map_err(|err| StorageError::internal(&format!("encode query page token item key: {err}")))?
        .ok_or_else(|| {
            StorageError::internal("encode query page token item key for GSI returned no key")
        })?
    } else {
        ItemKey::from_key_schema(table_name.clone(), &table_info.key_schema, primary_key).map_err(
            |err| StorageError::internal(&format!("encode query page token item key: {err}")),
        )?
    };
    item_key
        .next_page_token()
        .map_err(|err| StorageError::internal(&format!("encode query page token: {err}")))
}

fn placeholder_value_repr<F>(
    placeholder: &str,
    values: &HashMap<String, AttributeValue>,
    value_repr: F,
) -> StorageResult<String>
where
    F: Fn(&AttributeValue) -> StorageResult<String>,
{
    let Some(value) = values.get(placeholder) else {
        return Err(StorageError::validation(format!(
            "query proof cache could not resolve expression value '{placeholder}'",
        )));
    };
    value_repr(value)
}

fn first_key_clause(expression: &str) -> Option<&str> {
    let upper = expression.to_ascii_uppercase();
    if let Some(and_pos) = upper.find(" AND ") {
        return expression.get(..and_pos).map(str::trim);
    }
    Some(expression.trim())
}

fn resolve_attribute_name(
    token: &str,
    names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    let token = token.trim();
    if token.starts_with('#') {
        let names = names.ok_or_else(|| {
            StorageError::validation(format!(
                "query proof cache missing expression names for '{token}'",
            ))
        })?;
        return names.get(token).cloned().ok_or_else(|| {
            StorageError::validation(format!(
                "query proof cache expression name '{token}' not found",
            ))
        });
    }
    Ok(token.to_string())
}
