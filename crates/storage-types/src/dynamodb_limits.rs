use std::collections::{HashMap, HashSet};

use crate::{
    AttributeValue, CreateGlobalSecondaryIndex, IndexName, KeyAttributes, KeySchemaElement,
    KeyType, LocalSecondaryIndex, Projection, ProjectionType, StorageError, dynamodb_binary,
};

pub const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;
pub const MAX_PARTITION_KEY_BYTES: usize = 2_048;
pub const MAX_SORT_KEY_BYTES: usize = 1_024;
pub const MAX_ATTRIBUTE_NAME_BYTES: usize = 65_535;
pub const MAX_ATTRIBUTE_NESTING_DEPTH: usize = 32;
pub const MAX_PROJECTED_ATTRIBUTES: usize = 100;
pub const MIN_INDEX_NAME_BYTES: usize = 3;
pub const MAX_INDEX_NAME_BYTES: usize = 255;
pub const MAX_EXPRESSION_BYTES: usize = 4 * 1024;
pub const MAX_TRANSACTION_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_QUERY_SCAN_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_LIST_TABLES_LIMIT: u32 = 100;

pub fn validate_index_name(index_name: &IndexName, field: &str) -> Result<(), String> {
    let name = index_name.as_ref();
    let len = name.len();
    if !(MIN_INDEX_NAME_BYTES..=MAX_INDEX_NAME_BYTES).contains(&len) {
        return Err(format!("{field} must be between 3 and 255 characters"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(format!("{field} contains invalid characters"));
    }
    Ok(())
}

pub fn validate_attribute_name(name: &str, field: &str) -> Result<(), String> {
    let len = name.len();
    if len == 0 {
        return Err(format!("{field} cannot be empty"));
    }
    if len > MAX_ATTRIBUTE_NAME_BYTES {
        return Err(format!(
            "{field} cannot exceed {MAX_ATTRIBUTE_NAME_BYTES} bytes"
        ));
    }
    Ok(())
}

pub fn validate_expression_size(expression: Option<&str>, field: &str) -> Result<(), String> {
    if let Some(expression) = expression
        && expression.len() > MAX_EXPRESSION_BYTES
    {
        return Err(format!(
            "{field} cannot exceed {MAX_EXPRESSION_BYTES} bytes"
        ));
    }
    Ok(())
}

pub fn validate_item(item: &HashMap<String, AttributeValue>, field: &str) -> Result<(), String> {
    let mut size = 0;
    for (name, value) in item {
        validate_attribute_name(name, &format!("{field} attribute name"))?;
        validate_attribute_value(value, 0, field)?;
        size += name.len() + attribute_value_size(value);
    }
    if size > MAX_ITEM_SIZE_BYTES {
        return Err("Item size has exceeded the maximum allowed size".to_string());
    }
    Ok(())
}

pub fn validate_attribute_value_for_write(
    value: &AttributeValue,
    field: &str,
) -> Result<(), String> {
    validate_attribute_value(value, 0, field)
}

pub fn validate_transaction_request_size(
    value: &serde_json::Value,
    field: &str,
) -> Result<(), String> {
    let size = serde_json::to_vec(value)
        .map_err(|err| format!("Invalid request format: {err}"))?
        .len();
    if size > MAX_TRANSACTION_REQUEST_BYTES {
        return Err(format!(
            "{field} cannot exceed {MAX_TRANSACTION_REQUEST_BYTES} bytes"
        ));
    }
    Ok(())
}

pub fn validate_key_attributes_for_schema(
    key_schema: &[KeySchemaElement],
    key: &KeyAttributes,
) -> Result<(), StorageError> {
    for element in key_schema {
        let value = key
            .get(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        validate_key_value_size(element, value)?;
    }
    Ok(())
}

pub fn validate_item_key_attributes_for_schema(
    key_schema: &[KeySchemaElement],
    item: &HashMap<String, AttributeValue>,
) -> Result<(), StorageError> {
    for element in key_schema {
        let value = item
            .get(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        validate_key_value_size(element, value)?;
    }
    Ok(())
}

pub fn validate_projected_attribute_limit(
    gsis: Option<&[CreateGlobalSecondaryIndex]>,
    lsis: Option<&[LocalSecondaryIndex]>,
) -> Result<(), String> {
    let gsi_count = gsis
        .into_iter()
        .flatten()
        .map(|index| projected_attribute_count(&index.projection))
        .sum::<usize>();
    let lsi_count = lsis
        .into_iter()
        .flatten()
        .map(|index| projected_attribute_count(&index.projection))
        .sum::<usize>();
    let total = gsi_count + lsi_count;
    if total > MAX_PROJECTED_ATTRIBUTES {
        return Err(format!(
            "Projected attributes across all indexes cannot exceed {MAX_PROJECTED_ATTRIBUTES}"
        ));
    }
    Ok(())
}

fn validate_key_value_size(
    element: &KeySchemaElement,
    value: &AttributeValue,
) -> Result<(), StorageError> {
    let size = key_value_size(value).map_err(StorageError::validation)?;
    if size == 0 {
        let value_type = match value {
            AttributeValue::S(_) => "string",
            AttributeValue::B(_) => "binary",
            _ => {
                return Err(StorageError::validation(
                    "Key attributes must be scalar string, number, or binary values",
                ));
            }
        };
        return Err(StorageError::validation(format!(
            "One or more parameter values are not valid. The AttributeValue for a key attribute \
             cannot contain an empty {value_type} value. Key: {}",
            element.attribute_name
        )));
    }
    let max = match element.key_type {
        KeyType::Hash => MAX_PARTITION_KEY_BYTES,
        KeyType::Range => MAX_SORT_KEY_BYTES,
    };
    if size > max {
        let message = match element.key_type {
            KeyType::Hash => {
                format!(
                    "One or more parameter values were invalid: Size of hashkey has exceeded the \
                     maximum size limit of{MAX_PARTITION_KEY_BYTES} bytes"
                )
            }
            KeyType::Range => {
                format!(
                    "One or more parameter values were invalid: Aggregated size of all range keys \
                     has exceeded the size limit of {MAX_SORT_KEY_BYTES} bytes"
                )
            }
        };
        return Err(StorageError::validation(message));
    }
    Ok(())
}

fn key_value_size(value: &AttributeValue) -> Result<usize, String> {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => Ok(value.len()),
        AttributeValue::B(value) => dynamodb_binary::decode_base64_string(value)
            .map(|bytes| bytes.len())
            .map_err(|err| format!("invalid base64 for DynamoDB binary field: {err}")),
        _ => Err("Key attributes must be scalar string, number, or binary values".to_string()),
    }
}

fn validate_attribute_value(
    value: &AttributeValue,
    depth: usize,
    field: &str,
) -> Result<(), String> {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return Err(format!(
            "{field} attribute nesting depth cannot exceed {MAX_ATTRIBUTE_NESTING_DEPTH}"
        ));
    }
    match value {
        AttributeValue::SS(values) => validate_string_set(values)?,
        AttributeValue::NS(values) => validate_number_set(values)?,
        AttributeValue::BS(values) => validate_binary_set(values)?,
        AttributeValue::L(values) => {
            for value in values {
                validate_attribute_value(value, depth + 1, field)?;
            }
        }
        AttributeValue::M(values) => {
            for (name, value) in values {
                validate_attribute_name(name, &format!("{field} nested attribute name"))?;
                validate_attribute_value(value, depth + 1, field)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_string_set(values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(
            "One or more parameter values were invalid: An string set  may not be empty"
                .to_string(),
        );
    }
    if contains_duplicate(values.iter().map(String::as_str)) {
        return Err(format!(
            "One or more parameter values were invalid: Input collection {} contains duplicates.",
            quoted_collection(values)
        ));
    }
    Ok(())
}

fn validate_number_set(values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(
            "One or more parameter values were invalid: An number set  may not be empty"
                .to_string(),
        );
    }
    let mut seen = HashSet::with_capacity(values.len());
    for value in values {
        let key = canonical_number_set_member(value);
        if !seen.insert(key) {
            return Err(
                "One or more parameter values were invalid: Input collection contains duplicates."
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn validate_binary_set(values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(
            "One or more parameter values were invalid: Binary sets should not be empty"
                .to_string(),
        );
    }
    if contains_duplicate(values.iter().map(String::as_str)) {
        return Err(format!(
            "One or more parameter values were invalid: Input collection {}of type BS contains \
             duplicates.",
            compact_quoted_collection(values)
        ));
    }
    Ok(())
}

fn contains_duplicate<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    values.into_iter().any(|value| !seen.insert(value))
}

fn quoted_collection(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn compact_quoted_collection(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn canonical_number_set_member(value: &str) -> String {
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or((value, 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let mut chars = mantissa.chars();
    let negative = matches!(chars.clone().next(), Some('-'));
    if matches!(chars.clone().next(), Some('-' | '+')) {
        let _ = chars.next();
    }

    let mut digits = Vec::new();
    let mut fractional_digits = 0i32;
    let mut after_decimal = false;
    for ch in chars {
        match ch {
            '0'..='9' => {
                digits.push(ch);
                if after_decimal {
                    fractional_digits += 1;
                }
            }
            '.' if !after_decimal => after_decimal = true,
            _ => {}
        }
    }

    while matches!(digits.first(), Some('0')) {
        digits.remove(0);
    }
    if digits.is_empty() {
        return "0".to_string();
    }

    let mut scale = fractional_digits - exponent;
    while scale < 0 {
        digits.push('0');
        scale += 1;
    }
    while scale > 0 && matches!(digits.last(), Some('0')) {
        let _ = digits.pop();
        scale -= 1;
    }
    if digits.is_empty() {
        return "0".to_string();
    }

    format!(
        "{}{}:{scale}",
        if negative { "-" } else { "" },
        digits.into_iter().collect::<String>()
    )
}

fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(value) => value.len(),
        AttributeValue::N(value) => dynamodb_number_size(value),
        AttributeValue::B(value) => dynamodb_binary_size(value),
        AttributeValue::SS(values) => values.iter().map(String::len).sum(),
        AttributeValue::NS(values) => values.iter().map(|value| dynamodb_number_size(value)).sum(),
        AttributeValue::BS(values) => values.iter().map(|value| dynamodb_binary_size(value)).sum(),
        AttributeValue::BOOL(_) | AttributeValue::NULL(_) => 1,
        AttributeValue::L(values) => {
            3 + values.len() + values.iter().map(attribute_value_size).sum::<usize>()
        }
        AttributeValue::M(values) => {
            3 + values.len()
                + values
                    .iter()
                    .map(|(name, value)| name.len() + attribute_value_size(value))
                    .sum::<usize>()
        }
    }
}

fn dynamodb_binary_size(value: &str) -> usize {
    dynamodb_binary::decode_base64_string(value)
        .map(|bytes| bytes.len())
        .unwrap_or(value.len())
}

pub(crate) fn dynamodb_number_size(value: &str) -> usize {
    let mantissa = value.split_once(['e', 'E']).map_or(value, |(head, _)| head);
    let is_negative = mantissa.starts_with('-');
    let mut significant_digits = 0usize;
    let mut trailing_zero_digits = 0usize;
    let mut seen_non_zero_digit = false;

    for byte in mantissa.trim_start_matches(['+', '-']).bytes() {
        match byte {
            b'0' if seen_non_zero_digit => {
                significant_digits += 1;
                trailing_zero_digits += 1;
            }
            b'0' => {}
            b'1'..=b'9' => {
                seen_non_zero_digit = true;
                significant_digits += 1;
                trailing_zero_digits = 0;
            }
            _ => {}
        }
    }

    let significant_digits = if seen_non_zero_digit {
        significant_digits - trailing_zero_digits
    } else {
        1
    };
    significant_digits.div_ceil(2) + 1 + usize::from(is_negative)
}

fn projected_attribute_count(projection: &Projection) -> usize {
    if !matches!(projection.projection_type, Some(ProjectionType::Include)) {
        return 0;
    }
    projection.non_key_attributes.as_ref().map_or(0, Vec::len)
}
