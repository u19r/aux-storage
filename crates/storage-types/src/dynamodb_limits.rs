use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

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

pub fn normalize_dynamodb_number_for_write(value: &str) -> Cow<'_, str> {
    expand_scientific_number(value).map_or(Cow::Borrowed(value), Cow::Owned)
}

pub fn normalize_attribute_map_numbers_for_write(
    item: &mut HashMap<String, AttributeValue>,
) -> bool {
    let mut changed = false;
    for value in item.values_mut() {
        changed |= normalize_attribute_value_numbers_for_write(value);
    }
    changed
}

pub fn attribute_map_numbers_need_write_normalization(
    item: &HashMap<String, AttributeValue>,
) -> bool {
    item.values()
        .any(attribute_value_numbers_need_write_normalization)
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
        validate_key_attribute_value_for_schema(element, value)?;
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
        validate_key_attribute_value_for_schema(element, value)?;
    }
    Ok(())
}

pub fn validate_key_attribute_value_for_schema(
    element: &KeySchemaElement,
    value: &AttributeValue,
) -> Result<(), StorageError> {
    validate_key_value_size(element, value)
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
    if let AttributeValue::N(value) = value {
        validate_number(value).map_err(StorageError::validation)?;
    }
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
        AttributeValue::N(value) => validate_number(value)?,
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
        validate_number(value)?;
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

fn validate_number(value: &str) -> Result<(), String> {
    let parsed = parse_dynamodb_number(value)?;
    if parsed.significant_digits > 38 {
        return Err("Attempting to store more than 38 significant digits in a Number".to_string());
    }
    if !parsed.is_zero && parsed.adjusted_exponent < -130 {
        return Err(
            "Number underflow. Attempting to store a number with magnitude smaller than supported \
             range"
                .to_string(),
        );
    }
    Ok(())
}

fn normalize_attribute_value_numbers_for_write(value: &mut AttributeValue) -> bool {
    match value {
        AttributeValue::N(number) => match normalize_dynamodb_number_for_write(number) {
            Cow::Borrowed(_) => false,
            Cow::Owned(normalized) => {
                *number = normalized;
                true
            }
        },
        AttributeValue::NS(values) => {
            let mut changed = false;
            for number in values {
                if let Cow::Owned(normalized) = normalize_dynamodb_number_for_write(number) {
                    *number = normalized;
                    changed = true;
                }
            }
            changed
        }
        AttributeValue::L(values) => values
            .iter_mut()
            .any(normalize_attribute_value_numbers_for_write),
        AttributeValue::M(values) => normalize_attribute_map_numbers_for_write(values),
        _ => false,
    }
}

fn attribute_value_numbers_need_write_normalization(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::N(number) => number.contains(['e', 'E']),
        AttributeValue::NS(values) => values.iter().any(|number| number.contains(['e', 'E'])),
        AttributeValue::L(values) => values
            .iter()
            .any(attribute_value_numbers_need_write_normalization),
        AttributeValue::M(values) => attribute_map_numbers_need_write_normalization(values),
        _ => false,
    }
}

fn expand_scientific_number(value: &str) -> Option<String> {
    let (mantissa, exponent) = value.split_once(['e', 'E'])?;
    let exponent = exponent.parse::<i32>().ok()?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches(['+', '-']);
    let mut digits = String::new();
    let mut fractional_digits = 0i32;
    let mut after_decimal = false;
    for character in mantissa.chars() {
        match character {
            '0'..='9' => {
                digits.push(character);
                if after_decimal {
                    fractional_digits += 1;
                }
            }
            '.' if !after_decimal => after_decimal = true,
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }

    let decimal_position = digits.len() as i32 - fractional_digits + exponent;
    let expanded = if decimal_position <= 0 {
        format!(
            "0.{}{}",
            "0".repeat(decimal_position.unsigned_abs() as usize),
            digits
        )
    } else if decimal_position as usize >= digits.len() {
        format!(
            "{}{}",
            digits,
            "0".repeat(decimal_position as usize - digits.len())
        )
    } else {
        let split = decimal_position as usize;
        let (integer, fractional) = digits.split_at(split);
        format!("{integer}.{fractional}")
    };

    if negative && expanded != "0" {
        Some(format!("-{expanded}"))
    } else {
        Some(expanded)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedDynamoNumber {
    significant_digits: usize,
    adjusted_exponent: i32,
    is_zero: bool,
}

fn parse_dynamodb_number(value: &str) -> Result<ParsedDynamoNumber, String> {
    if value.is_empty() {
        return Err("The parameter cannot be converted to a numeric value".to_string());
    }

    let bytes = value.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let mut integer_digits = 0usize;
    let mut fractional_digits = 0usize;
    let mut significant_digits = 0usize;
    let mut first_significant_position = None;
    let mut last_significant_position = None;
    let mut saw_digit = false;

    while let Some(byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                let digit = byte - b'0';
                if digit != 0 || first_significant_position.is_some() {
                    first_significant_position.get_or_insert(integer_digits);
                    last_significant_position = Some(integer_digits);
                    significant_digits += 1;
                }
                integer_digits += 1;
                index += 1;
            }
            b'.' => {
                index += 1;
                break;
            }
            _ => break,
        }
    }

    while let Some(byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                let digit = byte - b'0';
                if digit != 0 || first_significant_position.is_some() {
                    first_significant_position.get_or_insert(integer_digits);
                    last_significant_position = Some(integer_digits);
                    significant_digits += 1;
                }
                integer_digits += 1;
                fractional_digits += 1;
                index += 1;
            }
            _ => break,
        }
    }

    if !saw_digit {
        return Err("The parameter cannot be converted to a numeric value".to_string());
    }

    let mut exponent = 0i32;
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        let exponent_sign = match bytes.get(index) {
            Some(b'+') => {
                index += 1;
                1
            }
            Some(b'-') => {
                index += 1;
                -1
            }
            _ => 1,
        };
        let exponent_start = index;
        while matches!(bytes.get(index), Some(b'0'..=b'9')) {
            index += 1;
        }
        if exponent_start == index {
            return Err("The parameter cannot be converted to a numeric value".to_string());
        }
        exponent = value
            .get(exponent_start..index)
            .ok_or_else(|| "The parameter cannot be converted to a numeric value".to_string())?
            .parse::<i32>()
            .map_err(|_| "The parameter cannot be converted to a numeric value".to_string())?
            * exponent_sign;
    }

    if index != bytes.len() {
        return Err("The parameter cannot be converted to a numeric value".to_string());
    }

    let Some(last_significant_position) = last_significant_position else {
        return Ok(ParsedDynamoNumber {
            significant_digits: 1,
            adjusted_exponent: 0,
            is_zero: true,
        });
    };

    significant_digits -= trailing_zero_count(value, last_significant_position);
    let adjusted_exponent = last_significant_position as i32 - fractional_digits as i32 + exponent;
    Ok(ParsedDynamoNumber {
        significant_digits,
        adjusted_exponent,
        is_zero: false,
    })
}

fn trailing_zero_count(value: &str, last_significant_position: usize) -> usize {
    let mut position = 0usize;
    let mut trailing_zeroes = 0usize;
    for byte in value.bytes() {
        match byte {
            b'0' if position <= last_significant_position => {
                trailing_zeroes += 1;
                position += 1;
            }
            b'1'..=b'9' if position <= last_significant_position => {
                trailing_zeroes = 0;
                position += 1;
            }
            b'.' | b'+' | b'-' => {}
            _ => break,
        }
    }
    trailing_zeroes
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
