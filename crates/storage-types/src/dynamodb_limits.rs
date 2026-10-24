use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use crate::{
    AttributeValue, AttributeValueLookup, CreateGlobalSecondaryIndex, IndexName, KeyAttributes,
    KeySchemaElement, KeyType, LocalSecondaryIndex, Projection, ProjectionType, StorageEnum,
    StorageError, StorageValidationKind, TableKey,
    constants::{
        DYNAMODB_NUMBER_ZERO_PADDING, DYNAMODB_WRITE_NUMBER_BYTES, MAX_ATTRIBUTE_NAME_BYTES,
        MAX_ATTRIBUTE_NESTING_DEPTH, MAX_INDEX_NAME_BYTES, MAX_PARTITION_KEY_BYTES,
        MAX_PROJECTED_ATTRIBUTES, MAX_SORT_KEY_BYTES, MAX_TRANSACTION_REQUEST_BYTES,
        MIN_INDEX_NAME_BYTES, SMALL_SET_DUPLICATE_SCAN_LIMIT,
    },
    context::WrappedError as _,
    dynamodb_binary,
};

pub fn validate_index_name(index_name: &IndexName, field: &str) -> Result<(), String> {
    validate_index_name_lazy(index_name, || field.to_owned())
}

pub(crate) fn validate_index_name_lazy<F>(index_name: &IndexName, field: F) -> Result<(), String>
where F: FnOnce() -> String {
    let name = index_name.as_ref();
    let len = name.len();
    if !(MIN_INDEX_NAME_BYTES..=MAX_INDEX_NAME_BYTES).contains(&len) {
        return Err(format!("{} must be between 3 and 255 characters", field()));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(format!("{} contains invalid characters", field()));
    }
    Ok(())
}

pub(crate) fn validate_attribute_name(
    name: &str,
    field: impl FnOnce() -> String,
) -> Result<(), String> {
    let len = name.len();
    if len == 0 {
        return Err(format!("{} cannot be empty", field()));
    }
    if len > MAX_ATTRIBUTE_NAME_BYTES {
        return Err(format!(
            "{} cannot exceed {MAX_ATTRIBUTE_NAME_BYTES} bytes",
            field()
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ByteCounter {
    size: usize,
}

impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.size += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn validate_attribute_value_for_write(
    value: &AttributeValue,
    field: &str,
) -> Result<(), String> {
    validate_attribute_value(value, 0, field)
}

pub fn normalize_dynamodb_number_for_write(value: &str) -> Cow<'_, str> {
    if !number_needs_write_normalization(value) {
        return Cow::Borrowed(value);
    }
    if value.len() <= DYNAMODB_WRITE_NUMBER_BYTES
        && let Some(expanded) = expand_scientific_number(value)
    {
        return Cow::Owned(expanded);
    }
    crate::canonical_dynamodb_number(value).map_or(Cow::Borrowed(value), Cow::Owned)
}

fn number_needs_write_normalization(value: &str) -> bool {
    value.len() > DYNAMODB_WRITE_NUMBER_BYTES || value.contains(['e', 'E'])
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

/// Borrow an attribute map whose numbers already use the write form, and
/// normalize only when an owned copy is needed.
pub fn normalized_attribute_map_for_write(
    item: &HashMap<String, AttributeValue>,
) -> Cow<'_, HashMap<String, AttributeValue>> {
    if !attribute_map_numbers_need_write_normalization(item) {
        return Cow::Borrowed(item);
    }

    let mut normalized = item.clone();
    normalize_attribute_map_numbers_for_write(&mut normalized);
    Cow::Owned(normalized)
}

pub fn validate_transaction_request_size(
    value: &serde_json::Value,
    field: &str,
) -> Result<(), String> {
    let mut writer = ByteCounter::default();
    serde_json::to_writer(&mut writer, value)
        .map_err(|err| format!("Invalid request format: {err}"))?;
    let size = writer.size;
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

/// Point reads and Query report malformed numeric key operands without the
/// write-request validation prefix.
pub fn read_key_validation_error(error: StorageError) -> StorageError {
    numeric_key_validation_error(error, ": ")
}

/// BatchWriteItem returns malformed numeric delete keys without the ordinary
/// validation prefix or the point-read empty-number suffix.
pub fn batch_write_key_validation_error(error: StorageError) -> StorageError {
    numeric_key_validation_error(error, "")
}

fn numeric_key_validation_error(error: StorageError, empty_number_suffix: &str) -> StorageError {
    let StorageEnum::Validation { message, .. } = error.to_enum() else {
        return error;
    };
    match message.as_str() {
        "The parameter cannot be converted to a numeric value" => {
            StorageError::raw_validation(format!("{message}{empty_number_suffix}"))
        }
        "Attempting to store more than 38 significant digits in a Number"
        | "Number underflow. Attempting to store a number with magnitude smaller than supported \
         range"
        | "Number overflow. Attempting to store a number with magnitude larger than supported \
         range" => StorageError::raw_validation(message.clone()),
        _ => error,
    }
}

pub fn validate_item_key_attributes_for_schema(
    key_schema: &[KeySchemaElement],
    item: &impl AttributeValueLookup,
) -> Result<(), StorageError> {
    for element in key_schema {
        let value = item
            .get_attribute_value(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        validate_key_attribute_value_for_schema(element, value)?;
    }
    Ok(())
}

pub fn validate_key_attribute_value_for_schema(
    element: &KeySchemaElement,
    value: &AttributeValue,
) -> Result<(), StorageError> {
    validate_key_value_size(&element.key_type, &element.attribute_name, value)
}

pub(crate) fn validate_table_key(key: &TableKey) -> Result<(), StorageError> {
    validate_key_value_size(&KeyType::Hash, "partition key", &key.hash_key)?;
    if let Some(range_key) = key.range_key.as_ref() {
        validate_key_value_size(&KeyType::Range, "sort key", range_key)?;
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

pub(crate) fn validate_key_value_size(
    key_type: &KeyType,
    attribute_name: &str,
    value: &AttributeValue,
) -> Result<(), StorageError> {
    if let AttributeValue::N(value) = value {
        crate::dynamodb_number::validate_dynamodb_number(value)
            .map_err(|error| StorageError::validation(error.validation_kind()))?;
    }
    let size = key_value_size(value).map_err(StorageError::validation)?;
    if size == 0 {
        let (kind, value_type) = match value {
            AttributeValue::S(_) => (StorageValidationKind::EmptyStringKey, "string"),
            AttributeValue::B(_) => (StorageValidationKind::EmptyBinaryKey, "binary"),
            _ => {
                return Err(StorageError::validation(
                    "Key attributes must be scalar string, number, or binary values",
                ));
            }
        };
        return Err(StorageError::validation_detail(
            kind,
            format!(
                "One or more parameter values are not valid. The AttributeValue for a key \
                 attribute cannot contain an empty {value_type} value. Key: {}",
                attribute_name
            ),
        ));
    }
    let max = match key_type {
        KeyType::Hash => MAX_PARTITION_KEY_BYTES,
        KeyType::Range => MAX_SORT_KEY_BYTES,
    };
    if size > max {
        let message = match key_type {
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

pub(crate) fn validate_attribute_value(
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
        AttributeValue::B(value) => validate_binary(value)?,
        AttributeValue::BS(values) => validate_binary_set(values)?,
        AttributeValue::L(values) => {
            for value in values {
                validate_attribute_value(value, depth + 1, field)?;
            }
        }
        AttributeValue::M(values) => {
            for (name, value) in values {
                validate_attribute_name(name, || format!("{field} nested attribute name"))?;
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
    if has_duplicate_members(values) {
        return Err(format!(
            "One or more parameter values were invalid: Input collection {} contains duplicates.",
            format_quoted_collection(values, ", ")
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
        let key = crate::canonical_dynamodb_number(value).map_err(number_validation_error)?;
        if !seen.insert(key) {
            return Err(
                "One or more parameter values were invalid: Input collection contains duplicates."
                    .to_string(),
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_number(value: &str) -> Result<(), String> {
    crate::dynamodb_number::validate_dynamodb_number(value).map_err(number_validation_error)
}

fn number_validation_error(error: crate::dynamodb_number::DynamodbNumberError) -> String {
    error.validation_kind().message().to_string()
}

pub(crate) fn normalize_attribute_value_numbers_for_write(value: &mut AttributeValue) -> bool {
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
        AttributeValue::L(values) => values.iter_mut().fold(false, |changed, value| {
            normalize_attribute_value_numbers_for_write(value) | changed
        }),
        AttributeValue::M(values) => normalize_attribute_map_numbers_for_write(values),
        _ => false,
    }
}

fn attribute_value_numbers_need_write_normalization(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::N(number) => number_needs_write_normalization(number),
        AttributeValue::NS(values) => values
            .iter()
            .any(|number| number_needs_write_normalization(number)),
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
    let (integer, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digit_count = integer.len() + fractional.len();
    if digit_count == 0
        || !integer
            .bytes()
            .chain(fractional.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let point = integer.len() as i64 + i64::from(exponent);
    let prefix_zeroes = point.min(0).unsigned_abs() as usize;
    let suffix_zeroes = (point - digit_count as i64).max(0) as usize;
    let decimal_bytes = usize::from(point < digit_count as i64) + usize::from(point <= 0);
    let sign = negative && !(digit_count == 1 && point == 1 && mantissa.contains('0'));
    let output_bytes =
        digit_count + prefix_zeroes + suffix_zeroes + decimal_bytes + usize::from(sign);
    if output_bytes > DYNAMODB_WRITE_NUMBER_BYTES {
        return None;
    }
    let mut output = String::with_capacity(output_bytes);
    if sign {
        output.push('-');
    }
    if point <= 0 {
        output.push_str("0.");
        append_zero_padding(&mut output, prefix_zeroes);
    }
    append_decimal_digits(&mut output, integer, fractional, point);
    append_zero_padding(&mut output, suffix_zeroes);
    Some(output)
}

fn append_decimal_digits(output: &mut String, integer: &str, fractional: &str, point: i64) {
    for (index, byte) in integer.bytes().chain(fractional.bytes()).enumerate() {
        if index > 0 && index as i64 == point {
            output.push('.');
        }
        output.push(char::from(byte));
    }
}

fn append_zero_padding(output: &mut String, zeroes: usize) {
    for _ in 0..zeroes / DYNAMODB_NUMBER_ZERO_PADDING.len() {
        output.push_str(DYNAMODB_NUMBER_ZERO_PADDING);
    }
    // The ASCII padding and bounded remainder always form a valid string range.
    output.extend(
        DYNAMODB_NUMBER_ZERO_PADDING
            .chars()
            .take(zeroes % DYNAMODB_NUMBER_ZERO_PADDING.len()),
    );
}

fn validate_binary_set(values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(
            "One or more parameter values were invalid: Binary sets should not be empty"
                .to_string(),
        );
    }
    if has_duplicate_members(values) {
        return Err(format!(
            "One or more parameter values were invalid: Input collection {}of type BS contains \
             duplicates.",
            format_quoted_collection(values, ",")
        ));
    }
    for value in values {
        validate_binary(value)?;
    }
    Ok(())
}

fn validate_binary(value: &str) -> Result<(), String> {
    dynamodb_binary::decode_base64_string(value)
        .map(|_| ())
        .map_err(|err| format!("invalid base64 for DynamoDB binary field: {err}"))
}

fn has_duplicate_members(values: &[String]) -> bool {
    let prefix = &values[..values.len().min(SMALL_SET_DUPLICATE_SCAN_LIMIT)];
    let has_duplicate = prefix
        .iter()
        .enumerate()
        .any(|(index, value)| prefix[..index].contains(value));
    if has_duplicate || values.len() == prefix.len() {
        return has_duplicate;
    }
    let mut seen = HashSet::with_capacity(values.len());
    values.iter().any(|value| !seen.insert(value.as_str()))
}

fn format_quoted_collection(values: &[String], separator: &str) -> String {
    let mut output = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(separator);
        }
        output.push('"');
        output.push_str(value);
        output.push('"');
    }
    output.push(']');
    output
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
