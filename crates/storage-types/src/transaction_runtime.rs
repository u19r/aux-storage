use std::{
    collections::{HashMap, HashSet},
    fmt::Write as _,
};

use crate::{
    AttributeValue, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, KeyAttributeType, KeyAttributes,
    StorageEnum, StorageError, StorageResult, StoredTableInfo, TableName, TransactGetItem,
    TransactWriteItem, context::WrappedError as _, normalize_dynamodb_number_for_write,
    validate_item_key_attributes_for_schema, validate_key_attributes_for_schema,
};

#[derive(Debug, Clone, Default)]
pub struct TransactionKeyPreflight {
    pub validation_reason: Option<String>,
    pub key_fingerprint: Option<String>,
}

pub fn transaction_canceled_for_item_error(index: usize, error: StorageError) -> StorageError {
    transaction_canceled_for_item_error_with_len(index, index + 1, error)
}

pub fn transaction_canceled_for_item_error_with_len(
    index: usize,
    total_len: usize,
    error: StorageError,
) -> StorageError {
    if matches!(error.to_enum(), StorageEnum::TransactionCanceled { .. }) {
        return pad_transaction_canceled_reasons(error, total_len);
    }

    let Some(reason) = transaction_cancellation_reason(&error) else {
        return error;
    };

    transaction_canceled_for_reason_with_len(index, total_len, reason)
}

pub fn transaction_canceled_for_reason(index: usize, reason: String) -> StorageError {
    transaction_canceled_for_reason_with_len(index, index + 1, reason)
}

pub fn transaction_canceled_for_reason_with_len(
    index: usize,
    total_len: usize,
    reason: String,
) -> StorageError {
    let len = total_len.max(index + 1);
    let mut reasons = vec!["None".to_string(); len];
    reasons[index] = reason;
    StorageEnum::TransactionCanceled { reasons }.into()
}

fn pad_transaction_canceled_reasons(error: StorageError, total_len: usize) -> StorageError {
    let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
        return error;
    };
    if reasons.len() >= total_len {
        return StorageEnum::TransactionCanceled {
            reasons: reasons.clone(),
        }
        .into();
    }
    let mut reasons = reasons.clone();
    reasons.resize(total_len, "None".to_string());
    StorageEnum::TransactionCanceled { reasons }.into()
}

pub fn transaction_canceled_for_indexed_reasons(
    reasons: Vec<Option<String>>,
) -> Option<StorageError> {
    reasons.iter().any(Option::is_some).then(|| {
        StorageEnum::TransactionCanceled {
            reasons: reasons
                .into_iter()
                .map(|reason| reason.unwrap_or_else(|| "None".to_string()))
                .collect(),
        }
        .into()
    })
}

pub fn transaction_canceled_for_preflights(
    preflights: &[TransactionKeyPreflight],
) -> Option<StorageError> {
    let first_index = preflights
        .iter()
        .position(|preflight| preflight.validation_reason.is_some())?;
    let mut reasons = vec!["None".to_string(); preflights.len()];
    for (index, preflight) in preflights.iter().enumerate().skip(first_index) {
        if let Some(reason) = preflight.validation_reason.as_ref() {
            reasons[index] = reason.clone();
        }
    }
    Some(StorageEnum::TransactionCanceled { reasons }.into())
}

pub fn transaction_cancellation_reason_at(error: &StorageError, index: usize) -> Option<String> {
    match error.to_enum() {
        StorageEnum::TransactionCanceled { reasons } => reasons
            .get(index)
            .filter(|reason| reason.as_str() != "None")
            .cloned(),
        StorageEnum::ConditionalCheckFailed => Some("ConditionalCheckFailed".to_string()),
        StorageEnum::Validation { message } => Some(format!("ValidationError\t{message}")),
        _ => None,
    }
}

pub fn transaction_validation_reason(error: &StorageError) -> Option<String> {
    match error.to_enum() {
        StorageEnum::Validation { message } => Some(format!("ValidationError\t{message}")),
        _ => None,
    }
}

pub fn transaction_key_preflight_from_key_result(
    result: StorageResult<String>,
) -> StorageResult<TransactionKeyPreflight> {
    match result {
        Ok(key_fingerprint) => Ok(TransactionKeyPreflight {
            validation_reason: None,
            key_fingerprint: (!key_fingerprint.is_empty()).then_some(key_fingerprint),
        }),
        Err(error) => transaction_validation_reason(&error)
            .map(|validation_reason| TransactionKeyPreflight {
                validation_reason: Some(validation_reason),
                key_fingerprint: None,
            })
            .ok_or(error),
    }
}

#[must_use]
pub fn transact_item_table_name(item: &TransactWriteItem) -> Option<&TableName> {
    match item {
        TransactWriteItem { put: Some(put), .. } => Some(&put.table_name),
        TransactWriteItem {
            delete: Some(delete),
            ..
        } => Some(&delete.table_name),
        TransactWriteItem {
            update: Some(update),
            ..
        } => Some(&update.table_name),
        TransactWriteItem {
            condition_check: Some(check),
            ..
        } => Some(&check.table_name),
        _ => None,
    }
}

pub fn preflight_transact_item_key_with_table_info(
    item: &TransactWriteItem,
    table_info: &StoredTableInfo,
) -> StorageResult<TransactionKeyPreflight> {
    match item {
        TransactWriteItem { put: Some(put), .. } => {
            preflight_transact_put_item_key_with_table_info(table_info, &put.item)
        }
        TransactWriteItem {
            delete: Some(delete),
            ..
        } => preflight_transact_write_key_with_table_info(table_info, &delete.key),
        TransactWriteItem {
            update: Some(update),
            ..
        } => preflight_transact_write_key_with_table_info(table_info, &update.key),
        TransactWriteItem {
            condition_check: Some(check),
            ..
        } => preflight_transact_write_key_with_table_info(table_info, &check.key),
        _ => Ok(TransactionKeyPreflight::default()),
    }
}

pub fn preflight_transact_put_item_key_with_table_info(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<TransactionKeyPreflight> {
    preflight_transact_write_key_result(
        validate_transact_put_item_key(table_info, item)
            .and_then(|()| transact_put_item_key_fingerprint(table_info, item)),
    )
}

pub fn preflight_transact_write_key_with_table_info(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
) -> StorageResult<TransactionKeyPreflight> {
    preflight_transact_write_key_result(
        validate_transact_key(table_info, key)
            .and_then(|()| transact_key_fingerprint(table_info, key)),
    )
}

fn preflight_transact_write_key_result(
    result: StorageResult<String>,
) -> StorageResult<TransactionKeyPreflight> {
    match result {
        Err(error) if transact_write_key_error_is_top_level(&error) => {
            Err(raw_validation_from_error(error))
        }
        result => transaction_key_preflight_from_key_result(result),
    }
}

pub fn preflight_transact_get_item_key_with_table_info(
    item: &TransactGetItem,
    table_info: &StoredTableInfo,
) -> StorageResult<TransactionKeyPreflight> {
    if let Err(error) = validate_transact_key_shape(table_info, &item.get.key) {
        return transaction_key_preflight_from_key_result(Err(error));
    }
    if let Err(error) = validate_key_attributes_for_schema(&table_info.key_schema, &item.get.key) {
        if transact_get_key_size_error_is_top_level(&error) {
            return Err(error);
        }
        return transaction_key_preflight_from_key_result(Err(error));
    }
    transaction_key_preflight_from_key_result(transact_key_fingerprint(table_info, &item.get.key))
}

fn transact_get_key_size_error_is_top_level(error: &StorageError) -> bool {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return false;
    };
    message.contains("key attribute cannot contain an empty string value")
        || message.contains("key attribute cannot contain an empty binary value")
}

fn transact_write_key_error_is_top_level(error: &StorageError) -> bool {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return false;
    };
    message.contains("key attribute cannot contain an empty string value")
        || message.contains("key attribute cannot contain an empty binary value")
        || message.contains("The parameter cannot be converted to a numeric value")
        || message.contains("Attempting to store more than 38 significant digits in a Number")
        || message.contains(
            "Number underflow. Attempting to store a number with magnitude smaller than supported \
             range",
        )
}

fn raw_validation_from_error(error: StorageError) -> StorageError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return error;
    };
    StorageError::raw_validation(message.clone())
}

pub fn validate_no_duplicate_transact_item_keys(
    preflights: &[TransactionKeyPreflight],
) -> StorageResult<()> {
    let mut seen = HashSet::with_capacity(preflights.len());
    for preflight in preflights {
        let Some(fingerprint) = preflight.key_fingerprint.as_deref() else {
            continue;
        };
        if !seen.insert(fingerprint) {
            return Err(StorageError::validation(
                "Transaction request cannot include multiple operations on one item",
            ));
        }
    }
    Ok(())
}

pub fn conditional_check_failed_reason(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<String> {
    if let Some(old_item) = old_item {
        return Ok(format!(
            "ConditionalCheckFailed\t{}\t{}",
            DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
            serde_json::to_string(old_item)?
        ));
    }
    Ok("ConditionalCheckFailed".to_string())
}

#[must_use]
pub fn return_values_on_condition_check_failure_all_old(value: Option<&String>) -> bool {
    value.is_some_and(|value| value == "ALL_OLD")
}

pub fn validate_transact_put_item_key(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<()> {
    for key in &table_info.key_schema {
        let value = item.get(&key.attribute_name).ok_or_else(|| {
            StorageError::validation(format!(
                "One or more parameter values were invalid: Missing the key {} in the item",
                key.attribute_name
            ))
        })?;
        let expected = key_attribute_type(table_info, &key.attribute_name);
        if !attribute_value_matches_key_type(value, &expected) {
            return Err(StorageError::validation(format!(
                "One or more parameter values were invalid: Type mismatch for key {} expected: {} \
                 actual: {}",
                key.attribute_name,
                key_type_name(&expected),
                attribute_value_type_name(value)
            )));
        }
    }
    validate_item_key_attributes_for_schema(&table_info.key_schema, item)?;
    Ok(())
}

pub fn transact_put_item_key_fingerprint(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    transact_key_fingerprint_from_values(table_info, |name| item.get(name))
}

pub fn validate_transact_key(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
) -> StorageResult<()> {
    validate_transact_key_shape(table_info, key)?;
    validate_key_attributes_for_schema(&table_info.key_schema, key)?;
    Ok(())
}

fn validate_transact_key_shape(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
) -> StorageResult<()> {
    for key_schema in &table_info.key_schema {
        let Some(value) = key.get(&key_schema.attribute_name) else {
            return Err(transact_key_schema_error());
        };
        let expected = key_attribute_type(table_info, &key_schema.attribute_name);
        if !attribute_value_matches_key_type(value, &expected) {
            return Err(transact_key_schema_error());
        }
    }
    Ok(())
}

pub fn transact_key_fingerprint(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
) -> StorageResult<String> {
    transact_key_fingerprint_from_values(table_info, |name| key.get(name))
}

fn transact_key_fingerprint_from_values<'a>(
    table_info: &StoredTableInfo,
    mut value_for: impl FnMut(&str) -> Option<&'a AttributeValue>,
) -> StorageResult<String> {
    let mut fingerprint = String::with_capacity(128);
    append_fingerprint_part(
        &mut fingerprint,
        "T",
        table_info.table_name.dynamodb_resource_name(),
    );
    for key_schema in &table_info.key_schema {
        let Some(value) = value_for(&key_schema.attribute_name) else {
            continue;
        };
        append_fingerprint_part(&mut fingerprint, "K", &key_schema.attribute_name);
        append_attribute_value_fingerprint(&mut fingerprint, value);
    }
    Ok(fingerprint)
}

fn append_attribute_value_fingerprint(fingerprint: &mut String, value: &AttributeValue) {
    match value {
        AttributeValue::S(value) => append_fingerprint_part(fingerprint, "S", value),
        AttributeValue::N(value) => {
            append_fingerprint_part(fingerprint, "N", normalize_dynamodb_number_for_write(value));
        }
        AttributeValue::B(value) => append_fingerprint_part(fingerprint, "B", value),
        _ => {}
    }
}

fn append_fingerprint_part(fingerprint: &mut String, tag: &str, value: impl AsRef<str>) {
    let value = value.as_ref();
    let _ = write!(fingerprint, "{tag}:{}:", value.len());
    fingerprint.push_str(value);
    fingerprint.push(';');
}

fn transaction_cancellation_reason(error: &StorageError) -> Option<String> {
    transaction_cancellation_reason_at(error, 0)
}

fn transact_key_schema_error() -> StorageError {
    StorageError::validation("The provided key element does not match the schema")
}

fn key_attribute_type(table_info: &StoredTableInfo, name: &str) -> KeyAttributeType {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == name)
        .map_or(KeyAttributeType::S, |definition| {
            definition.attribute_type.clone()
        })
}

fn attribute_value_matches_key_type(value: &AttributeValue, expected: &KeyAttributeType) -> bool {
    matches!(
        (value, expected),
        (AttributeValue::S(_), KeyAttributeType::S)
            | (AttributeValue::N(_), KeyAttributeType::N)
            | (AttributeValue::B(_), KeyAttributeType::B)
    )
}

fn key_type_name(key_type: &KeyAttributeType) -> &'static str {
    match key_type {
        KeyAttributeType::S => "S",
        KeyAttributeType::N => "N",
        KeyAttributeType::B => "B",
    }
}

fn attribute_value_type_name(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::BOOL(_) => "BOOL",
        AttributeValue::NULL(_) => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}
