use std::collections::{HashMap, HashSet};

use crate::{
    AttributeValue, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, KeyAttributeType, KeyAttributes,
    StorageEnum, StorageError, StorageResult, StoredTableInfo, TableName, TransactGetItem,
    TransactWriteItem, context::WrappedError as _,
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
    transaction_canceled_for_indexed_reasons(
        preflights
            .iter()
            .map(|preflight| preflight.validation_reason.clone())
            .collect(),
    )
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
    let result = match item {
        TransactWriteItem { put: Some(put), .. } => {
            validate_transact_put_item_key(table_info, &put.item)
                .and_then(|()| transact_put_item_key_fingerprint(table_info, &put.item))
        }
        TransactWriteItem {
            delete: Some(delete),
            ..
        } => validate_transact_key(table_info, &delete.key)
            .and_then(|()| transact_key_fingerprint(table_info, &delete.key)),
        TransactWriteItem {
            update: Some(update),
            ..
        } => validate_transact_key(table_info, &update.key)
            .and_then(|()| transact_key_fingerprint(table_info, &update.key)),
        TransactWriteItem {
            condition_check: Some(check),
            ..
        } => validate_transact_key(table_info, &check.key)
            .and_then(|()| transact_key_fingerprint(table_info, &check.key)),
        _ => Ok(String::new()),
    };
    transaction_key_preflight_from_key_result(result)
}

pub fn preflight_transact_get_item_key_with_table_info(
    item: &TransactGetItem,
    table_info: &StoredTableInfo,
) -> StorageResult<TransactionKeyPreflight> {
    transaction_key_preflight_from_key_result(
        validate_transact_key(table_info, &item.get.key)
            .and_then(|()| transact_key_fingerprint(table_info, &item.get.key)),
    )
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
    Ok(())
}

pub fn transact_put_item_key_fingerprint(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let mut key = KeyAttributes::with_capacity(table_info.key_schema.len());
    for key_schema in &table_info.key_schema {
        let value = item.get(&key_schema.attribute_name).ok_or_else(|| {
            StorageError::validation(format!(
                "One or more parameter values were invalid: Missing the key {} in the item",
                key_schema.attribute_name
            ))
        })?;
        key.insert(key_schema.attribute_name.clone(), value.clone());
    }
    transact_key_fingerprint(table_info, &key)
}

pub fn validate_transact_key(
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
    let key_json = key
        .canonical_dynamo_json()
        .map_err(|err| StorageError::internal(&err.to_string()))?;
    Ok(format!(
        "{}\t{}",
        table_info.table_name.dynamodb_resource_name(),
        key_json
    ))
}

fn transaction_cancellation_reason(error: &StorageError) -> Option<String> {
    match error.to_enum() {
        StorageEnum::ConditionalCheckFailed => Some("ConditionalCheckFailed".to_string()),
        StorageEnum::Validation { message } => Some(format!("ValidationError\t{message}")),
        _ => None,
    }
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
