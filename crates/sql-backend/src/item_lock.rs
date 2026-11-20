//! Native exact-item protection, including absent keys, without durable lock
//! rows.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_types::{AttributeValue, BoundTableInfo, KeyAttributes, StorageError, StorageResult};

use crate::{SqlStatement, SqlValue};

#[cfg(test)]
pub(crate) fn item_lock_statement<'a>(
    targets: impl Iterator<Item = (&'a BoundTableInfo, &'a KeyAttributes)>,
) -> StorageResult<SqlStatement> {
    Ok(lock_statement(&item_lock_identities(targets)?))
}

/// The canonical lock identity of each target item.
pub(crate) fn item_lock_identities<'a>(
    targets: impl Iterator<Item = (&'a BoundTableInfo, &'a KeyAttributes)>,
) -> StorageResult<Vec<String>> {
    targets
        .map(|(table, key)| {
            let identity = uuid::Uuid::from_bytes(*table.history_id.as_ref()).simple();
            Ok(format!("item:{identity}:{}", encode_lock_key(key)?))
        })
        .collect()
}

pub(crate) fn token_lock_statement(token: &str) -> SqlStatement {
    lock_statement(&[format!("token:{token}")])
}

pub(crate) fn receipt_lock_statement(slot: storage_provider::ReceiptSlot<'_>) -> SqlStatement {
    match slot {
        storage_provider::ReceiptSlot::Request(token) => token_lock_statement(token),
        storage_provider::ReceiptSlot::JobEffect(key) => {
            lock_statement(&[format!("job-effect:{}", STANDARD.encode(key.as_bytes()))])
        }
    }
}

/// One Postgres `text[]` literal bound as text: a statement that takes a
/// list of names has one shape whatever the list length.
pub(crate) fn postgres_text_array<S: AsRef<str>>(values: &[S]) -> SqlValue {
    let mut literal = String::from("{");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            literal.push(',');
        }
        literal.push('"');
        for character in value.as_ref().chars() {
            if matches!(character, '"' | '\\') {
                literal.push('\\');
            }
            literal.push(character);
        }
        literal.push('"');
    }
    literal.push('}');
    SqlValue::Text(literal)
}

fn encode_lock_key(key: &KeyAttributes) -> StorageResult<String> {
    let mut canonical = KeyAttributes::with_capacity(key.len());
    for (name, value) in key.iter() {
        let value = match value {
            AttributeValue::B(raw) => AttributeValue::B(
                STANDARD.encode(
                    storage_types::dynamodb_binary::decode_base64_string(raw)
                        .map_err(|_| StorageError::invalid_or_missing_key())?,
                ),
            ),
            other => other.clone(),
        };
        canonical.insert(name, value);
    }
    canonical_item_lock_key(&canonical)
}

pub(crate) fn lock_statement(identities: &[String]) -> SqlStatement {
    SqlStatement::new(
        "SELECT count(*) FROM (SELECT pg_advisory_xact_lock(lock_id) FROM (SELECT DISTINCT \
         hashtextextended(identity, 0) AS lock_id FROM unnest($1::text::text[]) AS \
         targets(identity) ORDER BY lock_id) AS ids) AS locked",
        vec![postgres_text_array(identities)],
    )
}

fn canonical_item_lock_key(key: &KeyAttributes) -> StorageResult<String> {
    if key.is_empty() {
        return Err(StorageError::invalid_or_missing_key());
    }
    key.canonical_dynamo_json().map_err(|error| match error {
        storage_types::ConversionError::Serialization(message) => StorageError::validation(message),
        other => StorageError::validation(format!(
            "item lock key must be Dynamo JSON encodable: {other}"
        )),
    })
}
