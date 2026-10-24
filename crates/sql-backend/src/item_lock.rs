//! Native exact-item protection, including absent keys, without durable lock
//! rows.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_types::{AttributeValue, BoundTableInfo, KeyAttributes, StorageError, StorageResult};

use crate::{SqlStatement, SqlValue};

pub(crate) fn item_lock_statement<'a>(
    targets: impl Iterator<Item = (&'a BoundTableInfo, &'a KeyAttributes)>,
) -> StorageResult<SqlStatement> {
    let identities = targets
        .map(|(table, key)| {
            let identity = uuid::Uuid::from_bytes(*table.history_id.as_ref()).simple();
            Ok(SqlValue::Text(format!(
                "item:{identity}:{}",
                encode_lock_key(key)?
            )))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(lock_statement(identities))
}

pub(crate) fn token_lock_statement(token: &str) -> SqlStatement {
    lock_statement(vec![SqlValue::Text(format!("token:{token}"))])
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
    storage_provider::canonical_revision_key(&canonical)
}

fn lock_statement(identities: Vec<SqlValue>) -> SqlStatement {
    let values = (1..=identities.len())
        .map(|index| format!("(${index}::text)"))
        .collect::<Vec<_>>()
        .join(", ");
    SqlStatement::new(
        format!(
            "SELECT count(*) FROM (SELECT pg_advisory_xact_lock(lock_id) FROM (SELECT DISTINCT \
             hashtextextended(identity, 0) AS lock_id FROM (VALUES {values}) AS targets(identity) \
             ORDER BY lock_id) AS ids) AS locked"
        ),
        identities,
    )
}
