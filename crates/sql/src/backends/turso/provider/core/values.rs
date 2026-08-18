use crate::backends::turso::provider::core::*;

pub(crate) fn row_required_i64(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<i64> {
    let value = row
        .get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))?;
    value_to_i64(value)
}

pub(crate) fn row_required_blob(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Vec<u8>> {
    match row
        .get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))?
    {
        TursoValue::Blob(raw) => Ok(raw.clone()),
        _ => Err(StorageError::internal(&format!(
            "column '{column}' is not a blob"
        ))),
    }
}

pub(crate) fn value_to_i64(value: &TursoValue) -> StorageResult<i64> {
    match value {
        TursoValue::Integer(raw) => Ok(*raw),
        TursoValue::Real(raw) => Ok(*raw as i64),
        TursoValue::Text(raw) => raw
            .parse::<i64>()
            .map_err(|error| StorageError::internal(&format!("parse i64 failed: {error}"))),
        TursoValue::Null => Ok(0),
        TursoValue::Blob(_) => Err(StorageError::internal("cannot convert blob to i64")),
    }
}

pub(crate) fn value_to_string(value: &TursoValue) -> StorageResult<String> {
    match value {
        TursoValue::Null => Ok(String::new()),
        TursoValue::Integer(raw) => Ok(raw.to_string()),
        TursoValue::Real(raw) => Ok(raw.to_string()),
        TursoValue::Text(raw) => Ok(raw.clone()),
        TursoValue::Blob(raw) => String::from_utf8(raw.clone())
            .map_err(|_| StorageError::internal("blob value is not utf8")),
    }
}

pub(crate) fn option_string_to_value(value: Option<String>) -> TursoValue {
    match value {
        Some(value) => TursoValue::Text(value),
        None => TursoValue::Null,
    }
}

pub(crate) fn canonical_revision_key(key: &KeyAttributes) -> StorageResult<String> {
    if key.is_empty() {
        return Err(StorageError::invalid_or_missing_key());
    }
    key.canonical_dynamo_json().map_err(|error| {
        StorageError::validation(format!(
            "revision key must be Dynamo JSON encodable: {error}"
        ))
    })
}

pub(crate) fn revision_from_guard_bytes(bytes: &[u8]) -> StorageResult<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::validation("durable guard revision must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

pub(crate) fn is_key_absence_condition(
    condition: Option<&Condition>,
    table_info: &StoredTableInfo,
) -> bool {
    let Some(Condition::NotExists { field }) = condition else {
        return false;
    };
    table_info
        .key_schema
        .iter()
        .any(|key| key.key_type == KeyType::Hash && key.attribute_name == *field)
}

pub(crate) fn is_constraint_storage_error(error: &StorageError) -> bool {
    matches!(error.as_ref(), StorageEnum::Validation { .. })
}

pub(crate) fn plan_turso_gsi_sql_statements(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
    new_indexers: &[String],
) -> StorageResult<WriteMaintenancePlan<TursoValue>> {
    let options = GsiSqlPlanOptions::new(
        gsi_table_name,
        attribute_scalar_to_turso_value,
        || TursoValue::Null,
        |index, _| format!("?{index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdateNonKey,
        TableKeyColumnStyle::FixedPkSk,
        PlaceholderNumbering::PerStatement,
        GsiAttributesBlobStyle::FullProjectedItem,
    );
    plan_gsi_sql_statements(table_info, old_item, new_item, new_indexers, &options)
}

#[cfg(test)]
pub(crate) fn classify_query_sql(sql: &str) -> &'static str {
    if sql.contains("FROM tables") {
        "sql_query_table_info"
    } else if sql.contains("FROM \"table_") {
        "sql_query_main_row"
    } else if sql.contains("item_revisions") {
        "sql_query_revision"
    } else {
        "sql_query_other"
    }
}

pub(crate) fn classify_execute_sql(sql: &str) -> &'static str {
    if sql.starts_with("INSERT INTO \"table_") {
        "sql_execute_main_upsert"
    } else if sql.starts_with("INSERT INTO \"gsi_") {
        "sql_execute_gsi_upsert"
    } else if sql.starts_with("DELETE FROM \"gsi_") {
        "sql_execute_gsi_delete"
    } else if sql.contains("item_revisions") {
        "sql_execute_revision"
    } else if sql.contains("ttl") {
        "sql_execute_ttl"
    } else if sql.contains("stream") {
        "sql_execute_stream"
    } else {
        "sql_execute_other"
    }
}

pub(crate) async fn read_pragma_text(
    conn: &TursoConnection,
    pragma_name: &str,
) -> StorageResult<String> {
    let sql = sql_statements::read_pragma(pragma_name);
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(map_turso_error)
        .context("read pragma")?;

    let Some(row) = rows.next().await.map_err(map_turso_error)? else {
        return Err(StorageError::internal("pragma query returned no value"));
    };

    row.get::<String>(0).map_err(map_turso_error)
}

pub(crate) fn map_turso_error(error: TursoError) -> StorageError {
    match error {
        TursoError::Busy(message)
        | TursoError::BusySnapshot(message)
        | TursoError::Interrupt(message) => {
            tracing::debug!(message, "turso transaction conflict");
            StorageEnum::TransactionConflict { message }.into()
        }
        TursoError::Error(message) => {
            if is_turso_conflict_message(&message) {
                tracing::debug!(message, "turso transaction conflict");
                return StorageEnum::TransactionConflict { message }.into();
            }
            tracing::error!(message, "turso backend sql error");
            StorageError::internal(&format!("turso error: {message}"))
        }
        TursoError::Constraint(message) => {
            if is_turso_conflict_message(&message) {
                tracing::debug!(message, "turso transaction conflict");
                return StorageEnum::TransactionConflict { message }.into();
            }
            tracing::debug!(message, "turso constraint error");
            StorageError::validation(message)
        }
        other => {
            tracing::error!(error = ?other, "turso backend sql error");
            StorageError::internal(&format!("turso error: {other}"))
        }
    }
}

fn is_turso_conflict_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("conflict")
        || lower.contains("database is locked")
        || lower.contains("locked")
        || lower.contains("busy")
        || lower.contains("schema changed")
        || lower.contains("no transaction is active")
        || lower.contains("ongoing transaction")
}

pub(crate) fn is_conflict_storage_error(error: &StorageError) -> bool {
    matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. } | StorageEnum::TransactionInProgress { .. }
    )
}

pub(crate) async fn sleep_backoff(attempt: u32) {
    let exp = BASE_BACKOFF_MS.saturating_mul(1_u64 << attempt.min(8));
    let jitter = rand::random::<u64>() % (exp + 1);
    tokio::time::sleep(std::time::Duration::from_millis(exp + jitter / 2)).await;
}

#[cfg(test)]
pub(crate) fn reset_turso_statement_counters() {
    TURSO_QUERY_CALLS.store(0, Ordering::Relaxed);
    TURSO_EXECUTE_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn turso_statement_counters() -> (usize, usize) {
    (
        TURSO_QUERY_CALLS.load(Ordering::Relaxed),
        TURSO_EXECUTE_CALLS.load(Ordering::Relaxed),
    )
}
