use std::{collections::HashMap, fmt::Write as _};

use queue_provider::{QueueMessage, ReceiptHandle};
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    TableName,
};

use crate::{
    error_handler::map_sqlite_error,
    names::{AttributeName, GsiPhysicalName},
};

/// Build SQL type string from `DynamoDB` attribute type (`SQLite`)
pub fn dynamodb_type_to_sql_type(attr_type: &KeyAttributeType) -> &'static str {
    match attr_type {
        KeyAttributeType::S => "TEXT",
        KeyAttributeType::N => "NUMERIC",
        KeyAttributeType::B => "BLOB",
    }
}

pub(crate) fn main_table_attributes_blob(
    key_attributes: &KeyAttributes,
    non_key_attributes: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let has_number_key = key_attributes
        .iter()
        .any(|(_, value)| matches!(value, AttributeValue::N(_)));
    if !has_number_key {
        return if non_key_attributes.is_empty() {
            Ok("{}".to_string())
        } else {
            serde_json::to_string(non_key_attributes)
                .map_err(|error| StorageEnum::Serialization(error).into())
        };
    }

    let mut attributes = HashMap::with_capacity(key_attributes.len() + non_key_attributes.len());
    attributes.extend(
        key_attributes
            .iter()
            .map(|(name, value)| (name.to_string(), normalize_wire_number(value))),
    );
    attributes.extend(
        non_key_attributes
            .iter()
            .map(|(name, value)| (name.clone(), normalize_wire_number(value))),
    );
    serde_json::to_string(&attributes).map_err(|error| StorageEnum::Serialization(error).into())
}

fn normalize_wire_number(value: &AttributeValue) -> AttributeValue {
    match value {
        AttributeValue::N(number) => AttributeValue::N(expand_scientific_number(number)),
        AttributeValue::NS(values) => AttributeValue::NS(
            values
                .iter()
                .map(|value| expand_scientific_number(value))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn expand_scientific_number(value: &str) -> String {
    let Some((mantissa, exponent)) = value.split_once(['e', 'E']) else {
        return value.to_string();
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return value.to_string();
    };
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
            _ => return value.to_string(),
        }
    }
    if digits.is_empty() {
        return value.to_string();
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
        format!("-{expanded}")
    } else {
        expanded
    }
}

pub enum SqliteConn<'a> {
    Connection(&'a rusqlite::Connection),
    Transaction(&'a rusqlite::Transaction<'a>),
}

#[derive(Clone, Copy)]
pub(crate) enum SqliteTableRowidMode {
    #[cfg(any(feature = "turso-backend", test))]
    WithRowid,
    WithoutRowid,
}

impl SqliteTableRowidMode {
    fn create_table_suffix(self) -> &'static str {
        match self {
            #[cfg(any(feature = "turso-backend", test))]
            Self::WithRowid => "",
            Self::WithoutRowid => " WITHOUT ROWID",
        }
    }
}

pub(crate) async fn call_sqlite<F, R>(
    connection: &tokio_rusqlite::Connection,
    function: F,
) -> StorageResult<R>
where
    F: FnOnce(&mut rusqlite::Connection) -> StorageResult<R> + Send + 'static,
    R: Send + 'static,
{
    connection
        .call(move |conn| Ok(function(conn)))
        .await
        .map_err(|err| StorageError::internal(&format!("sqlite connection call failed: {err}")))?
}

pub(crate) async fn call_sqlite_raw<F, R>(
    connection: &tokio_rusqlite::Connection,
    function: F,
) -> StorageResult<R>
where
    F: FnOnce(&mut rusqlite::Connection) -> rusqlite::Result<R> + Send + 'static,
    R: Send + 'static,
{
    connection
        .call(move |conn| function(conn).map_err(tokio_rusqlite::Error::from))
        .await
        .map_err(map_tokio_rusqlite_error)
}

fn map_tokio_rusqlite_error(err: tokio_rusqlite::Error) -> StorageError {
    match err {
        tokio_rusqlite::Error::Rusqlite(err) => map_sqlite_error(err),
        other => StorageError::internal(&format!("sqlite connection call failed: {other}")),
    }
}

impl std::ops::Deref for SqliteConn<'_> {
    type Target = rusqlite::Connection;

    #[inline]
    fn deref(&self) -> &rusqlite::Connection {
        match self {
            Self::Connection(conn) => conn,
            Self::Transaction(txn) => txn,
        }
    }
}

pub(crate) fn build_table_creation_sql(
    table_name: &TableName,
    attribute_definitions: &[AttributeDefinition],
    key_schema: &[KeySchemaElement],
    global_secondary_indexes: Option<&[GlobalSecondaryIndex]>,
    rowid_mode: SqliteTableRowidMode,
) -> String {
    let sanitized_name = table_name.sanitized_name();
    let mut create_sql = format!("CREATE TABLE \"table_{sanitized_name}\" (");

    // Add key attributes as individual columns
    let mut key_columns = Vec::new();
    let mut processed_attributes = std::collections::HashSet::new();

    // Add main table key attributes
    for key_element in key_schema {
        if processed_attributes.insert(&key_element.attribute_name) {
            // Find the attribute definition for this key
            if let Some(attr_def) = attribute_definitions
                .iter()
                .find(|attr| attr.attribute_name == key_element.attribute_name)
            {
                let attr_name = AttributeName::new(&attr_def.attribute_name);
                let sql_type = dynamodb_type_to_sql_type(&attr_def.attribute_type);
                key_columns.push(format!("{} {}", attr_name.sanitized(), sql_type));
            }
        }
    }

    // Add GSI key attributes as columns
    if let Some(gsis) = global_secondary_indexes {
        for gsi in gsis {
            for key_element in &gsi.key_schema {
                if processed_attributes.insert(&key_element.attribute_name) {
                    // Find the attribute definition for this GSI key
                    if let Some(attr_def) = attribute_definitions
                        .iter()
                        .find(|attr| attr.attribute_name == key_element.attribute_name)
                    {
                        let attr_name = AttributeName::new(&attr_def.attribute_name);
                        let sql_type = dynamodb_type_to_sql_type(&attr_def.attribute_type);
                        key_columns.push(format!("{} {}", attr_name.sanitized(), sql_type));
                    }
                }
            }
        }
    }
    let _ = write!(
        create_sql,
        "{}, attributes_blob TEXT",
        key_columns.join(", ")
    );

    // Add primary key constraint
    let mut primary_key_columns = Vec::new();
    for key_element in key_schema {
        match key_element.key_type {
            KeyType::Hash => {
                primary_key_columns.insert(0, key_element.attribute_name.clone()); // raw kept; already sanitized earlier for column definition
            }
            KeyType::Range => {
                primary_key_columns.push(key_element.attribute_name.clone());
            }
        }
    }

    if !primary_key_columns.is_empty() {
        create_sql.push_str(&format!(
            ", PRIMARY KEY ({})",
            primary_key_columns.join(", ")
        ));
    }

    create_sql.push(')');
    create_sql.push_str(rowid_mode.create_table_suffix());
    create_sql
}

pub(crate) fn build_gsi_creation_sqls(
    table_name: &TableName,
    attribute_definitions: &[AttributeDefinition],
    table_key_schema: &[KeySchemaElement],
    global_secondary_indexes: &[GlobalSecondaryIndex],
    rowid_mode: SqliteTableRowidMode,
) -> Vec<String> {
    let sanitized_table_name = table_name.sanitized_name();
    let mut gsi_sqls = Vec::new();

    for gsi in global_secondary_indexes {
        let gsi_table_name =
            GsiPhysicalName::compose(&sanitized_table_name, &gsi.index_name.sanitized_name())
                .to_string();
        let mut create_sql = format!("CREATE TABLE \"{gsi_table_name}\" (");

        // Add GSI key attributes as individual columns
        let mut key_columns = Vec::new();
        for key_element in &gsi.key_schema {
            // Find the attribute definition for this GSI key
            if let Some(attr_def) = attribute_definitions
                .iter()
                .find(|attr| attr.attribute_name == key_element.attribute_name)
            {
                let attr_name = AttributeName::new(&attr_def.attribute_name);
                let sql_type = dynamodb_type_to_sql_type(&attr_def.attribute_type);
                key_columns.push(format!("{} {}", attr_name.sanitized(), sql_type));
            }
        }

        for key_element in table_key_schema {
            let column_name = format!(
                "table_{}",
                AttributeName::new(&key_element.attribute_name).sanitized()
            );
            let sql_type = attribute_definitions
                .iter()
                .find(|attr| attr.attribute_name == key_element.attribute_name)
                .map(|attr| dynamodb_type_to_sql_type(&attr.attribute_type))
                .unwrap_or("TEXT");
            key_columns.push(format!("{column_name} {sql_type}"));
        }

        let _ = write!(
            create_sql,
            "{}, attributes_blob TEXT, __aux_tombstone INTEGER NOT NULL DEFAULT 0, \
             __aux_item_version INTEGER NOT NULL DEFAULT 0",
            key_columns.join(", ")
        );

        // Add primary key constraint for GSI: (pk, sk, table_pk, table_sk)
        let mut primary_key_columns = Vec::new();
        for key_element in &gsi.key_schema {
            match key_element.key_type {
                KeyType::Hash => {
                    primary_key_columns.insert(
                        0,
                        AttributeName::new(&key_element.attribute_name)
                            .sanitized()
                            .to_string(),
                    );
                }
                KeyType::Range => {
                    primary_key_columns.push(
                        AttributeName::new(&key_element.attribute_name)
                            .sanitized()
                            .to_string(),
                    );
                }
            }
        }
        primary_key_columns.extend(table_key_schema.iter().map(|key| {
            format!(
                "table_{}",
                AttributeName::new(&key.attribute_name).sanitized()
            )
        }));

        create_sql.push_str(&format!(
            ", PRIMARY KEY ({})",
            primary_key_columns.join(", ")
        ));

        create_sql.push(')');
        create_sql.push_str(rowid_mode.create_table_suffix());
        gsi_sqls.push(create_sql);
    }

    gsi_sqls
}

pub(crate) fn sql_row_to_queue_message(
    queue_url: &str,
    row: &rusqlite::Row,
) -> rusqlite::Result<QueueMessage> {
    Ok(QueueMessage {
        message_id: row.get::<_, String>("message_id")?.into(),
        queue_url: queue_url.to_string(),
        body: row.get::<_, String>("body")?,
        message_attributes: row
            .get::<_, Option<String>>("message_attributes")?
            .as_ref()
            .filter(|json| !json.is_empty())
            .and_then(|json| serde_json::from_str(json).ok()),
        receipt_handle: row
            .get::<_, Option<String>>("receipt_handle")?
            .as_ref()
            .map(|rh| ReceiptHandle::from(rh.as_str())),
        created_at: row.get::<_, i64>("created_at")?.into(),
        visibility_timestamp: row
            .get::<_, Option<i64>>("visibility_timestamp")?
            .map(Into::into),
    })
}

pub(crate) fn sql_row_to_stored_stable_info(
    row: &rusqlite::Row,
) -> rusqlite::Result<StoredTableInfo> {
    let created_at: i64 = row.get("created_at")?;
    let attribute_definitions: String = row.get("attribute_definitions")?;
    let key_schema: String = row.get("key_schema")?;
    let global_secondary_indexes: Option<String> = row.get("global_secondary_indexes")?;
    let stream_specification: Option<String> = row.get("stream_specification")?;
    let global_secondary_indexes = match global_secondary_indexes {
        Some(value) if is_json_null_like(&value) => None,
        other => other,
    };
    let stream_specification = match stream_specification {
        Some(value) if is_json_null_like(&value) => None,
        other => other,
    };

    Ok(StoredTableInfo {
        table_name: TableName::new(&row.get::<_, String>("table_name")?),
        table_status: row.get::<_, String>("table_status")?.as_str().into(),

        created_at: created_at.into(),
        attribute_definitions: parse_attribute_definitions(&attribute_definitions),
        key_schema: parse_key_schema(&key_schema),
        global_secondary_indexes: global_secondary_indexes
            .map(|gsi| serde_json::from_str(&gsi))
            .transpose()
            .map_err(rusqlite_type_err)?,
        table_size_bytes: row_non_negative_u64(row, "table_size_bytes")?,
        item_count: row_non_negative_u64(row, "item_count")?,
        stream_specification: stream_specification
            .map(|ss| serde_json::from_str(&ss))
            .transpose()
            .map_err(rusqlite_type_err)?,
        deletion_protection_enabled: row.get("deletion_protection_enabled")?,
    })
}

pub(crate) fn rusqlite_type_err(e: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}

fn row_non_negative_u64(row: &rusqlite::Row, column: &str) -> rusqlite::Result<u64> {
    let value: i64 = row.get(column)?;
    u64::try_from(value).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, Box::new(err))
    })
}

fn normalize_json_list(raw: &str) -> std::borrow::Cow<'_, str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        std::borrow::Cow::Borrowed("[]")
    } else {
        std::borrow::Cow::Borrowed(raw)
    }
}

fn is_json_null_like(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") || trimmed == "\"null\""
}

fn parse_attribute_definitions(raw: &str) -> Vec<AttributeDefinition> {
    let normalized = normalize_json_list(raw);
    match serde_json::from_str::<Vec<AttributeDefinition>>(&normalized) {
        Ok(parsed) => parsed,
        Err(err) => {
            tracing::warn!(error = %err, raw = %normalized, "sqlite.table_json_parse_failed");
            Vec::new()
        }
    }
}

fn parse_key_schema(raw: &str) -> Vec<KeySchemaElement> {
    let normalized = normalize_json_list(raw);
    match serde_json::from_str::<Vec<KeySchemaElement>>(&normalized) {
        Ok(parsed) => parsed,
        Err(err) => {
            tracing::warn!(error = %err, raw = %normalized, "sqlite.table_json_parse_failed");
            Vec::new()
        }
    }
}

pub(crate) fn add_non_key_attributes_from_blob(
    row: &rusqlite::Row,
    result: &mut HashMap<String, AttributeValue>,
) {
    if let Ok(Some(blob)) = row.get::<_, Option<String>>("attributes_blob")
        && !blob.is_empty()
        && blob != "{}"
        && let Ok(non_key_attrs) = serde_json::from_str::<HashMap<String, AttributeValue>>(&blob)
    {
        result.extend(non_key_attrs);
    }
}

#[cfg(test)]
pub(crate) fn add_gsi_attributes_from_columns_test_helper(
    row: &rusqlite::Row,
    table_info: &StoredTableInfo,
    gsi_key_schema: &[KeySchemaElement],
    result: &mut HashMap<String, AttributeValue>,
) {
    // Read GSI key attributes (they have their original names)
    for key_elem in gsi_key_schema {
        let column_name = &key_elem.attribute_name;

        let value_string = if let Ok(s) = row.get::<_, String>(column_name.as_str()) {
            Some(s)
        } else if let Ok(i) = row.get::<_, i64>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(f) = row.get::<_, f64>(column_name.as_str()) {
            Some(f.to_string())
        } else if let Ok(Some(s)) = row.get::<_, Option<String>>(column_name.as_str()) {
            Some(s)
        } else if let Ok(Some(i)) = row.get::<_, Option<i64>>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(Some(f)) = row.get::<_, Option<f64>>(column_name.as_str()) {
            Some(f.to_string())
        } else {
            None
        };

        if let Some(value_str) = value_string {
            // Find the attribute definition to determine the correct type
            if let Some(attr_def) = table_info
                .attribute_definitions
                .iter()
                .find(|attr| attr.attribute_name == key_elem.attribute_name)
            {
                // Convert to the correct AttributeValue type based on the definition
                let attr_value = match attr_def.attribute_type {
                    KeyAttributeType::S => AttributeValue::S(value_str),
                    KeyAttributeType::N => AttributeValue::N(value_str),
                    KeyAttributeType::B => {
                        // For binary data, assume it was base64 encoded
                        AttributeValue::B(value_str)
                    }
                };
                result.insert(column_name.clone(), attr_value);
            } else {
                // Fallback: if no attribute definition found, treat as string
                result.insert(column_name.clone(), AttributeValue::S(value_str));
            }
        }
    }

    // Read main table key attributes (they have "table_" prefix)
    for key_elem in &table_info.key_schema {
        let column_name = format!("table_{}", key_elem.attribute_name);

        let value_string = if let Ok(s) = row.get::<_, String>(column_name.as_str()) {
            Some(s)
        } else if let Ok(i) = row.get::<_, i64>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(f) = row.get::<_, f64>(column_name.as_str()) {
            Some(f.to_string())
        } else if let Ok(Some(s)) = row.get::<_, Option<String>>(column_name.as_str()) {
            Some(s)
        } else if let Ok(Some(i)) = row.get::<_, Option<i64>>(column_name.as_str()) {
            Some(i.to_string())
        } else if let Ok(Some(f)) = row.get::<_, Option<f64>>(column_name.as_str()) {
            Some(f.to_string())
        } else {
            None
        };

        if let Some(value_str) = value_string {
            // Parse the value back to AttributeValue
            if let Ok(attr_value) =
                serde_json::from_str::<AttributeValue>(&format!("\"{value_str}\""))
            {
                result.insert(key_elem.attribute_name.clone(), attr_value);
            } else {
                // Fallback: treat as string
                result.insert(
                    key_elem.attribute_name.clone(),
                    AttributeValue::S(value_str),
                );
            }
        }
    }
}
