use std::collections::HashMap;

use storage_types::{
    AttributeValue, KeyAttributes, StorageError, StorageResult, TableName, WireItem,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    key_attribute_handler::{add_key_attributes_from_columns, wire_item_key_attributes_from_row},
    utils::{SqliteConn, add_non_key_attributes_from_blob},
};

impl SQLiteStorageProvider {
    pub fn do_get_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let table_name_safe = table_name.sanitized_name();

        if key.is_empty() {
            return Ok(None);
        }

        // Get the table's key schema
        let table_info = Self::do_get_table_info(table_name, sqlite)?;

        let conditions: Vec<String> = key
            .iter()
            .enumerate()
            .map(|(i, (name, _))| format!("{name} = ?{}", i + 1))
            .collect();
        let conditions_str = conditions.join(" AND ");
        let sql = format!("SELECT * FROM \"table_{table_name_safe}\" WHERE {conditions_str}");

        let values: Vec<&str> = key
            .iter()
            .map(|(_, value)| value)
            .map(|value| {
                value.inner_str().map_err(|err| {
                    StorageError::validation(format!("key attribute must be scalar: {err}"))
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;

        let mapper = |row: &rusqlite::Row| -> rusqlite::Result<HashMap<String, AttributeValue>> {
            let mut storage_result = HashMap::new();
            add_key_attributes_from_columns(row, &table_info, &mut storage_result);
            add_non_key_attributes_from_blob(row, &mut storage_result);
            Ok(storage_result)
        };

        let query_result = sqlite.query_row(&sql, rusqlite::params_from_iter(values), mapper);

        match query_result {
            Ok(item) => {
                tracing::Span::current().record("found", true);
                Ok(Some(item))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                tracing::Span::current().record("found", false);
                Ok(None)
            }
            Err(e) => Err(map_get_item_error(e)),
        }
    }

    pub fn do_get_wire_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<WireItem>> {
        let table_name_safe = table_name.sanitized_name();

        if key.is_empty() {
            return Ok(None);
        }

        let table_info = Self::do_get_table_info(table_name, sqlite)?;

        let conditions: Vec<String> = key
            .iter()
            .enumerate()
            .map(|(i, (name, _))| format!("{name} = ?{}", i + 1))
            .collect();
        let conditions_str = conditions.join(" AND ");
        let sql = format!("SELECT * FROM \"table_{table_name_safe}\" WHERE {conditions_str}");

        let values: Vec<&str> = key
            .iter()
            .map(|(_, value)| value)
            .map(|value| {
                value.inner_str().map_err(|err| {
                    StorageError::validation(format!("key attribute must be scalar: {err}"))
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;

        let mapper = |row: &rusqlite::Row| -> rusqlite::Result<WireItem> {
            let primary_key = wire_item_key_attributes_from_row(
                row,
                &table_info.key_schema,
                &table_info.attribute_definitions,
                None,
            )
            .map_err(|err| storage_error_to_rusqlite(&err))?;
            let non_key_attributes_blob = row
                .get::<_, Option<String>>("attributes_blob")?
                .map(String::into_bytes);

            Ok(WireItem::local_split(
                primary_key,
                None,
                non_key_attributes_blob,
            ))
        };

        let query_result = sqlite.query_row(&sql, rusqlite::params_from_iter(values), mapper);

        match query_result {
            Ok(item) => {
                tracing::Span::current().record("found", true);
                Ok(Some(item))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                tracing::Span::current().record("found", false);
                Ok(None)
            }
            Err(e) => Err(map_get_item_error(e)),
        }
    }
}

#[cold]
#[inline(never)]
fn map_get_item_error(error: rusqlite::Error) -> StorageError {
    tracing::error!(error = %error, "sqlite.get_item.failed");
    map_sqlite_error(error)
}

fn storage_error_to_rusqlite(err: &StorageError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            err.to_string(),
        )),
    )
}
