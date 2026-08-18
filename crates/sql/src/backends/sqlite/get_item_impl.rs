use std::collections::HashMap;

use storage_types::{
    AttributeValue, KeyAttributes, StorageError, StorageResult, TableName, WireItem,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    indexed_item::{SqlDecodedItem, SqlLogicalItem, sqlite_row_to_decoded_item},
    utils::SqliteConn,
};

impl SQLiteStorageProvider {
    pub fn do_get_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        Self::do_get_wire_item(table_name, key, sqlite)?
            .map(WireItem::into_attribute_map)
            .transpose()
    }

    pub(crate) fn do_get_item_with_indexers(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<SqlLogicalItem>> {
        Self::do_get_decoded_item(table_name, key, sqlite)?
            .map(|decoded| {
                decoded
                    .item
                    .into_attribute_map()
                    .map(|item| (item, decoded.indexers))
            })
            .transpose()
    }

    pub(crate) fn do_get_wire_item_with_indexers(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<(WireItem, Vec<String>)>> {
        Self::do_get_decoded_item(table_name, key, sqlite)
            .map(|item| item.map(|decoded| (decoded.item, decoded.indexers)))
    }

    pub fn do_get_wire_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<WireItem>> {
        Self::do_get_decoded_item(table_name, key, sqlite)
            .map(|item| item.map(|decoded| decoded.item))
    }

    fn do_get_decoded_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn,
    ) -> StorageResult<Option<SqlDecodedItem>> {
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

        let mapper = |row: &rusqlite::Row| sqlite_row_to_decoded_item(row, &table_info, None);

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
