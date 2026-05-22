use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};

use crate::{
    SQLiteStorageProvider, sql_statements,
    utils::{SqliteConn, sql_row_to_stored_stable_info},
};

impl SQLiteStorageProvider {
    pub fn do_get_table_info(
        table_name: &TableName,
        sqlite: &SqliteConn,
    ) -> StorageResult<StoredTableInfo> {
        let (sql, params) = sql_statements::get_table_info(table_name);
        let result = sqlite.query_row(sql, params, sql_row_to_stored_stable_info);

        result.map_err(|_e| StorageError::table_not_found(table_name))
    }
}
