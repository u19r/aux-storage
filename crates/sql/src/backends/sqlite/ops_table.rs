use std::sync::Arc;

use storage_types::{
    CreateTableRequest, StorageResult, StoredTableInfo, TableName, TableStatus,
    context::ErrorContext,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    sql_statements,
    utils::{SqliteTableRowidMode, build_gsi_creation_sqls, build_table_creation_sql, call_sqlite},
};

impl SQLiteStorageProvider {
    pub async fn invalidate_table_info_cache(&self, table_name: &TableName) {
        self.table_info_cache.write().await.remove(table_name);
    }

    pub async fn get_table_info_cached_arc(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        if let Some(cached) = self.table_info_cache.read().await.get(table_name).cloned() {
            return Ok(cached);
        }

        let table_info = self.get_table_info_internal(table_name).await?;
        let table_info = Arc::new(table_info);
        self.table_info_cache
            .write()
            .await
            .insert(table_name.clone(), Arc::clone(&table_info));
        Ok(table_info)
    }

    pub async fn update_table_status_internal(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        let table_name = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::update_table_status(&status, &table_name);
            conn.execute(sql, params).map_err(map_sqlite_error)
        })
        .await?;
        Ok(())
    }

    pub async fn get_table_info_internal(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        let table_name = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let conn = crate::utils::SqliteConn::Connection(conn);
            Self::do_get_table_info(&table_name, &conn)
        })
        .await
    }

    pub async fn create_table_storage_internal(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        let global_secondary_indexes = request
            .global_secondary_indexes
            .as_ref()
            .map(|indexes| indexes.iter().cloned().map(Into::into).collect::<Vec<_>>());
        let create_sql = build_table_creation_sql(
            table_name,
            &request.attribute_definitions,
            &request.key_schema,
            global_secondary_indexes.as_deref(),
            SqliteTableRowidMode::WithoutRowid,
        );
        let table_name_clone = table_name.clone();

        call_sqlite(&self.connection, move |conn| {
            conn.execute(&create_sql, []).map_err(map_sqlite_error)
        })
        .await
        .with_context(|| format!("Table name: {table_name_clone}"))?;
        if let Some(gsis) = &global_secondary_indexes {
            let gsi_sqls = build_gsi_creation_sqls(
                &table_name_clone,
                &request.attribute_definitions,
                &request.key_schema,
                gsis,
                SqliteTableRowidMode::WithoutRowid,
            );
            for gsi_sql in gsi_sqls {
                let tn = table_name_clone.clone();

                call_sqlite(&self.connection, move |conn| {
                    conn.execute(&gsi_sql, []).map_err(map_sqlite_error)
                })
                .await
                .with_context(|| format!("GSI creation for table: {tn}"))?;
            }
        }
        Ok(())
    }
}
