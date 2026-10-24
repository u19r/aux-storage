//! SQL catalog column for the Dynamo TTL policy.

use storage_provider::{
    CATALOG_READ_BYTES, DescribeTimeToLiveAttempt, TtlConfigRecord, UpdateTimeToLiveAttempt,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, storage_serde};

use crate::{
    ColumnIndex, SqlAttempt, SqlLayout, SqlQueryPage, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
};

pub(crate) fn ttl_config_statement(engine: impl SqlLayout, table: &BoundTableInfo) -> SqlStatement {
    let history = uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string();
    let sql = format!(
        "SELECT ttl_config_blob FROM {relation_tables} WHERE table_name = {} AND id = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_tables = engine.relation_name("tables"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(table.info.table_name.to_string()),
            SqlValue::Text(history),
        ],
    )
}

pub(crate) fn decode_ttl_config_page(page: SqlQueryPage) -> StorageResult<Option<TtlConfigRecord>> {
    if page.more || page.rows.len() != 1 {
        return Err(StorageError::internal(
            "TTL catalog row is missing or duplicated",
        ));
    }
    match page.rows[0].value(ColumnIndex::new(0))? {
        SqlValue::Null => Ok(None),
        SqlValue::Blob(bytes) => storage_serde::from_bytes(bytes).map(Some),
        _ => Err(StorageError::internal(
            "TTL catalog blob has invalid SQL type",
        )),
    }
}

impl<A: SqlAttempt, L: SqlLayout> DescribeTimeToLiveAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> StorageResult<Vec<Option<TtlConfigRecord>>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let statements = tables
            .iter()
            .map(|table| ttl_config_statement(self.engine, table))
            .collect();
        let pages = self
            .attempt
            .query_batch(statements, self.protection, CATALOG_READ_BYTES)
            .await?;
        if pages.len() != tables.len() {
            return Err(StorageError::internal(
                "SQL TTL policy observation count differs",
            ));
        }
        pages.into_iter().map(decode_ttl_config_page).collect()
    }
}

impl<A: SqlAttempt, L: SqlLayout> UpdateTimeToLiveAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
    ) -> StorageResult<()> {
        let engine = self.engine;
        let history = uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string();
        let bytes = storage_serde::to_bytes(&config)?;
        let sql = format!(
            "UPDATE {relation_tables} SET ttl_config_blob = {}, catalog_revision = \
             catalog_revision + 1 WHERE table_name = {} AND id = {} AND catalog_revision < {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            i64::MAX,
            relation_tables = engine.relation_name("tables"),
        );
        let changed = self
            .attempt
            .execute(SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(bytes),
                    SqlValue::Text(table.info.table_name.to_string()),
                    SqlValue::Text(history),
                ],
            ))
            .await?;
        if changed != 1 {
            return Err(StorageError::internal(
                "SQL table incarnation changed during TTL update",
            ));
        }
        Ok(())
    }
}
