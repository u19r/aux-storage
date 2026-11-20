//! SQL catalog column for the Dynamo TTL policy and its sweep checkpoint.
//!
//! `tables.ttl_blob` holds the policy without its checkpoint; it is part of
//! the catalog binding, so each change goes through `execute_catalog_change`
//! and changes the stamp. The checkpoint is job state in `table_jobs` and
//! changes no stamp (W6 A15).

use storage_common::ttl::TtlMaintenanceCheckpoint;
use storage_provider::{CATALOG_READ_BYTES, TtlConfigRecord, TtlPolicyAttempt};
use storage_types::{BoundTableInfo, MissExpectation, StorageError, StorageResult, storage_serde};

use crate::{
    ColumnIndex, SqlAttempt, SqlLayout, SqlQueryPage, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
};

/// The `table_jobs` row of the TTL sweep checkpoint.
const TTL_SWEEP_JOB: &str = "ttl_sweep";

/// The stored policy bytes: the record without its checkpoint.
pub(crate) fn encode_ttl_policy(config: &TtlConfigRecord) -> StorageResult<Vec<u8>> {
    storage_serde::to_bytes(&TtlConfigRecord {
        checkpoint: TtlMaintenanceCheckpoint::default(),
        ..config.clone()
    })
}

fn history_text(table: &BoundTableInfo) -> String {
    uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string()
}

fn ttl_config_statement(engine: impl SqlLayout, table: &BoundTableInfo) -> SqlStatement {
    let sql = format!(
        "SELECT ttl_blob, (SELECT state FROM {relation_jobs} WHERE table_id = \
         {relation_tables}.id AND job = '{TTL_SWEEP_JOB}') FROM {relation_tables} WHERE \
         table_name = {} AND id = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_tables = engine.relation_name("tables"),
        relation_jobs = engine.relation_name("table_jobs"),
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(table.info.table_name.to_string()),
            SqlValue::Text(history_text(table)),
        ],
    )
}

fn decode_ttl_config_page(page: SqlQueryPage) -> StorageResult<Option<TtlConfigRecord>> {
    if page.more || page.rows.len() != 1 {
        return Err(StorageError::internal(
            "TTL catalog row is missing or duplicated",
        ));
    }
    let row = &page.rows[0];
    let mut config: TtlConfigRecord = match row.value(ColumnIndex::new(0))? {
        SqlValue::Null => return Ok(None),
        SqlValue::Blob(bytes) => storage_serde::from_bytes(bytes)?,
        _ => {
            return Err(StorageError::internal(
                "TTL catalog blob has invalid SQL type",
            ));
        }
    };
    match row.value(ColumnIndex::new(1))? {
        SqlValue::Null => {}
        SqlValue::Blob(bytes) => config.checkpoint = storage_serde::from_bytes(bytes)?,
        _ => {
            return Err(StorageError::internal(
                "TTL checkpoint has invalid SQL type",
            ));
        }
    }
    Ok(Some(config))
}

fn checkpoint_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    checkpoint: &TtlMaintenanceCheckpoint,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_jobs} (table_id, job, state) VALUES ({}, '{TTL_SWEEP_JOB}', \
             {}) ON CONFLICT (table_id, job) DO UPDATE SET state = excluded.state",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_jobs = engine.relation_name("table_jobs"),
        ),
        vec![
            SqlValue::Text(history_text(table)),
            SqlValue::Blob(storage_serde::to_bytes(checkpoint)?),
        ],
    ))
}

impl<A: SqlAttempt, L: SqlLayout> TtlPolicyAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> StorageResult<storage_types::Exact<Option<TtlConfigRecord>>> {
        if tables.is_empty() {
            return Ok(storage_types::Exact::empty());
        }
        // Every TTL policy change takes the catalog lock exclusive.
        let protection = self.catalog_lock.row_protection(self.protection);
        let statements = tables
            .iter()
            .map(|table| (ttl_config_statement(self.engine, table), protection))
            .collect();
        let names = tables
            .iter()
            .map(|table| &table.info.table_name)
            .collect::<Vec<_>>();
        let pages = self
            .query_with_catalog_lock(&names, statements, CATALOG_READ_BYTES)
            .await?;
        pages.try_map(decode_ttl_config_page)
    }

    async fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
    ) -> StorageResult<()> {
        let engine = self.engine;
        let sql = format!(
            "UPDATE {relation_tables} SET ttl_blob = {}, catalog_revision = catalog_revision + 1 \
             WHERE table_name = {} AND id = {} AND catalog_revision < {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            i64::MAX,
            relation_tables = engine.relation_name("tables"),
        );
        let changed = self
            .execute_catalog_change(
                &table.info.table_name,
                vec![
                    SqlStatement::new(
                        sql,
                        vec![
                            SqlValue::Blob(encode_ttl_policy(&config)?),
                            SqlValue::Text(table.info.table_name.to_string()),
                            SqlValue::Text(history_text(table)),
                        ],
                    ),
                    checkpoint_statement(engine, table, &config.checkpoint)?,
                ],
            )
            .await?;
        if changed != [1, 1] {
            return Err(StorageError::internal(
                "SQL table incarnation changed during TTL update",
            ));
        }
        Ok(())
    }

    async fn write_ttl_checkpoint(
        &mut self,
        table: &BoundTableInfo,
        change: storage_provider::TtlCheckpointChange,
    ) -> StorageResult<()> {
        let changed = self
            .attempt
            .execute(SqlStatement::new(
                format!(
                    "UPDATE {} SET state = {} WHERE table_id = {} AND job = '{TTL_SWEEP_JOB}' AND \
                     state = {}",
                    self.engine.relation_name("table_jobs"),
                    self.engine.engine().placeholder(1),
                    self.engine.engine().placeholder(2),
                    self.engine.engine().placeholder(3)
                ),
                vec![
                    SqlValue::Blob(storage_serde::to_bytes(&change.next.checkpoint)?),
                    SqlValue::Text(history_text(table)),
                    SqlValue::Blob(storage_serde::to_bytes(&change.previous)?),
                ],
            ))
            .await?;
        if changed != 1 {
            return Err(StorageError::write_miss(
                "ttl_sweep_checkpoint",
                MissExpectation::Revision,
            ));
        }
        Ok(())
    }
}
