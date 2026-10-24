//! SQL receipt persistence for the shared TransactWriteItems executor.

use storage_provider::{DynamoTransactWriteAttempt, TRANSACT_RECEIPT_READ_BYTES};
use storage_types::{StorageError, StorageResult};

use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlRow, SqlValue, catalog::SqlCatalogAttempt,
    transact_write_statements as sql_statements,
};

pub async fn initialize_transact_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(sql_statements::create_receipts(engine))
        .await?;
    Ok(())
}

impl<A: SqlAttempt, L: SqlLayout> DynamoTransactWriteAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_scheduled_jobs(
        &mut self,
        keys: &[scheduled_bg_jobs::store::StoreKey],
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut values = Vec::new();
            values
                .try_reserve_exact(keys.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for key in keys {
                let key = std::slice::from_ref(key);
                // Await the job-row lock before reading its cancellation
                // marker. A fresh READ COMMITTED statement must see a
                // cancellation that committed while the job-row read waited for
                // that same lock.
                let page = catalog
                    .attempt
                    .query(
                        crate::job_store::points(catalog.engine.engine(), key)?,
                        crate::ReadProtection::Conflict,
                        crate::constants::SCHEDULED_JOB_POINT_READ_BYTES,
                    )
                    .await?;
                values.extend(
                    crate::job_store::decode_many(page, key, |value: &Vec<u8>| {
                        crate::job_store::copy_blob(value)
                    })
                    .map_err(|error| match error {
                        storage_provider::RawOrderedJobStoreError::Backend(error) => error,
                        _ => StorageError::internal("scheduled-job authority row is corrupt"),
                    })?,
                );
            }
            Ok(values)
        }
        .await;
        settlement.complete(result)
    }

    async fn read_transact_receipt(&mut self, token: &str) -> StorageResult<Option<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut statements = Vec::with_capacity(2);
            if catalog.engine.engine() == SqlEngine::Postgres {
                statements.push((
                    crate::item_lock::token_lock_statement(token),
                    crate::ReadProtection::Snapshot,
                ));
            }
            let protection = if catalog.engine.engine() == SqlEngine::Postgres {
                crate::ReadProtection::Snapshot
            } else {
                catalog.protection
            };
            statements.push((
                sql_statements::read_receipt(catalog.engine, token),
                protection,
            ));
            let expected = statements.len();
            let mut pages = catalog
                .attempt
                .query_batch_protected(statements, TRANSACT_RECEIPT_READ_BYTES)
                .await?;
            if pages.len() != expected {
                return Err(StorageError::internal(
                    "SQL transact receipt page count differs",
                ));
            }
            let page = pages
                .pop()
                .ok_or_else(|| StorageError::internal("SQL transact receipt page is missing"))?;
            if page.more {
                return Err(StorageError::internal(
                    "SQL transact token receipt exceeds the read bound",
                ));
            }
            if page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL transact token has duplicate receipt rows",
                ));
            }
            page.rows
                .into_iter()
                .next()
                .map(decode_receipt_row)
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn write_transact_receipt(&mut self, token: &str, receipt: Vec<u8>) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let changed = catalog
                .attempt
                .execute(sql_statements::write_receipt(
                    catalog.engine,
                    token,
                    receipt,
                ))
                .await?;
            if changed != 1 {
                return Err(StorageError::internal(
                    "SQL transact token receipt write affected another row count",
                ));
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}

fn decode_receipt_row(row: SqlRow) -> StorageResult<Vec<u8>> {
    let invalid =
        || StorageError::internal("SQL transact token receipt has invalid payload columns");
    let mut values = row.into_values();
    if values.len() != 1 {
        return Err(invalid());
    }
    let Some(SqlValue::Blob(bytes)) = values.pop() else {
        return Err(invalid());
    };
    Ok(bytes)
}
