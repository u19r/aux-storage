//! SQL receipt persistence for the shared TransactWriteItems executor.

use scheduled_bg_jobs::store::StoreKey;
use storage_provider::{
    DynamoTransactWriteAttempt, ReceiptSlot, TRANSACT_RECEIPT_PAGE_BYTES,
    maintenance::TransactReceiptRow,
};
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
    async fn read_write_observations(
        &mut self,
        batch: storage_provider::WriteObservationBatch<'_>,
    ) -> StorageResult<storage_provider::WriteObservationBatchResult> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if catalog.engine.engine() != SqlEngine::Postgres {
                return storage_provider::read_write_observations_default(catalog, batch).await;
            }
            catalog.read_combined_write_observations(batch).await
        }
        .await;
        settlement.complete(result)
    }

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
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            for key in keys {
                let key = std::slice::from_ref(key);
                // Postgres locks the row with a no-op UPDATE. A SERIALIZABLE
                // job transition that later writes this row then fails and
                // retries on a fresh snapshot, which sees this effect's
                // receipt; a FOR UPDATE lock leaves no row version and would
                // not. Await the job-row lock before reading the cancellation
                // marker: a fresh READ COMMITTED statement must see a
                // cancellation that committed while the job-row read waited.
                let (statement, protection) = if catalog.engine.engine() == SqlEngine::Postgres {
                    (
                        crate::job_store::lock_point(catalog.engine.engine(), &key[0])?,
                        crate::ReadProtection::Snapshot,
                    )
                } else {
                    (
                        crate::job_store::points(catalog.engine.engine(), key)?,
                        crate::ReadProtection::Conflict,
                    )
                };
                let page = catalog
                    .attempt
                    .query(
                        statement,
                        protection,
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

    async fn read_transact_receipt(
        &mut self,
        slot: ReceiptSlot<'_>,
    ) -> StorageResult<Option<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut statements = Vec::with_capacity(2);
            let prefix = catalog
                .catalog_lock
                .token_lock_prefix(slot, &mut statements);
            let protection = if catalog.engine.engine() == SqlEngine::Postgres {
                crate::ReadProtection::Snapshot
            } else {
                catalog.protection
            };
            statements.push((receipt_statement(catalog.engine, slot)?, protection));
            let pages = catalog
                .attempt
                .query_batch_protected_exact(statements, TRANSACT_RECEIPT_PAGE_BYTES)
                .await?;
            let mut pages = pages.into_iter();
            catalog
                .catalog_lock
                .finish_lock_prefix(prefix, &mut pages)?;
            let page = pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL transact receipt page is missing"))?;
            decode_receipt_page(slot, page)
        }
        .await;
        settlement.complete(result)
    }

    fn stage_transact_receipt(
        &mut self,
        slot: ReceiptSlot<'_>,
        receipt: Vec<u8>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            use crate::catalog::{PendingDynamoWrites, SqlDynamoCount, SqlDynamoWriteKind};
            let engine = catalog.engine;
            match &mut catalog.dynamo_writes {
                PendingDynamoWrites::KeepOpen(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    let statement = match slot {
                        ReceiptSlot::Request(token) => {
                            sql_statements::write_receipt(engine, token, receipt)
                        }
                        ReceiptSlot::JobEffect(key) => {
                            crate::job_store::put(engine.engine(), key.as_bytes(), &receipt)?
                        }
                    };
                    writes.push((
                        statement,
                        SqlDynamoCount::ExactlyOne,
                        SqlDynamoWriteKind::Receipt,
                    ));
                }
                PendingDynamoWrites::Commit(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    writes.push(match slot {
                        ReceiptSlot::Request(token) => {
                            sql_statements::counted_receipt(engine, token, receipt)?
                        }
                        ReceiptSlot::JobEffect(key) => {
                            crate::job_store::counted_put(engine.engine(), key.as_bytes(), receipt)?
                        }
                    });
                }
            }
            Ok(())
        })();
        settlement.complete(result)
    }
}

pub(crate) fn decode_receipt_row(row: SqlRow) -> StorageResult<Vec<u8>> {
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

pub(crate) fn receipt_statement(
    engine: impl SqlLayout,
    slot: ReceiptSlot<'_>,
) -> StorageResult<crate::SqlStatement> {
    match slot {
        ReceiptSlot::Request(token) => Ok(sql_statements::read_receipt(engine, token)),
        ReceiptSlot::JobEffect(key) => {
            crate::job_store::points(engine.engine(), std::slice::from_ref(key))
        }
    }
}

pub(crate) fn decode_receipt_page(
    slot: ReceiptSlot<'_>,
    page: crate::SqlQueryPage,
) -> StorageResult<Option<Vec<u8>>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL transact receipt page is invalid",
        ));
    }
    let Some(row) = page.rows.into_iter().next() else {
        return Ok(None);
    };
    match slot {
        ReceiptSlot::Request(_) => decode_receipt_row(row).map(Some),
        ReceiptSlot::JobEffect(key) => decode_job_effect_row(row, key).map(Some),
    }
}

fn decode_job_effect_row(row: SqlRow, key: &StoreKey) -> StorageResult<Vec<u8>> {
    let invalid = || StorageError::internal("SQL job effect receipt row is invalid");
    let mut values = row.into_values().into_iter();
    let (Some(SqlValue::Blob(stored)), Some(SqlValue::Blob(value)), None) =
        (values.next(), values.next(), values.next())
    else {
        return Err(invalid());
    };
    if stored.as_slice() != key.as_bytes() {
        return Err(invalid());
    }
    Ok(value)
}

pub(crate) async fn scan_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    after: Option<&str>,
    limit: std::num::NonZeroU32,
) -> StorageResult<Vec<TransactReceiptRow>> {
    let page = attempt
        .query(
            sql_statements::scan_receipts(engine, after, limit.get()),
            crate::ReadProtection::Conflict,
            TRANSACT_RECEIPT_PAGE_BYTES,
        )
        .await?;
    page.rows.into_iter().map(decode_receipt_pair).collect()
}

pub(crate) async fn delete_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    receipts: &[TransactReceiptRow],
) -> StorageResult<()> {
    for row in receipts {
        attempt
            .execute(sql_statements::delete_receipt(
                engine,
                &row.token,
                &row.receipt,
            ))
            .await?;
    }
    Ok(())
}

fn decode_receipt_pair(row: SqlRow) -> StorageResult<TransactReceiptRow> {
    let invalid = || StorageError::internal("SQL transact token receipt has invalid scan columns");
    let mut values = row.into_values().into_iter();
    let (Some(SqlValue::Text(token)), Some(SqlValue::Blob(receipt)), None) =
        (values.next(), values.next(), values.next())
    else {
        return Err(invalid());
    };
    Ok(TransactReceiptRow { token, receipt })
}

pub(crate) fn receipt_count_error() -> StorageError {
    StorageError::internal("SQL transact token receipt write affected another row count")
}
