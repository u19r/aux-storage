//! SQL storage of the store's stream-enable stamp.

use storage_provider::{CATALOG_READ_BYTES, StreamLabelClockAttempt};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlValue, catalog::SqlCatalogAttempt,
    stream_label_clock_sql_statements as sql_statements,
};

impl<A: SqlAttempt, L: SqlLayout> StreamLabelClockAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_stream_label_clock(&mut self) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let page = catalog
                .attempt
                .query(
                    sql_statements::load_clock(catalog.engine),
                    ReadProtection::Conflict,
                    CATALOG_READ_BYTES,
                )
                .await?;
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal("SQL stream label clock is invalid"));
            }
            page.rows
                .first()
                .map(|row| match row.value(ColumnIndex::new(0))? {
                    SqlValue::Integer(value) => Ok(TimestampMillis::from_timestamp(*value)),
                    _ => Err(StorageError::internal(
                        "SQL stream label clock has invalid type",
                    )),
                })
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn store_stream_label_clock(&mut self, stamp: TimestampMillis) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = catalog
            .attempt
            .execute(sql_statements::store_clock(catalog.engine, stamp))
            .await
            .map(|_| ());
        settlement.complete(result)
    }
}
