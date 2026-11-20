//! Exact history deletions join the cleanup owner's protected native attempt.

use storage_types::{MissExpectation, StorageError, StorageResult};
use stream_provider::{StreamEntryAddress, StreamEntryDeleteAttempt};

use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> StreamEntryDeleteAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn delete_stream_entries(&mut self, rows: &[StreamEntryAddress]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statements = rows
                .iter()
                .map(|row| {
                    let StreamEntryAddress {
                        history_id,
                        position,
                    } = row;
                    super::sql_statements::delete_entry(catalog.engine, *history_id, position)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let counts = catalog.attempt.execute_batch_exact(statements).await?;
            if counts.iter().any(|count| *count != 1) {
                return Err(StorageError::write_miss(
                    "stream_history_entry",
                    MissExpectation::Present,
                ));
            }
            Ok(())
        }
        .await;
        operation.complete(result)
    }
}
