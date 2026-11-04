//! Exact history deletions join the cleanup owner's protected native attempt.

use storage_types::{StorageEnum, StorageResult};
use stream_provider::{HistoryRowDelete, HistoryRowDeleteAttempt};

use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> HistoryRowDeleteAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn delete_history_rows(&mut self, rows: &[HistoryRowDelete]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statements = rows
                .iter()
                .map(|row| {
                    let (stream, version) = row.encode_address()?;
                    Ok(super::sql_statements::delete_history_row(
                        catalog.engine,
                        stream,
                        version,
                    ))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let counts = catalog.attempt.execute_batch(statements).await?;
            if counts.len() != rows.len() || counts.iter().any(|count| *count != 1) {
                return Err(StorageEnum::TransactionConflict {
                    message: "Observed history row changed before deletion".to_owned(),
                }
                .into());
            }
            Ok(())
        }
        .await;
        operation.complete(result)
    }
}
