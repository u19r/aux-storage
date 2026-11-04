//! Batched qualified table-index rows from the caller's SQL snapshot.

use storage_types::{StorageError, StorageResult};
use stream_provider::{
    StreamRowPage, TableHistoryIndexAttempt, TableHistoryIndexRead, decode_table_history_index_row,
};

use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> TableHistoryIndexAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<Vec<StreamRowPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statements = reads
                .iter()
                .map(|read| super::sql_statements::read_table_history_index(catalog.engine, read))
                .collect::<StorageResult<Vec<_>>>()?;
            let pages = catalog
                .attempt
                .query_batch(
                    statements,
                    catalog.protection,
                    crate::constants::STREAM_READ_BYTES,
                )
                .await?;
            if pages.len() != reads.len() {
                return Err(StorageError::internal("SQL table index page count differs"));
            }
            pages
                .into_iter()
                .map(|page| {
                    let rows = page
                        .rows
                        .into_iter()
                        .map(|row| {
                            let (id, bytes) = super::row::decode_stream_address(row)?;
                            decode_table_history_index_row(id, &bytes)
                        })
                        .collect::<StorageResult<Vec<_>>>()?;
                    Ok(StreamRowPage {
                        rows,
                        more: page.more,
                    })
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }
}
