//! Batched qualified table-index rows from the caller's SQL snapshot.

use storage_types::{StorageError, StorageResult};
use stream_provider::{TableHistoryIndexAttempt, TableHistoryIndexPage, TableHistoryIndexRead};

use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> TableHistoryIndexAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_table_history_boundaries(
        &mut self,
        reads: &[stream_provider::TableHistoryBoundaryRead],
    ) -> StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statements = reads.iter().map(|read| super::sql_statements::read_table_history_boundary(catalog.engine, read)).collect();
            let pages = catalog.attempt.query_batch_exact(statements, catalog.protection, crate::constants::STREAM_READ_BYTES).await?;
            pages.try_zip_map(reads, |read, mut page| {
                if page.more || page.rows.len() > 1 { return Err(StorageError::internal("Invalid SQL table boundary page")); }
                page.rows.pop().map(|row| {
                    let [crate::SqlValue::Blob(order), crate::SqlValue::Text(offset)]: [crate::SqlValue; 2] = row.into_values().try_into().map_err(|_| StorageError::internal("Invalid SQL table boundary width"))? else {
                        return Err(StorageError::internal("Invalid SQL table boundary types"));
                    };
                    let position = storage_types::SystemStreamPosition::try_new(read.lane.shard_id().as_str(), read.lane.lineage_id().as_str(), offset).map_err(|_| StorageError::internal("Invalid SQL table boundary offset"))?;
                    if order != position.offset().order().map_err(|_| StorageError::internal("Invalid SQL table boundary order"))?.to_be_bytes() {
                        return Err(StorageError::internal("SQL table boundary order differs"));
                    }
                    Ok(position)
                }).transpose()
            })
        }.await;
        operation.complete(result)
    }

    async fn load_table_history_observations(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<storage_types::Exact<stream_provider::TableHistoryObservationPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let statements = reads
                .iter()
                .map(|read| {
                    super::sql_statements::read_table_history_observations(catalog.engine, read)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let pages = catalog
                .attempt
                .query_batch_exact(
                    statements,
                    catalog.protection,
                    crate::constants::STREAM_READ_BYTES,
                )
                .await?;
            pages.try_zip_map(reads, |read, page| {
                if page.rows.len() > read.limit.get() as usize + 1
                    || page.more && page.rows.is_empty()
                {
                    return Err(StorageError::internal("Invalid SQL table observation page"));
                }
                let more = page.more || page.rows.len() > read.limit.get() as usize;
                let entries = page
                    .rows
                    .into_iter()
                    .take(read.limit.get() as usize)
                    .map(super::row::decode_observation_row)
                    .collect::<StorageResult<Vec<_>>>()?;
                let last_position = entries.last().map(|entry| entry.position.clone());
                Ok(stream_provider::TableHistoryObservationPage {
                    entries,
                    last_position,
                    more,
                })
            })
        }
        .await;
        operation.complete(result)
    }

    async fn load_table_history_rows(
        &mut self,
        reads: &[TableHistoryIndexRead],
    ) -> StorageResult<storage_types::Exact<TableHistoryIndexPage>> {
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
                .query_batch_exact(
                    statements,
                    catalog.protection,
                    crate::constants::STREAM_READ_BYTES,
                )
                .await?;
            pages.try_zip_map(reads, |read, page| {
                let mut entries = page
                    .rows
                    .into_iter()
                    .map(super::row::decode_entry_row)
                    .collect::<StorageResult<Vec<_>>>()?;
                let more = page.more || entries.len() > read.limit.get() as usize;
                entries.truncate(read.limit.get() as usize);
                let last_position = entries.last().map(|entry| entry.position.clone());
                Ok(TableHistoryIndexPage {
                    entries,
                    more,
                    last_position,
                })
            })
        }
        .await;
        operation.complete(result)
    }
}
