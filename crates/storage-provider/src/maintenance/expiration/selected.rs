//! Replay the captured TTL selection; never discover another page of victims.

use std::sync::Arc;

use storage_common::{TtlSweepPageCommand, TtlSweepPageResult};
use storage_types::{BoundTableInfo, StorageEnum, StorageError, StorageResult};

use super::{
    AcceptedDirectExpirationPage, DirectExpirationCandidateRead, DirectExpirationPageWrite,
    DirectExpirationScanRead, DirectExpirationTransaction as _, ExpirationAttempt,
    ExpirationDelete, ExpirationDeletionBatch, ExpirationPort, ExpirationSelection,
    page_write::observe_expiration_item_deletes,
    selection::{SelectedExpirationRow, invalid_selection},
};
use crate::AcceptedDeleteItem;

impl<'native, P: ExpirationPort> ExpirationAttempt<'native, P> {
    /// Observe exactly the captured keys in the original native attempt. A
    /// replaced victim with the same expired value retains its current index
    /// row for a later sweep instead of losing TTL tracking.
    pub async fn prepare_selected_expiration(
        mut self,
        command: &TtlSweepPageCommand,
        selection: ExpirationSelection,
    ) -> StorageResult<AcceptedDirectExpirationPage<'native, P>> {
        let (config, table) = self
            .read_direct_policy(&command.table_name)
            .await?
            .ok_or_else(|| StorageError::from(StorageEnum::ConditionalCheckFailed))?;
        let scan = DirectExpirationScanRead::try_new(command, config, (*table).clone())?;
        selection.validate_range(&scan)?;
        let read = DirectExpirationCandidateRead::try_new(
            scan,
            selection.rows.iter().map(|row| row.index.clone()).collect(),
            selection.has_more,
        )?;
        let (write, items) = selection.evaluate_selection(self.port, read, table).await?;
        Ok(AcceptedDirectExpirationPage {
            attempt: self.port,
            write,
            items,
        })
    }
}

enum SelectedDisposition {
    Expire,
    Cleanup,
    Retain,
}

impl SelectedExpirationRow {
    fn select_disposition(
        &self,
        item: &AcceptedDeleteItem,
        attribute: &str,
    ) -> StorageResult<SelectedDisposition> {
        let expires = item
            .old_item
            .as_ref()
            .map(|old| old.number_attribute_i64(attribute))
            .transpose()?
            .flatten();
        if expires != Some(self.index.indexed_expiration_seconds) {
            return Ok(SelectedDisposition::Cleanup);
        }
        Ok(
            if self
                .victim
                .as_ref()
                .is_some_and(|expected| item.old_provenance.as_ref() == Some(expected))
            {
                SelectedDisposition::Expire
            } else {
                SelectedDisposition::Retain
            },
        )
    }
}

impl ExpirationSelection {
    async fn evaluate_selection<P: ExpirationPort>(
        self,
        attempt: &mut P,
        mut read: DirectExpirationCandidateRead,
        table: Arc<BoundTableInfo>,
    ) -> StorageResult<(DirectExpirationPageWrite, Vec<AcceptedDeleteItem>)> {
        let candidates = std::mem::take(&mut read.batch.candidates)
            .into_iter()
            .map(|candidate| ExpirationDelete { key: candidate.key })
            .collect();
        let (bound, observations) =
            observe_expiration_item_deletes(attempt, table, candidates).await?;
        if bound.len() != self.rows.len() {
            return Err(invalid_selection());
        }
        let mut items = Vec::with_capacity(bound.len());
        let mut deletions = ExpirationDeletionBatch::default();
        for ((row, item), (observed, lifecycle)) in
            self.rows.into_iter().zip(bound).zip(observations)
        {
            let item = item.accept_old(observed, lifecycle)?.into_result()?;
            match row.select_disposition(&item, &read.batch.ttl_attribute)? {
                SelectedDisposition::Expire => {
                    deletions.index_entries.push(row.index);
                    items.push(item);
                }
                SelectedDisposition::Cleanup => deletions.index_entries.push(row.index),
                SelectedDisposition::Retain => {}
            }
        }
        read.config.checkpoint.cursor = read.next_cursor.clone();
        let result = TtlSweepPageResult {
            deleted_items: u64::try_from(items.len())
                .map_err(|_| StorageError::internal("expiration deletion count exceeds u64"))?,
            next_cursor: read.next_cursor,
        };
        Ok((
            DirectExpirationPageWrite {
                config: read.config,
                table_info: read.table_info,
                table_name: read.table_name,
                deletions,
                captured_at: read.captured_at,
                result,
            },
            items,
        ))
    }
}
