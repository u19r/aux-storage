//! Logical deletes also clear every chunk of the selected history value.

use storage_types::StorageResult;
use stream_provider::{HistoryRowDelete, HistoryRowDeleteAttempt};

use crate::{KvAttempt, KvMutation, catalog::KvCatalogAttempt};

impl<A: KvAttempt> HistoryRowDeleteAttempt for KvCatalogAttempt<'_, A> {
    async fn delete_history_rows(&mut self, rows: &[HistoryRowDelete]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            let mutations = rows
                .iter()
                .map(|row| {
                    let (stream, version) = row.encode_address()?;
                    Ok(KvMutation::Delete {
                        key: super::key_layout::item_key(&stream, version)?,
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            catalog.attempt.stage(mutations)
        })();
        operation.complete(result)
    }
}
