//! Logical deletes also clear every chunk of the selected history value.

use storage_types::StorageResult;
use stream_provider::{StreamEntryAddress, StreamEntryDeleteAttempt};

use crate::{KvAttempt, KvMutation, catalog::KvCatalogAttempt};

impl<A: KvAttempt> StreamEntryDeleteAttempt for KvCatalogAttempt<'_, A> {
    async fn delete_stream_entries(&mut self, rows: &[StreamEntryAddress]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            let mut mutations = Vec::with_capacity(rows.len() * 2);
            for row in rows {
                let StreamEntryAddress {
                    history_id,
                    position,
                } = row;
                mutations.push(KvMutation::Delete {
                    key: super::key_layout::entry_key(*history_id, position)?,
                });
                mutations.push(KvMutation::Delete {
                    key: super::key_layout::record_key(position)?,
                });
            }
            catalog.attempt.stage(mutations)
        })();
        operation.complete(result)
    }
}
