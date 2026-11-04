//! Remove only regional catalog bindings still owned by the reclaimed ID.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use replication_types::identity::resource::TableIdentity;
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::JournalStore;
use crate::{constants, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn reclaim_table_catalog<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let pointer = self.identity_key(constants::CAPTURE_SUFFIX, &history)?;
        let Some(row) = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&pointer),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten()
        else {
            return Ok(());
        };
        let table: TableIdentity = self.records.decode(&row)?;
        if row.revision != storage_types::Revision::INITIAL || row.key != pointer {
            return Err(corrupt_record());
        }
        let mut changes = vec![RecordChange::Delete {
            key: pointer,
            expected: row.revision,
        }];
        if let Some(state) = self
            .observe_table_state(attempt, table, ReadProtection::Conflict)
            .await?
            && state.state().physical == history
        {
            changes.extend(self.prepare_table_binding_removals(attempt, table).await?);
            changes.push(RecordChange::Delete {
                key: self.identity_key(constants::TABLE_STATE_SUFFIX, &table)?,
                expected: state.revision,
            });
        }
        self.records.store(attempt, &changes).await
    }

    async fn prepare_table_binding_removals<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: TableIdentity,
    ) -> StorageResult<Vec<RecordChange>> {
        let prefixes = [
            constants::CONTROL_SLOT_SUFFIX,
            constants::CONTROL_TABLE_SUFFIX,
            constants::ADDITION_TABLE_SUFFIX,
        ];
        let keys = prefixes
            .iter()
            .map(|prefix| self.identity_key(prefix, &table))
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?;
        if let Some(slot) = &rows[0] {
            self.decode_control_slot(slot, table)?;
        }
        Ok(rows
            .into_iter()
            .flatten()
            .map(|row| RecordChange::Delete {
                key: row.key,
                expected: row.revision,
            })
            .collect())
    }
}
