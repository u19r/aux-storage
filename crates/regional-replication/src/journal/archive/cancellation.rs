//! Retiring a table revokes only its in-flight archive family selections.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use crate::{
    archive::{ArchiveCursor, ArchiveKind, ArchiveState},
    journal::JournalStore,
};

impl JournalStore {
    pub(in crate::journal) async fn cancel_table_archives<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let attempt = operation.as_mut();
            let mut changes = Vec::new();
            for kind in ArchiveKind::kinds() {
                let Some(family) = self
                    .find_archive_state(attempt, kind, ReadProtection::Conflict)
                    .await?
                else {
                    continue;
                };
                changes.extend(self.prepare_table_archive_cancellation(family, history)?);
            }
            if !changes.is_empty() {
                self.retain_journal_epoch(attempt).await?;
                self.records.store(attempt, &changes).await?;
            }
            Ok(())
        }
        .await;
        operation.complete(result)
    }
    fn prepare_table_archive_cancellation(
        &self,
        family: super::scan::ArchiveFamilyObservation,
        history: TableHistoryId,
    ) -> StorageResult<Vec<RecordChange>> {
        let hold = match family.state {
            ArchiveState::Scanning(_) => return Ok(Vec::new()),
            ArchiveState::Pending(hold) | ArchiveState::Qualifying { hold, .. } => hold,
        };
        if hold.pointer.history != history {
            return Ok(Vec::new());
        }
        let kind = hold.cursor.kind;
        let mut changes = vec![
            self.records.prepare(
                family.row.key,
                Some(family.row.revision),
                &ArchiveState::Scanning(kind),
            )?,
            self.records.prepare(
                family.cursor_row.key,
                Some(family.cursor_row.revision),
                &ArchiveCursor { kind, after: None },
            )?,
        ];
        if let Some(index) = family.index {
            changes.push(RecordChange::Delete {
                key: index.key,
                expected: index.revision,
            });
        }
        Ok(changes)
    }
}
