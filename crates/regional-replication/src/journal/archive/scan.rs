//! Independent family custody and durable selection preserve progress across
//! outages.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    archive::{ArchiveCursor, ArchiveKind, ArchiveSchedule, ArchiveState, ArchiveTurn},
    journal::JournalStore,
    native_record::corrupt_record,
};

pub(super) struct ArchiveFamilyObservation {
    pub(super) row: DurableRecord,
    pub(super) state: ArchiveState,
    pub(super) cursor_row: DurableRecord,
    pub(super) cursor: ArchiveCursor,
    pub(super) index: Option<DurableRecord>,
}

impl JournalStore {
    pub(super) async fn initialize_archive_scan<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<ArchiveTurn> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let cursors = ArchiveKind::kinds()
                .map(|kind| {
                    Ok((
                        self.archive_scan_key(kind)?,
                        ArchiveCursor { kind, after: None },
                    ))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            self.require_absent_archive_families(attempt).await?;
            let mut changes = cursors
                .into_iter()
                .map(|(key, cursor)| self.records.prepare(key, None, &cursor))
                .collect::<StorageResult<Vec<_>>>()?;
            for kind in ArchiveKind::kinds() {
                changes.push(self.records.prepare(
                    self.archive_state_key(kind)?,
                    None,
                    &ArchiveState::Scanning(kind),
                )?);
            }
            changes.push(self.records.prepare(
                self.record_key(crate::constants::ARCHIVE_STATE_SUFFIX.to_vec())?,
                None,
                &ArchiveSchedule {
                    next: ArchiveKind::Event,
                },
            )?);
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await?;
            Ok(ArchiveTurn::Advanced)
        }
        .await;
        settlement.complete(result)
    }

    async fn require_absent_archive_families<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let prefixes = [
            self.record_key(crate::constants::ARCHIVE_FAMILY_SUFFIX.to_vec())?,
            self.record_key(crate::constants::ARCHIVE_SCAN_SUFFIX.to_vec())?,
            self.archive_hold_prefix()?,
        ];
        for first in prefixes {
            let range = self.record_prefix_range(&first)?;
            let (first, last, _) = range.bounds();
            let range = infrastructure_provider::page::BoundedRecordRange::try_new(
                first.clone(),
                last.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let page = self
                .records
                .scan(
                    attempt,
                    infrastructure_provider::page::RecordPageRequest {
                        range,
                        after: None,
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Conflict,
                    },
                )
                .await?;
            if !page.records.is_empty() {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) async fn select_archive_family<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<Option<ArchiveKind>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let attempt = settlement.as_mut();
            let key = self.record_key(crate::constants::ARCHIVE_STATE_SUFFIX.to_vec())?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let Some(row) = row else {
                self.initialize_archive_scan(attempt).await?;
                return Ok(None);
            };
            let schedule: ArchiveSchedule = self.records.decode(&row)?;
            let next = ArchiveSchedule {
                next: schedule.next.next(),
            };
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(key, Some(row.revision), &next)?],
                )
                .await?;
            Ok(Some(schedule.next))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn archive_state_key(
        &self,
        kind: ArchiveKind,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        self.identity_key(crate::constants::ARCHIVE_FAMILY_SUFFIX, &kind)
    }

    pub(super) async fn load_archive_state<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        kind: ArchiveKind,
        protection: ReadProtection,
    ) -> StorageResult<ArchiveFamilyObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = [
                self.archive_state_key(kind)?,
                self.archive_scan_key(kind)?,
                self.archive_hold_key(kind)?,
            ];
            let mut rows = self
                .records
                .load(attempt, &keys, protection)
                .await?
                .into_iter();
            let row = rows.next().flatten().ok_or_else(corrupt_record)?;
            let cursor_row = rows.next().flatten().ok_or_else(corrupt_record)?;
            let index = rows.next().flatten();
            let state: ArchiveState = self.records.decode(&row)?;
            let cursor: ArchiveCursor = self.records.decode(&cursor_row)?;
            self.require_archive_cursor(&cursor)?;
            let actual = match &state {
                ArchiveState::Scanning(kind) => *kind,
                ArchiveState::Qualifying { hold, .. } | ArchiveState::Pending(hold) => {
                    if hold.cursor != cursor {
                        return Err(corrupt_record());
                    }
                    hold.cursor.kind
                }
            };
            if actual != kind || cursor.kind != kind {
                return Err(corrupt_record());
            }
            self.require_archive_state_index(&state, index.as_ref())?;
            Ok(ArchiveFamilyObservation {
                row,
                state,
                cursor_row,
                cursor,
                index,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn prepare_archive_scan_advance(
        &self,
        observed: (DurableRecord, ArchiveCursor),
        after: &infrastructure_provider::record::DurableRecordKey,
    ) -> StorageResult<RecordChange> {
        let (row, prior) = observed;
        if row.key != self.archive_scan_key(prior.kind)?
            || prior
                .after
                .as_ref()
                .is_some_and(|previous| after <= previous)
        {
            return Err(corrupt_record());
        }
        let cursor = ArchiveCursor {
            kind: prior.kind,
            after: Some(after.clone()),
        };
        self.require_archive_cursor(&cursor)?;
        self.records.prepare(row.key, Some(row.revision), &cursor)
    }

    fn archive_scan_key(
        &self,
        kind: ArchiveKind,
    ) -> StorageResult<infrastructure_provider::record::DurableRecordKey> {
        self.identity_key(crate::constants::ARCHIVE_SCAN_SUFFIX, &kind)
    }
}
